#!/usr/bin/env python3
"""Marathon Intel — Network Capture Agent

Captures live network telemetry from Marathon game sessions using tshark
and submits performance data (ping, jitter, packet loss, server IPs) to
the Marathon Intel API.

Requires:
  - tshark (Wireshark CLI) installed and accessible
  - Run as admin/root (needed for packet capture)
  - Python 3.10+

Usage:
  python netcapture.py --api-url https://marathon.straightfirefood.blog --user-hash myname123

The agent will:
  1. Detect Marathon game traffic on UDP ports 3074/3097
  2. Track server IPs and measure RTT from packet timing
  3. Calculate ping, jitter, and packet loss per server
  4. Submit snapshots to /api/network every 60 seconds
"""

import argparse
import asyncio
import json
import logging
import re
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from statistics import mean, stdev

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger("netcapture")

# Marathon game ports (UDP) — main range 63006-63059, plus auxiliary ports
GAME_PORTS_RANGE = (63006, 63059)
GAME_PORTS_EXTRA = {53932, 55575, 57787}
# How often to submit a snapshot (seconds)
SUBMIT_INTERVAL = 60
# Minimum packets to consider a server active
MIN_PACKETS = 10
# Packet loss window (seconds) — expect at least 1 pkt/sec from active server
LOSS_WINDOW = 10


@dataclass
class ServerStats:
    """Tracks per-server network metrics from captured packets."""
    ip: str
    first_seen: float = 0.0
    last_seen: float = 0.0
    packet_count: int = 0
    bytes_total: int = 0
    intervals: list[float] = field(default_factory=list)
    # For loss detection: track packets per LOSS_WINDOW-second bucket
    buckets: dict[int, int] = field(default_factory=lambda: defaultdict(int))

    def record_packet(self, ts: float, size: int) -> None:
        if self.first_seen == 0:
            self.first_seen = ts
        if self.last_seen > 0:
            interval = ts - self.last_seen
            if interval > 0:
                self.intervals.append(interval)
        self.last_seen = ts
        self.packet_count += 1
        self.bytes_total += size
        bucket = int(ts) // LOSS_WINDOW
        self.buckets[bucket] += 1

    @property
    def avg_ping_ms(self) -> float:
        """Estimate ping from average inter-packet interval.

        In a game session, the server sends packets at a regular tick rate.
        The interval between received packets approximates one-way latency
        variation. We use the median interval as a rough RTT proxy.
        """
        if len(self.intervals) < 2:
            return 0.0
        # Filter out outlier gaps (disconnects, loading screens)
        filtered = [i for i in self.intervals if i < 1.0]
        if not filtered:
            return 0.0
        avg_interval = mean(filtered)
        # Convert to ms; multiply by factor to approximate RTT
        return round(avg_interval * 1000, 1)

    @property
    def jitter_ms(self) -> float:
        """Jitter = standard deviation of inter-packet intervals."""
        if len(self.intervals) < 3:
            return 0.0
        filtered = [i for i in self.intervals if i < 1.0]
        if len(filtered) < 3:
            return 0.0
        return round(stdev(filtered) * 1000, 1)

    @property
    def packet_loss_pct(self) -> float:
        """Estimate packet loss from gaps in packet flow.

        Compares actual packets received per time bucket against expected
        packets based on the observed average rate.
        """
        if len(self.buckets) < 2:
            return 0.0
        counts = list(self.buckets.values())
        if not counts:
            return 0.0
        expected = max(counts)  # Best bucket = expected rate
        if expected == 0:
            return 0.0
        total_expected = expected * len(counts)
        total_actual = sum(counts)
        loss = max(0, (total_expected - total_actual) / total_expected * 100)
        return round(loss, 2)

    @property
    def tick_rate(self) -> int:
        """Estimate server tick rate from packet frequency."""
        if len(self.intervals) < 5:
            return 0
        filtered = [i for i in self.intervals if 0.001 < i < 0.5]
        if not filtered:
            return 0
        avg = mean(filtered)
        if avg <= 0:
            return 0
        return round(1.0 / avg)

    def to_dict(self, user_hash: str, region: str = "unknown", patch: str = "1.0") -> dict:
        return {
            "user_hash": user_hash,
            "server_ip": self.ip,
            "region": region,
            "map_name": "unknown",
            "avg_ping_ms": self.avg_ping_ms,
            "jitter_ms": self.jitter_ms,
            "packet_loss": self.packet_loss_pct,
            "tick_rate": self.tick_rate,
            "patch": patch,
        }

    def reset(self) -> None:
        """Reset stats for next window while keeping the IP."""
        self.first_seen = 0.0
        self.last_seen = 0.0
        self.packet_count = 0
        self.bytes_total = 0
        self.intervals.clear()
        self.buckets.clear()


def find_tshark() -> str:
    """Locate tshark binary."""
    for path in ["tshark", "/usr/bin/tshark", "/usr/local/bin/tshark",
                  r"C:\Program Files\Wireshark\tshark.exe"]:
        try:
            result = subprocess.run(
                [path, "--version"], capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                version = result.stdout.split("\n")[0]
                log.info("Found tshark: %s", version)
                return path
        except (FileNotFoundError, subprocess.TimeoutExpired):
            continue
    return ""


def detect_interface(tshark: str) -> str:
    """Pick the best capture interface."""
    try:
        result = subprocess.run(
            [tshark, "-D"], capture_output=True, text=True, timeout=5
        )
        lines = result.stdout.strip().split("\n")
        for line in lines:
            lower = line.lower()
            # Prefer ethernet/wifi interfaces
            if any(kw in lower for kw in ["ethernet", "eth0", "en0", "wi-fi", "wlan", "wifi"]):
                iface = line.split(".")[0].strip()
                log.info("Selected interface: %s", line.strip())
                return iface
        # Fall back to first non-loopback
        for line in lines:
            if "loopback" not in line.lower() and "lo" not in line.lower():
                iface = line.split(".")[0].strip()
                log.info("Selected interface: %s", line.strip())
                return iface
    except Exception as exc:
        log.warning("Could not detect interface: %s", exc)
    return "1"  # Default to first interface


# IP-to-region mapping — Marathon uses Steam/Valve relay servers
REGION_HINTS = {
    "us-east": ["162.254.194.", "162.254.199.", "205.196.6."],
    "us-west": ["162.254.192.", "162.254.193.", "162.254.195.", "162.254.196.",
                "162.254.197.", "162.254.198."],
    "eu-west": ["155.133.244.", "155.133.246.", "155.133.248.", "155.133.249.",
                "155.133.252.", "155.133.255.", "185.25.182.", "185.25.183."],
    "eu-central": ["155.133.227.", "155.133.230.", "155.133.238.", "146.66.155.",
                   "188.42.106."],
    "asia-east": ["103.10.124.", "103.10.125.", "103.28.54."],
    "asia-south": ["145.190.24."],
    "australia": ["103.10.125."],
    "south-america": [],
}


def guess_region(ip: str) -> str:
    for region, prefixes in REGION_HINTS.items():
        for prefix in prefixes:
            if ip.startswith(prefix):
                return region
    return "unknown"


async def submit_stats(api_url: str, payload: dict) -> bool:
    """Submit network stats to the Marathon Intel API."""
    try:
        import httpx
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(f"{api_url}/api/network", json=payload)
            if resp.status_code == 200:
                return True
            log.warning("API returned %d: %s", resp.status_code, resp.text[:200])
    except ImportError:
        # Fallback to urllib if httpx not available
        import urllib.request
        import urllib.error
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            f"{api_url}/api/network",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status == 200
        except urllib.error.URLError as exc:
            log.warning("API submit failed: %s", exc)
    except Exception as exc:
        log.warning("API submit failed: %s", exc)
    return False


async def run_capture(
    tshark: str,
    interface: str,
    api_url: str,
    user_hash: str,
    patch: str = "1.0",
) -> None:
    """Main capture loop — runs tshark and processes packets."""

    # Build capture filter for Marathon game ports
    lo, hi = GAME_PORTS_RANGE
    range_filter = f"udp portrange {lo}-{hi}"
    extra_filter = " or ".join(f"udp port {p}" for p in GAME_PORTS_EXTRA)
    capture_filter = f"{range_filter} or {extra_filter}"

    cmd = [
        tshark,
        "-i", interface,
        "-f", capture_filter,
        "-T", "fields",
        "-e", "frame.time_epoch",
        "-e", "ip.src",
        "-e", "ip.dst",
        "-e", "udp.srcport",
        "-e", "udp.dstport",
        "-e", "frame.len",
        "-l",  # Line-buffered output
        "-q",  # Suppress packet count summary
    ]

    log.info("Starting capture: %s", " ".join(cmd))
    log.info("Listening for Marathon traffic on UDP ports %d-%d + %s...", lo, hi, ", ".join(str(p) for p in GAME_PORTS_EXTRA))

    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    servers: dict[str, ServerStats] = {}
    last_submit = time.time()
    local_ips = _get_local_ips()

    log.info("Local IPs: %s", ", ".join(local_ips))
    log.info("Waiting for Marathon game traffic...")

    try:
        while True:
            line = await process.stdout.readline()
            if not line:
                break

            decoded = line.decode().strip()
            if not decoded:
                continue

            parts = decoded.split("\t")
            if len(parts) < 6:
                continue

            try:
                ts = float(parts[0])
                src_ip = parts[1]
                dst_ip = parts[2]
                src_port = int(parts[3]) if parts[3] else 0
                dst_port = int(parts[4]) if parts[4] else 0
                pkt_len = int(parts[5]) if parts[5] else 0
            except (ValueError, IndexError):
                continue

            # Determine the remote server IP (the one that's not us)
            if src_ip in local_ips:
                server_ip = dst_ip
            elif dst_ip in local_ips:
                server_ip = src_ip
            else:
                continue  # Neither IP is ours, skip

            # Track this server
            if server_ip not in servers:
                servers[server_ip] = ServerStats(ip=server_ip)
                log.info("New server detected: %s (region: %s)", server_ip, guess_region(server_ip))

            servers[server_ip].record_packet(ts, pkt_len)

            # Submit periodically
            now = time.time()
            if now - last_submit >= SUBMIT_INTERVAL:
                await _submit_all(servers, api_url, user_hash, patch)
                last_submit = now

    except asyncio.CancelledError:
        log.info("Capture cancelled")
    finally:
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except asyncio.TimeoutError:
            process.kill()

        # Final submit
        if servers:
            await _submit_all(servers, api_url, user_hash, patch)


async def _submit_all(
    servers: dict[str, "ServerStats"],
    api_url: str,
    user_hash: str,
    patch: str,
) -> None:
    """Submit stats for all active servers and reset counters."""
    active = {ip: s for ip, s in servers.items() if s.packet_count >= MIN_PACKETS}

    if not active:
        log.debug("No active servers to report")
        return

    for ip, stats in active.items():
        region = guess_region(ip)
        payload = stats.to_dict(user_hash, region=region, patch=patch)

        log.info(
            "Server %s [%s]: ping=%sms jitter=%sms loss=%s%% tick=%dHz (%d pkts)",
            ip, region, payload["avg_ping_ms"], payload["jitter_ms"],
            payload["packet_loss"], payload["tick_rate"], stats.packet_count,
        )

        success = await submit_stats(api_url, payload)
        if success:
            log.info("  -> Submitted to API")
        else:
            log.warning("  -> API submission failed")

        stats.reset()


def _get_local_ips() -> set[str]:
    """Get this machine's local IP addresses."""
    import socket
    ips = {"127.0.0.1"}
    try:
        hostname = socket.gethostname()
        for info in socket.getaddrinfo(hostname, None, socket.AF_INET):
            ips.add(info[4][0])
    except Exception:
        pass
    # Also try connecting to a public address to find our LAN IP
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            ips.add(s.getsockname()[0])
    except Exception:
        pass
    return ips


def main():
    parser = argparse.ArgumentParser(
        description="Marathon Intel Network Capture Agent",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python netcapture.py --user-hash myname123
  python netcapture.py --api-url https://marathon.straightfirefood.blog --user-hash myname123 --interface eth0
  python netcapture.py --user-hash myname123 --patch 1.0.1 --dry-run
        """,
    )
    parser.add_argument(
        "--api-url",
        default="https://marathon.straightfirefood.blog",
        help="Marathon Intel API URL (default: https://marathon.straightfirefood.blog)",
    )
    parser.add_argument(
        "--user-hash",
        required=True,
        help="Your anonymous identifier for data correlation",
    )
    parser.add_argument(
        "--interface", "-i",
        default="",
        help="Network interface to capture on (auto-detected if omitted)",
    )
    parser.add_argument(
        "--patch",
        default="1.0",
        help="Current game patch version (default: 1.0)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print stats to console without submitting to API",
    )
    args = parser.parse_args()

    # Find tshark
    tshark = find_tshark()
    if not tshark:
        log.error(
            "tshark not found. Install Wireshark/tshark:\n"
            "  Windows: https://www.wireshark.org/download.html\n"
            "  macOS:   brew install wireshark\n"
            "  Linux:   sudo apt install tshark"
        )
        sys.exit(1)

    # Detect interface
    interface = args.interface or detect_interface(tshark)

    if args.dry_run:
        log.info("DRY RUN — stats will be printed but not submitted")

    api_url = "" if args.dry_run else args.api_url

    log.info("Marathon Intel Network Capture Agent")
    log.info("API: %s", api_url or "(dry run)")
    log.info("User: %s", args.user_hash)
    log.info("Interface: %s", interface)
    log.info("")
    log.info("Start Marathon and play a match. Network data will be captured automatically.")
    log.info("Press Ctrl+C to stop.")
    log.info("")

    try:
        asyncio.run(run_capture(tshark, interface, api_url, args.user_hash, args.patch))
    except KeyboardInterrupt:
        log.info("Capture stopped by user.")


if __name__ == "__main__":
    main()
