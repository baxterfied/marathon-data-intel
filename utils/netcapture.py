#!/usr/bin/env python3
"""Marathon Intel — Network Capture Agent

Captures live network telemetry from Marathon game sessions and submits
performance data (ping, jitter, packet loss, server IPs) to the
Marathon Intel API.

Capture backends (tried in order):
  1. scapy   — pip install scapy (lightweight, no Wireshark needed)
  2. tshark  — Wireshark CLI (fallback if scapy unavailable)

Requires:
  - Python 3.10+
  - Run as admin/root (needed for packet capture)
  - One of: scapy (pip install scapy) OR tshark/Wireshark

Usage:
  pip install scapy
  python netcapture.py --user-hash myname123

The agent will:
  1. Detect Marathon game traffic on Steam relay ports (27015-27200)
  2. Track server IPs and measure RTT from packet timing
  3. Calculate ping, jitter, and packet loss per server
  4. Auto-detect match start/end from traffic patterns
  5. Track matchmaking queue times
  6. Push live status to companion dashboard every 5 seconds
  7. Submit snapshots to /api/network every 60 seconds
  8. Submit match session data to /api/sessions on match end
"""

import argparse
import asyncio
import json
import logging
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from statistics import mean, stdev

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger("netcapture")

# Marathon game ports (UDP)
# Primary: Steam Datagram Relay ports used by Marathon
# Secondary: previously observed auxiliary ports
GAME_PORTS_RANGE = (27015, 27200)  # Steam/Valve relay range
GAME_PORTS_EXTRA = {63006, 63007, 63008, 63009, 63059, 53932, 55575, 57787}
ALL_GAME_PORTS = set(range(GAME_PORTS_RANGE[0], GAME_PORTS_RANGE[1] + 1)) | GAME_PORTS_EXTRA

# How often to submit a snapshot (seconds)
SUBMIT_INTERVAL = 60
# Minimum packets to consider a server active
MIN_PACKETS = 10
# Packet loss window (seconds) — expect at least 1 pkt/sec from active server
LOSS_WINDOW = 10

# Match detection thresholds
MATCH_START_PPS = 20       # Packets/sec to consider a match started
MATCH_END_SILENCE = 10     # Seconds of low traffic to consider match ended
MATCH_MIN_DURATION = 30    # Minimum match duration (seconds) to be valid
MATCH_LOW_PPS = 3          # Below this PPS, match is considered over
QUEUE_DETECT_PPS = 2       # Low steady traffic = matchmaking queue

# Live status push interval
LIVE_PUSH_INTERVAL = 5


# ── Capture backend detection ──

def _check_scapy() -> bool:
    """Check if scapy is available."""
    try:
        from scapy.all import sniff, UDP, IP  # noqa: F401
        log.info("Scapy available — using as capture backend")
        return True
    except ImportError:
        return False


def _find_tshark() -> str:
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


def _detect_backend() -> tuple[str, str]:
    """Detect best available capture backend. Returns (backend, path)."""
    if _check_scapy():
        return ("scapy", "")
    tshark = _find_tshark()
    if tshark:
        return ("tshark", tshark)
    return ("none", "")


# ── Data classes ──

class MatchState(Enum):
    IDLE = "idle"
    QUEUING = "queuing"
    IN_MATCH = "in_match"


@dataclass
class MatchSession:
    """Tracks a detected match session."""
    server_ip: str
    state: MatchState = MatchState.IDLE
    queue_start: float = 0.0
    match_start: float = 0.0
    match_end: float = 0.0
    peak_pps: float = 0.0
    total_packets: int = 0
    peak_ping_ms: float = 0.0
    avg_ping_ms: float = 0.0
    _recent_pps: list[float] = field(default_factory=list)

    def update_pps(self, current_pps: float) -> None:
        self._recent_pps.append(current_pps)
        if len(self._recent_pps) > 10:
            self._recent_pps = self._recent_pps[-10:]
        self.peak_pps = max(self.peak_pps, current_pps)

    @property
    def avg_recent_pps(self) -> float:
        return mean(self._recent_pps) if self._recent_pps else 0

    @property
    def duration_s(self) -> int:
        if self.match_start and self.match_end:
            return int(self.match_end - self.match_start)
        return 0

    @property
    def queue_time_s(self) -> int:
        if self.queue_start and self.match_start:
            return int(self.match_start - self.queue_start)
        return 0

    def to_session_dict(self, user_hash: str, region: str = "unknown", patch: str = "1.0") -> dict:
        return {
            "user_hash": user_hash,
            "server_ip": self.server_ip,
            "region": region,
            "started_at": datetime.fromtimestamp(self.match_start, tz=timezone.utc).isoformat() if self.match_start else "",
            "ended_at": datetime.fromtimestamp(self.match_end, tz=timezone.utc).isoformat() if self.match_end else "",
            "duration_s": self.duration_s,
            "peak_ping_ms": self.peak_ping_ms,
            "avg_ping_ms": self.avg_ping_ms,
            "total_packets": self.total_packets,
            "queue_time_s": self.queue_time_s,
            "patch": patch,
        }


@dataclass
class ServerStats:
    """Tracks per-server network metrics from captured packets."""
    ip: str
    first_seen: float = 0.0
    last_seen: float = 0.0
    packet_count: int = 0
    bytes_total: int = 0
    intervals: list[float] = field(default_factory=list)
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
        if len(self.intervals) < 2:
            return 0.0
        filtered = [i for i in self.intervals if i < 1.0]
        if not filtered:
            return 0.0
        return round(mean(filtered) * 1000, 1)

    @property
    def jitter_ms(self) -> float:
        if len(self.intervals) < 3:
            return 0.0
        filtered = [i for i in self.intervals if i < 1.0]
        if len(filtered) < 3:
            return 0.0
        return round(stdev(filtered) * 1000, 1)

    @property
    def packet_loss_pct(self) -> float:
        if len(self.buckets) < 2:
            return 0.0
        counts = list(self.buckets.values())
        if not counts:
            return 0.0
        expected = max(counts)
        if expected == 0:
            return 0.0
        total_expected = expected * len(counts)
        total_actual = sum(counts)
        loss = max(0, (total_expected - total_actual) / total_expected * 100)
        return round(loss, 2)

    @property
    def tick_rate(self) -> int:
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
        self.first_seen = 0.0
        self.last_seen = 0.0
        self.packet_count = 0
        self.bytes_total = 0
        self.intervals.clear()
        self.buckets.clear()


# ── Region detection ──

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


# ── Network helpers ──

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
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            ips.add(s.getsockname()[0])
    except Exception:
        pass
    return ips


# ── API submission ──

async def _api_post(api_url: str, path: str, payload: dict, timeout: int = 10) -> bool:
    """POST JSON to an API endpoint. Returns True on success."""
    if not api_url:
        return False
    url = f"{api_url}{path}"
    try:
        import httpx
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(url, json=payload)
            return resp.status_code == 200
    except ImportError:
        import urllib.request
        import urllib.error
        data = json.dumps(payload, default=str).encode()
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status == 200
        except urllib.error.URLError:
            pass
    except Exception:
        pass
    return False


async def submit_stats(api_url: str, payload: dict) -> bool:
    return await _api_post(api_url, "/api/network", payload)


async def push_live_status(api_url: str, user_hash: str, status: dict) -> None:
    await _api_post(api_url, f"/api/live/{user_hash}", {"user_hash": user_hash, **status}, timeout=5)


async def submit_session(api_url: str, payload: dict) -> bool:
    return await _api_post(api_url, "/api/sessions", payload)


# ── Interface detection ──

def detect_interface_scapy() -> str:
    """Auto-detect the best network interface using scapy."""
    try:
        from scapy.arch import get_if_list
        ifaces = get_if_list()
        skip = ["lo", "loopback", "vmware", "virtualbox", "docker", "vethernet", "wsl", "npcap"]
        prefer = ["ethernet", "eth", "en0", "en1", "wi-fi", "wlan", "wifi"]

        # Try preferred first
        for iface in ifaces:
            lower = iface.lower()
            if any(s in lower for s in skip):
                continue
            if any(p in lower for p in prefer):
                log.info("Selected interface (scapy): %s", iface)
                return iface

        # Fall back to first non-virtual
        for iface in ifaces:
            lower = iface.lower()
            if any(s in lower for s in skip):
                continue
            log.info("Selected interface (scapy): %s", iface)
            return iface
    except Exception as exc:
        log.warning("Could not detect interface via scapy: %s", exc)

    # Windows: try to use conf.iface
    try:
        from scapy.config import conf
        if conf.iface:
            iface = str(conf.iface)
            log.info("Using scapy default interface: %s", iface)
            return iface
    except Exception:
        pass

    return ""


def detect_interface_tshark(tshark: str) -> str:
    """Auto-detect the best network interface using tshark."""
    try:
        result = subprocess.run(
            [tshark, "-D"], capture_output=True, text=True, timeout=5
        )
        lines = result.stdout.strip().split("\n")
        skip = ["loopback", "lo", "vethernet", "vmware", "virtualbox", "hyper-v", "docker", "wsl", "npcap"]
        prefer = ["ethernet", "eth0", "en0", "wi-fi", "wlan", "wifi"]

        for line in lines:
            lower = line.lower()
            if any(kw in lower for kw in skip):
                continue
            if any(kw in lower for kw in prefer):
                iface = line.split(".")[0].strip()
                log.info("Selected interface (tshark): %s", line.strip())
                return iface
        for line in lines:
            lower = line.lower()
            if any(kw in lower for kw in skip):
                continue
            iface = line.split(".")[0].strip()
            log.info("Selected interface (tshark): %s", line.strip())
            return iface
    except Exception as exc:
        log.warning("Could not detect interface via tshark: %s", exc)
    return "1"


# ── Scapy capture backend ──

async def run_capture_scapy(
    interface: str,
    api_url: str,
    user_hash: str,
    patch: str = "1.0",
) -> None:
    """Capture using scapy — no Wireshark needed."""
    from scapy.all import AsyncSniffer, UDP, IP

    local_ips = _get_local_ips()
    log.info("Local IPs: %s", ", ".join(local_ips))
    log.info("Capture backend: scapy")

    lo, hi = GAME_PORTS_RANGE
    bpf = f"udp portrange {lo}-{hi}"
    for p in GAME_PORTS_EXTRA:
        bpf += f" or udp port {p}"

    # Packet queue for async processing
    pkt_queue: asyncio.Queue = asyncio.Queue()

    def _packet_handler(pkt):
        """Called by scapy sniffer thread for each matching packet."""
        if not pkt.haslayer(IP) or not pkt.haslayer(UDP):
            return
        ip_layer = pkt[IP]
        udp_layer = pkt[UDP]
        ts = float(pkt.time)
        src_ip = ip_layer.src
        dst_ip = ip_layer.dst
        src_port = udp_layer.sport
        dst_port = udp_layer.dport
        pkt_len = len(pkt)
        # Quick port filter (BPF should handle this but be safe)
        if src_port not in ALL_GAME_PORTS and dst_port not in ALL_GAME_PORTS:
            return
        pkt_queue.put_nowait((ts, src_ip, dst_ip, src_port, dst_port, pkt_len))

    # Start async sniffer in background thread
    sniffer_kwargs = {
        "prn": _packet_handler,
        "filter": bpf,
        "store": False,
    }
    if interface:
        sniffer_kwargs["iface"] = interface

    sniffer = AsyncSniffer(**sniffer_kwargs)
    sniffer.start()
    log.info("Scapy sniffer started on %s", interface or "(default)")
    log.info("Listening for Marathon traffic on UDP ports %d-%d + extras...", lo, hi)
    log.info("Waiting for Marathon game traffic...")
    log.info("Match auto-detection: ENABLED")

    try:
        await _process_packets(pkt_queue, local_ips, api_url, user_hash, patch)
    finally:
        sniffer.stop()
        log.info("Scapy sniffer stopped")


# ── tshark capture backend ──

async def run_capture_tshark(
    tshark: str,
    interface: str,
    api_url: str,
    user_hash: str,
    patch: str = "1.0",
) -> None:
    """Capture using tshark (Wireshark CLI)."""
    local_ips = _get_local_ips()
    log.info("Local IPs: %s", ", ".join(local_ips))
    log.info("Capture backend: tshark")

    lo, hi = GAME_PORTS_RANGE
    range_filter = f"udp portrange {lo}-{hi}"
    extra_filter = " or ".join(f"udp port {p}" for p in GAME_PORTS_EXTRA)
    capture_filter = f"{range_filter} or {extra_filter}"

    cmd = [
        tshark, "-i", interface, "-f", capture_filter,
        "-T", "fields",
        "-e", "frame.time_epoch", "-e", "ip.src", "-e", "ip.dst",
        "-e", "udp.srcport", "-e", "udp.dstport", "-e", "frame.len",
        "-l", "-q",
    ]

    log.info("Starting capture: %s", " ".join(cmd))
    log.info("Listening for Marathon traffic on UDP ports %d-%d + extras...", lo, hi)
    log.info("Waiting for Marathon game traffic...")
    log.info("Match auto-detection: ENABLED")

    process = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )

    pkt_queue: asyncio.Queue = asyncio.Queue()

    async def _reader():
        """Read tshark stdout and push parsed packets to the queue."""
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
            await pkt_queue.put((ts, src_ip, dst_ip, src_port, dst_port, pkt_len))

    reader_task = asyncio.create_task(_reader())

    try:
        await _process_packets(pkt_queue, local_ips, api_url, user_hash, patch)
    finally:
        reader_task.cancel()
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except asyncio.TimeoutError:
            process.kill()


# ── Shared packet processing ──

async def _process_packets(
    pkt_queue: asyncio.Queue,
    local_ips: set[str],
    api_url: str,
    user_hash: str,
    patch: str,
) -> None:
    """Process packets from either backend. Handles stats, match detection, live push, submissions."""
    servers: dict[str, ServerStats] = {}
    match_sessions: dict[str, MatchSession] = {}
    pps_counters: dict[str, int] = defaultdict(int)
    last_pps_check = time.time()
    last_submit = time.time()
    last_live_push = 0.0
    session_matches = 0
    session_wins = 0
    session_losses = 0

    while True:
        try:
            ts, src_ip, dst_ip, src_port, dst_port, pkt_len = await asyncio.wait_for(
                pkt_queue.get(), timeout=1.0
            )
        except asyncio.TimeoutError:
            # No packet — still run periodic checks
            now = time.time()
            if api_url and now - last_live_push >= LIVE_PUSH_INTERVAL:
                live_state = _build_live_state(servers, match_sessions, now, session_matches, session_wins, session_losses)
                asyncio.ensure_future(push_live_status(api_url, user_hash, live_state))
                last_live_push = now
            continue

        # Determine remote server IP
        if src_ip in local_ips:
            server_ip = dst_ip
        elif dst_ip in local_ips:
            server_ip = src_ip
        else:
            continue

        # Track server
        if server_ip not in servers:
            servers[server_ip] = ServerStats(ip=server_ip)
            log.info("New server detected: %s (region: %s)", server_ip, guess_region(server_ip))

        servers[server_ip].record_packet(ts, pkt_len)
        pps_counters[server_ip] += 1

        if server_ip not in match_sessions:
            match_sessions[server_ip] = MatchSession(server_ip=server_ip)
        match_sessions[server_ip].total_packets += 1

        # PPS check every second
        now = time.time()
        if now - last_pps_check >= 1.0:
            for ip, count in pps_counters.items():
                if ip in match_sessions:
                    session = match_sessions[ip]
                    session.update_pps(count)
                    _match_state_machine(session, ip, count, now, servers, match_sessions,
                                        user_hash, api_url, patch,
                                        lambda: _inc_session_matches(locals()))
                    # Handle match end submission inline
                    if session.state == MatchState.IN_MATCH:
                        srv = servers.get(ip)
                        if srv:
                            session.peak_ping_ms = max(session.peak_ping_ms, srv.avg_ping_ms)
                            session.avg_ping_ms = srv.avg_ping_ms
                        if count < MATCH_LOW_PPS:
                            elapsed = now - session.match_start if session.match_start else 0
                            if elapsed >= MATCH_MIN_DURATION:
                                session.match_end = now
                                region = guess_region(ip)
                                log.info(
                                    "[%s] MATCH ENDED — Duration: %ds | Queue: %ds | Packets: %d | Region: %s",
                                    ip, session.duration_s, session.queue_time_s,
                                    session.total_packets, region,
                                )
                                session_matches += 1
                                payload = session.to_session_dict(user_hash, region=region, patch=patch)
                                if api_url:
                                    success = await submit_session(api_url, payload)
                                    if success:
                                        log.info("  -> Session submitted to API")
                                    else:
                                        log.warning("  -> Session API submission failed")
                                else:
                                    log.info("  -> Session (dry run): %s", json.dumps(payload, indent=2))
                                match_sessions[ip] = MatchSession(server_ip=ip)
                            else:
                                session.state = MatchState.IDLE

            pps_counters.clear()
            last_pps_check = now

        # Push live status every 5 seconds
        if api_url and now - last_live_push >= LIVE_PUSH_INTERVAL:
            live_state = _build_live_state(servers, match_sessions, now, session_matches, session_wins, session_losses)
            asyncio.ensure_future(push_live_status(api_url, user_hash, live_state))
            last_live_push = now

        # Submit network stats periodically
        if now - last_submit >= SUBMIT_INTERVAL:
            await _submit_all(servers, api_url, user_hash, patch)
            last_submit = now


def _match_state_machine(session, ip, count, now, servers, match_sessions, user_hash, api_url, patch, on_match_end):
    """State transitions for match detection (IDLE/QUEUING only — IN_MATCH handled by caller)."""
    if session.state == MatchState.IDLE:
        if QUEUE_DETECT_PPS <= count < MATCH_START_PPS:
            session.state = MatchState.QUEUING
            session.queue_start = now
            log.info("[%s] Queue detected (PPS: %d)", ip, count)
        elif count >= MATCH_START_PPS:
            session.state = MatchState.IN_MATCH
            session.match_start = now
            log.info("[%s] MATCH STARTED (PPS: %d)", ip, count)
    elif session.state == MatchState.QUEUING:
        if count >= MATCH_START_PPS:
            session.state = MatchState.IN_MATCH
            session.match_start = now
            queue_time = int(now - session.queue_start) if session.queue_start else 0
            log.info("[%s] MATCH STARTED after %ds queue (PPS: %d)", ip, queue_time, count)


def _build_live_state(servers, match_sessions, now, session_matches, session_wins, session_losses) -> dict:
    """Build live status dict for the dashboard."""
    active_session = None
    active_server = None
    for ip, session in match_sessions.items():
        if session.state != MatchState.IDLE:
            active_session = session
            active_server = servers.get(ip)
            break
    if not active_session:
        for ip, srv in sorted(servers.items(), key=lambda x: x[1].last_seen, reverse=True):
            if srv.packet_count > 0:
                active_session = match_sessions.get(ip, MatchSession(server_ip=ip))
                active_server = srv
                break

    return {
        "state": active_session.state.value if active_session else "idle",
        "server_ip": active_session.server_ip if active_session else "",
        "region": guess_region(active_session.server_ip) if active_session else "unknown",
        "ping_ms": active_server.avg_ping_ms if active_server else 0,
        "jitter_ms": active_server.jitter_ms if active_server else 0,
        "packet_loss": active_server.packet_loss_pct if active_server else 0,
        "tick_rate": active_server.tick_rate if active_server else 0,
        "match_duration_s": int(now - active_session.match_start) if active_session and active_session.match_start and active_session.state == MatchState.IN_MATCH else 0,
        "queue_time_s": int(now - active_session.queue_start) if active_session and active_session.queue_start and active_session.state == MatchState.QUEUING else 0,
        "packets_per_sec": active_session.avg_recent_pps if active_session else 0,
        "session_matches": session_matches,
        "session_wins": session_wins,
        "session_losses": session_losses,
    }


def _inc_session_matches(local_vars):
    """Placeholder — session match counting handled inline."""
    pass


async def _submit_all(
    servers: dict[str, ServerStats],
    api_url: str,
    user_hash: str,
    patch: str,
) -> None:
    """Submit stats for all active servers and reset counters."""
    active = {ip: s for ip, s in servers.items() if s.packet_count >= MIN_PACKETS}
    if not active:
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


# ── Main ──

def main():
    parser = argparse.ArgumentParser(
        description="Marathon Intel Network Capture Agent",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Setup (pick one):
  pip install scapy          (recommended — lightweight)
  Install Wireshark/tshark   (fallback)

Examples:
  python netcapture.py --user-hash MyName
  python netcapture.py --user-hash MyName -i 8
  python netcapture.py --user-hash MyName --dry-run
        """,
    )
    parser.add_argument("--api-url", default="https://marathon.straightfirefood.blog",
                        help="Marathon Intel API URL")
    parser.add_argument("--user-hash", required=True, help="Your gamertag")
    parser.add_argument("--interface", "-i", default="", help="Network interface (auto-detected if omitted)")
    parser.add_argument("--patch", default="1.0", help="Game patch version")
    parser.add_argument("--dry-run", action="store_true", help="Print stats without submitting")
    parser.add_argument("--backend", choices=["auto", "scapy", "tshark"], default="auto",
                        help="Force a specific capture backend")
    args = parser.parse_args()

    # Detect capture backend
    if args.backend == "scapy":
        if not _check_scapy():
            log.error("scapy not available. Install with: pip install scapy")
            sys.exit(1)
        backend, tshark_path = "scapy", ""
    elif args.backend == "tshark":
        tshark_path = _find_tshark()
        if not tshark_path:
            log.error("tshark not found. Install Wireshark.")
            sys.exit(1)
        backend = "tshark"
    else:
        backend, tshark_path = _detect_backend()

    if backend == "none":
        log.error(
            "No capture backend found. Install one:\n"
            "  Option 1 (recommended): pip install scapy\n"
            "  Option 2: Install Wireshark (https://www.wireshark.org/download.html)"
        )
        sys.exit(1)

    # Detect interface
    if args.interface:
        interface = args.interface
    elif backend == "scapy":
        interface = detect_interface_scapy()
    else:
        interface = detect_interface_tshark(tshark_path)

    api_url = "" if args.dry_run else args.api_url

    log.info("===================================")
    log.info("  MARATHON INTEL - Capture Agent")
    log.info("===================================")
    log.info("Backend:   %s", backend)
    log.info("API:       %s", api_url or "(dry run)")
    log.info("Gamertag:  %s", args.user_hash)
    log.info("Interface: %s", interface or "(default)")
    log.info("")
    log.info("Start Marathon and play. Data captures automatically.")
    log.info("Press Ctrl+C to stop.")
    log.info("")

    try:
        if backend == "scapy":
            asyncio.run(run_capture_scapy(interface, api_url, args.user_hash, args.patch))
        else:
            asyncio.run(run_capture_tshark(tshark_path, interface, api_url, args.user_hash, args.patch))
    except KeyboardInterrupt:
        log.info("Capture stopped by user.")


if __name__ == "__main__":
    main()
