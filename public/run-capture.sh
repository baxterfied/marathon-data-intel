#!/usr/bin/env bash
# ============================================================================
#  Marathon Intel - Network Capture Agent Launcher (Linux / macOS)
# ============================================================================
#
#  Usage:
#    ./run-capture.sh                   (will prompt for username/hash)
#    ./run-capture.sh myname123         (pass username/hash as argument)
#
# ============================================================================

set -euo pipefail

API_URL="https://marathon.straightfirefood.blog"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CAPTURE_SCRIPT="${SCRIPT_DIR}/netcapture.py"

# ---------------------------------------------------------------------------
#  Helpers
# ---------------------------------------------------------------------------

info()  { printf "\033[1;36m[*]\033[0m %s\n" "$1"; }
ok()    { printf "\033[1;32m[+]\033[0m %s\n" "$1"; }
warn()  { printf "\033[1;33m[!]\033[0m %s\n" "$1"; }
error() { printf "\033[1;31m[-]\033[0m %s\n" "$1"; }

# ---------------------------------------------------------------------------
#  Root / sudo check
# ---------------------------------------------------------------------------

if [ "$(id -u)" -ne 0 ]; then
    error "Packet capture requires root/sudo privileges."
    echo ""
    echo "  Please re-run with sudo:"
    echo ""
    echo "    sudo $0 $*"
    echo ""
    exit 1
fi

ok "Running with root privileges."

# ---------------------------------------------------------------------------
#  Detect Python
# ---------------------------------------------------------------------------

PYTHON=""

if command -v python3 &>/dev/null; then
    PYTHON="python3"
elif command -v python &>/dev/null; then
    PYTHON="python"
fi

if [ -z "$PYTHON" ]; then
    error "Python is not installed."
    echo ""
    echo "  Install Python 3.10+:"
    echo ""
    echo "    Ubuntu/Debian : sudo apt install python3"
    echo "    Fedora/RHEL   : sudo dnf install python3"
    echo "    macOS          : brew install python3"
    echo "    Or visit       : https://www.python.org/downloads/"
    echo ""
    exit 1
fi

PYTHON_VERSION=$($PYTHON --version 2>&1)
ok "Found $PYTHON_VERSION ($PYTHON)"

# ---------------------------------------------------------------------------
#  Check for capture backend (scapy or tshark)
# ---------------------------------------------------------------------------

HAS_BACKEND=false

if $PYTHON -c "import scapy" 2>/dev/null; then
    ok "Found scapy (capture backend)"
    HAS_BACKEND=true
elif command -v tshark &>/dev/null; then
    TSHARK_VERSION=$(tshark --version 2>&1 | head -n1)
    ok "Found tshark: $TSHARK_VERSION (capture backend)"
    HAS_BACKEND=true
fi

if [ "$HAS_BACKEND" = false ]; then
    error "No capture backend found."
    echo ""
    echo "  Install one of:"
    echo ""
    echo "    Option 1 (recommended): pip3 install scapy"
    echo "    Option 2:"
    echo "      Ubuntu/Debian : sudo apt install tshark"
    echo "      Fedora/RHEL   : sudo dnf install wireshark-cli"
    echo "      macOS          : brew install wireshark"
    echo ""
    exit 1
fi

# ---------------------------------------------------------------------------
#  Check that netcapture.py exists
# ---------------------------------------------------------------------------

if [ ! -f "$CAPTURE_SCRIPT" ]; then
    error "Cannot find netcapture.py at: $CAPTURE_SCRIPT"
    exit 1
fi

ok "Found capture script: $CAPTURE_SCRIPT"

# ---------------------------------------------------------------------------
#  Get user hash (from argument or prompt)
# ---------------------------------------------------------------------------

USER_HASH="${1:-}"

if [ -z "$USER_HASH" ]; then
    echo ""
    info "Enter your username or hash (an anonymous identifier used to correlate your data):"
    printf "  > "
    read -r USER_HASH
fi

if [ -z "$USER_HASH" ]; then
    error "Username/hash cannot be empty."
    exit 1
fi

ok "User hash: $USER_HASH"

# ---------------------------------------------------------------------------
#  Launch the capture agent
# ---------------------------------------------------------------------------

echo ""
echo "============================================"
echo "  Marathon Intel - Network Capture Agent"
echo "============================================"
echo ""
info "API URL  : $API_URL"
info "User     : $USER_HASH"
echo ""
info "Starting capture... Press Ctrl+C to stop."
echo ""

exec $PYTHON "$CAPTURE_SCRIPT" \
    --api-url "$API_URL" \
    --user-hash "$USER_HASH"
