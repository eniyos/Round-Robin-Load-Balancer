#!/usr/bin/env bash
set -euo pipefail

# Record the demo GIF using VHS (https://github.com/charmbracelet/vhs)
# Requires: brew install vhs

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Prepare mock backend content
mkdir -p /tmp/lb_b1 /tmp/lb_b2 /tmp/lb_b3
echo "Response from Backend 1 (port 9001)" > /tmp/lb_b1/index.html
echo "Response from Backend 2 (port 9002)" > /tmp/lb_b2/index.html
echo "Response from Backend 3 (port 9003)" > /tmp/lb_b3/index.html

# Ensure output directory exists
mkdir -p assets

# Record
echo "[*] Recording demo GIF with VHS..."
vhs demo.tape

echo "[+] GIF saved to assets/demo.gif"
