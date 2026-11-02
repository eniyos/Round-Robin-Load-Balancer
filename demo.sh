#!/usr/bin/env bash
set -euo pipefail

# Demo script for L4 Load Balancer: Round Robin, Failover, Recovery

WORKDIR=$(mktemp -d /tmp/l4_demo.XXXXXX)
PIDS=()

cleanup() {
    echo -e "\n[+] Cleaning up background processes..."
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    rm -rf "$WORKDIR"
    echo "[+] Done."
}
trap cleanup EXIT INT TERM

echo "[1] Creating mock backend payload directories..."
mkdir -p "$WORKDIR/b1" "$WORKDIR/b2" "$WORKDIR/b3"
echo "Response from Backend 1 (Port 9001)" > "$WORKDIR/b1/index.html"
echo "Response from Backend 2 (Port 9002)" > "$WORKDIR/b2/index.html"
echo "Response from Backend 3 (Port 9003)" > "$WORKDIR/b3/index.html"

echo "[2] Starting 3 HTTP backends (9001, 9002, 9003)..."
python3 -m http.server 9001 --directory "$WORKDIR/b1" >/dev/null 2>&1 &
PIDS+=($!)
python3 -m http.server 9002 --directory "$WORKDIR/b2" >/dev/null 2>&1 &
B2_PID=$!
PIDS+=($B2_PID)
python3 -m http.server 9003 --directory "$WORKDIR/b3" >/dev/null 2>&1 &
PIDS+=($!)

sleep 1

echo "[3] Starting Load Balancer on port 8080..."
python3 balancer.py >/dev/null 2>&1 &
PIDS+=($!)

sleep 2

echo -e "\n=== Test A: Round-Robin Distribution (6 requests) ==="
for i in {1..6}; do
    curl -s http://127.0.0.1:8080/
done

echo -e "\n=== Test B: Dynamic Failover (Killing Backend 2) ==="
echo "[*] Killing Backend 2 (PID: $B2_PID)..."
kill -9 "$B2_PID"
echo "[*] Waiting 5 seconds for health checker fall threshold (2 probes)..."
sleep 5

echo "[*] Sending 4 requests (expecting only Backend 1 and Backend 3):"
for i in {1..4}; do
    curl -s http://127.0.0.1:8080/
done

echo -e "\n=== Test C: Automatic Recovery (Reviving Backend 2) ==="
echo "[*] Reviving Backend 2 on port 9002..."
python3 -m http.server 9002 --directory "$WORKDIR/b2" >/dev/null 2>&1 &
PIDS+=($!)
echo "[*] Waiting 5 seconds for health checker rise threshold (2 probes)..."
sleep 5

echo "[*] Sending 6 requests (expecting Backend 2 back in rotation):"
for i in {1..6}; do
    curl -s http://127.0.0.1:8080/
done

echo -e "\n[+] Interactive demo completed successfully."
