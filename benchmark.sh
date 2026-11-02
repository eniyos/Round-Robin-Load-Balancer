#!/usr/bin/env bash
# Benchmark guide and runner for L4 Load Balancer

TARGET="http://127.0.0.1:8080/"
DURATION="10s"
CONCURRENCY=50

echo "=================================================="
echo " Layer 4 Load Balancer Benchmark Guide"
echo " Target: $TARGET"
echo " Concurrency: $CONCURRENCY workers | Duration: $DURATION"
echo "=================================================="

if command -v wrk >/dev/null 2>&1; then
    echo "[*] Found 'wrk'. Running benchmark..."
    wrk -t4 -c"$CONCURRENCY" -d"$DURATION" --latency "$TARGET"
elif command -v hey >/dev/null 2>&1; then
    echo "[*] Found 'hey'. Running benchmark..."
    hey -z "$DURATION" -c "$CONCURRENCY" "$TARGET"
else
    echo "[!] Neither 'wrk' nor 'hey' installed."
    echo "[*] Running built-in Python concurrent async benchmark..."
    python3 - << 'EOF'
import asyncio
import time
import urllib.request

URL = "http://127.0.0.1:8080/"
CONCURRENCY = 20
DURATION = 5

stop = False
stats = {"reqs": 0, "errs": 0}

async def worker():
    loop = asyncio.get_running_loop()
    while not stop:
        try:
            await loop.run_in_executor(None, lambda: urllib.request.urlopen(URL, timeout=1).read())
            stats["reqs"] += 1
        except Exception:
            stats["errs"] += 1

async def main():
    global stop
    print(f"Benchmarking with {CONCURRENCY} workers for {DURATION}s...")
    tasks = [asyncio.create_task(worker()) for _ in range(CONCURRENCY)]
    t0 = time.perf_counter()
    await asyncio.sleep(DURATION)
    stop = True
    await asyncio.gather(*tasks, return_exceptions=True)
    elapsed = time.perf_counter() - t0
    rps = stats["reqs"] / elapsed
    print(f"Results: {stats['reqs']} requests in {elapsed:.2f}s ({rps:.2f} req/s), {stats['errs']} errors")

asyncio.run(main())
EOF
    echo ""
    echo "Tip: Install 'wrk' (brew install wrk) or 'hey' (brew install hey) for kernel-level TCP benchmarking."
fi
