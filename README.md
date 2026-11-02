# L4 TCP Load Balancer

Minimal, zero-dependency Layer 4 (TCP) stream proxy and round-robin load balancer implemented in Python using native `asyncio`.

```
                    +-------------------+
                    |    TCP Clients    |
                    +---------+---------+
                              |
                              v
                    +-------------------+
                    |  balancer :8080   |
                    +---------+---------+
                              |
          +-------------------+-------------------+
          |                   |                   |
          v                   v                   v
+-------------------+ +-------------------+ +-------------------+
|  backend-1 :9001  | |  backend-2 :9002  | |  backend-3 :9003  |
+-------------------+ +-------------------+ +-------------------+
          ^                   ^                   ^
          |                   |                   |
          +===================+===================+
                    Active TCP Health Probes
```

![Failover and Recovery Demo](assets/demo.svg)

---

## Architecture

| Component | File | Responsibility |
|---|---|---|
| Listener | `balancer.py` | TCP socket bind (`SO_REUSEADDR`), signal traps (`SIGINT`/`SIGTERM`), lifecycle management. |
| Health Engine | `health.py` | Asynchronous TCP handshake probes, hysteresis filtering (rise/fall thresholds), active pool rotation. |
| Stream Proxy | `proxy.py` | Full-duplex byte piping (64 KB buffers), `FIN`/`EOF` propagation, connection teardown on `RST`. |
| Configuration | `config.json` | Listener addresses, target pool, probe intervals, and timeout parameters. |

---

## Failure Modes & Resilience

- **Backend Crash / Timeout**: If upstream fails to accept connection within `connection_timeout` seconds, proxy aborts the client socket cleanly and increments failure counters.
- **Flap Damping**: Backends require `fall_threshold` consecutive failed probes to be ejected, and `rise_threshold` consecutive successful handshakes to re-enter rotation.
- **Client Disconnect**: Early client drop (`ECONNRESET` / `EPIPE`) terminates the corresponding backend pipe immediately; no dangling upstream sockets.
- **Zero Active Backends**: If all targets fail health checks, incoming client handshakes are terminated immediately to prevent resource exhaustion.

---

## Quickstart

### Prerequisites
Python 3.8+ (no third-party packages required).

### Run
```bash
python3 balancer.py config.json
```

### Run End-to-End Test Suite
Tests round-robin distribution, failover on node death, and automatic recovery on service restore:
```bash
python3 test_balancer.py
```

### Interactive Failover Demo
Spawns 3 local HTTP targets, runs round-robin checks, kills a node, verifies failover, revives the node, and verifies recovery:
```bash
./demo.sh

# Record terminal session to GIF (requires vhs)
./record_demo.sh
```

---

## Configuration

`config.json`:
```json
{
  "bind_host": "0.0.0.0",
  "bind_port": 8080,
  "backends": [
    {"host": "127.0.0.1", "port": 9001, "weight": 1},
    {"host": "127.0.0.1", "port": 9002, "weight": 1},
    {"host": "127.0.0.1", "port": 9003, "weight": 1}
  ],
  "health_check": {
    "interval_seconds": 2.0,
    "timeout_seconds": 1.0,
    "rise_threshold": 2,
    "fall_threshold": 2
  },
  "connection_timeout": 5.0
}
```

---

## Performance & Benchmarking

Execute load tests using `wrk`, `hey`, or the included benchmark harness:

```bash
# Automated runner (auto-detects wrk / hey / python fallback)
./benchmark.sh

# Direct wrk invocation
wrk -t4 -c100 -d10s --latency http://127.0.0.1:8080/

# Direct hey invocation
hey -z 10s -c 100 http://127.0.0.1:8080/
```

### Runtime Characteristics
- **Concurrency**: Event-driven via OS I/O multiplexing (`kqueue` on macOS, `epoll` on Linux). Memory per connection is bounded to the socket buffer state machine (~few KB), avoiding OS thread stack allocation (~8 MB default on POSIX).
- **L4 vs L7**: Operates at the transport layer without inspecting HTTP headers or terminating TLS. Suitable for arbitrary TCP protocols (HTTP, Redis, Postgres, SSH, raw sockets).

---

## Extension: Least Connections Scheduling

To switch from Round-Robin to Least Connections:

1. Add connection counter to `Backend` in `health.py`:
```python
active_connections: int = 0
```

2. Track lifecycle in `proxy.py`:
```python
# Before pipe start:
backend.active_connections += 1

# In finally block:
backend.active_connections = max(0, backend.active_connections - 1)
```

3. Update dispatch in `health.py`:
```python
async def get_next(self) -> Optional[Backend]:
    async with self._lock:
        if not self._alive_backends:
            return None
        return min(self._alive_backends, key=lambda b: b.active_connections)
```
