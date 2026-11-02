"""Lightweight internal HTTP admin and Prometheus metrics server using native asyncio."""

import asyncio
import json
import logging
import time
from typing import Optional

from health import BackendPool
from proxy import METRICS

logger = logging.getLogger("balancer.admin")


def generate_prometheus_metrics(pool: BackendPool) -> str:
    """Generate Prometheus exposition format metrics."""
    lines = [
        "# HELP balancer_uptime_seconds Load balancer uptime in seconds",
        "# TYPE balancer_uptime_seconds gauge",
        f"balancer_uptime_seconds {time.time() - METRICS.start_time:.2f}",
        "",
        "# HELP balancer_active_connections Current active client connections",
        "# TYPE balancer_active_connections gauge",
        f"balancer_active_connections {METRICS.active_connections}",
        "",
        "# HELP balancer_connections_total Total client connections processed",
        "# TYPE balancer_connections_total counter",
        f"balancer_connections_total {METRICS.total_connections}",
        "",
        "# HELP balancer_rate_limited_total Total connections rejected by rate limiter",
        "# TYPE balancer_rate_limited_total counter",
        f"balancer_rate_limited_total {METRICS.rate_limited_connections}",
        "",
        "# HELP balancer_backend_timeouts_total Total backend connection timeouts",
        "# TYPE balancer_backend_timeouts_total counter",
        f"balancer_backend_timeouts_total {METRICS.backend_connect_timeouts}",
        "",
        "# HELP balancer_bytes_in_total Total bytes received from clients",
        "# TYPE balancer_bytes_in_total counter",
        f"balancer_bytes_in_total {METRICS.total_bytes_in}",
        "",
        "# HELP balancer_bytes_out_total Total bytes returned to clients",
        "# TYPE balancer_bytes_out_total counter",
        f"balancer_bytes_out_total {METRICS.total_bytes_out}",
        "",
        "# HELP balancer_backend_alive Backend health status (1=healthy, 0=dead)",
        "# TYPE balancer_backend_alive gauge",
    ]

    for b in pool.all_backends:
        alive_val = 1 if b.alive and not b.draining else 0
        lines.append(f'balancer_backend_alive{{endpoint="{b.endpoint}",draining="{str(b.draining).lower()}"}} {alive_val}')

    lines.append("")
    lines.append("# HELP balancer_backend_active_connections Active connections per backend")
    lines.append("# TYPE balancer_backend_active_connections gauge")
    for b in pool.all_backends:
        lines.append(f'balancer_backend_active_connections{{endpoint="{b.endpoint}"}} {b.active_connections}')

    lines.append("")
    lines.append("# HELP balancer_backend_total_connections Cumulative connections per backend")
    lines.append("# TYPE balancer_backend_total_connections counter")
    for b in pool.all_backends:
        lines.append(f'balancer_backend_total_connections{{endpoint="{b.endpoint}"}} {b.total_connections}')

    lines.append("")
    return "\n".join(lines)


def generate_json_stats(pool: BackendPool) -> str:
    """Generate structured operational summary JSON."""
    stats = {
        "status": "UP" if pool.alive_count > 0 else "DEGRADED",
        "algorithm": pool.algorithm,
        "uptime_seconds": round(time.time() - METRICS.start_time, 2),
        "active_connections": METRICS.active_connections,
        "total_connections": METRICS.total_connections,
        "rate_limited_connections": METRICS.rate_limited_connections,
        "backend_timeouts": METRICS.backend_connect_timeouts,
        "total_bytes_in": METRICS.total_bytes_in,
        "total_bytes_out": METRICS.total_bytes_out,
        "pool": {
            "healthy": pool.alive_count,
            "total": len(pool.all_backends),
            "backends": [
                {
                    "endpoint": b.endpoint,
                    "alive": b.alive,
                    "draining": b.draining,
                    "weight": b.weight,
                    "active_connections": b.active_connections,
                    "total_connections": b.total_connections,
                    "bytes_in": b.bytes_in,
                    "bytes_out": b.bytes_out,
                }
                for b in pool.all_backends
            ],
        },
    }
    return json.dumps(stats, indent=2)


async def handle_admin_request(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    pool: BackendPool,
) -> None:
    """Handle incoming HTTP admin requests using standard stream reader."""
    try:
        request_line = await reader.readline()
        if not request_line:
            writer.close()
            await writer.wait_closed()
            return

        parts = request_line.decode("utf-8", errors="ignore").strip().split()
        if len(parts) < 2:
            writer.close()
            await writer.wait_closed()
            return

        method, path = parts[0], parts[1]

        # Read remainder of HTTP headers
        while True:
            line = await reader.readline()
            if line in (b"\r\n", b"\n", b""):
                break

        if method != "GET":
            body = "Method Not Allowed"
            res = f"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: {len(body)}\r\n\r\n{body}"
        elif path == "/metrics":
            metrics_text = generate_prometheus_metrics(pool)
            res = (
                f"HTTP/1.1 200 OK\r\n"
                f"Content-Type: text/plain; version=0.0.4\r\n"
                f"Content-Length: {len(metrics_text.encode('utf-8'))}\r\n"
                f"Connection: close\r\n\r\n"
                f"{metrics_text}"
            )
        elif path == "/stats":
            stats_json = generate_json_stats(pool)
            res = (
                f"HTTP/1.1 200 OK\r\n"
                f"Content-Type: application/json\r\n"
                f"Content-Length: {len(stats_json.encode('utf-8'))}\r\n"
                f"Connection: close\r\n\r\n"
                f"{stats_json}"
            )
        elif path == "/healthz":
            is_up = pool.alive_count > 0
            code = "200 OK" if is_up else "503 Service Unavailable"
            payload = json.dumps({"status": "UP" if is_up else "DOWN", "healthy_backends": pool.alive_count})
            res = (
                f"HTTP/1.1 {code}\r\n"
                f"Content-Type: application/json\r\n"
                f"Content-Length: {len(payload)}\r\n"
                f"Connection: close\r\n\r\n"
                f"{payload}"
            )
        else:
            body = "Not Found"
            res = f"HTTP/1.1 404 Not Found\r\nContent-Length: {len(body)}\r\n\r\n{body}"

        writer.write(res.encode("utf-8"))
        await writer.drain()
    except (OSError, asyncio.CancelledError):
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except OSError:
            pass


async def start_admin_server(host: str, port: int, pool: BackendPool) -> asyncio.Server:
    """Start the admin/metrics server."""
    server = await asyncio.start_server(
        lambda r, w: handle_admin_request(r, w, pool),
        host=host,
        port=port,
        reuse_address=True,
    )
    logger.info("Admin & Prometheus server listening on http://%s:%d (/metrics, /stats, /healthz)", host, port)
    return server
