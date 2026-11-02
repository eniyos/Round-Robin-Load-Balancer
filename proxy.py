"""Layer 4 TCP stream proxying, rate limiting, and PROXY protocol support."""

import asyncio
import logging
import time
from typing import Dict, Optional, Tuple

from health import Backend, BackendPool

logger = logging.getLogger("balancer.proxy")

BUFFER_SIZE = 65536  # 64 KB


class TokenBucket:
    """Thread-safe token bucket rate limiter for a single IP."""

    def __init__(self, rate: float, capacity: float):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.monotonic()

    def consume(self) -> bool:
        now = time.monotonic()
        elapsed = now - self.last_update
        self.last_update = now
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


class IPRateLimiter:
    """Per-client IP rate limiter with LRU/TTL cleanup."""

    def __init__(self, rate: float = 50.0, burst: float = 100.0, enabled: bool = False):
        self.rate = rate
        self.burst = burst
        self.enabled = enabled
        self._buckets: Dict[str, TokenBucket] = {}
        self._lock = asyncio.Lock()

    async def allow(self, ip: str) -> bool:
        if not self.enabled:
            return True
        async with self._lock:
            now = time.monotonic()
            bucket = self._buckets.get(ip)
            if not bucket:
                # Cleanup if table grows excessively (>10k IPs)
                if len(self._buckets) > 10000:
                    stale_cutoff = now - 60
                    self._buckets = {k: v for k, v in self._buckets.items() if v.last_update > stale_cutoff}
                bucket = TokenBucket(self.rate, self.burst)
                self._buckets[ip] = bucket
            return bucket.consume()


class ProxyMetrics:
    """Thread-safe global proxy operational metrics."""

    def __init__(self):
        self.active_connections = 0
        self.total_connections = 0
        self.rate_limited_connections = 0
        self.backend_connect_timeouts = 0
        self.client_disconnects = 0
        self.total_bytes_in = 0
        self.total_bytes_out = 0
        self.start_time = time.time()


# Global singleton metrics instance
METRICS = ProxyMetrics()


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, direction: str) -> int:
    """Pump bytes from reader to writer until EOF or error. Return total bytes piped."""
    bytes_piped = 0
    try:
        while True:
            data = await reader.read(BUFFER_SIZE)
            if not data:
                if writer.can_write_eof():
                    writer.write_eof()
                break
            writer.write(data)
            await writer.drain()
            bytes_piped += len(data)
    except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError):
        pass
    except OSError as err:
        logger.debug("Socket error on %s: %s", direction, err)
    return bytes_piped


async def _safe_close(writer: asyncio.StreamWriter) -> None:
    """Close writer socket without throwing unhandled exceptions."""
    try:
        writer.close()
        await writer.wait_closed()
    except (OSError, asyncio.CancelledError):
        pass


def _format_proxy_v1_header(client_addr: Tuple[str, int], server_addr: Tuple[str, int]) -> bytes:
    """Format HAProxy PROXY Protocol v1 header."""
    src_ip, src_port = client_addr[0], client_addr[1]
    dst_ip, dst_port = server_addr[0], server_addr[1]
    proto = "TCP6" if ":" in src_ip else "TCP4"
    return f"PROXY {proto} {src_ip} {dst_ip} {src_port} {dst_port}\r\n".encode("ascii")


async def handle_connection(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    pool: BackendPool,
    connect_timeout: float = 5.0,
    rate_limiter: Optional[IPRateLimiter] = None,
    max_connections: int = 10000,
    proxy_protocol: bool = False,
) -> None:
    """Route incoming client connection to next healthy backend."""
    client_addr = client_writer.get_extra_info("peername") or ("127.0.0.1", 0)
    server_addr = client_writer.get_extra_info("sockname") or ("127.0.0.1", 8080)
    client_ip = client_addr[0]

    # Global connection ceiling
    if METRICS.active_connections >= max_connections:
        logger.warning("Global max connection limit reached (%d), rejecting %s", max_connections, client_addr)
        await _safe_close(client_writer)
        return

    # Per-IP rate limiting
    if rate_limiter and not await rate_limiter.allow(client_ip):
        METRICS.rate_limited_connections += 1
        logger.warning("Rate limit exceeded for %s, closing connection", client_ip)
        await _safe_close(client_writer)
        return

    METRICS.active_connections += 1
    METRICS.total_connections += 1

    backend: Optional[Backend] = await pool.get_next(client_ip)
    if not backend:
        logger.error("No healthy backends available for client %s", client_addr)
        METRICS.active_connections -= 1
        await _safe_close(client_writer)
        return

    backend_writer = None
    bytes_in = 0
    bytes_out = 0

    try:
        backend_reader, backend_writer = await asyncio.wait_for(
            asyncio.open_connection(backend.host, backend.port),
            timeout=connect_timeout,
        )

        # Inject PROXY protocol v1 header if enabled
        if proxy_protocol:
            header = _format_proxy_v1_header(client_addr, server_addr)
            backend_writer.write(header)
            await backend_writer.drain()

    except (asyncio.TimeoutError, OSError) as err:
        METRICS.backend_connect_timeouts += 1
        logger.warning("Failed connecting to backend %s: %s", backend.endpoint, err)
        backend.consecutive_failure += 1
        if backend.consecutive_failure >= 2:
            await pool.update_status(backend, False)
        METRICS.active_connections -= 1
        await pool.release(backend, 0, 0)
        await _safe_close(client_writer)
        return

    logger.debug("Proxying %s <-> %s", client_addr, backend.endpoint)

    try:
        pipe_c2b = asyncio.create_task(_pipe(client_reader, backend_writer, "client->backend"))
        pipe_b2c = asyncio.create_task(_pipe(backend_reader, client_writer, "backend->client"))

        done, pending = await asyncio.wait(
            [pipe_c2b, pipe_b2c],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

        bytes_in = pipe_c2b.result() if pipe_c2b in done and not pipe_c2b.cancelled() else 0
        bytes_out = pipe_b2c.result() if pipe_b2c in done and not pipe_b2c.cancelled() else 0

    finally:
        METRICS.active_connections = max(0, METRICS.active_connections - 1)
        METRICS.total_bytes_in += bytes_in
        METRICS.total_bytes_out += bytes_out
        await pool.release(backend, bytes_in, bytes_out)
        await _safe_close(client_writer)
        if backend_writer:
            await _safe_close(backend_writer)
        logger.debug("Closed connection %s <-> %s", client_addr, backend.endpoint)
