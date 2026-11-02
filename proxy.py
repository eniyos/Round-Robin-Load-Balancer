"""Layer 4 TCP bidirectional stream piping and error isolation."""

import asyncio
import logging
from typing import Tuple

from health import BackendPool, Backend

logger = logging.getLogger("balancer.proxy")

BUFFER_SIZE = 65536  # 64 KB chunk size


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, direction: str) -> None:
    """Pump bytes from reader to writer until EOF or connection error."""
    try:
        while True:
            data = await reader.read(BUFFER_SIZE)
            if not data:
                # EOF reached: attempt TCP FIN propagation
                if writer.can_write_eof():
                    writer.write_eof()
                break
            writer.write(data)
            await writer.drain()
    except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError):
        # Abrupt peer disconnect is normal at L4
        pass
    except OSError as err:
        logger.debug("Socket error on %s: %s", direction, err)


async def _safe_close(writer: asyncio.StreamWriter) -> None:
    """Close writer socket without throwing unhandled exceptions."""
    try:
        writer.close()
        await writer.wait_closed()
    except (OSError, asyncio.CancelledError):
        pass


async def handle_connection(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    pool: BackendPool,
    connect_timeout: float = 5.0,
) -> None:
    """Route incoming client connection to next healthy backend."""
    client_addr = client_writer.get_extra_info("peername")
    backend = await pool.get_next()

    if not backend:
        logger.error("No healthy backends available for client %s", client_addr)
        await _safe_close(client_writer)
        return

    backend_writer = None
    try:
        # Connect to selected backend
        backend_reader, backend_writer = await asyncio.wait_for(
            asyncio.open_connection(backend.host, backend.port),
            timeout=connect_timeout,
        )
    except (asyncio.TimeoutError, OSError) as err:
        logger.warning("Failed connecting to backend %s: %s", backend.endpoint, err)
        # Immediate retry or failover: mark consecutive failure
        backend.consecutive_failure += 1
        if backend.consecutive_failure >= 2:
            await pool.update_status(backend, False)
        await _safe_close(client_writer)
        return

    logger.debug("Proxying %s <-> %s", client_addr, backend.endpoint)

    try:
        # Run bidirectional piping concurrently
        # ponytail: gather cancel on first termination terminates opposite direction promptly.
        pipe_c2b = asyncio.create_task(_pipe(client_reader, backend_writer, "client->backend"))
        pipe_b2c = asyncio.create_task(_pipe(backend_reader, client_writer, "backend->client"))

        done, pending = await asyncio.wait(
            [pipe_c2b, pipe_b2c],
            return_when=asyncio.FIRST_COMPLETED
        )

        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    finally:
        # Guarantee neither socket leaks
        await _safe_close(client_writer)
        if backend_writer:
            await _safe_close(backend_writer)
        logger.debug("Closed connection %s <-> %s", client_addr, backend.endpoint)
