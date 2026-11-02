"""Main entry point for Layer 4 TCP Load Balancer."""

import asyncio
import json
import logging
import signal
import sys
from pathlib import Path

from health import Backend, BackendPool, HealthChecker
from proxy import handle_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("balancer.main")


def load_config(config_path: str = "config.json") -> dict:
    path = Path(config_path)
    if not path.is_file():
        logger.error("Configuration file '%s' not found.", config_path)
        sys.exit(1)
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


async def main() -> None:
    config_file = sys.argv[1] if len(sys.argv) > 1 else "config.json"
    cfg = load_config(config_file)

    bind_host = cfg.get("bind_host", "0.0.0.0")
    bind_port = int(cfg.get("bind_port", 8080))
    conn_timeout = float(cfg.get("connection_timeout", 5.0))

    hc_cfg = cfg.get("health_check", {})
    interval = float(hc_cfg.get("interval_seconds", 2.0))
    timeout = float(hc_cfg.get("timeout_seconds", 1.0))
    rise = int(hc_cfg.get("rise_threshold", 2))
    fall = int(hc_cfg.get("fall_threshold", 2))

    backends = [
        Backend(
            host=b["host"],
            port=int(b["port"]),
            weight=int(b.get("weight", 1)),
        )
        for b in cfg.get("backends", [])
    ]

    if not backends:
        logger.error("No backends configured.")
        sys.exit(1)

    pool = BackendPool(backends)
    checker = HealthChecker(
        pool=pool,
        interval=interval,
        timeout=timeout,
        rise_threshold=rise,
        fall_threshold=fall,
    )

    # Initial synchronous probe so startup state is accurate
    for b in backends:
        await checker.check_backend(b)

    checker.start()
    logger.info("Initialized pool with %d healthy backends out of %d.", pool.alive_count, len(backends))

    server = await asyncio.start_server(
        lambda r, w: handle_connection(r, w, pool, conn_timeout),
        host=bind_host,
        port=bind_port,
        reuse_address=True,
    )

    logger.info("TCP Load Balancer listening on %s:%d", bind_host, bind_port)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # Windows fallback
            pass

    try:
        await stop_event.wait()
    finally:
        logger.info("Shutting down load balancer...")
        await checker.stop()
        server.close()
        await server.wait_closed()
        logger.info("Load balancer stopped cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
