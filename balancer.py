"""Main entry point for Layer 4 TCP Load Balancer."""

import asyncio
import json
import logging
import signal
import sys
from pathlib import Path

from admin import start_admin_server
from health import Backend, BackendPool, HealthChecker
from proxy import IPRateLimiter, handle_connection

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


def parse_backends(cfg: dict) -> list[Backend]:
    return [
        Backend(
            host=b["host"],
            port=int(b["port"]),
            weight=int(b.get("weight", 1)),
        )
        for b in cfg.get("backends", [])
    ]


async def main() -> None:
    config_file = sys.argv[1] if len(sys.argv) > 1 else "config.json"
    cfg = load_config(config_file)

    bind_host = cfg.get("bind_host", "0.0.0.0")
    bind_port = int(cfg.get("bind_port", 8080))
    algorithm = cfg.get("algorithm", "round_robin")
    conn_timeout = float(cfg.get("connection_timeout", 5.0))
    max_conns = int(cfg.get("max_connections", 10000))
    proxy_proto = bool(cfg.get("proxy_protocol", False))

    # Admin server settings
    admin_cfg = cfg.get("admin", {})
    admin_enabled = admin_cfg.get("enabled", True)
    admin_host = admin_cfg.get("host", "127.0.0.1")
    admin_port = int(admin_cfg.get("port", 8081))

    # Rate limiting
    rl_cfg = cfg.get("rate_limit", {})
    rate_limiter = IPRateLimiter(
        rate=float(rl_cfg.get("rate", 50.0)),
        burst=float(rl_cfg.get("burst", 100.0)),
        enabled=bool(rl_cfg.get("enabled", False)),
    )

    # Health check settings
    hc_cfg = cfg.get("health_check", {})
    interval = float(hc_cfg.get("interval_seconds", 2.0))
    timeout = float(hc_cfg.get("timeout_seconds", 1.0))
    rise = int(hc_cfg.get("rise_threshold", 2))
    fall = int(hc_cfg.get("fall_threshold", 2))

    backends = parse_backends(cfg)
    if not backends:
        logger.error("No backends configured.")
        sys.exit(1)

    pool = BackendPool(backends, algorithm=algorithm)
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
    logger.info(
        "Initialized pool (%s) with %d healthy backends out of %d.",
        pool.algorithm,
        pool.alive_count,
        len(backends),
    )

    # Start main L4 proxy listener
    server = await asyncio.start_server(
        lambda r, w: handle_connection(
            r,
            w,
            pool,
            connect_timeout=conn_timeout,
            rate_limiter=rate_limiter,
            max_connections=max_conns,
            proxy_protocol=proxy_proto,
        ),
        host=bind_host,
        port=bind_port,
        reuse_address=True,
    )
    logger.info("TCP Load Balancer listening on %s:%d", bind_host, bind_port)

    # Start admin server if enabled
    admin_server = None
    if admin_enabled:
        admin_server = await start_admin_server(admin_host, admin_port, pool)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    # SIGHUP configuration hot reload handler
    def handle_reload():
        logger.info("Received SIGHUP, reloading configuration from %s...", config_file)
        try:
            new_cfg = load_config(config_file)
            new_backends = parse_backends(new_cfg)
            new_algo = new_cfg.get("algorithm", "round_robin")
            asyncio.create_task(pool.reload_config(new_backends, new_algo))
        except Exception as err:
            logger.error("Failed to reload configuration: %s", err)

    if hasattr(signal, "SIGHUP"):
        try:
            loop.add_signal_handler(signal.SIGHUP, handle_reload)
        except (NotImplementedError, RuntimeError):
            pass

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    try:
        await stop_event.wait()
    finally:
        logger.info("Shutting down load balancer...")
        await checker.stop()
        server.close()
        await server.wait_closed()
        if admin_server:
            admin_server.close()
            await admin_server.wait_closed()
        logger.info("Load balancer stopped cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
