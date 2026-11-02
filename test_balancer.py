"""Comprehensive test suite for Layer 4 Load Balancer."""

import asyncio
import json
import unittest
from admin import start_admin_server
from health import Backend, BackendPool, HealthChecker
from proxy import IPRateLimiter, handle_connection


class MockEchoServer:
    """Mock TCP server returning fixed identifier tag and echoed payload."""

    def __init__(self, tag: str):
        self.tag = tag.encode()
        self.server = None
        self.port = 0
        self.received_headers = []

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        data = await reader.read(2048)
        self.received_headers.append(data)
        writer.write(self.tag + b":" + data)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    async def start(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()


class TestLoadBalancer(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.srv1 = MockEchoServer("SRV1")
        self.srv2 = MockEchoServer("SRV2")
        self.srv3 = MockEchoServer("SRV3")
        await self.srv1.start()
        await self.srv2.start()
        await self.srv3.start()

        self.b1 = Backend("127.0.0.1", self.srv1.port, weight=1)
        self.b2 = Backend("127.0.0.1", self.srv2.port, weight=2)
        self.b3 = Backend("127.0.0.1", self.srv3.port, weight=1)

        self.pool = BackendPool([self.b1, self.b2, self.b3], algorithm="round_robin")
        self.checker = HealthChecker(self.pool, interval=0.05, timeout=0.03, rise_threshold=1, fall_threshold=1)
        self.checker.start()

        self.proxy_protocol_enabled = False
        self.rate_limiter = None

        self.lb_server = await asyncio.start_server(
            lambda r, w: handle_connection(
                r,
                w,
                self.pool,
                connect_timeout=0.5,
                rate_limiter=self.rate_limiter,
                proxy_protocol=self.proxy_protocol_enabled,
            ),
            "127.0.0.1",
            0,
        )
        self.lb_port = self.lb_server.sockets[0].getsockname()[1]

        # Admin server
        self.admin_server = await start_admin_server("127.0.0.1", 0, self.pool)
        self.admin_port = self.admin_server.sockets[0].getsockname()[1]

    async def asyncTearDown(self):
        await self.checker.stop()
        self.lb_server.close()
        await self.lb_server.wait_closed()
        self.admin_server.close()
        await self.admin_server.wait_closed()
        await self.srv1.stop()
        await self.srv2.stop()
        await self.srv3.stop()

    async def _send_request(self, msg: bytes) -> bytes:
        r, w = await asyncio.open_connection("127.0.0.1", self.lb_port)
        w.write(msg)
        await w.drain()
        res = await r.read(1024)
        w.close()
        await w.wait_closed()
        return res

    async def _admin_get(self, path: str) -> tuple[int, str]:
        r, w = await asyncio.open_connection("127.0.0.1", self.admin_port)
        req = f"GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
        w.write(req.encode())
        await w.drain()
        data = await r.read(8192)
        w.close()
        await w.wait_closed()

        parts = data.decode("utf-8", errors="ignore").split("\r\n\r\n", 1)
        header_lines = parts[0].split("\r\n")
        status_code = int(header_lines[0].split()[1])
        body = parts[1] if len(parts) > 1 else ""
        return status_code, body

    async def test_round_robin_rotation(self):
        self.pool.algorithm = "round_robin"
        responses = [await self._send_request(b"ping") for _ in range(6)]
        tags = [r.split(b":")[0].decode() for r in responses]
        self.assertEqual(tags, ["SRV1", "SRV2", "SRV3", "SRV1", "SRV2", "SRV3"])

    async def test_least_connections_dispatch(self):
        self.pool.algorithm = "least_connections"
        # Artificially bias active connections
        self.b1.active_connections = 5
        self.b2.active_connections = 1
        self.b3.active_connections = 4

        b = await self.pool.get_next()
        self.assertEqual(b.endpoint, self.b2.endpoint)
        await self.pool.release(b)

    async def test_ip_hash_consistency(self):
        self.pool.algorithm = "ip_hash"
        b_ip1_first = await self.pool.get_next(client_ip="192.168.1.50")
        await self.pool.release(b_ip1_first)

        b_ip1_second = await self.pool.get_next(client_ip="192.168.1.50")
        await self.pool.release(b_ip1_second)

        self.assertEqual(b_ip1_first.endpoint, b_ip1_second.endpoint)

    async def test_weighted_round_robin(self):
        # Weights: b1=1, b2=2, b3=1 -> Pattern: b2, b1, b2, b3 (2 calls to b2 for each 1 to b1, b3)
        self.pool.algorithm = "weighted_round_robin"
        picks = []
        for _ in range(4):
            b = await self.pool.get_next()
            picks.append(b.endpoint)
            await self.pool.release(b)

        self.assertEqual(picks.count(self.b2.endpoint), 2)
        self.assertEqual(picks.count(self.b1.endpoint), 1)
        self.assertEqual(picks.count(self.b3.endpoint), 1)

    async def test_failover_and_recovery(self):
        await self.srv2.stop()
        await asyncio.sleep(0.15)

        responses = [await self._send_request(b"ping") for _ in range(4)]
        tags = [r.split(b":")[0].decode() for r in responses]
        self.assertNotIn("SRV2", tags)
        self.assertEqual(set(tags), {"SRV1", "SRV3"})

        recovered = MockEchoServer("SRV2_REVIVED")
        recovered.server = await asyncio.start_server(recovered._handle, "127.0.0.1", self.b2.port)
        try:
            await asyncio.sleep(0.15)
            responses = [await self._send_request(b"ping") for _ in range(6)]
            tags = [r.split(b":")[0].decode() for r in responses]
            self.assertIn("SRV2_REVIVED", tags)
        finally:
            await recovered.stop()

    async def test_admin_endpoints(self):
        # Test /healthz
        code, body = await self._admin_get("/healthz")
        self.assertEqual(code, 200)
        self.assertIn("UP", body)

        # Test /metrics
        code, body = await self._admin_get("/metrics")
        self.assertEqual(code, 200)
        self.assertIn("balancer_active_connections", body)
        self.assertIn("balancer_backend_alive", body)

        # Test /stats
        code, body = await self._admin_get("/stats")
        self.assertEqual(code, 200)
        data = json.loads(body)
        self.assertEqual(data["status"], "UP")
        self.assertEqual(len(data["pool"]["backends"]), 3)

    async def test_rate_limiter(self):
        limiter = IPRateLimiter(rate=2.0, burst=2.0, enabled=True)
        # First 2 allowed
        self.assertTrue(await limiter.allow("1.2.3.4"))
        self.assertTrue(await limiter.allow("1.2.3.4"))
        # 3rd burst exhausted
        self.assertFalse(await limiter.allow("1.2.3.4"))

    async def test_hot_reload_draining(self):
        # Hot reload: remove b3, keep b1 & b2
        new_backends = [
            Backend("127.0.0.1", self.srv1.port, weight=1),
            Backend("127.0.0.1", self.srv2.port, weight=2),
        ]
        await self.pool.reload_config(new_backends, "round_robin")

        # b3 should now be marked draining
        self.assertTrue(self.b3.draining)
        # Active backends should not route to b3
        endpoints = [b.endpoint for b in self.pool._alive_backends]
        self.assertNotIn(self.b3.endpoint, endpoints)

        # Releasing lingering connection should cull b3
        self.b3.active_connections = 1
        await self.pool.release(self.b3)
        self.assertNotIn(self.b3, self.pool.all_backends)


if __name__ == "__main__":
    unittest.main()
