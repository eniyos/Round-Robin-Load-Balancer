"""Automated self-contained verification suite for L4 Balancer."""

import asyncio
import unittest
from health import Backend, BackendPool, HealthChecker
from proxy import handle_connection


class MockEchoServer:
    """Mock TCP server returning fixed identifier tag."""

    def __init__(self, tag: str):
        self.tag = tag.encode()
        self.server = None
        self.port = 0

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        data = await reader.read(1024)
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

        self.b1 = Backend("127.0.0.1", self.srv1.port)
        self.b2 = Backend("127.0.0.1", self.srv2.port)
        self.b3 = Backend("127.0.0.1", self.srv3.port)

        self.pool = BackendPool([self.b1, self.b2, self.b3])
        self.checker = HealthChecker(self.pool, interval=0.1, timeout=0.05, rise_threshold=1, fall_threshold=1)
        self.checker.start()

        # Start LB server
        self.lb_server = await asyncio.start_server(
            lambda r, w: handle_connection(r, w, self.pool, connect_timeout=0.5),
            "127.0.0.1",
            0,
        )
        self.lb_port = self.lb_server.sockets[0].getsockname()[1]

    async def asyncTearDown(self):
        await self.checker.stop()
        self.lb_server.close()
        await self.lb_server.wait_closed()
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

    async def test_round_robin_rotation(self):
        responses = [await self._send_request(b"ping") for _ in range(6)]
        tags = [r.split(b":")[0].decode() for r in responses]
        self.assertEqual(tags, ["SRV1", "SRV2", "SRV3", "SRV1", "SRV2", "SRV3"])

    async def test_failover_and_recovery(self):
        # Stop SRV2
        await self.srv2.stop()
        await asyncio.sleep(0.3)  # Allow health checker to detect failure

        responses = [await self._send_request(b"ping") for _ in range(4)]
        tags = [r.split(b":")[0].decode() for r in responses]
        # Should alternate between SRV1 and SRV3 only
        self.assertNotIn("SRV2", tags)
        self.assertEqual(set(tags), {"SRV1", "SRV3"})

        # Recover SRV2 on same port
        recovered_srv2 = MockEchoServer("SRV2_REVIVED")
        recovered_srv2.server = await asyncio.start_server(recovered_srv2._handle, "127.0.0.1", self.b2.port)
        try:
            await asyncio.sleep(0.3)  # Allow health check to rise
            responses = [await self._send_request(b"ping") for _ in range(6)]
            tags = [r.split(b":")[0].decode() for r in responses]
            self.assertIn("SRV2_REVIVED", tags)
        finally:
            await recovered_srv2.stop()


if __name__ == "__main__":
    unittest.main()
