"""Backend pool manager and TCP health checker."""

import asyncio
import logging
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger("balancer.health")


@dataclass
class Backend:
    host: str
    port: int
    weight: int = 1
    alive: bool = True
    consecutive_success: int = 0
    consecutive_failure: int = 0

    @property
    def endpoint(self) -> str:
        return f"{self.host}:{self.port}"


class BackendPool:
    """Thread-safe / task-safe backend pool with round-robin dispatch."""

    def __init__(self, backends: List[Backend]):
        self._all_backends = backends
        self._alive_backends: List[Backend] = [b for b in backends if b.alive]
        self._lock = asyncio.Lock()
        self._index = 0

    @property
    def alive_count(self) -> int:
        return len(self._alive_backends)

    async def get_next(self) -> Optional[Backend]:
        """Atomically pick next healthy backend using round-robin."""
        async with self._lock:
            if not self._alive_backends:
                return None
            backend = self._alive_backends[self._index % len(self._alive_backends)]
            self._index = (self._index + 1) % len(self._alive_backends)
            return backend

    async def update_status(self, backend: Backend, is_alive: bool) -> None:
        """Update backend state and refresh active list if status flipped."""
        async with self._lock:
            if backend.alive == is_alive:
                return
            backend.alive = is_alive
            # ponytail: flat list slice recreation works up to ~10k backends. Upgrade to bisect or bitset if huge.
            self._alive_backends = [b for b in self._all_backends if b.alive]
            self._index = 0
            state_str = "HEALTHY" if is_alive else "DEAD"
            logger.warning("Backend %s changed state to %s (active: %d/%d)",
                           backend.endpoint, state_str, len(self._alive_backends), len(self._all_backends))


class HealthChecker:
    """Periodic TCP probe for backend alive detection."""

    def __init__(
        self,
        pool: BackendPool,
        interval: float = 2.0,
        timeout: float = 1.0,
        rise_threshold: int = 2,
        fall_threshold: int = 2,
    ):
        self.pool = pool
        self.interval = interval
        self.timeout = timeout
        self.rise_threshold = rise_threshold
        self.fall_threshold = fall_threshold
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def probe(self, backend: Backend) -> bool:
        """Attempt TCP handshake to determine backend readiness."""
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(backend.host, backend.port),
                timeout=self.timeout
            )
            writer.close()
            await writer.wait_closed()
            return True
        except (OSError, asyncio.TimeoutError):
            return False

    async def check_backend(self, backend: Backend) -> None:
        """Probe single backend and apply rise/fall hysteresis."""
        success = await self.probe(backend)
        if success:
            backend.consecutive_success += 1
            backend.consecutive_failure = 0
            if not backend.alive and backend.consecutive_success >= self.rise_threshold:
                await self.pool.update_status(backend, True)
        else:
            backend.consecutive_failure += 1
            backend.consecutive_success = 0
            if backend.alive and backend.consecutive_failure >= self.fall_threshold:
                await self.pool.update_status(backend, False)

    async def _run_loop(self) -> None:
        while self._running:
            # Probe all backends concurrently
            await asyncio.gather(
                *(self.check_backend(b) for b in self.pool._all_backends),
                return_exceptions=True
            )
            await asyncio.sleep(self.interval)

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
