"""Backend pool manager, active TCP health checker, and multi-algorithm dispatch."""

import asyncio
import hashlib
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("balancer.health")


@dataclass
class Backend:
    host: str
    port: int
    weight: int = 1
    alive: bool = True
    draining: bool = False
    consecutive_success: int = 0
    consecutive_failure: int = 0
    active_connections: int = 0
    total_connections: int = 0
    bytes_in: int = 0
    bytes_out: int = 0
    # For Nginx-style smooth weighted round-robin
    effective_weight: int = 1
    current_weight: int = 0

    def __post_init__(self):
        self.effective_weight = self.weight

    @property
    def endpoint(self) -> str:
        return f"{self.host}:{self.port}"


class BackendPool:
    """Thread-safe backend pool supporting multiple dispatch strategies."""

    SUPPORTED_ALGORITHMS = (
        "round_robin",
        "least_connections",
        "ip_hash",
        "weighted_round_robin",
    )

    def __init__(self, backends: List[Backend], algorithm: str = "round_robin"):
        self._all_backends: List[Backend] = backends
        self._alive_backends: List[Backend] = [b for b in backends if b.alive and not b.draining]
        self._lock = asyncio.Lock()
        self._index = 0
        self.algorithm = algorithm if algorithm in self.SUPPORTED_ALGORITHMS else "round_robin"

    @property
    def alive_count(self) -> int:
        return len(self._alive_backends)

    @property
    def all_backends(self) -> List[Backend]:
        return list(self._all_backends)

    def _select_round_robin(self) -> Backend:
        backend = self._alive_backends[self._index % len(self._alive_backends)]
        self._index = (self._index + 1) % len(self._alive_backends)
        return backend

    def _select_least_connections(self) -> Backend:
        return min(self._alive_backends, key=lambda b: b.active_connections)

    def _select_ip_hash(self, client_ip: str) -> Backend:
        # Stable MD5 hash modulo healthy backends
        digest = hashlib.md5(client_ip.encode()).hexdigest()
        idx = int(digest, 16) % len(self._alive_backends)
        return self._alive_backends[idx]

    def _select_weighted_round_robin(self) -> Backend:
        # Nginx smooth weighted round-robin algorithm
        total_weight = sum(b.effective_weight for b in self._alive_backends)
        if total_weight <= 0:
            return self._select_round_robin()

        best: Optional[Backend] = None
        for b in self._alive_backends:
            b.current_weight += b.effective_weight
            if best is None or b.current_weight > best.current_weight:
                best = b

        if best:
            best.current_weight -= total_weight
            return best
        return self._alive_backends[0]

    async def get_next(self, client_ip: str = "127.0.0.1") -> Optional[Backend]:
        """Atomically pick next healthy backend using configured algorithm."""
        async with self._lock:
            if not self._alive_backends:
                return None

            if self.algorithm == "least_connections":
                backend = self._select_least_connections()
            elif self.algorithm == "ip_hash":
                backend = self._select_ip_hash(client_ip)
            elif self.algorithm == "weighted_round_robin":
                backend = self._select_weighted_round_robin()
            else:
                backend = self._select_round_robin()

            backend.active_connections += 1
            backend.total_connections += 1
            return backend

    async def release(self, backend: Backend, bytes_in: int = 0, bytes_out: int = 0) -> None:
        """Release active connection and record traffic metrics."""
        async with self._lock:
            backend.active_connections = max(0, backend.active_connections - 1)
            backend.bytes_in += bytes_in
            backend.bytes_out += bytes_out

            # Culling drained backend with zero connections
            if backend.draining and backend.active_connections == 0:
                if backend in self._all_backends:
                    self._all_backends.remove(backend)
                if backend in self._alive_backends:
                    self._alive_backends.remove(backend)
                logger.info("Drained backend %s culling complete", backend.endpoint)

    async def update_status(self, backend: Backend, is_alive: bool) -> None:
        """Update backend alive state and refresh active list."""
        async with self._lock:
            if backend.alive == is_alive:
                return
            backend.alive = is_alive
            self._alive_backends = [b for b in self._all_backends if b.alive and not b.draining]
            self._index = 0
            state_str = "HEALTHY" if is_alive else "DEAD"
            logger.warning(
                "Backend %s changed state to %s (active: %d/%d)",
                backend.endpoint,
                state_str,
                len(self._alive_backends),
                len(self._all_backends),
            )

    async def reload_config(self, new_backends: List[Backend], algorithm: str) -> None:
        """Zero-downtime hot reload: preserve live state, drain deleted, add new."""
        async with self._lock:
            self.algorithm = algorithm if algorithm in self.SUPPORTED_ALGORITHMS else "round_robin"
            new_map: Dict[Tuple[str, int], Backend] = {(b.host, b.port): b for b in new_backends}
            cur_map: Dict[Tuple[str, int], Backend] = {(b.host, b.port): b for b in self._all_backends}

            # Update existing or mark removed for draining
            for key, existing in cur_map.items():
                if key in new_map:
                    # Update weights and un-drain
                    target = new_map[key]
                    existing.weight = target.weight
                    existing.effective_weight = target.weight
                    existing.draining = False
                else:
                    existing.draining = True
                    logger.info("Backend %s marked DRAINING", existing.endpoint)

            # Add newly discovered backends
            for key, fresh in new_map.items():
                if key not in cur_map:
                    self._all_backends.append(fresh)
                    logger.info("Backend %s added to pool", fresh.endpoint)

            self._alive_backends = [b for b in self._all_backends if b.alive and not b.draining]
            self._index = 0
            logger.info("Pool reloaded: %d active, %d total", len(self._alive_backends), len(self._all_backends))


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
                timeout=self.timeout,
            )
            writer.close()
            await writer.wait_closed()
            return True
        except (OSError, asyncio.TimeoutError):
            return False

    async def check_backend(self, backend: Backend) -> None:
        """Probe single backend and apply rise/fall hysteresis."""
        if backend.draining:
            return
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
            await asyncio.gather(
                *(self.check_backend(b) for b in self.pool._all_backends),
                return_exceptions=True,
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
