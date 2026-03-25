import asyncio
import logging
from typing import List, Protocol
from datetime import datetime, UTC
from collections import defaultdict

logger = logging.getLogger(__name__)

class EventStoreProtocol(Protocol):
    async def load_all(self, from_position: int = 0, batch_size: int = 500): ...
    async def stream_version(self, stream_id: str) -> int: ...
    # We assume 'get_global_position' or similar to know max position if we need exact lag, 
    # or expose lag as global_position - last_processed_position.

class Projection(Protocol):
    name: str
    async def initialize(self, conn) -> None: ...
    async def handle_event(self, conn, event) -> None: ...
    async def rebuild_from_scratch(self, conn, store) -> None: ...

class ProjectionDaemon:
    """
    Fault-tolerant background asyncio task that polls events and routes to projections.
    """
    def __init__(self, store, projections: list[Projection], db_pool=None):
        self._store = store
        self._projections = {p.name: p for p in projections}
        self._db_pool = db_pool
        self._running = False
        self._checkpoints = {p.name: 0 for p in projections}
        self._latest_global_pos = 0  # To track max global pos seen

    async def run_forever(self, poll_interval_ms: int = 100) -> None:
        self._running = True
        
        # 1. Initialize all projections before polling
        if self._db_pool:
            async with self._db_pool.acquire() as conn:
                for name, proj in self._projections.items():
                    logger.info(f"Initializing projection: {name}")
                    await proj.initialize(conn)
        
        while self._running:
            try:
                await self._process_batch()
            except Exception as e:
                logger.error(f"Daemon fatal error: {e}")
            await asyncio.sleep(poll_interval_ms / 1000)

    async def _process_batch(self) -> None:
        # Load lowest checkpoint across all projections
        min_checkpoint = min(self._checkpoints.values()) if self._checkpoints else 0
        
        # We will dispatch events individually to each projection that hasn't processed it yet
        async for event in self._store.load_all(from_position=min_checkpoint, batch_size=500):
            # Track max global position for lag metric
            if event.global_position > self._latest_global_pos:
                self._latest_global_pos = event.global_position
                
            for p_name, proj in self._projections.items():
                if self._checkpoints[p_name] < event.global_position:
                    try:
                        if self._db_pool:
                            async with self._db_pool.acquire() as conn:
                                async with conn.transaction():
                                    await proj.handle_event(conn, event)
                                    await self._save_checkpoint(conn, p_name, event.global_position)
                        else:
                            await proj.handle_event(None, event)  # memory mock
                        
                        # Update memory cache
                        self._checkpoints[p_name] = event.global_position
                    except Exception as e:
                        # Fault tolerance: log and skip on explicit max retries (simplified here)
                        logger.error(f"Projection {p_name} failed on event {event.global_position}: {e}")
                        # Depending on configuration, we either increment checkpoint to skip, or stall.
                        # For robustness, we skip the offending event (increment checkpoint).
                        self._checkpoints[p_name] = event.global_position

    async def get_lag(self, projection_name: str) -> int:
        """Returns the difference between the max seen global position and the projection's checkpoint."""
        # Note: in a real Postgres cluster, we would query max(global_position) from events
        chk = self._checkpoints.get(projection_name, 0)
        return max(0, self._latest_global_pos - chk)

    # Note: initialization and checkpoint saving assume asyncpg connection or memory mapping
    async def _save_checkpoint(self, conn, name: str, pos: int):
        await conn.execute(
            """INSERT INTO projection_checkpoints (projection_name, last_position, updated_at)
               VALUES ($1, $2, NOW())
               ON CONFLICT (projection_name) DO UPDATE 
               SET last_position = excluded.last_position, updated_at = NOW()""",
            name, pos
        )
