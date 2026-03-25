import asyncio
import json
import logging
import asyncpg
from datetime import datetime, UTC
from uuid import UUID

logger = logging.getLogger("ledger.outbox_relay")

class OutboxRelay:
    """
    Background worker that polls the 'outbox' table and dispatches events.
    In the Ledger project, this simulates reliable event delivery to external
    systems (webhooks, email, etc.) or internal non-blocking side effects.
    """
    def __init__(self, db_url: str, poll_interval: float = 2.0):
        self.db_url = db_url
        self.poll_interval = poll_interval
        self._running = False
        self._pool = None

    async def start(self):
        self._running = True
        self._pool = await asyncpg.create_pool(self.db_url)
        logger.info(f"OutboxRelay started. Polling every {self.poll_interval}s")
        asyncio.create_task(self._run_loop())

    async def stop(self):
        self._running = False
        if self._pool:
            await self._pool.close()
        logger.info("OutboxRelay stopped.")

    async def _run_loop(self):
        while self._running:
            try:
                await self._process_outbox()
            except Exception as e:
                logger.error(f"Error in OutboxRelay loop: {e}")
            await asyncio.sleep(self.poll_interval)

    async def _process_outbox(self):
        async with self._pool.acquire() as conn:
            # 1. Fetch unprocessed entries (lock them)
            rows = await conn.fetch(
                "SELECT id, event_id, destination, payload FROM outbox "
                "WHERE published_at IS NULL ORDER BY created_at ASC LIMIT 20 FOR UPDATE SKIP LOCKED"
            )
            
            if not rows:
                return

            for row in rows:
                outbox_id = row["id"]
                event_id = row["event_id"]
                dest = row["destination"]
                payload = row["payload"]
                
                logger.debug(f"Processing outbox {outbox_id}: {dest}")
                
                try:
                    # 2. Dispatch (Simulated)
                    await self._dispatch(dest, payload)
                    
                    # 3. Mark as processed
                    await conn.execute(
                        "UPDATE outbox SET published_at = $1 WHERE id = $2",
                        datetime.now(UTC), outbox_id
                    )
                except Exception as e:
                    logger.error(f"Failed to dispatch outbox {outbox_id}: {e}")
                    # In a real system, we'd increment a retry count here

    async def _dispatch(self, destination: str, payload: dict):
        """
        Simulate dispatch to an external system.
        In this implementation, we just log it.
        """
        # Simulate network latency
        await asyncio.sleep(0.1)
        # logger.info(f" -> DISPATCHED [{destination}] event_id: {id}")
