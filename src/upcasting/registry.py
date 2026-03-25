"""
src/upcasting/registry.py
=========================
Registry for event upcasters to transparently migrate old events.
"""
from typing import Callable, Awaitable, Any
import logging

logger = logging.getLogger(__name__)

class UpcasterRegistry:
    """
    Transforms old event versions to current versions on load.
    Upcasters are PURE functions — they never write to the database.
    """
    def __init__(self):
        self._upcasters: dict[str, dict[int, Callable[[dict, Any], Awaitable[dict]]]] = {}
        self._store = None  # Reference to EventStore for lookups

    def set_store(self, store):
        self._store = store

    def register(self, event_type: str, from_version: int):
        """Decorator. Registers async fn as upcaster from event_type@from_version."""
        def decorator(fn: Callable[[dict, Any], Awaitable[dict]]):
            self._upcasters.setdefault(event_type, {})[from_version] = fn
            return fn
        return decorator

    async def upcast(self, event: dict) -> dict:
        """Apply chain of async upcasters until latest version reached."""
        et = event["event_type"]
        v = event.get("event_version", 1)
        
        chain = self._upcasters.get(et, {})
        while v in chain:
            logger.debug(f"Upcasting {et} from v{v} to v{v+1}")
            event["payload"] = await chain[v](dict(event["payload"]), self._store)
            v += 1
            event["event_version"] = v
        return event

