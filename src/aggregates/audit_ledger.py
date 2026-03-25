# src/aggregates/audit_ledger.py

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from src.models.events import StoredEvent

logger = logging.getLogger(__name__)

@dataclass
class AuditLedgerAggregate:
    entity_id: str
    last_hash: str | None = None
    verification_count: int = 0
    tamper_detected: bool = False
    version: int = 0

    @classmethod
    async def load(cls, store, entity_id: str) -> "AuditLedgerAggregate":
        """
        Load and replay event stream to rebuild aggregate state.
        """
        logger.debug(f"Loading AuditLedgerAggregate for {entity_id}")
        agg = cls(entity_id=entity_id)
        stream_id = f"audit-{entity_id}"
        events = await store.load_stream(stream_id)
        for event in events:
            agg.apply(event)
        return agg

    def apply(self, event: StoredEvent) -> None:
        """
        Apply one event to update aggregate state.
        """
        logger.debug(f"[{self.entity_id}] Applying {event.event_type}")
        et = event.event_type
        p = event.payload
        
        if et == "AuditIntegrityCheckRun":
            self.last_hash = p.get("integrity_hash")
            self.verification_count += p.get("events_verified_count", 0)
            self.tamper_detected = p.get("tamper_detected", False)

        self.version = event.stream_position

    def compute_next_hash(self, event_ids: list[str]) -> str:
        """Rule 6: Causal chain. Computes next SHA-256 integrity hash."""
        # Simple implementation: hash previous hash + sorted event IDs
        ctx = hashlib.sha256()
        if self.last_hash:
            ctx.update(self.last_hash.encode())
        for eid in sorted(event_ids):
            ctx.update(eid.encode())
        return ctx.hexdigest()
