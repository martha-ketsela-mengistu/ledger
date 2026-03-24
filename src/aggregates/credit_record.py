# src/aggregates/credit_record.py

from dataclasses import dataclass, field
from src.models.events import StoredEvent, CreditDecision

@dataclass
class CreditRecordAggregate:
    application_id: str
    decision: CreditDecision | None = None
    model_version: str | None = None
    version: int = 0

    @classmethod
    async def load(cls, store, application_id: str) -> "CreditRecordAggregate":
        """Load and replay event stream to rebuild aggregate state."""
        agg = cls(application_id=application_id)
        stream_id = f"credit-{application_id}"
        events = await store.load_stream(stream_id)
        for event in events:
            agg.apply(event)
        return agg

    def apply(self, event: StoredEvent) -> None:
        """Apply one event to update aggregate state."""
        et = event.event_type
        p = event.payload
        
        if et == "CreditAnalysisCompleted":
            # Map payload back to Pydantic model
            self.decision = CreditDecision(**p.get("decision", {}))
            self.model_version = p.get("model_version")

        self.version = event.stream_position
