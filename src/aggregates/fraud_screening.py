# src/aggregates/fraud_screening.py

from dataclasses import dataclass, field
from src.models.events import StoredEvent

@dataclass
class FraudScreeningAggregate:
    application_id: str
    anomalies_detected: int = 0
    risk_level: str = "LOW"
    fraud_score: float = 0.0
    is_cleared: bool = True
    version: int = 0

    @classmethod
    async def load(cls, store, application_id: str) -> "FraudScreeningAggregate":
        """Load and replay event stream to rebuild aggregate state."""
        agg = cls(application_id=application_id)
        stream_id = f"fraud-{application_id}"
        events = await store.load_stream(stream_id)
        for event in events:
            agg.apply(event)
        return agg

    def apply(self, event: StoredEvent) -> None:
        """Apply one event to update aggregate state."""
        et = event.event_type
        p = event.payload
        
        if et == "FraudAnomalyDetected":
            self.anomalies_detected += 1
            # Simple rule: any anomaly detected makes it uncleared until human review?
            # Or based on score/severity.
        elif et == "FraudScreeningCompleted":
            self.fraud_score = p.get("fraud_score", 0.0)
            self.risk_level = p.get("risk_level", "LOW")
            # Mark NOT cleared if risk is HIGH or score > threshold
            if self.risk_level == "HIGH" or self.fraud_score > 0.8:
                self.is_cleared = False
            else:
                self.is_cleared = True

        self.version = event.stream_position
