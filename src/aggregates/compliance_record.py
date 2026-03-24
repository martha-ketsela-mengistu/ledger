# src/aggregates/compliance_record.py

from dataclasses import dataclass, field
from src.models.events import ComplianceVerdict, StoredEvent

@dataclass
class ComplianceRecordAggregate:
    application_id: str
    rules_passed: set[str] = field(default_factory=set)
    rules_failed: set[str] = field(default_factory=set)
    has_hard_block: bool = False
    overall_verdict: ComplianceVerdict | None = None
    version: int = 0

    @classmethod
    async def load(cls, store, application_id: str) -> "ComplianceRecordAggregate":
        """Load and replay event stream to rebuild aggregate state."""
        agg = cls(application_id=application_id)
        stream_id = f"compliance-{application_id}"
        events = await store.load_stream(stream_id)
        for event in events:
            agg.apply(event)
        return agg

    def apply(self, event: StoredEvent) -> None:
        """Apply one event to update aggregate state."""
        et = event.event_type
        p = event.payload
        
        if et == "ComplianceRulePassed":
            self.rules_passed.add(p.get("rule_id"))
            self.rules_failed.discard(p.get("rule_id"))
        elif et == "ComplianceRuleFailed":
            self.rules_failed.add(p.get("rule_id"))
            self.rules_passed.discard(p.get("rule_id"))
            if p.get("is_hard_block"):
                self.has_hard_block = True
        elif et == "ComplianceCheckCompleted":
            self.overall_verdict = ComplianceVerdict(p.get("overall_verdict"))
            self.has_hard_block = p.get("has_hard_block", self.has_hard_block)

        self.version = event.stream_position

    def is_fully_cleared(self) -> bool:
        """Check if all requirements are met (Rule 5)."""
        if self.overall_verdict != ComplianceVerdict.CLEAR:
            return False
        if self.has_hard_block:
            return False
        if len(self.rules_failed) > 0:
            return False
        return True
