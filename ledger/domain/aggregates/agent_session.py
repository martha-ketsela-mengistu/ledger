# ledger/domain/aggregates/agent_session.py

from dataclasses import dataclass, field
from ledger.domain.errors import DomainError

@dataclass
class AgentSessionAggregate:
    session_id: str
    application_id: str | None = None
    context_loaded: bool = False
    version: int = 0

    @classmethod
    async def load(cls, store, session_id: str) -> "AgentSessionAggregate":
        """Load and replay event stream to rebuild aggregate state."""
        agg = cls(session_id=session_id)
        # Assuming session_id is unique across agent types for simplicity, 
        # or we need the full stream_id "agent-{type}-{id}"
        # For now, let's assume we use the full stream_id
        events = await store.load_stream(session_id)
        for event in events:
            agg.apply(event)
        return agg

    def apply(self, event: dict) -> None:
        """Apply one event to update aggregate state."""
        et = event.get("event_type")
        method_name = f"_apply_{et}"
        if hasattr(self, method_name):
            getattr(self, method_name)(event)
        
        if "stream_position" in event:
            self.version = event["stream_position"]
        else:
            self.version += 1

    def _apply_AgentSessionStarted(self, event: dict) -> None:
        p = event.get("payload", {})
        self.application_id = p.get("application_id")

    def _apply_AgentContextLoaded(self, event: dict) -> None:
        self.context_loaded = True

    def assert_context_loaded(self, action: str) -> None:
        """Rule 2: MUST have AgentContextLoaded before any decision/decision-related work."""
        if not self.context_loaded:
            raise DomainError(f"Cannot perform {action} - AgentContextLoaded must be the first event.")

    def assert_model_version_current(self, model_version: str) -> None:
        """Enforces that the model version used matches the session's intended version."""
        # TODO: Implement version check logic if session stores intended version
        pass
