# ledger/domain/aggregates/agent_session.py

import logging
from dataclasses import dataclass, field
from src.models.events import DomainError, StoredEvent, AgentType

logger = logging.getLogger(__name__)

@dataclass
class AgentSessionAggregate:
    session_id: str
    agent_type: AgentType | None = None
    application_id: str | None = None
    model_version: str | None = None
    context_loaded: bool = False
    version: int = 0

    @classmethod
    async def load(cls, store, agent_id: str, session_id: str) -> "AgentSessionAggregate":
        """
        Load and replay event stream to rebuild aggregate state.
        """
        logger.debug(f"Loading AgentSessionAggregate for {agent_id}:{session_id}")
        stream_id = f"agent-{agent_id}-{session_id}"
        agg = cls(session_id=session_id)
        events = await store.load_stream(stream_id)
        for event in events:
            agg.apply(event)
        return agg

    def apply(self, event: StoredEvent) -> None:
        """
        Apply one event to update aggregate state.
        """
        logger.debug(f"[{self.session_id}] Applying {event.event_type}")
        method_name = f"_on_{event.event_type}"
        if hasattr(self, method_name):
            getattr(self, method_name)(event)
        
        self.version = event.stream_position

    def _on_AgentSessionStarted(self, event: StoredEvent) -> None:
        p = event.payload
        self.application_id = p.get("application_id")
        self.agent_type = p.get("agent_type")
        self.model_version = p.get("model_version")

    def _on_AgentContextLoaded(self, event: StoredEvent) -> None:
        p = event.payload
        self.context_loaded = True
        # Record model version if present in context loading event (as per rubric)
        if "model_version" in p:
            self.model_version = p["model_version"]

    def assert_context_loaded(self, action: str) -> None:
        """Rule 2: MUST have AgentContextLoaded before any decision/decision-related work."""
        if not self.context_loaded:
            raise DomainError(f"Cannot perform {action} - AgentContextLoaded must be the first event.")

    def assert_model_version_current(self, model_version: str) -> None:
        """Enforces that the model version used matches the session's intended version."""
        if self.model_version and model_version != self.model_version:
            raise DomainError(f"Model version mismatch: session expects {self.model_version}, got {model_version}")
