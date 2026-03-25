"""
src/integrity/gas_town.py
=========================
Gas Town Agent Memory Pattern — prevents catastrophic agent memory loss.

An AI agent that crashes mid-session must be able to restart and reconstruct
its exact context from the event store, then continue where it left off
without repeating completed work.
"""
from __future__ import annotations
import json
import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class AgentContext:
    """Reconstructed agent context from event store replay."""
    context_text: str
    last_event_position: int
    pending_work: list[str] = field(default_factory=list)
    session_health_status: str = "HEALTHY"
    completed_nodes: list[str] = field(default_factory=list)
    last_completed_node: str | None = None
    application_id: str | None = None
    agent_type: str | None = None
    session_id: str | None = None
    total_events: int = 0
    needs_reconciliation: bool = False
    reconciliation_reason: str | None = None


# Standard LangGraph node ordering for each agent type
AGENT_NODE_ORDER = {
    "fraud_detection": [
        "validate_inputs", "load_document_facts",
        "cross_reference_registry", "analyze_fraud_patterns", "write_output",
    ],
    "credit_analysis": [
        "validate_inputs", "load_document_facts", "load_registry_data",
        "run_credit_analysis", "write_output",
    ],
    "document_processing": [
        "validate_inputs", "extract_documents", "assess_quality", "write_output",
    ],
    "compliance": [
        "validate_inputs", "load_context", "evaluate_rules", "write_output",
    ],
    "decision_orchestrator": [
        "validate_inputs", "load_analyses", "generate_decision", "write_output",
    ],
}

# Events that indicate partial/pending state
PARTIAL_STATE_EVENTS = {
    "AgentSessionStarted",      # Session begun but not completed
    "AgentNodeExecuted",        # Node ran but session didn't finish
    "AgentToolCalled",          # Tool was called
}

COMPLETION_EVENTS = {
    "AgentSessionCompleted",
    "AgentSessionFailed",
}


def _summarize_events(events: list, token_budget: int) -> str:
    """
    Summarize old events into prose (token-efficient).
    Preserves verbatim: last 3 events, any PENDING or ERROR state events.
    """
    if not events:
        return "No events to summarize."

    lines = []
    total_events = len(events)

    # Always preserve last 3 events verbatim
    preserved_indices = set(range(max(0, total_events - 3), total_events))

    # Also preserve PENDING/ERROR events
    for i, ev in enumerate(events):
        if ev.event_type in ("AgentSessionFailed", "AgentInputValidationFailed"):
            preserved_indices.add(i)

    # Summarize old events
    summarized_count = 0
    for i, ev in enumerate(events):
        if i in preserved_indices:
            payload_str = json.dumps(ev.payload, default=str)
            # Truncate individual payloads if too long
            if len(payload_str) > 500:
                payload_str = payload_str[:500] + "..."
            lines.append(f"[VERBATIM] {ev.event_type} @ pos {ev.stream_position}: {payload_str}")
        else:
            summarized_count += 1

    if summarized_count > 0:
        # Group summarized events by type
        type_counts: dict[str, int] = {}
        for i, ev in enumerate(events):
            if i not in preserved_indices:
                type_counts[ev.event_type] = type_counts.get(ev.event_type, 0) + 1

        summary_parts = [f"{t}×{c}" for t, c in type_counts.items()]
        lines.insert(0, f"[SUMMARY] {summarized_count} earlier events: {', '.join(summary_parts)}")

    # Respect token budget (~4 chars per token)
    result = "\n".join(lines)
    char_budget = token_budget * 4
    if len(result) > char_budget:
        result = result[:char_budget] + "\n... [TRUNCATED to fit token budget]"

    return result


async def reconstruct_agent_context(
    store: Any,
    agent_id: str,
    session_id: str,
    token_budget: int = 8000,
) -> AgentContext:
    """
    1. Load full AgentSession stream for agent_id + session_id
    2. Identify: last completed action, pending work items, current application state
    3. Summarise old events into prose (token-efficient)
    4. Preserve verbatim: last 3 events, any PENDING or ERROR state events
    5. Return: AgentContext with context_text, last_event_position,
              pending_work[], session_health_status

    CRITICAL: if the agent's last event was a partial decision (no corresponding
    completion event), flag the context as NEEDS_RECONCILIATION — the agent
    must resolve the partial state before proceeding.
    """
    # Try to find the agent session stream
    # Agent streams follow pattern: agent-{agent_type}-{session_id}
    # We need to search across agent types since we only have agent_id
    events = []
    agent_type = None

    for at in AGENT_NODE_ORDER.keys():
        stream_id = f"agent-{at}-{session_id}"
        try:
            stream_events = await store.load_stream(stream_id)
            if stream_events:
                events = stream_events
                agent_type = at
                break
        except Exception:
            continue

    # Fallback: try with agent_id as the type
    if not events:
        stream_id = f"agent-{agent_id}-{session_id}"
        try:
            events = await store.load_stream(stream_id)
            agent_type = agent_id
        except Exception:
            pass

    if not events:
        return AgentContext(
            context_text="No events found for this session.",
            last_event_position=0,
            session_health_status="NOT_FOUND",
        )

    # 2. Identify completed nodes, pending work, application state
    completed_nodes = []
    application_id = None
    last_completed_node = None
    session_completed = False
    session_failed = False

    for ev in events:
        if ev.event_type == "AgentSessionStarted":
            application_id = ev.payload.get("application_id")
        elif ev.event_type == "AgentNodeExecuted":
            node_name = ev.payload.get("node_name")
            if node_name:
                completed_nodes.append(node_name)
                last_completed_node = node_name
        elif ev.event_type == "AgentSessionCompleted":
            session_completed = True
        elif ev.event_type == "AgentSessionFailed":
            session_failed = True

    # Determine pending work from the known node order
    pending_work = []
    if agent_type and agent_type in AGENT_NODE_ORDER:
        all_nodes = AGENT_NODE_ORDER[agent_type]
        for node in all_nodes:
            if node not in completed_nodes:
                pending_work.append(node)

    # Determine health status
    if session_completed:
        health = "COMPLETED"
    elif session_failed:
        health = "FAILED"
    elif not completed_nodes:
        health = "INITIALIZING"
    else:
        health = "IN_PROGRESS"

    # CRITICAL: Check for NEEDS_RECONCILIATION
    needs_reconciliation = False
    reconciliation_reason = None

    last_event = events[-1]
    if last_event.event_type in PARTIAL_STATE_EVENTS and not session_completed:
        # The session has partial state — no completion event
        if last_event.event_type == "AgentNodeExecuted":
            node_name = last_event.payload.get("node_name", "unknown")
            # Check if this was a decision/output node without completion
            if "write_output" in node_name or "decision" in node_name.lower():
                needs_reconciliation = True
                reconciliation_reason = (
                    f"Partial decision detected: node '{node_name}' executed "
                    f"but no AgentSessionCompleted event found. "
                    f"Agent must resolve partial state before proceeding."
                )
                health = "NEEDS_RECONCILIATION"

    # 3-4. Summarize events
    context_text = _summarize_events(events, token_budget)

    last_position = events[-1].stream_position if events else 0

    return AgentContext(
        context_text=context_text,
        last_event_position=last_position,
        pending_work=pending_work,
        session_health_status=health,
        completed_nodes=completed_nodes,
        last_completed_node=last_completed_node,
        application_id=application_id,
        agent_type=agent_type,
        session_id=session_id,
        total_events=len(events),
        needs_reconciliation=needs_reconciliation,
        reconciliation_reason=reconciliation_reason,
    )
