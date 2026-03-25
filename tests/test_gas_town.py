"""
tests/test_gas_town.py
======================
Gas Town Pattern test: simulated crash recovery.

1. Start an agent session
2. Append 5 events
3. Call reconstruct_agent_context() WITHOUT the in-memory agent object
4. Verify that the reconstructed context contains enough information
   for the agent to continue correctly
"""
import pytest
import json
from uuid import uuid4
from datetime import datetime, UTC

DB_URL = "postgresql://postgres:apex@localhost/apex_ledger"


@pytest.mark.asyncio
async def test_gas_town_crash_recovery():
    """
    Simulated crash: 5 events appended to a session stream,
    then reconstruct_agent_context() called without in-memory agent.
    Verify that the context is sufficient for continuation.
    """
    from src.event_store import EventStore
    from src.integrity.gas_town import reconstruct_agent_context

    store = EventStore(DB_URL)
    await store.connect()

    session_id = f"sess-fra-{uuid4().hex[:8]}"
    agent_type = "fraud_detection"
    stream_id = f"agent-{agent_type}-{session_id}"
    app_id = f"APEX-GT-{uuid4().hex[:6]}"

    # Simulate 5 events from a fraud detection agent session
    events = [
        {
            "event_type": "AgentSessionStarted",
            "event_version": 1,
            "payload": {
                "session_id": session_id,
                "agent_type": agent_type,
                "agent_id": "fraud-agent-01",
                "application_id": app_id,
                "model_version": "gpt-4o-2026-03",
                "langgraph_graph_version": "0.2.0",
                "context_source": "fresh",
                "context_token_count": 1000,
                "started_at": datetime.now(UTC).isoformat(),
            },
        },
        {
            "event_type": "AgentNodeExecuted",
            "event_version": 1,
            "payload": {
                "session_id": session_id,
                "agent_type": agent_type,
                "node_name": "validate_inputs",
                "node_sequence": 1,
                "input_keys": ["application_id"],
                "output_keys": [],
                "llm_called": False,
                "duration_ms": 50,
                "executed_at": datetime.now(UTC).isoformat(),
            },
        },
        {
            "event_type": "AgentNodeExecuted",
            "event_version": 1,
            "payload": {
                "session_id": session_id,
                "agent_type": agent_type,
                "node_name": "load_document_facts",
                "node_sequence": 2,
                "input_keys": ["application_id"],
                "output_keys": ["extracted_facts"],
                "llm_called": False,
                "duration_ms": 120,
                "executed_at": datetime.now(UTC).isoformat(),
            },
        },
        {
            "event_type": "AgentNodeExecuted",
            "event_version": 1,
            "payload": {
                "session_id": session_id,
                "agent_type": agent_type,
                "node_name": "cross_reference_registry",
                "node_sequence": 3,
                "input_keys": ["extracted_facts"],
                "output_keys": ["registry_profile", "historical_financials"],
                "llm_called": False,
                "duration_ms": 200,
                "executed_at": datetime.now(UTC).isoformat(),
            },
        },
        {
            "event_type": "AgentToolCalled",
            "event_version": 1,
            "payload": {
                "session_id": session_id,
                "agent_type": agent_type,
                "tool_name": "registry_query",
                "tool_input_summary": "COMP-005",
                "tool_output_summary": "profile and financials",
                "tool_duration_ms": 150,
                "called_at": datetime.now(UTC).isoformat(),
            },
        },
    ]

    # Append all 5 events
    await store.append(stream_id, events, -1)

    # === SIMULATE CRASH: Agent object is gone. Only the event store remains. ===

    # Reconstruct context WITHOUT any in-memory agent
    ctx = await reconstruct_agent_context(
        store=store,
        agent_id="fraud-agent-01",
        session_id=session_id,
        token_budget=8000,
    )

    # Assertions
    assert ctx.total_events == 5, f"Expected 5 events, got {ctx.total_events}"
    assert ctx.application_id == app_id
    assert ctx.agent_type == agent_type
    assert ctx.session_id == session_id

    # Completed nodes
    assert "validate_inputs" in ctx.completed_nodes
    assert "load_document_facts" in ctx.completed_nodes
    assert "cross_reference_registry" in ctx.completed_nodes
    assert ctx.last_completed_node == "cross_reference_registry"

    # Pending work — should know what's left
    assert "analyze_fraud_patterns" in ctx.pending_work
    assert "write_output" in ctx.pending_work

    # Health status — session didn't complete or fail
    assert ctx.session_health_status == "IN_PROGRESS"

    # Context text should contain enough for continuation
    assert len(ctx.context_text) > 0
    assert "AgentNodeExecuted" in ctx.context_text or "SUMMARY" in ctx.context_text

    print(f"✅ Gas Town reconstruction: {ctx.total_events} events, "
          f"{len(ctx.completed_nodes)} nodes done, {len(ctx.pending_work)} pending")
    print(f"   Last node: {ctx.last_completed_node}")
    print(f"   Pending: {ctx.pending_work}")
    print(f"   Health: {ctx.session_health_status}")

    await store.close()


@pytest.mark.asyncio
async def test_gas_town_needs_reconciliation():
    """
    CRITICAL spec requirement: If an agent's last event was a partial decision
    (write_output executed but no AgentSessionCompleted), flag as NEEDS_RECONCILIATION.
    """
    from src.event_store import EventStore
    from src.integrity.gas_town import reconstruct_agent_context

    store = EventStore(DB_URL)
    await store.connect()

    session_id = f"sess-fra-{uuid4().hex[:8]}"
    agent_type = "fraud_detection"
    stream_id = f"agent-{agent_type}-{session_id}"
    app_id = f"APEX-REC-{uuid4().hex[:6]}"

    # Agent executed write_output but crashed before completing
    events = [
        {
            "event_type": "AgentSessionStarted",
            "event_version": 1,
            "payload": {
                "session_id": session_id,
                "agent_type": agent_type,
                "agent_id": "fraud-agent-01",
                "application_id": app_id,
                "model_version": "gpt-4o",
                "langgraph_graph_version": "0.2.0",
                "context_source": "fresh",
                "context_token_count": 1000,
                "started_at": datetime.now(UTC).isoformat(),
            },
        },
        {
            "event_type": "AgentNodeExecuted",
            "event_version": 1,
            "payload": {
                "session_id": session_id,
                "agent_type": agent_type,
                "node_name": "validate_inputs",
                "node_sequence": 1,
                "input_keys": ["application_id"],
                "output_keys": [],
                "llm_called": False,
                "duration_ms": 50,
                "executed_at": datetime.now(UTC).isoformat(),
            },
        },
        {
            "event_type": "AgentNodeExecuted",
            "event_version": 1,
            "payload": {
                "session_id": session_id,
                "agent_type": agent_type,
                "node_name": "write_output",
                "node_sequence": 5,
                "input_keys": ["fraud_score"],
                "output_keys": ["next_agent"],
                "llm_called": False,
                "duration_ms": 300,
                "executed_at": datetime.now(UTC).isoformat(),
            },
        },
        # NOTE: No AgentSessionCompleted — simulating crash after write_output
    ]

    await store.append(stream_id, events, -1)

    ctx = await reconstruct_agent_context(
        store=store,
        agent_id="fraud-agent-01",
        session_id=session_id,
        token_budget=8000,
    )

    assert ctx.needs_reconciliation is True, "Should detect partial decision state"
    assert ctx.session_health_status == "NEEDS_RECONCILIATION"
    assert "write_output" in ctx.reconciliation_reason

    print(f"✅ NEEDS_RECONCILIATION correctly detected: {ctx.reconciliation_reason}")

    await store.close()


@pytest.mark.asyncio
async def test_gas_town_completed_session():
    """Verify that a completed session is correctly identified."""
    from src.event_store import EventStore
    from src.integrity.gas_town import reconstruct_agent_context

    store = EventStore(DB_URL)
    await store.connect()

    session_id = f"sess-fra-{uuid4().hex[:8]}"
    agent_type = "fraud_detection"
    stream_id = f"agent-{agent_type}-{session_id}"
    app_id = f"APEX-COMP-{uuid4().hex[:6]}"

    events = [
        {
            "event_type": "AgentSessionStarted",
            "event_version": 1,
            "payload": {
                "session_id": session_id,
                "agent_type": agent_type,
                "agent_id": "fraud-agent-01",
                "application_id": app_id,
                "model_version": "gpt-4o",
                "langgraph_graph_version": "0.2.0",
                "context_source": "fresh",
                "context_token_count": 1000,
                "started_at": datetime.now(UTC).isoformat(),
            },
        },
        {
            "event_type": "AgentSessionCompleted",
            "event_version": 1,
            "payload": {
                "session_id": session_id,
                "agent_type": agent_type,
                "application_id": app_id,
                "total_nodes_executed": 5,
                "total_llm_calls": 1,
                "total_tokens_used": 5000,
                "total_cost_usd": 0.05,
                "total_duration_ms": 3000,
                "next_agent_triggered": "compliance",
                "completed_at": datetime.now(UTC).isoformat(),
            },
        },
    ]

    await store.append(stream_id, events, -1)

    ctx = await reconstruct_agent_context(
        store=store,
        agent_id="fraud-agent-01",
        session_id=session_id,
        token_budget=8000,
    )

    assert ctx.session_health_status == "COMPLETED"
    assert ctx.needs_reconciliation is False

    print(f"✅ Completed session correctly identified")

    await store.close()
