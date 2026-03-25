"""
tests/test_upcasting.py
=======================
The Immutability Test (MANDATORY per spec):
  1. Directly query the events table to get the raw stored payload of a v1 event
  2. Load the same event through EventStore.load_stream() and verify it is upcasted to v2
  3. Directly query the events table again and verify the raw stored payload is UNCHANGED

Also tests the upcaster chain for CreditAnalysisCompleted and DecisionGenerated.
"""
import pytest
import json
import asyncpg
from uuid import uuid4
from datetime import datetime, UTC

DB_URL = "postgresql://postgres:apex@localhost/apex_ledger"


@pytest.mark.asyncio
async def test_immutability_credit_analysis_v1_to_v2():
    """
    THE IMMUTABILITY TEST:
    Verifies upcasting transforms events on READ but NEVER modifies stored data.
    """
    from src.event_store import EventStore
    from src.upcasting.upcasters import upcaster_registry

    # Connect a store WITH the upcaster
    store = EventStore(DB_URL, upcaster_registry=upcaster_registry)
    await store.connect()

    # Also connect a raw pool for direct DB queries
    raw_pool = await asyncpg.create_pool(DB_URL)

    app_id = f"APEX-UPC-{uuid4().hex[:6]}"
    stream_id = f"credit-{app_id}"

    # 1. Insert a v1 CreditAnalysisCompleted event (no model_version, no confidence_score)
    v1_payload = {
        "application_id": app_id,
        "session_id": "sess-cred-test01",
        "decision": {
            "risk_tier": "MEDIUM",
            "recommended_limit_usd": "500000",
            "confidence": 0.85,
            "rationale": "Stable revenue",
            "key_concerns": [],
            "data_quality_caveats": [],
            "policy_overrides_applied": [],
        },
        "model_deployment_id": "deploy-001",
        "input_data_hash": "abc123",
        "analysis_duration_ms": 1500,
        "completed_at": datetime.now(UTC).isoformat(),
    }

    v1_event = {
        "event_type": "CreditAnalysisCompleted",
        "event_version": 1,  # Deliberately v1
        "payload": v1_payload,
    }

    # Use a store WITHOUT upcasters for insertion, to store it as v1
    raw_store = EventStore(DB_URL)
    await raw_store.connect()
    await raw_store.append(stream_id, [v1_event], -1)

    # STEP 1: Directly query the events table to get raw stored payload
    async with raw_pool.acquire() as conn:
        raw_row = await conn.fetchrow(
            "SELECT event_version, payload FROM events "
            "WHERE stream_id = $1 ORDER BY stream_position DESC LIMIT 1",
            stream_id,
        )

    raw_version = raw_row["event_version"]
    raw_payload = json.loads(raw_row["payload"]) if isinstance(raw_row["payload"], str) else dict(raw_row["payload"])

    assert raw_version == 1, f"Raw event should be v1, got v{raw_version}"
    assert "model_version" not in raw_payload, "Raw v1 payload should NOT have model_version"
    assert "confidence_score" not in raw_payload, "Raw v1 payload should NOT have confidence_score"
    assert "regulatory_basis" not in raw_payload, "Raw v1 payload should NOT have regulatory_basis"

    # STEP 2: Load through EventStore.load_stream() WITH upcaster — should be v2
    events = await store.load_stream(stream_id)
    assert len(events) >= 1
    upcasted = events[-1]

    assert upcasted.event_version == 2, f"Upcasted event should be v2, got v{upcasted.event_version}"
    assert upcasted.payload.get("model_version") == "legacy-pre-2026"
    assert upcasted.payload.get("confidence_score") is None  # Genuinely unknown
    assert "regulatory_basis" in upcasted.payload

    # STEP 3: Re-query the raw events table — payload MUST be unchanged
    async with raw_pool.acquire() as conn:
        raw_row_after = await conn.fetchrow(
            "SELECT event_version, payload FROM events "
            "WHERE stream_id = $1 ORDER BY stream_position DESC LIMIT 1",
            stream_id,
        )

    raw_version_after = raw_row_after["event_version"]
    raw_payload_after = json.loads(raw_row_after["payload"]) if isinstance(raw_row_after["payload"], str) else dict(raw_row_after["payload"])

    assert raw_version_after == 1, "Raw event version MUST remain v1 after upcasting read!"
    assert raw_payload == raw_payload_after, (
        "IMMUTABILITY VIOLATION: Raw stored payload was modified by upcasting! "
        f"Before: {raw_payload}, After: {raw_payload_after}"
    )

    print("✅ IMMUTABILITY TEST PASSED: Upcasting transforms on read, stored data unchanged.")

    await raw_pool.close()
    await store.close()
    await raw_store.close()


@pytest.mark.asyncio
async def test_immutability_decision_generated_v1_to_v2():
    """
    Immutability test for DecisionGenerated v1→v2.
    """
    from src.event_store import EventStore
    from src.upcasting.upcasters import upcaster_registry

    store = EventStore(DB_URL, upcaster_registry=upcaster_registry)
    await store.connect()
    raw_pool = await asyncpg.create_pool(DB_URL)

    app_id = f"APEX-UPD-{uuid4().hex[:6]}"
    stream_id = f"loan-{app_id}"

    v1_payload = {
        "application_id": app_id,
        "orchestrator_session_id": "sess-orch-test01",
        "recommendation": "APPROVE",
        "confidence": 0.92,
        "approved_amount_usd": "750000",
        "conditions": ["Annual review"],
        "executive_summary": "Strong applicant",
        "key_risks": ["Market volatility"],
        "contributing_sessions": [],
        "generated_at": datetime.now(UTC).isoformat(),
    }

    v1_event = {
        "event_type": "DecisionGenerated",
        "event_version": 1,
        "payload": v1_payload,
    }

    raw_store = EventStore(DB_URL)
    await raw_store.connect()
    await raw_store.append(stream_id, [v1_event], -1)

    # Direct DB query — raw v1
    async with raw_pool.acquire() as conn:
        raw_row = await conn.fetchrow(
            "SELECT event_version, payload FROM events "
            "WHERE stream_id = $1 AND event_type = 'DecisionGenerated' "
            "ORDER BY stream_position DESC LIMIT 1",
            stream_id,
        )
    raw_payload = json.loads(raw_row["payload"]) if isinstance(raw_row["payload"], str) else dict(raw_row["payload"])
    assert raw_row["event_version"] == 1
    assert "model_versions" not in raw_payload

    # Load via store — should be upcasted
    events = await store.load_stream(stream_id)
    decision_events = [e for e in events if e.event_type == "DecisionGenerated"]
    assert len(decision_events) >= 1
    upcasted = decision_events[-1]
    assert upcasted.event_version == 2
    assert "model_versions" in upcasted.payload

    # Re-check raw — MUST be unchanged
    async with raw_pool.acquire() as conn:
        raw_row_after = await conn.fetchrow(
            "SELECT event_version, payload FROM events "
            "WHERE stream_id = $1 AND event_type = 'DecisionGenerated' "
            "ORDER BY stream_position DESC LIMIT 1",
            stream_id,
        )
    assert raw_row_after["event_version"] == 1, "IMMUTABILITY VIOLATION: raw version changed!"
    raw_payload_after = json.loads(raw_row_after["payload"]) if isinstance(raw_row_after["payload"], str) else dict(raw_row_after["payload"])
    assert raw_payload == raw_payload_after, "IMMUTABILITY VIOLATION: raw payload changed!"

    print("✅ DecisionGenerated IMMUTABILITY TEST PASSED")

    await raw_pool.close()
    await store.close()
    await raw_store.close()


@pytest.mark.asyncio
async def test_upcaster_chain_idempotent_on_v2():
    """Verify that loading a v2 event does NOT re-apply upcasters."""
    from src.event_store import EventStore
    from src.upcasting.upcasters import upcaster_registry

    store = EventStore(DB_URL, upcaster_registry=upcaster_registry)
    await store.connect()

    app_id = f"APEX-V2-{uuid4().hex[:6]}"
    stream_id = f"credit-{app_id}"

    # Insert a PROPER v2 event
    v2_payload = {
        "application_id": app_id,
        "session_id": "sess-cred-v2",
        "decision": {
            "risk_tier": "LOW",
            "recommended_limit_usd": "1000000",
            "confidence": 0.95,
            "rationale": "Excellent financials",
            "key_concerns": [],
            "data_quality_caveats": [],
            "policy_overrides_applied": [],
        },
        "model_version": "gpt-4o-2026-03",
        "model_deployment_id": "deploy-v2",
        "input_data_hash": "xyz789",
        "analysis_duration_ms": 800,
        "confidence_score": 0.95,
        "regulatory_basis": ["REG-001", "REG-002"],
        "completed_at": datetime.now(UTC).isoformat(),
    }

    v2_event = {
        "event_type": "CreditAnalysisCompleted",
        "event_version": 2,
        "payload": v2_payload,
    }

    await store.append(stream_id, [v2_event], -1)
    events = await store.load_stream(stream_id)
    loaded = events[-1]

    assert loaded.event_version == 2
    assert loaded.payload["model_version"] == "gpt-4o-2026-03"  # NOT overwritten to legacy
    assert loaded.payload["confidence_score"] == 0.95  # NOT set to None

    print("✅ V2 events pass through upcaster chain unchanged")
    await store.close()
