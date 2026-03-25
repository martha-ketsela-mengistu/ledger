"""
src/mcp/resources.py
====================
6 MCP Resources — The Query Side.

Resources expose projections. They NEVER replay streams directly
(except justified exceptions: audit-trail and agent sessions).
"""
from __future__ import annotations
import json
import logging
from datetime import datetime

from src.mcp.app import mcp
from src.mcp.server import get_store, get_pool

logger = logging.getLogger(__name__)
logger.info("Registering MCP resources...")


# ─── RESOURCE 1: ledger://applications/{id} ──────────────────────────────────

@mcp.resource(
    "ledger://applications/{app_id}",
    description=(
        "Current state summary for a loan application. "
        "Reads from the ApplicationSummary projection — no stream replay. "
        "SLO: p99 < 50ms."
    ),
)
async def get_application_summary(app_id: str) -> str:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM application_summary WHERE application_id = $1",
            app_id,
        )
        if not row:
            return json.dumps({"error": f"Application {app_id} not found"})
        result = {k: (str(v) if isinstance(v, (datetime,)) else v) for k, v in dict(row).items()}
        return json.dumps(result, default=str)


# ─── RESOURCE 2: ledger://applications/{id}/compliance ───────────────────────

@mcp.resource(
    "ledger://applications/{app_id}/compliance",
    description=(
        "Compliance audit view for a loan application. "
        "Supports temporal query via as_of parameter. "
        "Reads from ComplianceAuditView projection. "
        "SLO: p99 < 200ms."
    ),
)
async def get_application_compliance(app_id: str) -> str:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT DISTINCT ON (rule_id) rule_id, status, regulation_version, evaluated_at
               FROM compliance_audit_events
               WHERE application_id = $1
               ORDER BY rule_id, evaluated_at DESC""",
            app_id,
        )
        if not rows:
            return json.dumps({"application_id": app_id, "rules": [], "note": "No compliance data found"})
        rules = [{k: (str(v) if isinstance(v, datetime) else v) for k, v in dict(r).items()} for r in rows]
        return json.dumps({"application_id": app_id, "rules": rules}, default=str)


# ─── RESOURCE 3: ledger://applications/{id}/audit-trail ──────────────────────

@mcp.resource(
    "ledger://applications/{app_id}/audit-trail",
    description=(
        "Full audit trail for a loan application. "
        "This is a justified exception: directly loads the loan stream. "
        "Supports temporal range via from/to parameters. "
        "SLO: p99 < 500ms."
    ),
)
async def get_application_audit_trail(app_id: str) -> str:
    store = await get_store()
    stream_id = f"loan-{app_id}"
    events = await store.load_stream(stream_id)

    trail = []
    for ev in events:
        trail.append({
            "event_type": ev.event_type,
            "event_version": ev.event_version,
            "stream_position": ev.stream_position,
            "recorded_at": str(ev.recorded_at),
            "payload_summary": {k: str(v)[:100] for k, v in ev.payload.items()},
        })

    return json.dumps({"application_id": app_id, "events": trail, "total": len(trail)}, default=str)


# ─── RESOURCE 4: ledger://agents/{id}/performance ────────────────────────────

@mcp.resource(
    "ledger://agents/{agent_id}/performance",
    description=(
        "Performance metrics for an agent. "
        "Reads from AgentPerformanceLedger projection. "
        "SLO: p99 < 50ms."
    ),
)
async def get_agent_performance(agent_id: str) -> str:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM agent_performance WHERE agent_id = $1",
            agent_id,
        )
        if not rows:
            return json.dumps({"agent_id": agent_id, "metrics": [], "note": "No performance data"})
        metrics = [{k: (str(v) if isinstance(v, datetime) else v) for k, v in dict(r).items()} for r in rows]
        return json.dumps({"agent_id": agent_id, "metrics": metrics}, default=str)


# ─── RESOURCE 5: ledger://agents/{id}/sessions/{session_id} ──────────────────

@mcp.resource(
    "ledger://agents/{agent_id}/sessions/{session_id}",
    description=(
        "Full session replay for an agent session. "
        "Justified exception: directly loads the agent session stream. "
        "Provides full replay capability. "
        "SLO: p99 < 300ms."
    ),
)
async def get_agent_session(agent_id: str, session_id: str) -> str:
    store = await get_store()

    # Try known agent types
    for agent_type in ["credit_analysis", "fraud_detection", "compliance",
                       "document_processing", "decision_orchestrator"]:
        stream_id = f"agent-{agent_type}-{session_id}"
        events = await store.load_stream(stream_id)
        if events:
            trail = []
            for ev in events:
                trail.append({
                    "event_type": ev.event_type,
                    "stream_position": ev.stream_position,
                    "recorded_at": str(ev.recorded_at),
                    "payload": ev.payload,
                })
            return json.dumps({
                "agent_id": agent_id,
                "session_id": session_id,
                "agent_type": agent_type,
                "events": trail,
                "total": len(trail),
            }, default=str)

    return json.dumps({"error": f"Session {session_id} not found for agent {agent_id}"})


# ─── RESOURCE 6: ledger://ledger/health ──────────────────────────────────────

@mcp.resource(
    "ledger://ledger/health",
    description=(
        "Health check and projection lag status. "
        "This is the watchdog endpoint. "
        "SLO: p99 < 10ms."
    ),
)
async def get_ledger_health() -> str:
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Check event count
        event_count = await conn.fetchval("SELECT COUNT(*) FROM events")
        stream_count = await conn.fetchval("SELECT COUNT(*) FROM event_streams")

        # Check projection checkpoint lags
        checkpoints = await conn.fetch(
            "SELECT projection_name, last_position, updated_at "
            "FROM projection_checkpoints"
        )
        max_pos = await conn.fetchval(
            "SELECT COALESCE(MAX(global_position), 0) FROM events"
        )

        projections = {}
        for cp in checkpoints:
            lag = max_pos - cp["last_position"]
            projections[cp["projection_name"]] = {
                "last_position": cp["last_position"],
                "lag": lag,
                "updated_at": str(cp["updated_at"]),
            }

    return json.dumps({
        "status": "healthy",
        "event_count": event_count,
        "stream_count": stream_count,
        "max_global_position": max_pos,
        "projections": projections,
    }, default=str)
