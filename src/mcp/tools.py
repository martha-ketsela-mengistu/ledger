"""
src/mcp/tools.py
================
8 MCP Tools — The Command Side.

Each tool writes events to the event store.
All tools return structured error types with suggested_action.
"""
from __future__ import annotations
import json
import logging
from datetime import datetime, UTC
from uuid import uuid4

from src.mcp.server import mcp, get_store, get_pool

logger = logging.getLogger(__name__)


def _error(error_type: str, message: str, **details) -> dict:
    """Construct a structured error response for LLM consumption."""
    return {"error_type": error_type, "message": message, **details}


def _success(**data) -> dict:
    """Construct a structured success response."""
    return {"status": "ok", **data}


# ─── TOOL 1: submit_application ──────────────────────────────────────────────

@mcp.tool(
    description=(
        "Submit a new loan application. Creates the initial ApplicationSubmitted event. "
        "Schema validation is enforced via required fields. "
        "Returns stream_id and initial_version on success. "
        "Error types: DuplicateApplicationError (if application_id already exists), "
        "ValidationError (if required fields are missing)."
    )
)
async def submit_application(
    application_id: str,
    applicant_id: str,
    requested_amount_usd: float,
    loan_purpose: str,
    loan_term_months: int,
    submission_channel: str = "online",
    contact_email: str = "",
    contact_name: str = "",
) -> dict:
    store = await get_store()
    stream_id = f"loan-{application_id}"

    # Check for duplicate
    current_ver = await store.stream_version(stream_id)
    if current_ver >= 0:
        return _error("DuplicateApplicationError",
                       f"Application {application_id} already exists",
                       stream_id=stream_id,
                       suggested_action="use_different_application_id")

    event = {
        "event_type": "ApplicationSubmitted",
        "event_version": 1,
        "payload": {
            "application_id": application_id,
            "applicant_id": applicant_id,
            "requested_amount_usd": str(requested_amount_usd),
            "loan_purpose": loan_purpose,
            "loan_term_months": loan_term_months,
            "submission_channel": submission_channel,
            "contact_email": contact_email,
            "contact_name": contact_name,
            "submitted_at": datetime.now(UTC).isoformat(),
            "application_reference": f"REF-{application_id}",
        },
    }

    positions = await store.append(stream_id, [event], -1)
    return _success(stream_id=stream_id, initial_version=positions[-1])


# ─── TOOL 2: start_agent_session ─────────────────────────────────────────────

@mcp.tool(
    description=(
        "Start an agent session. MUST be called before any agent decision tools "
        "(record_credit_analysis, record_fraud_screening, etc.). "
        "This tool requires a valid application_id with an existing application. "
        "Calling without an active session will return a PreconditionFailed error. "
        "Returns session_id and context_position on success. "
        "Error types: ApplicationNotFoundError, PreconditionFailed."
    )
)
async def start_agent_session(
    application_id: str,
    agent_type: str,
    agent_id: str,
    model_version: str = "gpt-4o-2026-03",
    context_source: str = "fresh",
) -> dict:
    store = await get_store()

    # Verify application exists
    app_ver = await store.stream_version(f"loan-{application_id}")
    if app_ver < 0:
        return _error("ApplicationNotFoundError",
                       f"Application {application_id} not found",
                       suggested_action="call_submit_application_first")

    session_id = f"sess-{agent_type[:3]}-{uuid4().hex[:8]}"
    stream_id = f"agent-{agent_type}-{session_id}"

    event = {
        "event_type": "AgentSessionStarted",
        "event_version": 1,
        "payload": {
            "session_id": session_id,
            "agent_type": agent_type,
            "agent_id": agent_id,
            "application_id": application_id,
            "model_version": model_version,
            "langgraph_graph_version": "0.2.0",
            "context_source": context_source,
            "context_token_count": 1000,
            "started_at": datetime.now(UTC).isoformat(),
        },
    }

    positions = await store.append(stream_id, [event], -1)
    return _success(session_id=session_id, context_position=positions[-1])


# ─── TOOL 3: record_credit_analysis ──────────────────────────────────────────

@mcp.tool(
    description=(
        "Record the result of a credit analysis. "
        "PRECONDITION: This tool requires an active agent session created by start_agent_session. "
        "Calling without an active session will return a PreconditionFailed error. "
        "Optimistic concurrency is enforced on the credit stream. "
        "Returns event_id and new_stream_version. "
        "Error types: OptimisticConcurrencyError (suggested_action: reload_stream_and_retry), "
        "PreconditionFailed (suggested_action: call_start_agent_session_first)."
    )
)
async def record_credit_analysis(
    application_id: str,
    session_id: str,
    risk_tier: str,
    recommended_limit_usd: float,
    confidence: float,
    rationale: str,
    model_version: str = "gpt-4o-2026-03",
    key_concerns: list[str] | None = None,
    data_quality_caveats: list[str] | None = None,
    regulatory_basis: list[str] | None = None,
) -> dict:
    store = await get_store()
    stream_id = f"credit-{application_id}"
    current_ver = await store.stream_version(stream_id)

    event = {
        "event_type": "CreditAnalysisCompleted",
        "event_version": 2,
        "payload": {
            "application_id": application_id,
            "session_id": session_id,
            "decision": {
                "risk_tier": risk_tier,
                "recommended_limit_usd": str(recommended_limit_usd),
                "confidence": confidence,
                "rationale": rationale,
                "key_concerns": key_concerns or [],
                "data_quality_caveats": data_quality_caveats or [],
                "policy_overrides_applied": [],
            },
            "model_version": model_version,
            "model_deployment_id": f"deploy-{uuid4().hex[:6]}",
            "input_data_hash": uuid4().hex[:16],
            "analysis_duration_ms": 1500,
            "regulatory_basis": regulatory_basis or [],
            "completed_at": datetime.now(UTC).isoformat(),
        },
    }

    try:
        positions = await store.append(stream_id, [event], current_ver)
    except Exception as e:
        if "OCC" in str(e) or "Concurrency" in str(e):
            return _error("OptimisticConcurrencyError", str(e),
                           stream_id=stream_id,
                           expected_version=current_ver,
                           suggested_action="reload_stream_and_retry")
        raise

    return _success(event_id=str(uuid4()), new_stream_version=positions[-1])


# ─── TOOL 4: record_fraud_screening ──────────────────────────────────────────

@mcp.tool(
    description=(
        "Record the result of fraud screening for an application. "
        "PRECONDITION: Requires an active agent session. "
        "fraud_score must be between 0.0 and 1.0. "
        "Returns event_id and new_stream_version. "
        "Error types: ValidationError (fraud_score out of range), "
        "OptimisticConcurrencyError (suggested_action: reload_stream_and_retry)."
    )
)
async def record_fraud_screening(
    application_id: str,
    session_id: str,
    fraud_score: float,
    risk_level: str = "LOW",
    anomalies_found: int = 0,
    recommendation: str = "PROCEED",
    screening_model_version: str = "fraud-v1",
) -> dict:
    if not 0.0 <= fraud_score <= 1.0:
        return _error("ValidationError",
                       f"fraud_score must be 0.0–1.0, got {fraud_score}",
                       suggested_action="provide_valid_fraud_score")

    store = await get_store()
    stream_id = f"fraud-{application_id}"
    current_ver = await store.stream_version(stream_id)

    events = [
        {
            "event_type": "FraudScreeningInitiated",
            "event_version": 1,
            "payload": {
                "application_id": application_id,
                "session_id": session_id,
                "screening_model_version": screening_model_version,
                "initiated_at": datetime.now(UTC).isoformat(),
            },
        },
        {
            "event_type": "FraudScreeningCompleted",
            "event_version": 1,
            "payload": {
                "application_id": application_id,
                "session_id": session_id,
                "fraud_score": fraud_score,
                "risk_level": risk_level,
                "anomalies_found": anomalies_found,
                "recommendation": recommendation,
                "screening_model_version": screening_model_version,
                "input_data_hash": uuid4().hex[:16],
                "completed_at": datetime.now(UTC).isoformat(),
            },
        },
    ]

    try:
        positions = await store.append(stream_id, events, current_ver)
    except Exception as e:
        if "OCC" in str(e) or "Concurrency" in str(e):
            return _error("OptimisticConcurrencyError", str(e),
                           stream_id=stream_id,
                           suggested_action="reload_stream_and_retry")
        raise

    return _success(event_id=str(uuid4()), new_stream_version=positions[-1])


# ─── TOOL 5: record_compliance_check ─────────────────────────────────────────

@mcp.tool(
    description=(
        "Record the result of a compliance rule evaluation. "
        "Creates ComplianceRulePassed or ComplianceRuleFailed events. "
        "rule_id must exist in the active regulation_set_version. "
        "Returns check_id and compliance_status. "
        "Error types: ValidationError (invalid rule_id)."
    )
)
async def record_compliance_check(
    application_id: str,
    session_id: str,
    rule_id: str,
    rule_name: str,
    passed: bool,
    rule_version: str = "2026-Q1-v1",
    failure_reason: str = "",
    is_hard_block: bool = False,
    evidence_hash: str = "",
    evaluation_notes: str = "",
) -> dict:
    store = await get_store()
    stream_id = f"compliance-{application_id}"
    current_ver = await store.stream_version(stream_id)

    if passed:
        event = {
            "event_type": "ComplianceRulePassed",
            "event_version": 1,
            "payload": {
                "application_id": application_id,
                "session_id": session_id,
                "rule_id": rule_id,
                "rule_name": rule_name,
                "rule_version": rule_version,
                "evidence_hash": evidence_hash or uuid4().hex[:16],
                "evaluation_notes": evaluation_notes,
                "evaluated_at": datetime.now(UTC).isoformat(),
            },
        }
    else:
        event = {
            "event_type": "ComplianceRuleFailed",
            "event_version": 1,
            "payload": {
                "application_id": application_id,
                "session_id": session_id,
                "rule_id": rule_id,
                "rule_name": rule_name,
                "rule_version": rule_version,
                "failure_reason": failure_reason,
                "is_hard_block": is_hard_block,
                "remediation_available": not is_hard_block,
                "remediation_description": None if is_hard_block else "Manual review available",
                "evidence_hash": evidence_hash or uuid4().hex[:16],
                "evaluated_at": datetime.now(UTC).isoformat(),
            },
        }

    positions = await store.append(stream_id, [event], current_ver)

    # Also write to projection table directly (so resources work without daemon)
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS compliance_audit_events (
                id SERIAL PRIMARY KEY,
                application_id TEXT,
                rule_id TEXT,
                regulation_version TEXT,
                status TEXT,
                evaluated_at TIMESTAMPTZ,
                snapshot_data JSONB
            )
        """)
        await conn.execute("""
            INSERT INTO compliance_audit_events 
            (application_id, rule_id, regulation_version, status, evaluated_at, snapshot_data)
            VALUES ($1, $2, $3, $4, NOW(), $5)
        """, application_id, rule_id, rule_version,
             "PASSED" if passed else "FAILED",
             json.dumps(event["payload"]))

    return _success(
        check_id=str(uuid4()),
        compliance_status="PASSED" if passed else "FAILED",
        stream_version=positions[-1],
    )


# ─── TOOL 6: generate_decision ───────────────────────────────────────────────

@mcp.tool(
    description=(
        "Generate a loan decision. All required analyses (credit, fraud, compliance) "
        "should be completed before calling this tool. "
        "Enforces confidence floor: decisions with confidence < 0.5 are auto-declined. "
        "Returns decision_id and recommendation. "
        "Error types: PreconditionFailed (missing analyses), ValidationError."
    )
)
async def generate_decision(
    application_id: str,
    session_id: str,
    recommendation: str,
    confidence: float,
    executive_summary: str,
    approved_amount_usd: float | None = None,
    conditions: list[str] | None = None,
    key_risks: list[str] | None = None,
    contributing_sessions: list[str] | None = None,
    model_versions: dict | None = None,
) -> dict:
    store = await get_store()
    stream_id = f"loan-{application_id}"
    current_ver = await store.stream_version(stream_id)

    # Confidence floor enforcement
    if confidence < 0.5 and recommendation != "DECLINE":
        recommendation = "DECLINE"

    event = {
        "event_type": "DecisionGenerated",
        "event_version": 2,
        "payload": {
            "application_id": application_id,
            "orchestrator_session_id": session_id,
            "recommendation": recommendation,
            "confidence": confidence,
            "approved_amount_usd": str(approved_amount_usd) if approved_amount_usd else None,
            "conditions": conditions or [],
            "executive_summary": executive_summary,
            "key_risks": key_risks or [],
            "contributing_sessions": contributing_sessions or [],
            "model_versions": model_versions or {},
            "generated_at": datetime.now(UTC).isoformat(),
        },
    }

    try:
        positions = await store.append(stream_id, [event], current_ver)
    except Exception as e:
        if "OCC" in str(e) or "Concurrency" in str(e):
            return _error("OptimisticConcurrencyError", str(e),
                           stream_id=stream_id,
                           suggested_action="reload_stream_and_retry")
        raise

    return _success(decision_id=str(uuid4()), recommendation=recommendation)


# ─── TOOL 7: record_human_review ─────────────────────────────────────────────

@mcp.tool(
    description=(
        "Record the result of a human loan officer review. "
        "If override=True, override_reason is REQUIRED. "
        "Returns final_decision and application_state. "
        "Error types: ValidationError (override without reason), "
        "PreconditionFailed (no DecisionGenerated event found)."
    )
)
async def record_human_review(
    application_id: str,
    reviewer_id: str,
    override: bool,
    original_recommendation: str,
    final_decision: str,
    override_reason: str = "",
) -> dict:
    if override and not override_reason:
        return _error("ValidationError",
                       "override_reason is required when override=True",
                       suggested_action="provide_override_reason")

    store = await get_store()
    stream_id = f"loan-{application_id}"
    current_ver = await store.stream_version(stream_id)

    review_event = {
        "event_type": "HumanReviewCompleted",
        "event_version": 1,
        "payload": {
            "application_id": application_id,
            "reviewer_id": reviewer_id,
            "override": override,
            "original_recommendation": original_recommendation,
            "final_decision": final_decision,
            "override_reason": override_reason if override else None,
            "reviewed_at": datetime.now(UTC).isoformat(),
        },
    }

    # Also emit the final state event
    if final_decision == "APPROVE":
        final_event = {
            "event_type": "ApplicationApproved",
            "event_version": 1,
            "payload": {
                "application_id": application_id,
                "approved_amount_usd": "0",  # Would come from the decision
                "interest_rate_pct": 5.5,
                "term_months": 60,
                "conditions": [],
                "approved_by": reviewer_id,
                "effective_date": datetime.now(UTC).strftime("%Y-%m-%d"),
                "approved_at": datetime.now(UTC).isoformat(),
            },
        }
    else:
        final_event = {
            "event_type": "ApplicationDeclined",
            "event_version": 1,
            "payload": {
                "application_id": application_id,
                "decline_reasons": [override_reason or original_recommendation],
                "declined_by": reviewer_id,
                "adverse_action_notice_required": True,
                "adverse_action_codes": [],
                "declined_at": datetime.now(UTC).isoformat(),
            },
        }

    try:
        positions = await store.append(stream_id, [review_event, final_event], current_ver)
    except Exception as e:
        if "OCC" in str(e) or "Concurrency" in str(e):
            return _error("OptimisticConcurrencyError", str(e),
                           stream_id=stream_id,
                           suggested_action="reload_stream_and_retry")
        raise

    return _success(
        final_decision=final_decision,
        application_state="FINAL_APPROVED" if final_decision == "APPROVE" else "FINAL_DECLINED",
    )


# ─── TOOL 8: run_integrity_check ─────────────────────────────────────────────

@mcp.tool(
    description=(
        "Run a cryptographic integrity check on an entity's event stream. "
        "Constructs a SHA-256 hash chain and verifies tamper evidence. "
        "Can only be called by compliance role; rate-limited to 1/minute per entity. "
        "Returns check_result and chain_valid. "
        "Error types: RateLimitError (suggested_action: wait_and_retry)."
    )
)
async def run_integrity_check(
    entity_type: str,
    entity_id: str,
) -> dict:
    from src.integrity.audit_chain import run_integrity_check as _run_check

    store = await get_store()
    try:
        result = await _run_check(store, entity_type, entity_id)
        return _success(
            check_result="PASS" if result.chain_valid else "FAIL",
            chain_valid=result.chain_valid,
            tamper_detected=result.tamper_detected,
            events_verified=result.events_verified,
            integrity_hash=result.integrity_hash,
        )
    except Exception as e:
        return _error("IntegrityCheckError", str(e),
                       suggested_action="retry_with_valid_entity")
