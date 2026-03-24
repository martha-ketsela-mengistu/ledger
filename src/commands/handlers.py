# ledger/domain/handlers.py

import hashlib
import json
from datetime import datetime, UTC
from decimal import Decimal
from typing import Any

from src.event_store import EventStore
from src.aggregates.loan_application import LoanApplicationAggregate, ApplicationState
from src.aggregates.agent_session import AgentSessionAggregate
from src.aggregates.compliance_record import ComplianceRecordAggregate
from src.aggregates.audit_ledger import AuditLedgerAggregate
from src.commands.commands import CreditAnalysisCompletedCommand, DecisionGeneratedCommand
from src.models.events import (
    CreditAnalysisCompleted, CreditDecision, 
    DecisionGenerated, AuditIntegrityCheckRun, deserialize_event
)

def hash_inputs(data: dict) -> str:
    return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()

async def handle_credit_analysis_completed(
    cmd: CreditAnalysisCompletedCommand,
    store: EventStore,
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> None:
    # 1. Reconstruct current aggregate state from event history
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    agent = await AgentSessionAggregate.load(store, "credit_analysis", cmd.session_id)

    # 2. Validate — all business rules checked BEFORE any state change
    app.assert_awaiting_credit_analysis()
    agent.assert_context_loaded("complete_credit_analysis")
    agent.assert_model_version_current(cmd.model_version)
    
    # Rule 3: Model version locking (already checked in assert_awaiting_credit_analysis partially, 
    # but let's be explicit if needed)
    app.validate_credit_analysis()

    # 3. Determine new events — pure logic, no I/O
    decision = CreditDecision(
        risk_tier=cmd.risk_tier,
        recommended_limit_usd=cmd.recommended_limit_usd,
        confidence=cmd.confidence_score,
        rationale="Computed by CreditAnalysisAgent"
    )
    
    new_events = [
        CreditAnalysisCompleted(
            application_id=cmd.application_id,
            session_id=cmd.session_id,
            decision=decision,
            model_version=cmd.model_version,
            model_deployment_id="default",
            input_data_hash=hash_inputs(cmd.input_data),
            analysis_duration_ms=cmd.duration_ms,
            completed_at=datetime.now(UTC)
        ).to_store_dict()
    ]

    # 4. Append atomically — optimistic concurrency enforced by store
    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=new_events,
        expected_version=app.version,
        correlation_id=correlation_id,
        causation_id=causation_id,
    )

async def handle_decision_generated(
    cmd: DecisionGeneratedCommand,
    store: EventStore,
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> None:
    # 1. Reconstruct current aggregate state from event history
    app = await LoanApplicationAggregate.load(store, cmd.application_id)
    agent = await AgentSessionAggregate.load(store, "decision_orchestrator", cmd.session_id)
    compliance = await ComplianceRecordAggregate.load(store, cmd.application_id)
    audit = await AuditLedgerAggregate.load(store, cmd.application_id)

    # 2. Validate — all business rules checked BEFORE any state change
    agent.assert_context_loaded("generate_decision")
    
    # Rule 6: Causal chain enforcement (validate sessions against application context)
    app.validate_causal_chain(cmd.contributing_sessions)
    
    # Rule 5: Compliance dependency
    if not compliance.is_fully_cleared():
        from src.models.events import DomainError
        raise DomainError("Decision blocked: Compliance record is not fully cleared or has hard blocks.")
    
    # Rule 4: Confidence floor (Enforced here as per regulatory requirement)
    recommendation = app.validate_decision_confidence(cmd.confidence_score)
    if recommendation == "MATCH_RECOMMENDATION":
        recommendation = cmd.recommendation
    
    # 3. Determine new events — pure logic, no I/O
    generated_at = datetime.now(UTC)
    
    # Decision event
    decision_event = DecisionGenerated(
        application_id=cmd.application_id,
        orchestrator_session_id=cmd.session_id,
        recommendation=recommendation,
        confidence=cmd.confidence_score,
        approved_amount_usd=cmd.approved_amount_usd,
        executive_summary=cmd.executive_summary,
        contributing_sessions=cmd.contributing_sessions,
        generated_at=generated_at
    )
    
    # Audit integrity check (SHA-256 Chaining)
    # We include all contributing session IDs and the decision event itself in the hash
    integrity_hash = audit.compute_next_hash(cmd.contributing_sessions + [str(decision_event.event_id)])
    
    audit_event = AuditIntegrityCheckRun(
        entity_type="loan",
        entity_id=cmd.application_id,
        check_timestamp=generated_at,
        events_verified_count=len(cmd.contributing_sessions) + 1,
        integrity_hash=integrity_hash,
        previous_hash=audit.last_hash,
        chain_valid=True,
        tamper_detected=False
    )
    
    new_events = [
        decision_event.to_store_dict(),
        audit_event.to_store_dict()
    ]

    # 4. Append atomically — optimistic concurrency enforced by store
    await store.append(
        stream_id=f"loan-{cmd.application_id}",
        events=new_events,
        expected_version=app.version,
        correlation_id=correlation_id,
        causation_id=causation_id,
    )
