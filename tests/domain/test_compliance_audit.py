import pytest
import sys
from pathlib import Path
from datetime import datetime, UTC
from decimal import Decimal

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.aggregates.compliance_record import ComplianceRecordAggregate
from src.aggregates.audit_ledger import AuditLedgerAggregate
from src.aggregates.document_package import DocumentPackageAggregate
from src.aggregates.credit_record import CreditRecordAggregate
from src.aggregates.fraud_screening import FraudScreeningAggregate
from src.aggregates.loan_application import LoanApplicationAggregate, ApplicationState
from src.models.events import ComplianceVerdict, StoredEvent, DomainError

def _sev(event_type, stream_position=1, **payload):
    """Helper to create a StoredEvent directly."""
    from uuid import uuid4
    return StoredEvent(
        event_id=uuid4(),
        stream_id="test",
        stream_position=stream_position,
        global_position=100 + stream_position,
        event_type=event_type,
        event_version=1,
        payload=payload,
        metadata={},
        recorded_at=datetime.now(UTC)
    )

@pytest.mark.asyncio
async def test_compliance_dependency_rule():
    agg = ComplianceRecordAggregate("app-001")
    
    # Not cleared yet
    assert agg.is_fully_cleared() is False
    
    # Rule passed
    agg.apply(_sev("ComplianceRulePassed", rule_id="KYC-01"))
    assert agg.is_fully_cleared() is False # Still need overall verdict
    
    # Completed with CLEAR
    agg.apply(_sev("ComplianceCheckCompleted", overall_verdict="CLEAR", rules_passed=1))
    assert agg.is_fully_cleared() is True
    
    # Hard block added later (or in update)
    agg.apply(_sev("ComplianceRuleFailed", rule_id="AML-01", is_hard_block=True))
    assert agg.is_fully_cleared() is False

@pytest.mark.asyncio
async def test_audit_causal_chain_logic():
    agg = AuditLedgerAggregate("app-001")
    
    # First check
    sessions = ["sess-1", "sess-2"]
    event_ids = ["evt-1"]
    h1 = agg.compute_next_hash(sessions + event_ids)
    
    agg.apply(_sev("AuditIntegrityCheckRun", integrity_hash=h1, events_verified_count=3))
    assert agg.last_hash == h1
    
    # Second check building on first
    sessions2 = ["sess-3"]
    h2 = agg.compute_next_hash(sessions2)
    assert h2 != h1
    
    agg.apply(_sev("AuditIntegrityCheckRun", integrity_hash=h2, previous_hash=h1))
    assert agg.last_hash == h2

@pytest.mark.asyncio
async def test_loan_application_causal_chain_validation():
    agg = LoanApplicationAggregate("app-001")
    
    # Record a contribution
    agg.apply(_sev("CreditAnalysisCompleted", session_id="sess-credit-01"))
    
    # Valid contribution
    agg.validate_causal_chain(["sess-credit-01"])
    
    # Invalid contribution
    with pytest.raises(DomainError, match="Causal chain violation"):
        agg.validate_causal_chain(["sess-rogue-01"])

@pytest.mark.asyncio
async def test_document_package_ready_check():
    from src.models.events import DocumentType
    agg = DocumentPackageAggregate("app-001")
    
    required = [DocumentType.INCOME_STATEMENT, DocumentType.BALANCE_SHEET]
    assert agg.is_extraction_complete(required) is False
    
    agg.apply(_sev("ExtractionCompleted", document_type=DocumentType.INCOME_STATEMENT))
    assert agg.is_extraction_complete(required) is False
    
    agg.apply(_sev("ExtractionCompleted", document_type=DocumentType.BALANCE_SHEET))
    assert agg.is_extraction_complete(required) is True

@pytest.mark.asyncio
async def test_fraud_screening_cleared_check():
    agg = FraudScreeningAggregate("app-001")
    assert agg.is_cleared is True
    
    # High risk score
    agg.apply(_sev("FraudScreeningCompleted", fraud_score=0.9, risk_level="HIGH"))
    assert agg.is_cleared is False
    
    # Low risk score resets it (if that's the logic)
    agg.apply(_sev("FraudScreeningCompleted", fraud_score=0.1, risk_level="LOW"))
    assert agg.is_cleared is True

@pytest.mark.asyncio
async def test_credit_record_reconstruction():
    from src.models.events import CreditDecision
    agg = CreditRecordAggregate("app-001")
    
    decision = {
        "risk_tier": "LOW",
        "recommended_limit_usd": 50000,
        "confidence": 0.95,
        "rationale": "Strong financials"
    }
    agg.apply(_sev("CreditAnalysisCompleted", decision=decision, model_version="v1"))
    
    assert agg.model_version == "v1"
    assert agg.decision.risk_tier == "LOW"
    assert agg.decision.recommended_limit_usd == 50000
