"""
tests/test_mcp_lifecycle.py
===========================
Full loan application lifecycle driven entirely through MCP tool calls.

Flow: submit_application → start_agent_session → record_credit_analysis →
      record_fraud_screening → record_compliance_check → generate_decision →
      record_human_review → query ledger://applications/{id}/compliance

This simulates what a real AI agent would do via MCP interface.
No direct Python function calls — only MCP tool functions.
"""
import pytest
import json
from uuid import uuid4

DB_URL = "postgresql://postgres:apex@localhost/apex_ledger"


@pytest.mark.asyncio
async def test_full_loan_lifecycle_via_mcp():
    """
    MCP Integration Test: Complete loan application lifecycle.
    Drives the entire flow using only MCP tool functions.
    """
    # Import MCP tools and resources directly (simulating MCP calls)
    from src.mcp.tools import (
        submit_application,
        start_agent_session,
        record_credit_analysis,
        record_fraud_screening,
        record_compliance_check,
        generate_decision,
        record_human_review,
        run_integrity_check,
    )
    from src.mcp.resources import (
        get_application_summary,
        get_application_compliance,
        get_application_audit_trail,
        get_ledger_health,
    )

    app_id = f"APEX-MCP-{uuid4().hex[:6]}"

    # ── STEP 1: Submit Application ──
    result = await submit_application(
        application_id=app_id,
        applicant_id="COMP-010",
        requested_amount_usd=500000.0,
        loan_purpose="working_capital",
        loan_term_months=36,
        submission_channel="online",
        contact_email="test@example.com",
        contact_name="Test User",
    )
    assert result["status"] == "ok", f"Submit failed: {result}"
    assert result["stream_id"] == f"loan-{app_id}"
    print(f"✅ Step 1: Application submitted — {result}")

    # ── STEP 1b: Verify duplicate detection ──
    dup_result = await submit_application(
        application_id=app_id,
        applicant_id="COMP-010",
        requested_amount_usd=500000.0,
        loan_purpose="working_capital",
        loan_term_months=36,
    )
    assert dup_result["error_type"] == "DuplicateApplicationError"
    print(f"✅ Step 1b: Duplicate correctly rejected — {dup_result['error_type']}")

    # ── STEP 2: Start Credit Analysis Agent Session ──
    session1 = await start_agent_session(
        application_id=app_id,
        agent_type="credit_analysis",
        agent_id="credit-agent-01",
        model_version="gpt-4o-2026-03",
    )
    assert session1["status"] == "ok", f"Session start failed: {session1}"
    credit_session_id = session1["session_id"]
    print(f"✅ Step 2: Credit session started — {credit_session_id}")

    # ── STEP 3: Record Credit Analysis ──
    credit_result = await record_credit_analysis(
        application_id=app_id,
        session_id=credit_session_id,
        risk_tier="MEDIUM",
        recommended_limit_usd=450000.0,
        confidence=0.85,
        rationale="Strong revenue growth with manageable debt levels",
        model_version="gpt-4o-2026-03",
        key_concerns=["Market volatility in sector"],
        regulatory_basis=["REG-001", "REG-002"],
    )
    assert credit_result["status"] == "ok", f"Credit analysis failed: {credit_result}"
    print(f"✅ Step 3: Credit analysis recorded — v{credit_result['new_stream_version']}")

    # ── STEP 4: Start Fraud Detection Agent Session ──
    session2 = await start_agent_session(
        application_id=app_id,
        agent_type="fraud_detection",
        agent_id="fraud-agent-01",
    )
    fraud_session_id = session2["session_id"]

    # ── STEP 5: Record Fraud Screening ──
    fraud_result = await record_fraud_screening(
        application_id=app_id,
        session_id=fraud_session_id,
        fraud_score=0.12,
        risk_level="LOW",
        anomalies_found=0,
        recommendation="PROCEED",
    )
    assert fraud_result["status"] == "ok", f"Fraud screening failed: {fraud_result}"
    print(f"✅ Step 5: Fraud screening recorded — score=0.12")

    # ── STEP 5b: Validate fraud_score range ──
    bad_fraud = await record_fraud_screening(
        application_id=app_id,
        session_id=fraud_session_id,
        fraud_score=1.5,  # Invalid!
    )
    assert bad_fraud["error_type"] == "ValidationError"
    print(f"✅ Step 5b: Invalid fraud_score correctly rejected")

    # ── STEP 6: Start Compliance Agent Session ──
    session3 = await start_agent_session(
        application_id=app_id,
        agent_type="compliance",
        agent_id="compliance-agent-01",
    )
    compliance_session_id = session3["session_id"]

    # ── STEP 7: Record Compliance Checks (3 rules) ──
    for rule_id, rule_name, passed in [
        ("REG-001", "AML/KYC Verification", True),
        ("REG-002", "Debt-to-Income Ratio", True),
        ("REG-003", "Jurisdiction Check", True),
    ]:
        comp_result = await record_compliance_check(
            application_id=app_id,
            session_id=compliance_session_id,
            rule_id=rule_id,
            rule_name=rule_name,
            passed=passed,
        )
        assert comp_result["status"] == "ok"
    print(f"✅ Step 7: 3 compliance rules evaluated — all PASSED")

    # ── STEP 8: Generate Decision ──
    session4 = await start_agent_session(
        application_id=app_id,
        agent_type="decision_orchestrator",
        agent_id="orchestrator-01",
    )
    orchestrator_session = session4["session_id"]

    decision_result = await generate_decision(
        application_id=app_id,
        session_id=orchestrator_session,
        recommendation="APPROVE",
        confidence=0.88,
        executive_summary="Strong financials, clean fraud screen, full compliance",
        approved_amount_usd=450000.0,
        conditions=["Annual review required", "Quarterly financial reporting"],
        contributing_sessions=[credit_session_id, fraud_session_id, compliance_session_id],
    )
    assert decision_result["status"] == "ok"
    assert decision_result["recommendation"] == "APPROVE"
    print(f"✅ Step 8: Decision generated — {decision_result['recommendation']}")

    # ── STEP 9: Record Human Review ──
    review_result = await record_human_review(
        application_id=app_id,
        reviewer_id="LO-Sarah-Chen",
        override=False,
        original_recommendation="APPROVE",
        final_decision="APPROVE",
    )
    assert review_result["status"] == "ok"
    assert review_result["application_state"] == "FINAL_APPROVED"
    print(f"✅ Step 9: Human review completed — {review_result['application_state']}")

    # ── STEP 9b: Verify override validation ──
    bad_review = await record_human_review(
        application_id=app_id,
        reviewer_id="LO-Test",
        override=True,
        original_recommendation="APPROVE",
        final_decision="DECLINE",
        override_reason="",  # Empty — should fail
    )
    assert bad_review["error_type"] == "ValidationError"
    print(f"✅ Step 9b: Override without reason correctly rejected")

    # ── STEP 10: Query Compliance Resource ──
    compliance_data = await get_application_compliance(app_id)
    compliance = json.loads(compliance_data)
    assert compliance["application_id"] == app_id
    assert len(compliance["rules"]) == 3
    assert all(r["status"] == "PASSED" for r in compliance["rules"])
    print(f"✅ Step 10: Compliance resource verified — {len(compliance['rules'])} rules")

    # ── STEP 11: Query Audit Trail Resource ──
    trail_data = await get_application_audit_trail(app_id)
    trail = json.loads(trail_data)
    assert trail["total"] >= 4  # Submit + Decision + Review + Final
    event_types = [e["event_type"] for e in trail["events"]]
    assert "ApplicationSubmitted" in event_types
    assert "DecisionGenerated" in event_types
    assert "HumanReviewCompleted" in event_types
    assert "ApplicationApproved" in event_types
    print(f"✅ Step 11: Audit trail verified — {trail['total']} events")

    # ── STEP 12: Run Integrity Check ──
    integrity = await run_integrity_check(entity_type="loan", entity_id=app_id)
    assert integrity["status"] == "ok"
    assert integrity["chain_valid"] is True
    assert integrity["tamper_detected"] is False
    print(f"✅ Step 12: Integrity check — chain valid, no tampering")

    # ── STEP 13: Health Check ──
    health_data = await get_ledger_health()
    health = json.loads(health_data)
    assert health["status"] == "healthy"
    assert health["event_count"] > 0
    print(f"✅ Step 13: System healthy — {health['event_count']} events, {health['stream_count']} streams")

    print(f"\n🎉 FULL MCP LIFECYCLE TEST PASSED for {app_id}")
