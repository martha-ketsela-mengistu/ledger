"""
tests/test_narratives.py
========================
The 5 narrative scenario tests. These are the primary correctness gate.
These FAIL until all 5 agents and aggregates are implemented.

Run: pytest tests/test_narratives.py -v -s
"""
import pytest, sys
from pathlib import Path; sys.path.insert(0, str(Path(__file__).parent.parent))

# Narrative scenarios tested here match Section 7 of the challenge document.
# Each test drives a complete application through the real agent pipeline.

@pytest.mark.asyncio
async def test_narr01_concurrent_occ_collision():
    """
    NARR-01: Two CreditAnalysisAgent instances run simultaneously.
    Expected: exactly one CreditAnalysisCompleted in credit stream (not two),
              second agent gets OCC, reloads, retries successfully.
    """
    pytest.skip("Implement after CreditAnalysisAgent is working")

@pytest.mark.asyncio
async def test_narr02_document_extraction_failure():
    """
    NARR-02: Income statement PDF with missing EBITDA line.
    Expected: DocumentQualityFlagged with critical_missing_fields=['ebitda'],
              CreditAnalysisCompleted.confidence <= 0.75,
              CreditAnalysisCompleted.data_quality_caveats is non-empty.
    """
    pytest.skip("Implement after DocumentProcessingAgent + CreditAnalysisAgent working")

@pytest.mark.asyncio
async def test_narr03_agent_crash_recovery():
    """
    NARR-03: FraudDetectionAgent crashes mid-session.
    Expected: only ONE FraudScreeningCompleted event in fraud stream,
              second AgentSessionStarted has context_source starting with 'prior_session_replay:',
              no duplicate analysis work.
    """
    import asyncio
    from src.event_store import EventStore
    from src.registry.client import ApplicantRegistryClient
    from src.agents.fraud_detection_agent import FraudDetectionAgent
    import asyncpg
    from uuid import uuid4
    DB_URL = "postgresql://postgres:apex@localhost/apex_ledger"
    
    store = EventStore(DB_URL)
    await store.connect()
    pool = await asyncpg.create_pool(DB_URL)
    registry = ApplicantRegistryClient(pool)
    
    app_id = f"APEX-NARR-{uuid4().hex[:6]}"
    applicant_id = "COMP-005"
    
    # Bootstrap required prior events
    await store.append(f"loan-{app_id}", [{"event_type": "ApplicationSubmitted", "payload": {"application_id": app_id, "applicant_id": applicant_id}}], -1)
    await store.append(f"docpkg-{app_id}", [{"event_type": "ExtractionCompleted", "payload": {"facts": {"total_revenue": 5000000}}}], -1)

    agent1 = FraudDetectionAgent("fraud-agent-01", "fraud_detection", store, registry)
    
    # 1. Simulate a mid-session crash during LLM analysis
    original_node_analyze = agent1._node_analyze
    async def mock_node_analyze(state):
        raise TimeoutError("Simulated LLM Timeout Crash")
    agent1._node_analyze = mock_node_analyze
    
    with pytest.raises(TimeoutError):
        await agent1.process_application(app_id)
        
    session_id_1 = agent1.session_id
    
    # 2. Recover using a new agent instance
    agent2 = FraudDetectionAgent("fraud-agent-02", "fraud_detection", store, registry)
    # Mock LLM to succeed this time to save time
    async def mock_call_llm_success(*args, **kwargs):
        return "{}", 100, 100, 0.01
    agent2._call_llm = mock_call_llm_success
    
    await agent2.process_application(app_id, recover_from_session_id=session_id_1)
    session_id_2 = agent2.session_id
    
    # 3. Assertions
    # A) Only ONE FraudScreeningCompleted
    fraud_events = await store.load_stream(f"fraud-{app_id}")
    completed_events = [e for e in fraud_events if e.event_type == "FraudScreeningCompleted"]
    assert len(completed_events) == 1, f"Expected 1 FraudScreeningCompleted, got {len(completed_events)}"
    
    # B) Second session has prior_session_replay
    sess2_events = await store.load_stream(f"agent-fraud_detection-{session_id_2}")
    start_event = next(e for e in sess2_events if e.event_type == "AgentSessionStarted")
    assert start_event.payload["context_source"].startswith("prior_session_replay:")
    
    # C) AgentSessionRecovered is present
    recovered_event = next((e for e in sess2_events if e.event_type == "AgentSessionRecovered"), None)
    assert recovered_event is not None
    assert recovered_event.payload["recovered_from_session_id"] == session_id_1
    
    # D) Zero duplicate AgentNodeExecuted for load_facts
    sess1_events = await store.load_stream(f"agent-fraud_detection-{session_id_1}")
    all_load_events = [e for e in (sess1_events + sess2_events) 
                      if e.event_type == "AgentNodeExecuted" and e.payload["node_name"] == "load_document_facts"]
    assert len(all_load_events) == 1, "Duplicate load_document_facts execution detected!"
    
    await store.close()

@pytest.mark.asyncio
async def test_narr04_compliance_hard_block():
    """
    NARR-04: Montana applicant (jurisdiction='MT') triggers REG-003.
    Expected: ComplianceRuleFailed(rule_id='REG-003', is_hard_block=True),
              NO DecisionGenerated event,
              ApplicationDeclined with adverse_action_notice_required=True.
    """
    pytest.skip("Implement after ComplianceAgent is working")

@pytest.mark.asyncio
async def test_narr05_human_override():
    """
    NARR-05: Orchestrator recommends DECLINE; human loan officer overrides to APPROVE.
    Expected: DecisionGenerated(recommendation='DECLINE'),
              HumanReviewCompleted(override=True, reviewer_id='LO-Sarah-Chen'),
              ApplicationApproved(approved_amount_usd=750000, conditions has 2 items).
    """
    pytest.skip("Implement after all agents + HumanReviewCompleted command handler working")
