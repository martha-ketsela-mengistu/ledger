import pytest
from uuid import uuid4
from src.aggregates.loan_application import LoanApplicationAggregate, ApplicationState
from src.aggregates.agent_session import AgentSessionAggregate
from src.models.events import StoredEvent, AgentType, DomainError

def create_stored_event(event_type, payload, stream_id="test", pos=1):
    return StoredEvent(
        event_id=uuid4(),
        stream_id=stream_id,
        stream_position=pos,
        global_position=pos,
        event_type=event_type,
        event_version=1,
        payload=payload,
        metadata={},
        recorded_at=__import__("datetime").datetime.now(__import__("datetime").UTC)
    )

def test_loan_application_decision_states():
    # Test REFER -> APPROVED_PENDING_HUMAN
    agg = LoanApplicationAggregate(application_id="app-1")
    event = create_stored_event("DecisionGenerated", {"recommendation": "REFER"})
    agg.apply(event)
    assert agg.state == ApplicationState.APPROVED_PENDING_HUMAN
    assert agg.decision_generated is True

    # Test APPROVE -> FINAL_APPROVED
    agg = LoanApplicationAggregate(application_id="app-2")
    event = create_stored_event("DecisionGenerated", {"recommendation": "APPROVE"})
    agg.apply(event)
    assert agg.state == ApplicationState.FINAL_APPROVED

    # Test DECLINE -> FINAL_DECLINED
    agg = LoanApplicationAggregate(application_id="app-3")
    event = create_stored_event("DecisionGenerated", {"recommendation": "DECLINE"})
    agg.apply(event)
    assert agg.state == ApplicationState.FINAL_DECLINED

def test_agent_session_model_version_locking():
    agg = AgentSessionAggregate(session_id="sess-1")
    
    # Start session with version "v1"
    start_event = create_stored_event("AgentSessionStarted", {
        "agent_type": "credit_analysis",
        "model_version": "v1"
    })
    agg.apply(start_event)
    assert agg.model_version == "v1"
    
    # Assert same version passes
    agg.assert_model_version_current("v1")
    
    # Assert different version fails
    with pytest.raises(DomainError, match="Model version mismatch"):
        agg.assert_model_version_current("v2")

def test_agent_session_context_loaded():
    agg = AgentSessionAggregate(session_id="sess-1")
    
    with pytest.raises(DomainError, match="AgentContextLoaded must be the first event"):
        agg.assert_context_loaded("some_action")
        
    event = create_stored_event("AgentContextLoaded", {})
    agg.apply(event)
    agg.assert_context_loaded("some_action")
