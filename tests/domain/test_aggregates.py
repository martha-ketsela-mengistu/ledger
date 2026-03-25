import pytest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.aggregates.loan_application import LoanApplicationAggregate, ApplicationState
from src.aggregates.agent_session import AgentSessionAggregate
from src.models.events import DomainError

def _ev(event_type, **payload):
    return {"event_type": event_type, "payload": payload, "stream_position": 1}

@pytest.mark.asyncio
async def test_loan_application_state_machine():
    agg = LoanApplicationAggregate("app-001")
    assert agg.state == ApplicationState.NEW
    
    agg.apply(_ev("ApplicationSubmitted", applicant_id="user-1"))
    assert agg.state == ApplicationState.SUBMITTED
    
    agg.apply(_ev("CreditAnalysisRequested"))
    assert agg.state == ApplicationState.AWAITING_ANALYSIS
    
    # Test invalid transition
    with pytest.raises(DomainError, match="Invalid transition"):
        agg.assert_valid_transition(ApplicationState.FINAL_APPROVED)

@pytest.mark.asyncio
async def test_agent_session_context_requirement():
    agg = AgentSessionAggregate("sess-001")
    
    # Action before context loaded
    with pytest.raises(DomainError, match="AgentContextLoaded must be the first event"):
        agg.assert_context_loaded("perform_analysis")
        
    # Load context
    agg.apply(_ev("AgentContextLoaded", context_source="replay"))
    assert agg.context_loaded is True
    
    # Now it should be OK
    agg.assert_context_loaded("perform_analysis")

@pytest.mark.asyncio
async def test_loan_application_confidence_floor_logic():
    # Note: Confidence floor is often enforced in the command that produces the event,
    # but the aggregate should handle the resulting state.
    agg = LoanApplicationAggregate("app-001")
    
    # We simulate a "REFER" recommendation being applied
    agg.apply(_ev("DecisionGenerated", recommendation="REFER", confidence=0.55))
    assert agg.state == ApplicationState.APPROVED_PENDING_HUMAN
