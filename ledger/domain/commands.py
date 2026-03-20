# ledger/domain/commands.py

from pydantic import BaseModel, Field
from decimal import Decimal
from typing import Any

class DomainCommand(BaseModel):
    correlation_id: str | None = None
    causation_id: str | None = None

class CreditAnalysisCompletedCommand(DomainCommand):
    application_id: str
    agent_id: str
    session_id: str
    model_version: str
    confidence_score: float
    risk_tier: str
    recommended_limit_usd: Decimal
    duration_ms: int
    input_data: dict[str, Any]

class DecisionGeneratedCommand(DomainCommand):
    application_id: str
    agent_id: str
    session_id: str
    recommendation: str
    confidence_score: float
    approved_amount_usd: Decimal | None = None
    executive_summary: str
    contributing_sessions: list[str]
