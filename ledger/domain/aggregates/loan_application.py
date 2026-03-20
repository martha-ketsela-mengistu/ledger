"""
ledger/domain/aggregates/loan_application.py
=============================================
COMPLETION STATUS: STUB — implement apply() for each event, enforce business rules.

The aggregate replays its event stream to rebuild state.
Command handlers validate against current state before appending events.

BUSINESS RULES TO ENFORCE:
  1. State machine: only valid transitions allowed
  2. DocumentFactsExtracted must exist before CreditAnalysisCompleted
  3. All 6 compliance rules must complete before DecisionGenerated (unless hard block)
  4. confidence < 0.60 → recommendation must be REFER (enforced here, not in LLM)
  5. Compliance BLOCKED → only DECLINE allowed, not APPROVE or REFER
  6. Causal chain: every agent event must reference a triggering event_id

See: Section 4 of challenge document for full rule specifications.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum

class ApplicationState(str, Enum):
    NEW = "NEW"
    SUBMITTED = "SUBMITTED"
    AWAITING_ANALYSIS = "AWAITING_ANALYSIS"
    ANALYSIS_COMPLETE = "ANALYSIS_COMPLETE"
    COMPLIANCE_REVIEW = "COMPLIANCE_REVIEW"
    PENDING_DECISION = "PENDING_DECISION"
    APPROVED_PENDING_HUMAN = "APPROVED_PENDING_HUMAN"
    DECLINED_PENDING_HUMAN = "DECLINED_PENDING_HUMAN"
    FINAL_APPROVED = "FINAL_APPROVED"
    FINAL_DECLINED = "FINAL_DECLINED"

VALID_TRANSITIONS = {
    ApplicationState.NEW: [ApplicationState.SUBMITTED],
    ApplicationState.SUBMITTED: [ApplicationState.AWAITING_ANALYSIS],
    ApplicationState.AWAITING_ANALYSIS: [ApplicationState.ANALYSIS_COMPLETE],
    ApplicationState.ANALYSIS_COMPLETE: [ApplicationState.COMPLIANCE_REVIEW],
    ApplicationState.COMPLIANCE_REVIEW: [ApplicationState.PENDING_DECISION, ApplicationState.FINAL_DECLINED], # DeclinedCompliance case
    ApplicationState.PENDING_DECISION: [
        ApplicationState.APPROVED_PENDING_HUMAN, 
        ApplicationState.DECLINED_PENDING_HUMAN,
        ApplicationState.FINAL_APPROVED, 
        ApplicationState.FINAL_DECLINED
    ],
    ApplicationState.APPROVED_PENDING_HUMAN: [ApplicationState.FINAL_APPROVED, ApplicationState.FINAL_DECLINED],
    ApplicationState.DECLINED_PENDING_HUMAN: [ApplicationState.FINAL_APPROVED, ApplicationState.FINAL_DECLINED],
}

@dataclass
class LoanApplicationAggregate:
    application_id: str
    state: ApplicationState = ApplicationState.NEW
    applicant_id: str | None = None
    requested_amount_usd: float | None = None
    loan_purpose: str | None = None
    version: int = 0
    decision_generated: bool = False
    credit_analysis_complete: bool = False
    compliance_passed: bool = False
    human_review_override: bool = False
    decision_sessions: set[str] = field(default_factory=set)

    @classmethod
    async def load(cls, store, application_id: str) -> "LoanApplicationAggregate":
        """Load and replay event stream to rebuild aggregate state."""
        agg = cls(application_id=application_id)
        stream_id = f"loan-{application_id}"
        events = await store.load_stream(stream_id)
        for event in events:
            agg.apply(event)
        return agg

    def apply(self, event: dict) -> None:
        """Apply one event to update aggregate state."""
        et = event.get("event_type")
        method_name = f"_apply_{et}"
        if hasattr(self, method_name):
            getattr(self, method_name)(event)
        
        # Always increment version based on stream position if available
        if "stream_position" in event:
            self.version = event["stream_position"]
        else:
            self.version += 1

    def _apply_ApplicationSubmitted(self, event: dict) -> None:
        p = event.get("payload", {})
        self.state = ApplicationState.SUBMITTED
        self.applicant_id = p.get("applicant_id")
        self.requested_amount_usd = p.get("requested_amount_usd")
        self.loan_purpose = p.get("loan_purpose")

    def _apply_ExtractionCompleted(self, event: dict) -> None:
        # Implicitly transitions to AwaitingAnalysis when processing completes
        # This might be triggered via a domain event or a command
        pass

    def _apply_CreditAnalysisRequested(self, event: dict) -> None:
        self.state = ApplicationState.AWAITING_ANALYSIS

    def _apply_CreditAnalysisCompleted(self, event: dict) -> None:
        # Rule 3: Model version locking (handled in command validation but state reflects it)
        self.state = ApplicationState.ANALYSIS_COMPLETE
        self.credit_analysis_complete = True

    def _apply_ComplianceCheckRequested(self, event: dict) -> None:
        self.state = ApplicationState.COMPLIANCE_REVIEW

    def _apply_ComplianceCheckCompleted(self, event: dict) -> None:
        p = event.get("payload", {})
        self.state = ApplicationState.PENDING_DECISION
        if p.get("overall_verdict") == "CLEAR":
            self.compliance_passed = True

    def _apply_DecisionGenerated(self, event: dict) -> None:
        p = event.get("payload", {})
        # Rule 4: Confidence floor check (logic usually in command, but state reflects it)
        rec = p.get("recommendation")
        if rec == "REFER":
            self.state = ApplicationState.APPROVED_PENDING_HUMAN # Mapping REFER to human review
        else:
            # Placeholder for mapping APPROVED/DECLINED
            pass

    def _apply_HumanReviewCompleted(self, event: dict) -> None:
        p = event.get("payload", {})
        if p.get("override"):
            self.human_review_override = True

    def _apply_ApplicationApproved(self, event: dict) -> None:
        self.state = ApplicationState.FINAL_APPROVED

    def _apply_ApplicationDeclined(self, event: dict) -> None:
        self.state = ApplicationState.FINAL_DECLINED

    def assert_valid_transition(self, target: ApplicationState) -> None:
        allowed = VALID_TRANSITIONS.get(self.state, [])
        if target not in allowed:
            from ledger.domain.errors import DomainError
            raise DomainError(f"Invalid transition {self.state} → {target}. Allowed: {allowed}")

    def validate_credit_analysis(self) -> None:
        """Rule 3: Model version locking."""
        if self.credit_analysis_complete and not self.human_review_override:
            from ledger.domain.errors import DomainError
            raise DomainError("Credit analysis already complete. Supersede via HumanReviewOverride first.")

    def validate_decision_confidence(self, confidence: float) -> str:
        """Rule 4: Confidence floor enforcement."""
        if confidence < 0.60:
            return "REFER"
        return "MATCH_RECOMMENDATION" # Placeholder for actual recommendation

    def validate_approval_dependency(self) -> None:
        """Rule 5: Compliance dependency."""
        if not self.compliance_passed:
            from ledger.domain.errors import DomainError
            raise DomainError("Cannot approve application: Compliance check has not passed or is incomplete.")

    def validate_causal_chain(self, contributing_sessions: list[str]) -> None:
        """Rule 6: Causal chain enforcement."""
        # This requires the aggregate to know which sessions are valid.
        pass

    def assert_awaiting_credit_analysis(self) -> None:
        if self.state != ApplicationState.AWAITING_ANALYSIS:
            from ledger.domain.errors import DomainError
            raise DomainError(f"Cannot complete credit analysis: current state is {self.state}")
