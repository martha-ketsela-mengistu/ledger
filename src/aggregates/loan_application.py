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
import logging
from dataclasses import dataclass, field
from src.models.events import StoredEvent, ApplicationState

logger = logging.getLogger(__name__)

# ─── CONSTANTS ────────────────────────────────────────────────────────────────

VALID_TRANSITIONS = {
    None: [ApplicationState.SUBMITTED],
    ApplicationState.SUBMITTED: [ApplicationState.DOCUMENTS_PENDING, ApplicationState.DOCUMENTS_UPLOADED, ApplicationState.DOCUMENTS_PROCESSED],
    ApplicationState.DOCUMENTS_PENDING: [ApplicationState.DOCUMENTS_UPLOADED],
    ApplicationState.DOCUMENTS_UPLOADED: [ApplicationState.DOCUMENTS_PROCESSED],
    ApplicationState.DOCUMENTS_PROCESSED: [ApplicationState.CREDIT_ANALYSIS_REQUESTED],
    ApplicationState.CREDIT_ANALYSIS_REQUESTED: [ApplicationState.CREDIT_ANALYSIS_COMPLETE],
    ApplicationState.CREDIT_ANALYSIS_COMPLETE: [ApplicationState.FRAUD_SCREENING_REQUESTED],
    ApplicationState.FRAUD_SCREENING_REQUESTED: [ApplicationState.FRAUD_SCREENING_COMPLETE],
    ApplicationState.FRAUD_SCREENING_COMPLETE: [ApplicationState.COMPLIANCE_CHECK_REQUESTED],
    ApplicationState.COMPLIANCE_CHECK_REQUESTED: [ApplicationState.COMPLIANCE_CHECK_COMPLETE],
    ApplicationState.COMPLIANCE_CHECK_COMPLETE: [ApplicationState.PENDING_DECISION, ApplicationState.PENDING_HUMAN_REVIEW],
    ApplicationState.PENDING_DECISION: [ApplicationState.APPROVED, ApplicationState.DECLINED, ApplicationState.REFERRED],
    ApplicationState.PENDING_HUMAN_REVIEW: [ApplicationState.APPROVED, ApplicationState.DECLINED, ApplicationState.REFERRED],
}

@dataclass
class LoanApplicationAggregate:
    application_id: str
    state: ApplicationState | None = None
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
        """
        Load and replay event stream to rebuild aggregate state.
        
        Args:
            store: The event store instance.
            application_id: The ID of the application stream to load.
            
        Returns:
            A reconstructed LoanApplicationAggregate instance.
        """
        logger.debug(f"Loading LoanApplicationAggregate for {application_id}")
        agg = cls(application_id=application_id)
        stream_id = f"loan-{application_id}"
        events = await store.load_stream(stream_id)
        for event in events:
            agg.apply(event)
        return agg

    def apply(self, event: StoredEvent) -> None:
        """
        Apply one event to update aggregate state.
        
        This method dynamically calls the appropriate _on_<EventType> handler.
        """
        logger.info(f"[{self.application_id}] apply() called for event_type='{event.event_type}' v{event.stream_position}")
        method_name = f"_on_{event.event_type}"
        if hasattr(self, method_name):
            getattr(self, method_name)(event)
        else:
            logger.warning(f"[{self.application_id}] No handler for {event.event_type}")
        
        self.version = event.stream_position

    def _on_ApplicationSubmitted(self, event: StoredEvent) -> None:
        p = event.payload
        self.state = ApplicationState.SUBMITTED
        self.applicant_id = p.get("applicant_id")
        logger.info(f"[{self.application_id}] ApplicationSubmitted applied: applicant_id={self.applicant_id}")
        self.requested_amount_usd = p.get("requested_amount_usd")
        self.loan_purpose = p.get("loan_purpose")

    def _on_ExtractionCompleted(self, event: StoredEvent) -> None:
        self.state = ApplicationState.DOCUMENTS_PROCESSED

    def _on_CreditAnalysisRequested(self, event: StoredEvent) -> None:
        self.state = ApplicationState.CREDIT_ANALYSIS_REQUESTED

    def _on_CreditAnalysisCompleted(self, event: StoredEvent) -> None:
        self.state = ApplicationState.CREDIT_ANALYSIS_COMPLETE
        self.credit_analysis_complete = True
        p = event.payload
        if "session_id" in p:
            self.decision_sessions.add(p["session_id"])

    def _on_FraudScreeningRequested(self, event: StoredEvent) -> None:
        self.state = ApplicationState.FRAUD_SCREENING_REQUESTED

    def _on_FraudScreeningCompleted(self, event: StoredEvent) -> None:
        self.state = ApplicationState.FRAUD_SCREENING_COMPLETE

    def _on_ComplianceCheckRequested(self, event: StoredEvent) -> None:
        self.state = ApplicationState.COMPLIANCE_CHECK_REQUESTED

    def _on_ComplianceCheckCompleted(self, event: StoredEvent) -> None:
        p = event.payload
        self.state = ApplicationState.COMPLIANCE_CHECK_COMPLETE
        if p.get("overall_verdict") == "CLEAR":
            self.compliance_passed = True
        if "session_id" in p:
            self.decision_sessions.add(p["session_id"])

    def _on_DecisionGenerated(self, event: StoredEvent) -> None:
        p = event.payload
        rec = p.get("recommendation")
        if rec == "REFER":
            self.state = ApplicationState.PENDING_HUMAN_REVIEW
        elif rec == "APPROVE":
            self.state = ApplicationState.APPROVED
        elif rec == "DECLINE":
            self.state = ApplicationState.DECLINED
        self.decision_generated = True

    def _on_HumanReviewCompleted(self, event: StoredEvent) -> None:
        p = event.payload
        if p.get("override"):
            self.human_review_override = True

    def _on_ApplicationApproved(self, event: StoredEvent) -> None:
        self.state = ApplicationState.FINAL_APPROVED

    def _on_ApplicationDeclined(self, event: StoredEvent) -> None:
        self.state = ApplicationState.FINAL_DECLINED

    def assert_valid_transition(self, target: ApplicationState) -> None:
        """
        Enforce the application state machine.
        
        Raises:
            DomainError: If the transition is not explicitly allowed.
        """
        allowed = VALID_TRANSITIONS.get(self.state, [])
        if target not in allowed:
            from src.models.events import DomainError
            logger.error(f"[{self.application_id}] Invalid transition {self.state} → {target}")
            raise DomainError(f"Invalid transition {self.state} → {target}. Allowed: {allowed}")

    def validate_credit_analysis(self) -> None:
        """Rule 3: Model version locking."""
        if self.credit_analysis_complete and not self.human_review_override:
            from src.models.events import DomainError
            raise DomainError("Credit analysis already complete. Supersede via HumanReviewOverride first.")

    def validate_decision_confidence(self, confidence: float) -> str:
        """Rule 4: Confidence floor enforcement."""
        if confidence < 0.60:
            return "REFER"
        return "MATCH_RECOMMENDATION" # Placeholder for actual recommendation

    def validate_approval_dependency(self) -> None:
        """Rule 5: Compliance dependency."""
        if not self.compliance_passed:
            from src.models.events import DomainError
            raise DomainError("Cannot approve application: Compliance check has not passed or is incomplete.")

    def validate_causal_chain(self, contributing_sessions: list[str]) -> None:
        """Rule 6: Causal chain enforcement."""
        # Ensure every contributing session has actually reported a decision/event for this app
        for sid in contributing_sessions:
            if sid not in self.decision_sessions:
                from src.models.events import DomainError
                raise DomainError(f"Causal chain violation: Session {sid} did not contribute to this application.")

    def assert_awaiting_credit_analysis(self) -> None:
        if self.state not in (ApplicationState.CREDIT_ANALYSIS_REQUESTED, ApplicationState.DOCUMENTS_PROCESSED):
            from src.models.events import DomainError
            raise DomainError(f"Cannot complete credit analysis: current state is {self.state}")
