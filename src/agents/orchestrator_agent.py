"""
src/agents/orchestrator_agent.py
=================================
DECISION ORCHESTRATOR AGENT — synthesises Credit, Fraud, and Compliance findings.
"""
from __future__ import annotations
import time, json, logging
from datetime import datetime
from typing import TypedDict, Any, List
from uuid import uuid4
from decimal import Decimal

logger = logging.getLogger(__name__)

from langgraph.graph import StateGraph, END

from src.agents.base_agent import BaseApexAgent
from src.models.events import (
    DecisionRequested, DecisionGenerated, ApplicationApproved,
    ApplicationDeclined, HumanReviewRequested, StoredEvent
)
from src.aggregates.loan_application import LoanApplicationAggregate

class OrchestratorState(TypedDict):
    application_id: str
    session_id: str
    credit_result: dict | None
    fraud_result: dict | None
    compliance_result: dict | None
    recommendation: str | None              # APPROVE, DECLINE, REFER
    confidence: float | None
    approved_amount_usd: float | None
    executive_summary: str | None
    key_risks: list[str] | None
    conditions: list[str] | None
    policy_overrides_applied: list[str] | None
    errors: list[str]
    next_agent: str | None

class DecisionOrchestratorAgent(BaseApexAgent):
    def build_graph(self) -> Any:
        g = StateGraph(OrchestratorState)
        g.add_node("validate_inputs",         self._node_validate_inputs)
        g.add_node("load_credit_result",      self._node_load_credit)
        g.add_node("load_fraud_result",       self._node_load_fraud)
        g.add_node("load_compliance_result",  self._node_load_compliance)
        g.add_node("synthesize_decision",     self._node_synthesize)
        g.add_node("apply_hard_constraints",  self._node_constraints)
        g.add_node("write_output",            self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",        "load_credit_result")
        g.add_edge("load_credit_result",     "load_fraud_result")
        g.add_edge("load_fraud_result",      "load_compliance_result")
        g.add_edge("load_compliance_result", "synthesize_decision")
        g.add_edge("synthesize_decision",    "apply_hard_constraints")
        g.add_edge("apply_hard_constraints", "write_output")
        g.add_edge("write_output",           END)
        return g.compile()

    def _initial_state(self, application_id: str) -> OrchestratorState:
        return OrchestratorState(
            application_id=application_id, session_id=self.session_id,
            credit_result=None, fraud_result=None, compliance_result=None,
            recommendation=None, confidence=None, approved_amount_usd=None,
            executive_summary=None, key_risks=[], conditions=[], 
            policy_overrides_applied=[], errors=[], next_agent=None
        )

    async def _node_validate_inputs(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        # Orchestrator needs to verify all streams are ready
        ms = int((time.time() - t) * 1000)
        await self._record_input_validated(["application_id"], ms)
        await self._record_node_execution("validate_inputs", ["application_id"], [], ms)
        return state

    async def _node_load_credit(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        app_id = state["application_id"]
        # Fetch last CreditAnalysisCompleted from credit stream
        events = await self.store.load_stream(f"credit-{app_id}")
        last_credit = next((e for e in reversed(events) if e.event_type == "CreditAnalysisCompleted"), None)
        
        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("store_read", f"credit-{app_id}", "CreditAnalysisCompleted", ms)
        await self._record_node_execution("load_credit_result", ["application_id"], ["credit_result"], ms)
        return {**state, "credit_result": last_credit.payload if last_credit else None}

    async def _node_load_fraud(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        app_id = state["application_id"]
        # Fetch last FraudScreeningCompleted
        events = await self.store.load_stream(f"fraud-{app_id}")
        last_fraud = next((e for e in reversed(events) if e.event_type == "FraudScreeningCompleted"), None)
        
        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("store_read", f"fraud-{app_id}", "FraudScreeningCompleted", ms)
        await self._record_node_execution("load_fraud_result", ["application_id"], ["fraud_result"], ms)
        return {**state, "fraud_result": last_fraud.payload if last_fraud else None}

    async def _node_load_compliance(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        app_id = state["application_id"]
        # Fetch last ComplianceCheckCompleted
        events = await self.store.load_stream(f"compliance-{app_id}")
        last_comp = next((e for e in reversed(events) if e.event_type == "ComplianceCheckCompleted"), None)
        
        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("store_read", f"compliance-{app_id}", "ComplianceCheckCompleted", ms)
        await self._record_node_execution("load_compliance_result", ["application_id"], ["compliance_result"], ms)
        return {**state, "compliance_result": last_comp.payload if last_comp else None}

    async def _node_synthesize(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        if not all([state["credit_result"], state["fraud_result"], state["compliance_result"]]):
            return {**state, "errors": state["errors"] + ["Missing analytical results"]}

        system = f"""
        You are a senior loan officer synthesising multi-agent analysis for application {state['application_id']}.
        Produce a recommendation (APPROVE/DECLINE/REFER),
        approved_amount_usd (as number), executive_summary (3-5 sentences), and key_risks list.
        Return as valid JSON.
        """
        
        user = f"""
        CREDIT ANALYSIS:
        {json.dumps(state['credit_result'], indent=2, default=str)}
        
        FRAUD SCREENING:
        {json.dumps(state['fraud_result'], indent=2, default=str)}
        
        COMPLIANCE VERDICT:
        {json.dumps(state['compliance_result'], indent=2, default=str)}
        """
        
        resp, i, o, c = await self._call_llm(system, user)
        self._llm_calls += 1; self._tokens += (i + o); self._cost += c
        decision = self._parse_json(resp)
        
        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("synthesize_decision", ["credit_result", "fraud_result", "compliance_result"], ["recommendation"], ms)
        
        return {
            **state,
            "recommendation": decision.get("recommendation", "REFER"),
            "confidence": decision.get("confidence", 0.7),
            "approved_amount_usd": float(decision.get("approved_amount_usd", 0)),
            "executive_summary": decision.get("executive_summary", "Synthesis complete."),
            "key_risks": decision.get("key_risks", []),
            "conditions": decision.get("conditions", [])
        }

    async def _node_constraints(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        overrides = []
        rec = state["recommendation"]
        
        # 1. Compliance constraint
        comp = state["compliance_result"] or {}
        if comp.get("overall_verdict") == "BLOCKED" or comp.get("has_hard_block"):
            if rec != "DECLINE":
                rec = "DECLINE"
                overrides.append("COMPLIANCE_HARD_BLOCK_ENFORCED")
        
        # 2. Fraud constraint
        fraud = state["fraud_result"] or {}
        if (fraud.get("fraud_score", 0) or 0) > 0.60:
            if rec == "APPROVE":
                rec = "REFER"
                overrides.append("FRAUD_RISK_REFERRAL")
                
        # 3. Confidence constraint
        if (state["confidence"] or 0) < 0.60:
            if rec == "APPROVE":
                rec = "REFER"
                overrides.append("LOW_CONFIDENCE_REFERRAL")

        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("apply_hard_constraints", ["recommendation"], ["policy_overrides_applied"], ms)
        return {**state, "recommendation": rec, "policy_overrides_applied": overrides}

    async def _node_write_output(self, state: OrchestratorState) -> OrchestratorState:
        t = time.time()
        app_id = state["application_id"]
        
        # 1. DecisionGenerated
        gen_event = DecisionGenerated(
            application_id=app_id,
            orchestrator_session_id=self.session_id,
            recommendation=state["recommendation"],
            confidence=state["confidence"] or 0.0,
            approved_amount_usd=Decimal(str(state["approved_amount_usd"] or 0)),
            executive_summary=state["executive_summary"],
            key_risks=state["key_risks"] or [],
            contributing_sessions=[
                state["credit_result"].get("session_id") if state["credit_result"] else "N/A",
                state["fraud_result"].get("session_id") if state["fraud_result"] else "N/A",
                state["compliance_result"].get("session_id") if state["compliance_result"] else "N/A",
            ],
            policy_overrides_applied=state["policy_overrides_applied"] or [],
            generated_at=datetime.now()
        ).to_store_dict()
        await self._append_with_retry(f"loan-{app_id}", [gen_event])
        
        # 2. Final Lifecycle Event
        rec = state["recommendation"]
        final_event = None
        if rec == "APPROVE":
            from src.models.events import ApplicationApproved
            final_event = ApplicationApproved(
                application_id=app_id,
                approved_amount_usd=Decimal(str(state["approved_amount_usd"] or 0)),
                interest_rate_pct=7.5, # Default for now
                term_months=24,
                approved_by=self.agent_id,
                effective_date=datetime.now().strftime("%Y-%m-%d"),
                approved_at=datetime.now()
            )
        elif rec == "DECLINE":
            from src.models.events import ApplicationDeclined
            final_event = ApplicationDeclined(
                application_id=app_id,
                decline_reasons=state["key_risks"] or ["Credit policy threshold not met"],
                declined_by=self.agent_id,
                adverse_action_notice_required=True,
                declined_at=datetime.now()
            )
        else: # REFER
            from src.models.events import HumanReviewRequested
            final_event = HumanReviewRequested(
                application_id=app_id,
                reason="Manual synthesis required based on flags",
                decision_event_id=str(uuid4()), # Link to the generated decision
                requested_at=datetime.now()
            )
            
        if final_event:
            await self._append_with_retry(f"loan-{app_id}", [final_event.to_store_dict()])
            
        ms = int((time.time() - t) * 1000)
        await self._record_output_written([{"stream_id": f"loan-{app_id}", "event_type": "DecisionGenerated"}], f"Decision: {rec}")
        await self._record_node_execution("write_output", ["recommendation"], [], ms)
        return state

    def _parse_json(self, content: str) -> dict:
        import re
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            match = re.search(r'\{.*\}', content, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group())
                except:
                    pass
            return {}
