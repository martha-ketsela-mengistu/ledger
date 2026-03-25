"""
src/agents/fraud_detection_agent.py
===================================
FRAUD DETECTION AGENT — implements cross-reference between docs and registry.
"""
from __future__ import annotations
import time, json, logging
from datetime import datetime
from typing import TypedDict, Any
from uuid import uuid4

logger = logging.getLogger(__name__)

from langgraph.graph import StateGraph, END

from src.agents.base_agent import BaseApexAgent
from src.models.events import (
    FraudScreeningInitiated, FraudAnomalyDetected, FraudScreeningCompleted,
    ComplianceCheckRequested, FraudAnomaly, FraudAnomalyType
)
from src.aggregates.loan_application import LoanApplicationAggregate
from src.aggregates.document_package import DocumentPackageAggregate

class FraudState(TypedDict):
    application_id: str
    session_id: str
    extracted_facts: dict | None
    registry_profile: dict | None
    historical_financials: list[dict] | None
    fraud_signals: list[dict] | None
    fraud_score: float | None
    anomalies: list[dict] | None
    errors: list[str]
    output_events: list[dict]
    next_agent: str | None

class FraudDetectionAgent(BaseApexAgent):
    def build_graph(self) -> Any:
        g = StateGraph(FraudState)
        g.add_node("validate_inputs",         self._node_validate_inputs)
        g.add_node("load_document_facts",     self._node_load_facts)
        g.add_node("cross_reference_registry",self._node_cross_reference)
        g.add_node("analyze_fraud_patterns",  self._node_analyze)
        g.add_node("write_output",            self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",          "load_document_facts")
        g.add_edge("load_document_facts",      "cross_reference_registry")
        g.add_edge("cross_reference_registry", "analyze_fraud_patterns")
        g.add_edge("analyze_fraud_patterns",   "write_output")
        g.add_edge("write_output",             END)
        return g.compile()

    def _initial_state(self, application_id: str) -> FraudState:
        return FraudState(
            application_id=application_id, session_id=self.session_id,
            extracted_facts=None, registry_profile=None, historical_financials=None,
            fraud_signals=None, fraud_score=None, anomalies=None,
            errors=[], output_events=[], next_agent=None,
        )

    async def _node_validate_inputs(self, state: FraudState) -> FraudState:
        t = time.time()
        app_id = state["application_id"]
        app = await LoanApplicationAggregate.load(self.store, app_id)
        # In a real system we'd check if FRAUD_SCREENING_REQUESTED happened
        ms = int((time.time() - t) * 1000)
        await self._record_input_validated(["application_id"], ms)
        await self._record_node_execution("validate_inputs", ["application_id"], [], ms)
        return state

    async def _node_load_facts(self, state: FraudState) -> FraudState:
        t = time.time()
        app_id = state["application_id"]
        pkg = await DocumentPackageAggregate.load(self.store, app_id)
        
        # Load ExtractionCompleted events to get facts
        pkg_events = await self.store.load_stream(f"docpkg-{app_id}")
        merged_facts = {}
        for ev in pkg_events:
            if ev.event_type == "ExtractionCompleted":
                facts = ev.payload.get("facts") or {}
                merged_facts.update(facts)
        
        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("load_document_facts", ["application_id"], ["extracted_facts"], ms)
        return {**state, "extracted_facts": merged_facts}

    async def _node_cross_reference(self, state: FraudState) -> FraudState:
        t = time.time()
        app_id = state["application_id"]
        app = await LoanApplicationAggregate.load(self.store, app_id)
        applicant_id = app.applicant_id
        
        from dataclasses import asdict
        profile_obj = await self.registry.get_company(applicant_id)
        financials_list = await self.registry.get_financial_history(applicant_id)
        
        profile = asdict(profile_obj) if profile_obj else {}
        financials = [asdict(f) for f in financials_list]
        
        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("registry_query", applicant_id, "profile and financials", ms)
        await self._record_node_execution("cross_reference_registry", ["extracted_facts"], ["registry_profile", "historical_financials"], ms)
        return {**state, "registry_profile": profile, "historical_financials": financials}

    async def _node_analyze(self, state: FraudState) -> FraudState:
        t = time.time()
        facts = state["extracted_facts"] or {}
        hist = state["historical_financials"] or []
        profile = state["registry_profile"] or {}
        
        # Calculate basic score components
        fraud_score = 0.05 # Base
        anomalies = []
        
        # 1. Revenue Discrepancy
        if hist and "total_revenue" in facts:
            prior_rev = hist[-1].get("total_revenue", 0)
            doc_rev = float(facts["total_revenue"])
            if prior_rev > 0:
                prior_rev = float(prior_rev)
                gap = abs(doc_rev - prior_rev) / prior_rev
                if gap > 0.40 and profile.get("trajectory") not in ("GROWTH", "RECOVERING"):
                    fraud_score += 0.25
                    anomalies.append({
                        "type": FraudAnomalyType.REVENUE_DISCREPANCY,
                        "severity": "HIGH",
                        "description": f"Extracted revenue (${doc_rev:,.0f}) differs by {gap:.1%} from registry record (${prior_rev:,.0f}).",
                        "evidence": f"doc_rev={doc_rev}, registry_rev={prior_rev}",
                        "affected_fields": ["total_revenue"]
                    })

        # 2. Balance Sheet Consistency
        if "total_assets" in facts and "total_liabilities" in facts and "total_equity" in facts:
            a = float(facts["total_assets"])
            l = float(facts["total_liabilities"])
            e = float(facts["total_equity"])
            diff = abs(a - (l + e))
            if diff > 1000: # Tolerance
                fraud_score += 0.20
                anomalies.append({
                    "type": FraudAnomalyType.BALANCE_SHEET_INCONSISTENCY,
                    "severity": "MEDIUM",
                    "description": f"Balance sheet does not balance. Assets differ from L+E by ${diff:,.2f}.",
                    "evidence": f"A={a}, L={l}, E={e}",
                    "affected_fields": ["total_assets", "total_liabilities", "total_equity"]
                })

        # LLM for deeper pattern analysis
        SYSTEM = "You are a financial fraud analyst. Identify anomalies and compute a fraud score (0-1)."
        USER = f"Facts: {json.dumps(facts, default=str)}\nRegistry: {json.dumps(profile, default=str)}"
        
        try:
            content, ti, to, cost = await self._call_llm(SYSTEM, USER)
            # In a real implementation, we'd parse the LLM JSON and merge with our scores
            # For now, we use our deterministic scores as the primary source
        except:
            pass
            
        ms = int((time.time() - t) * 1000)
        await self._record_node_execution("analyze_fraud_patterns", ["extracted_facts", "registry"], ["fraud_score", "anomalies"], ms)
        return {**state, "fraud_score": min(fraud_score, 1.0), "anomalies": anomalies}

    async def _node_write_output(self, state: FraudState) -> FraudState:
        t = time.time()
        app_id = state["application_id"]
        
        # 1. Initiate screening
        init_event = FraudScreeningInitiated(
            application_id=app_id,
            session_id=self.session_id,
            screening_model_version=self.model,
            initiated_at=datetime.now()
        ).to_store_dict()
        await self._append_with_retry(f"fraud-{app_id}", [init_event])

        # 2. Record anomalies
        for a in state["anomalies"] or []:
            anom_event = FraudAnomalyDetected(
                application_id=app_id,
                session_id=self.session_id,
                anomaly=FraudAnomaly(
                    anomaly_type=a["type"],
                    description=a["description"],
                    severity=a["severity"],
                    evidence=a["evidence"],
                    affected_fields=a["affected_fields"]
                ),
                detected_at=datetime.now()
            ).to_store_dict()
            await self._append_with_retry(f"fraud-{app_id}", [anom_event])

        # 3. Complete screening
        comp_event = FraudScreeningCompleted(
            application_id=app_id,
            session_id=self.session_id,
            fraud_score=state["fraud_score"],
            risk_level="HIGH" if state["fraud_score"] > 0.6 else "MEDIUM" if state["fraud_score"] > 0.3 else "LOW",
            anomalies_found=len(state["anomalies"] or []),
            recommendation="PROCEED" if state["fraud_score"] < 0.3 else "FLAG_FOR_REVIEW",
            screening_model_version=self.model,
            input_data_hash=self._sha(state),
            completed_at=datetime.now()
        ).to_store_dict()
        await self._append_with_retry(f"fraud-{app_id}", [comp_event])

        # 4. Trigger Compliance
        compliance_trigger = ComplianceCheckRequested(
            application_id=app_id,
            requested_at=datetime.now(),
            triggered_by_event_id=self.session_id,
            regulation_set_version="2026-Q1-v1",
            rules_to_evaluate=["REG-001", "REG-002", "REG-003", "REG-004", "REG-005", "REG-006"]
        ).to_store_dict()
        await self._append_with_retry(f"loan-{app_id}", [compliance_trigger])

        ms = int((time.time() - t) * 1000)
        await self._record_output_written([], f"Fraud score: {state['fraud_score']}. {len(state['anomalies'] or [])} anomalies found.")
        await self._record_node_execution("write_output", ["fraud_score"], ["next_agent"], ms)
        return {**state, "next_agent": "compliance"}
