"""
src/agents/compliance_agent.py
==============================
COMPLIANCE AGENT — implements deterministic regulatory rule evaluation.
"""
from __future__ import annotations
import time, json, logging
from datetime import datetime
from typing import TypedDict, Any, Callable
from uuid import uuid4

logger = logging.getLogger(__name__)

from langgraph.graph import StateGraph, END

from src.agents.base_agent import BaseApexAgent
from src.models.events import (
    ComplianceCheckInitiated, ComplianceRulePassed, ComplianceRuleFailed,
    ComplianceRuleNoted, ComplianceCheckCompleted, ComplianceVerdict,
    DecisionRequested, ApplicationDeclined
)
from src.aggregates.loan_application import LoanApplicationAggregate

class ComplianceState(TypedDict):
    application_id: str
    session_id: str
    applicant_id: str | None
    company_profile: dict | None
    rule_results: list[dict]
    has_hard_block: bool
    block_rule_id: str | None
    errors: list[str]
    next_agent: str | None

# Regulation definitions — deterministic, no LLM in decision path
REGULATIONS: dict[str, dict[str, Any]] = {
    "REG-001": {
        "name": "Bank Secrecy Act (BSA) Check",
        "version": "2026-Q1-v1",
        "is_hard_block": False,
        "check": lambda co: not any(
            f.get("flag_type") == "AML_WATCH" and f.get("is_active")
            for f in co.get("compliance_flags", [])
        ),
        "failure_reason": "Active AML Watch flag present. Remediation required.",
        "remediation": "Provide enhanced due diligence documentation within 10 business days.",
    },
    "REG-002": {
        "name": "OFAC Sanctions Screening",
        "version": "2026-Q1-v1",
        "is_hard_block": True,
        "check": lambda co: not any(
            f.get("flag_type") == "SANCTIONS_REVIEW" and f.get("is_active")
            for f in co.get("compliance_flags", [])
        ),
        "failure_reason": "Active OFAC Sanctions Review. Application blocked.",
        "remediation": None,
    },
    "REG-003": {
        "name": "Jurisdiction Lending Eligibility",
        "version": "2026-Q1-v1",
        "is_hard_block": True,
        "check": lambda co: co.get("jurisdiction") != "MT",
        "failure_reason": "Jurisdiction MT not approved for commercial lending at this time.",
        "remediation": None,
    },
    "REG-004": {
        "name": "Legal Entity Type Eligibility",
        "version": "2026-Q1-v1",
        "is_hard_block": False,
        "check": lambda co: not (
            co.get("legal_type") == "Sole Proprietor"
            and (co.get("requested_amount_usd", 0) or 0) > 250_000
        ),
        "failure_reason": "Sole Proprietor loans >$250K require additional documentation.",
        "remediation": "Submit SBA Form 912 and personal financial statement.",
    },
    "REG-005": {
        "name": "Minimum Operating History",
        "version": "2026-Q1-v1",
        "is_hard_block": True,
        "check": lambda co: (datetime.now().year - (co.get("founded_year") or datetime.now().year)) >= 2,
        "failure_reason": "Business must have at least 2 years of operating history.",
        "remediation": None,
    },
    "REG-006": {
        "name": "CRA Community Reinvestment",
        "version": "2026-Q1-v1",
        "is_hard_block": False,
        "check": lambda co: True,   # Always noted, never fails
        "note_type": "CRA_CONSIDERATION",
        "note_text": "Jurisdiction qualifies for Community Reinvestment Act consideration.",
    },
}

class ComplianceAgent(BaseApexAgent):
    def build_graph(self) -> Any:
        g = StateGraph(ComplianceState)
        def make_node(rid: str):
            async def _node(state: ComplianceState) -> ComplianceState:
                return await self._evaluate_rule(state, rid)
            return _node

        g.add_node("validate_inputs",      self._node_validate_inputs)
        g.add_node("load_company_profile", self._node_load_profile)
        g.add_node("evaluate_reg001",      make_node("REG-001"))
        g.add_node("evaluate_reg002",      make_node("REG-002"))
        g.add_node("evaluate_reg003",      make_node("REG-003"))
        g.add_node("evaluate_reg004",      make_node("REG-004"))
        g.add_node("evaluate_reg005",      make_node("REG-005"))
        g.add_node("evaluate_reg006",      make_node("REG-006"))
        g.add_node("write_output",         self._node_write_output)

        g.set_entry_point("validate_inputs")
        g.add_edge("validate_inputs",      "load_company_profile")
        g.add_edge("load_company_profile", "evaluate_reg001")

        # Conditional edges: stop at hard block, proceed otherwise
        rules = ["REG-001", "REG-002", "REG-003", "REG-004", "REG-005", "REG-006"]
        for i in range(len(rules)):
            src = f"evaluate_{rules[i].lower().replace('-','')}"
            nxt = f"evaluate_{rules[i+1].lower().replace('-','')}" if i < len(rules)-1 else "write_output"
            
            # Closure to capture nxt
            def _router(s, _nxt=nxt):
                return "write_output" if s["has_hard_block"] else _nxt
            
            g.add_conditional_edges(src, _router)

        g.add_edge("write_output", END)
        return g.compile()

    def _initial_state(self, application_id: str) -> ComplianceState:
        return ComplianceState(
            application_id=application_id, session_id=self.session_id,
            applicant_id=None, company_profile=None, rule_results=[],
            has_hard_block=False, block_rule_id=None, errors=[], next_agent=None
        )

    async def _node_validate_inputs(self, state: ComplianceState) -> ComplianceState:
        t = time.time()
        app_id = state["application_id"]
        app = await LoanApplicationAggregate.load(self.store, app_id)
        
        ms = int((time.time() - t) * 1000)
        await self._record_input_validated(["application_id"], ms)
        await self._record_node_execution("validate_inputs", ["application_id"], ["applicant_id"], ms)
        return {**state, "applicant_id": app.applicant_id}

    async def _node_load_profile(self, state: ComplianceState) -> ComplianceState:
        t = time.time()
        applicant_id = state["applicant_id"]
        
        from dataclasses import asdict
        profile_obj = await self.registry.get_company(applicant_id)
        profile = asdict(profile_obj) if profile_obj else {}
        
        # Add compliance flags
        flags_obj = await self.registry.get_compliance_flags(applicant_id)
        profile["compliance_flags"] = [asdict(f) for f in flags_obj]
        
        ms = int((time.time() - t) * 1000)
        await self._record_tool_call("registry_query", applicant_id, "profile and flags", ms)
        await self._record_node_execution("load_company_profile", ["applicant_id"], ["company_profile"], ms)
        return {**state, "company_profile": profile}

    async def _evaluate_rule(self, state: ComplianceState, rule_id: str) -> ComplianceState:
        t = time.time()
        reg = REGULATIONS[rule_id]
        co = state["company_profile"] or {}
        app_id = state["application_id"]
        
        passes = reg["check"](co)
        evidence_hash = self._sha(f"{rule_id}-{co.get('company_id')}-{passes}")
        
        finding = {
            "rule_id": rule_id,
            "rule_name": reg["name"],
            "passed": passes,
            "is_hard_block": reg.get("is_hard_block", False),
            "evidence_hash": evidence_hash
        }
        
        # Record event for step
        if rule_id == "REG-006":
            event = ComplianceRuleNoted(
                application_id=app_id, session_id=self.session_id,
                rule_id=rule_id, rule_name=reg["name"],
                note_type=reg["note_type"], note_text=reg["note_text"],
                evaluated_at=datetime.now()
            ).to_store_dict()
        elif passes:
            event = ComplianceRulePassed(
                application_id=app_id, session_id=self.session_id,
                rule_id=rule_id, rule_name=reg["name"], rule_version=reg["version"],
                evidence_hash=evidence_hash, evaluation_notes="Deterministic check passed",
                evaluated_at=datetime.now()
            ).to_store_dict()
        else:
            event = ComplianceRuleFailed(
                application_id=app_id, session_id=self.session_id,
                rule_id=rule_id, rule_name=reg["name"], rule_version=reg["version"],
                failure_reason=reg["failure_reason"], is_hard_block=reg["is_hard_block"],
                remediation_available=reg.get("remediation") is not None,
                remediation_description=reg.get("remediation"),
                evidence_hash=evidence_hash, evaluated_at=datetime.now()
            ).to_store_dict()
            
        await self._append_with_retry(f"compliance-{app_id}", [event])
        
        new_results = state["rule_results"] + [finding]
        has_block = state["has_hard_block"] or (not passes and reg.get("is_hard_block", False))
        
        ms = int((time.time() - t) * 1000)
        node_name = f"evaluate_{rule_id.lower().replace('-','_')}"
        await self._record_node_execution(node_name, ["company_profile"], ["rule_results"], ms)
        
        return {
            **state,
            "rule_results": new_results,
            "has_hard_block": has_block,
            "block_rule_id": rule_id if not passes and reg.get("is_hard_block", False) else state["block_rule_id"]
        }

    async def _node_write_output(self, state: ComplianceState) -> ComplianceState:
        t = time.time()
        app_id = state["application_id"]
        
        # 1. ComplianceCheckInitiated
        init_event = ComplianceCheckInitiated(
            application_id=app_id, session_id=self.session_id,
            regulation_set_version="2026-Q1-v1",
            rules_to_evaluate=list(REGULATIONS.keys()),
            initiated_at=datetime.now()
        ).to_store_dict()
        await self._append_with_retry(f"compliance-{app_id}", [init_event])
        
        # 2. ComplianceCheckCompleted
        results = state["rule_results"]
        passed = len([r for r in results if r["passed"]])
        failed = len([r for r in results if not r["passed"] and r["rule_id"] != "REG-006"])
        noted = 1 if any(r["rule_id"] == "REG-006" for r in results) else 0
        
        verdict = ComplianceVerdict.CLEAR
        if state["has_hard_block"]:
            verdict = ComplianceVerdict.BLOCKED
        elif failed > 0:
            verdict = ComplianceVerdict.CONDITIONAL
            
        comp_event = ComplianceCheckCompleted(
            application_id=app_id, session_id=self.session_id,
            rules_evaluated=len(results), rules_passed=passed,
            rules_failed=failed, rules_noted=noted,
            has_hard_block=state["has_hard_block"],
            overall_verdict=verdict, completed_at=datetime.now()
        ).to_store_dict()
        positions = await self._append_with_retry(f"compliance-{app_id}", [comp_event])
        
        # 3. Trigger Downstream
        if state["has_hard_block"]:
            # Decline immediately
            decline_event = ApplicationDeclined(
                application_id=app_id,
                decline_reasons=[f"Failed critical compliance rule: {state['block_rule_id']}"],
                declined_by=self.agent_id,
                declined_at=datetime.now(),
                adverse_action_notice_required=True
            ).to_store_dict()
            await self._append_with_retry(f"loan-{app_id}", [decline_event])
            summary = f"Compliance BLOCKED by {state['block_rule_id']}. Application declined."
        else:
            # Request final decision
            decision_req = DecisionRequested(
                application_id=app_id,
                requested_at=datetime.now(),
                all_analyses_complete=True,
                triggered_by_event_id=self.session_id
            ).to_store_dict()
            await self._append_with_retry(f"loan-{app_id}", [decision_req])
            summary = f"Compliance {verdict.value}. Decision generation requested."
            
        ms = int((time.time() - t) * 1000)
        events_written = [
            {"stream_id": f"compliance-{app_id}", "event_type": "ComplianceCheckCompleted", "stream_position": positions[0]},
        ]
        await self._record_output_written(events_written, summary)
        await self._record_node_execution("write_output", ["rule_results"], ["next_agent"], ms)
        
        return {**state, "next_agent": "decision_orchestrator" if not state["has_hard_block"] else None}
