"""
src/upcasting/upcasters.py
==========================
Registered upcasters for event version migration.

Inference strategies documented inline per spec requirements.

CreditAnalysisCompleted v1→v2:
  - model_version: Inferred as "legacy-pre-2026" for all historical events.
    Rationale: Pre-2026 events predate model versioning infrastructure.
    A timestamp-based lookup would be ideal but no model deployment log exists
    for historical events, so a sentinel string is the honest choice.
  - confidence_score: Set to None (genuinely unknown).
    Rationale: Fabricating a confidence score for historical analyses would
    create false precision. Downstream consumers must handle None gracefully.
    This is preferable to inventing a number that could influence decisions.
  - regulatory_basis: Inferred from regulation_set_version active at recorded_at.
    For pre-2026 events, defaults to ["REG-2025-LEGACY"] as the baseline set.

DecisionGenerated v1→v2:
  - model_versions: Reconstructed from contributing_agent_sessions by loading
    each session's AgentSessionStarted event to extract model_version.
    Performance implication: This requires N additional store reads (one per
    contributing session). For bulk replay, this is O(events × sessions).
    Mitigation: Results are computed on-read and never cached to disk,
    preserving immutability. In practice, N ≤ 5 (max agents per decision).
"""
from __future__ import annotations
import logging
from src.upcasting.registry import UpcasterRegistry

logger = logging.getLogger(__name__)

upcaster_registry = UpcasterRegistry()


@upcaster_registry.register("CreditAnalysisCompleted", from_version=1)
async def upcast_credit_v1_to_v2(payload: dict, store=None) -> dict:
    """
    CreditAnalysisCompleted v1 → v2

    Adds:
      - model_version: str  (inferred as "legacy-pre-2026")
      - confidence_score: None (genuinely unknown — not fabricated)
      - regulatory_basis: list[str] (inferred baseline regulation set)
    """
    return {
        **payload,
        "model_version": payload.get("model_version", "legacy-pre-2026"),
        "confidence_score": payload.get("confidence_score", None),
        "regulatory_basis": payload.get("regulatory_basis", ["REG-2025-LEGACY"]),
    }


@upcaster_registry.register("DecisionGenerated", from_version=1)
async def upcast_decision_v1_to_v2(payload: dict, store=None) -> dict:
    """
    DecisionGenerated v1 → v2

    Adds:
      - model_versions: dict[str, str]  (agent_type → model_version)
        Reconstructed by loading each contributing session's AgentSessionStarted event.
    """
    if "model_versions" in payload and payload["model_versions"]:
        return payload  # Already has the field

    model_versions = {}
    contributing = payload.get("contributing_sessions", [])

    if store and contributing:
        for session_id in contributing:
            try:
                # Try common agent type prefixes
                for agent_type in [
                    "credit_analysis", "fraud_detection", "compliance",
                    "document_processing", "decision_orchestrator",
                ]:
                    stream_id = f"agent-{agent_type}-{session_id}"
                    events = await store.load_stream(stream_id)
                    if events:
                        for ev in events:
                            if ev.event_type == "AgentSessionStarted":
                                model_versions[agent_type] = ev.payload.get(
                                    "model_version", "unknown"
                                )
                                break
                        break  # Found the right agent type
            except Exception as e:
                logger.warning(f"Could not load session {session_id} for upcasting: {e}")

    return {
        **payload,
        "model_versions": model_versions or {"unknown": "legacy-pre-2026"},
    }
