"""
src/integrity/audit_chain.py
============================
Cryptographic audit chain for tamper-evident event logs.

Implements a SHA-256 hash chain over event streams. Each 
AuditIntegrityCheckRun event records a hash of all preceding events 
plus the previous integrity hash, forming a blockchain-style chain.
"""
from __future__ import annotations
import hashlib
import json
import logging
from datetime import datetime, UTC
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class IntegrityCheckResult:
    """Result of a cryptographic integrity check."""
    entity_type: str
    entity_id: str
    events_verified: int
    chain_valid: bool
    tamper_detected: bool
    integrity_hash: str
    previous_hash: str | None
    checked_at: datetime


def _hash_event(event) -> str:
    """Compute SHA-256 hash of an event's payload deterministically."""
    payload_str = json.dumps(event.payload, sort_keys=True, default=str)
    content = f"{event.event_type}:{event.event_version}:{payload_str}"
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _compute_chain_hash(previous_hash: str | None, event_hashes: list[str]) -> str:
    """Compute the chain hash: sha256(previous_hash + concatenated event hashes)."""
    combined = (previous_hash or "GENESIS") + "".join(event_hashes)
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()


async def run_integrity_check(
    store: Any,
    entity_type: str,
    entity_id: str,
) -> IntegrityCheckResult:
    """
    1. Load all events for the entity's primary stream
    2. Load the last AuditIntegrityCheckRun event (if any)
    3. Hash the payloads of all events since the last check
    4. Verify hash chain: new_hash = sha256(previous_hash + event_hashes)
    5. Append new AuditIntegrityCheckRun event to audit-{entity_type}-{entity_id} stream
    6. Return result with: events_verified, chain_valid (bool), tamper_detected (bool)
    """
    # Determine the primary stream
    stream_map = {
        "loan": f"loan-{entity_id}",
        "credit": f"credit-{entity_id}",
        "fraud": f"fraud-{entity_id}",
        "compliance": f"compliance-{entity_id}",
        "docpkg": f"docpkg-{entity_id}",
    }
    primary_stream = stream_map.get(entity_type, f"{entity_type}-{entity_id}")
    audit_stream = f"audit-{entity_type}-{entity_id}"

    # 1. Load all primary stream events
    primary_events = await store.load_stream(primary_stream)

    # 2. Load previous audit checks
    previous_hash = None
    last_check_position = 0
    try:
        audit_events = await store.load_stream(audit_stream)
        for ae in reversed(audit_events):
            if ae.event_type == "AuditIntegrityCheckRun":
                previous_hash = ae.payload.get("integrity_hash")
                last_check_position = ae.payload.get("events_verified_count", 0)
                break
    except Exception:
        audit_events = []

    # 3. Hash events since last check
    new_events = primary_events[last_check_position:]
    event_hashes = [_hash_event(e) for e in new_events]

    # 4. Compute chain hash and verify
    new_hash = _compute_chain_hash(previous_hash, event_hashes)

    # Verify: if there was a previous check, recompute from scratch to detect tampering
    tamper_detected = False
    chain_valid = True

    if previous_hash and last_check_position > 0:
        # Re-hash the events that were already verified
        old_events = primary_events[:last_check_position]
        old_hashes = [_hash_event(e) for e in old_events]
        recomputed = _compute_chain_hash(None, old_hashes)

        # Walk the audit chain to verify
        expected_hash = None
        for ae in audit_events:
            if ae.event_type == "AuditIntegrityCheckRun":
                check_events = primary_events[:ae.payload.get("events_verified_count", 0)]
                check_hashes = [_hash_event(e) for e in check_events[
                    (0 if expected_hash is None else len(check_events)):
                ]]
                expected = _compute_chain_hash(expected_hash, check_hashes)
                if expected != ae.payload.get("integrity_hash"):
                    tamper_detected = True
                    chain_valid = False
                    logger.error(f"TAMPER DETECTED in {primary_stream}! "
                                 f"Expected hash {expected}, got {ae.payload.get('integrity_hash')}")
                    break
                expected_hash = ae.payload.get("integrity_hash")

    total_verified = len(primary_events)

    # 5. Append AuditIntegrityCheckRun event
    audit_event = {
        "event_type": "AuditIntegrityCheckRun",
        "event_version": 1,
        "payload": {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "check_timestamp": datetime.now(UTC).isoformat(),
            "events_verified_count": total_verified,
            "integrity_hash": new_hash,
            "previous_hash": previous_hash,
            "chain_valid": chain_valid,
            "tamper_detected": tamper_detected,
        },
    }

    audit_version = await store.stream_version(audit_stream)
    await store.append(audit_stream, [audit_event], audit_version)

    # 6. Return result
    return IntegrityCheckResult(
        entity_type=entity_type,
        entity_id=entity_id,
        events_verified=total_verified,
        chain_valid=chain_valid,
        tamper_detected=tamper_detected,
        integrity_hash=new_hash,
        previous_hash=previous_hash,
        checked_at=datetime.now(UTC),
    )
