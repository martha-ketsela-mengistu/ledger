# DOMAIN_NOTES.md — The Ledger: Event Sourcing Reasoning

## 1. EDA vs. ES Distinction

**The scenario:** A component uses callbacks (like LangChain traces) to capture event-like data.

**Is it EDA or ES?**

It is **EDA** — not Event Sourcing.

LangChain callbacks are observability hooks. They emit notifications that something happened (a side-effect of execution), but the state they capture is not reconstructible from those events alone. The callbacks are fire-and-forget: losing them does not allow you to replay computation or recover state. They produce a log, not a ledger. If the tracing service is down, the agent will continue to function normally, but the traces will be lost. In ES, if the event store is down, the agent will not be able to append events and will fail.

**Distinction table:**

| Property | LangChain Callbacks | Event Sourcing (The Ledger) |
|---|---|---|
| Events are the source of truth | State lives in memory/DB; events are side-notes | State is *derived* from events |
| Full replay possible | Cannot reconstruct agent state from callbacks | Replaying stream rebuilds aggregate state |
| Append-only, immutable | Log may be overwritten/truncated | `events` table: never updated, never deleted |
| Optimistic concurrency | No version enforcement | `expected_version` on every append |
| Crash recovery | Agent restarts from scratch | Gas Town: replay session stream to resume |

**What exactly would change if redesigned as The Ledger:**

1. **Storage:** Callbacks write to `agent-{type}-{session_id}` streams in the append-only `events` table instead of emitting to a trace endpoint.
2. **State derivation:** Agent state would be reconstructed by calling `load_stream()` and applying events through `AgentSessionAggregate.apply()` — not by holding in-memory state that is lost on crash.
3. **Guarantees gained:** (a) Crash recovery without re-running LLM nodes that already completed. (b) An independently verifiable audit trail where every token cost and node execution is a durable event. (c) The ability to replay the agent's decision history at any point in time, which satisfies regulatory examination requirements.

The architectural shift: a LangChain trace is a **diagnostic side-channel**. An `AgentNodeExecuted` event in the event store is a **first-class fact** from which the system continues to operate.

---

## 2. The Aggregate Question

### The four aggregates 

- `LoanApplication`
- `AgentSession`
- `ComplianceRecord`
- `AuditLedger`

Their stream IDs are:

- `loan-{application_id}`
- `agent-{agent_id}-{session_id}`
- `compliance-{application_id}`
- `audit-{entity_type}-{entity_id}`

### One boundary I considered and rejected

I considered collapsing `ComplianceRecord` into `LoanApplication`, so all application-related facts would live in one stream:

- `loan-{application_id}` containing application events, compliance rule events, and final decision events

I rejected that boundary.

### Why I rejected it

It couples deterministic compliance evaluation too tightly to the broader application lifecycle.

That creates three concrete problems.

#### 1. Write contention problem

In Apex, multiple actors can touch the same application around the same time:

- credit agent updates credit outcome
- fraud agent updates fraud outcome
- compliance agent emits several rule evaluations
- decision orchestrator tries to generate a recommendation
- human reviewer may override

If all of that shares one `loan-{application_id}` stream, every compliance rule event contends on the same stream version as every other application-level change.

That means a burst of compliance rule appends increases OCC collisions for unrelated loan lifecycle updates.

#### 2. Coupling of invariants

Compliance has its own invariant set:

- no compliance clearance until all mandatory rules are evaluated
- every rule result must reference the regulation version evaluated
- hard-block rules should terminate the compliance flow early

Those are not the same invariants as:

- cannot approve while compliance is pending
- cannot transition from approved back to under review

If both live inside one giant aggregate, the aggregate becomes harder to reason about, slower to replay, and more fragile to change.

#### 3. Replay-noise problem

Loan officers and command handlers often need top-level loan state.

If the loan stream also contains every individual compliance rule evaluation, aggregate replay becomes noisier and semantically less clean. The aggregate stops representing one consistency boundary and starts representing “everything related to the application.”

### What the chosen boundary prevents

The chosen boundary prevents accidental coupling between:

- workflow coordination on the `LoanApplication` stream
- rule-by-rule regulatory evaluation on the `ComplianceRecord` stream

More concretely, it prevents this failure mode:

- compliance emits `REG-001`, `REG-002`, `REG-003`, `REG-004`, `REG-005`, `REG-006` results
- while those writes are happening, the orchestrator or a human review process also wants to append to the loan stream
- because everything shares one stream, unrelated business actions now collide on one version counter

By separating the streams:

- compliance contention stays inside `compliance-{application_id}`
- lifecycle contention stays inside `loan-{application_id}`
- the loan aggregate only needs the final compliance outcome or explicit triggering events to enforce application-level rules

That is the coupling problem the chosen boundary avoids.

## 3. Concurrency in Practice

**Scenario:** Two `CreditAnalysisAgent` instances simultaneously process the same loan application. Both read `credit-{id}` at `stream_version = 3` and call `append(stream_id="credit-{id}", events=[CreditAnalysisCompleted(...)], expected_version=3)`.

## Initial State

* Event stream version = **3**
* Two AI agents (**Agent A** and **Agent B**) both:

  * Read the stream at version 3
  * Independently compute a decision

---

## Sequence of Operations

### 1. Agent A attempts to append

```text
Agent A → append_events(expected_version=3)
```

---

### 2. Agent A succeeds

* Event store checks:

  * Current version = 3
  * Matches expected_version = 3 
* Events are appended
* Stream version becomes **4**

---


### 3. Agent B attempts to append

```text
Agent B → append_events(expected_version=3)
```

---

### 4. Agent B fails

* Event store checks:

  * Current version = 4
  * Does NOT match expected_version = 3
* Append is rejected

---

## What the Losing Agent Receives

Agent B receives a **`OptimisticConcurrencyError`**

---

## What Agent B Must Do Next

### 1. Reload the stream

* Fetch latest events from `credit-{id}`
* Observe updated version = **4**


### 2. Retry append

```text
append_events(expected_version=4)
```

---

## Why This Works

* Prevents **split-brain decisions**
* Ensures **business invariants are preserved**
* Avoids:

  * Locks
  * Distributed transactions

## 4. Projection Lag and Its Consequences

**Scenario:** Projection lag = 200ms. A loan officer queries "available credit limit" immediately after an agent commits a `DisbursementRecorded` event. The projection hasn't processed it yet — the officer sees the old limit.

**What the system does:**

The `ApplicationSummaryProjection` is an **eventually consistent read model**. It does not expose the live balance at the moment of the query; it exposes the balance as of the last processed `global_position`. This is a known property, not a bug.

**What I do not do:** I do not attempt to make the projection synchronously consistent by blocking the read until the event is processed. That defeats the purpose of the projection pattern and creates a bottleneck under load.

**What I do instead — the strategy has three parts:**

1. **Expose lag metadata:** Every projection response includes `{"data": {...}, "as_of_position": 1842, "lag_ms": 180}`. The UI can display "Balance as of 16:44:38" rather than implying real-time accuracy.

2. **UI communication:** The loan officer interface shows a badge: `⚡ Live balance pending — last updated 0.2s ago`. On the backend, the MCP resource returns HTTP header `X-Projection-Lag-Ms: 180`. The UI polls for refresh. For high-stakes decisions (approval), the UI makes a second call to the event-sourced endpoint to fetch current state before presenting a confirmation dialog.

**The failure mode I guard against:** Never use a stale projection value as a precondition for a write command. Command handlers always load the aggregate stream (`load_stream()` + `apply()`) before appending — never trust a projection for concurrency-sensitive decisions.

---

## 5. The Upcasting Scenario

**Schema evolution:**
- **2024:** `CreditDecisionMade { application_id, decision, reason }`
- **2026:** `CreditDecisionMade { application_id, decision, reason, model_version, confidence_score, regulatory_basis }`

**The upcaster:**

```python
class CreditDecisionMadeV1ToV2Upcaster:
    """
    Upcasts CreditDecisionMade from schema_version=1 (2024) to schema_version=2 (2026).
    Applied in-memory at read time. Never writes to the events table.
    """

    source_version: int = 1
    target_version: int = 2
    event_type: str = "CreditDecisionMade"

    def upcast(self, raw_payload: dict) -> dict:
        return {
            # Pass-through existing fields
            "application_id": raw_payload["application_id"],
            "decision": raw_payload["decision"],
            "reason": raw_payload["reason"],
            # Inferred / defaulted new fields
            "model_version": self._infer_model_version(raw_payload),
            "confidence_score": self._infer_confidence(raw_payload),
            "regulatory_basis": "pre-2026-framework",
        }

    def _infer_model_version(self, payload: dict) -> str:
        # Historical events predate model versioning.
        # We cannot recover the model that produced them.
        # We use a sentinel value that is unambiguous to downstream consumers.
        return "unknown-pre-2026"

    def _infer_confidence(self, payload: dict) -> float | None:
        # The original schema recorded no confidence.
        # We cannot fabricate a score — that would be fraudulent in a lending context.
        # None signals "not available" to consumers; they must treat it as unscored.
        return None
```

**Inference strategy for historical events:**

`model_version` → Use the sentinel `"unknown-pre-2026"`. This is **preferable to a plausible-looking value** for two reasons: (a) in a regulated lending system, an invented model version would corrupt audit trails — a regulator examining the event must know that this field was not recorded at origination time; (b) downstream projection code can branch on `model_version == "unknown-pre-2026"` to suppress model attribution in reports where it would be misleading.

`confidence_score` → `None`. Again, fabrication is not acceptable. The 2024 decision process may have had an internal confidence, but it was not persisted. Setting it to `0.5` (a common temptation) would imply medium confidence, which could affect re-run projections incorrectly. `None` is the honest value.

`regulatory_basis` → `"pre-2026-framework"` — a literal string indicating the event predates the current compliance schema. This allows compliance projections to filter or flag these events appropriately.

**Key principle:** Upcasters must be **honest about ignorance**. Sentinel values and `None` are correct. Invented plausible values are a data integrity violation in a financial audit context.

---

## 6. The Marten Async Daemon Parallel

**Marten 7.0 Async Daemon:** Distributes projection processing across multiple nodes using leader election (via Postgresql advisory locks) and shard assignment. Each node processes a subset of event streams in parallel. If a node fails, another node picks up its shard after a heartbeat timeout. This guards against a slow or failed node becoming a projection bottleneck.

**How I achieve the same pattern in Python:**

### Coordination Primitive: PostgreSQL Advisory Locks + Shard Assignment

```python
# Each projection worker acquires a shard lock before processing
# Shard = hash(stream_id) % NUM_SHARDS

async def acquire_shard_lock(conn: asyncpg.Connection, shard_id: int) -> bool:
    # pg_try_advisory_lock is non-blocking — returns False if another node holds it
    result = await conn.fetchval(
        "SELECT pg_try_advisory_lock($1)", shard_id
    )
    return result  # True = lock acquired, False = another worker owns this shard
```

**Full pattern:**

1. **Shard space:** Divide `stream_id` values into N shards (e.g., 8) using `hash(stream_id) % 8`.
2. **Worker acquisition:** Each `ProjectionDaemon` instance at startup tries to acquire advisory locks for all shards. It processes only the shards it holds.
3. **Heartbeat:** Each worker updates `projection_checkpoints.updated_at` every 5s. A watchdog checks: if `updated_at` is older than 15s, the shard is considered abandoned.
4. **Takeover:** Another worker calls `pg_try_advisory_lock` for the abandoned shard's ID. On success, it loads the checkpoint for that shard and resumes from `last_position`.
5. **Graceful release:** On shutdown, the worker calls `pg_advisory_unlock(shard_id)` — immediately making the shard available for another node to claim.

**Failure mode guarded against:**

**Split-brain duplicate processing.** Without the advisory lock, two workers could both believe they own the same shard after a network partition. Both would read the same events and apply them to the same projection tables, causing duplicate state mutations (e.g., double-counting application totals in `ApplicationSummaryProjection`).

The advisory lock is the single coordination primitive that makes this impossible: PostgreSQL guarantees only one session can hold `pg_advisory_lock(N)` at a time. The projection table rows are idempotent (using `ON CONFLICT DO UPDATE` keyed on `application_id + event_global_position`), which provides a second layer of defence if the lock is ever released after partial work.

**Why not Redis/ZooKeeper/etcd for coordination?**  
We already have PostgreSQL as the event store. Adding a separate coordination service introduces a second infrastructure dependency and a new failure mode (coordination service unavailable → all workers stall). PostgreSQL advisory locks are transactional, durable, and co-located with the data they protect — no additional infrastructure required.

