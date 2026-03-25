# DESIGN.md

## 1. Aggregate Boundary Justification

### LoanApplication vs. ComplianceRecord
In this implementation, `LoanApplication` is the primary consistency boundary for the lifecycle of a loan (submission, decisioning, and final status). 

**Separation Rationale:**
`ComplianceRecord` is treated increasingly as a separate sub-domain. While currently many compliance events land on the `loan-{id}` stream for audit simplicity, a separate `ComplianceRecord` aggregate is justified for:
- **Parallel Evaluation:** Under peak load, multiple compliance rules (e.g., AML, KYC, Sanctions) are evaluated by different agents or sub-processes concurrently. By using a dedicated **`compliance-{application_id}`** stream, these evaluations avoid collision with the main `loan-{application_id}` lifecycle events.
- **Write Contention failure mode:** If merged into a single stream, two agents completing different regulatory checks at the same millisecond would force one into a retry loop on the shared aggregate. In extreme contention, the agent's retry budget (5) could be exhausted, causing a legitimate regulatory evaluation to "fail" due to technical noise rather than domain logic.

## 2. Projection Strategy

| Projection | Strategy | SLO Commitment | Rationale |
|------------|----------|----------------|-----------|
| `ApplicationSummary` | Async (Daemon) | p99 < 50ms | Optimized for UI/Dashboard read speed. |
| `ComplianceAuditView` | Async (Daemon) | p99 < 200ms | Supports complex temporal queries for regulatory audits. |

### Snapshot Strategy: `ComplianceAuditView`
- **Trigger:** Manual / Event-count. Snapshots are recommended every 100 events to keep replay under 200ms.
- **Temporal Query logic:** The view uses a "Point-in-Time" query on the `compliance_audit_events` table (indexing `application_id` and `evaluated_at`). 
- **Invalidation:** Since events are immutable, snapshots are never "invalidated"—they are simply superseded by newer snapshots. A replay from the last snapshot version always yields the correct current state.

## 3. Concurrency Analysis

Under a peak load of **100 concurrent applications** with **4 agents** each, we expect:
- **Collision Rate:** Estimated 10-15% OCC errors on the `loan-{id}` stream if agents attempt to write recommendations simultaneously (e.g., Credit and Fraud finishing at the same time).
- **Retry Strategy:** implement **Exponential Backoff** (`BaseApexAgent._append_with_retry`) with a starting delay of 100ms.
- **Maximum Budget:** **5 retries**. If the budget is exhausted, the agent returns a `PreconditionFailed` or `InternalError` to the orchestrator, which must then re-queue the task.

## 4. Upcasting Inference Decisions

| Field | Inference Strategy | Error Risk | Consequence |
|-------|-------------------|------------|-------------|
| `model_version` | Sentinel: `"legacy-pre-2026"` | Low | Minor bias in historical performance analytics. |
| `confidence_score` | `None` (Unknown) | 0% | Consumers must handle nulls; avoids dangerous false precision. |
| `regulatory_basis` | Chronological default | Medium | Might misattribute a check to a newer regulation set version. |

**Null vs. Inference:**
We chose `None` for `confidence_score` because fabricating a number (e.g., `0.0` or `0.5`) could be misinterpreted as a low-confidence result by a human LO during a look-back audit. Inferences are only used when the field is necessary for system logic (e.g., `model_version`).

## 5. EventStoreDB Comparison

| Ledger Component | EventStoreDB Concept |
|------------------|----------------------|
| Postgres `events` table | Stream Storage (Append-only log) |
| `loan-{id}` streams | Stream IDs / Categories |
| `load_all()` | `$all` stream subscription |
| `ProjectionDaemon` | Persistent & Catch-up Subscriptions |

**Built-in Advantages of EventStoreDB:**
- **Native gRPC/HTTP:** Eliminates the need for custom `EventStore` classes and connection pooling logic.
- **Internal Projection Engine:** ESDB runs projections (JavaScript) on the server side, whereas our implementation requires a separate Python `Daemon` process and `outbox` logic.

## 6. What I would do differently

If given another full day, I would reconsider the **on-read upcasting for `DecisionGenerated`**. 
Currently, the upcaster for `DecisionGenerated` v1→v2 performs N lookup reads to other streams (`agent-{id}`) to reconstruct the `model_versions` dictionary. 

**The Reconsideration:**
Performing I/O inside an upcaster is a "code smell" in event sourcing because it makes stream loading non-deterministic (dependent on other streams) and slow. I would instead move this "enrichment" into a **Projection** or a **Read-Model Cache**. This would keep the core `EventStore` loaders pure and blisteringly fast, at the cost of slight eventual consistency in the audit view.
