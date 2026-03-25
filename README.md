# The Ledger — Agentic Event Store & Audit Infrastructure

Modular agentic pipeline for autonomous loan processing with full auditability, cryptographic integrity, and temporal query support.

---

## 1. Setup & Provisioning

### Database Provisioning
The Ledger requires PostgreSQL 16+. You can provision a local instance using Docker:

```powershell
docker run -d `
  --name apex-ledger `
  -e POSTGRES_PASSWORD=apex `
  -e POSTGRES_DB=apex_ledger `
  -p 5432:5432 `
  postgres:16
```

### Environment Setup
1. **Install Dependencies**:
   ```powershell
   uv sync
   ```
2. **Configure Environment**:
   ```powershell
   cp .env.example .env
   # Edit .env to set DATABASE_URL and OPENROUTER_API_KEY
   ```

---

## 2. Database & Data Migration

### Apply Schema
Initializes the event store, projections, and outbox tables:
```powershell
uv run python scripts/apply_schema.py
```

### Generate Baseline Data
Populates the applicant registry and seeds the event store with synthetic historical data:
```powershell
uv run python datagen/generate_all.py --db-url postgresql://postgres:apex@localhost/apex_ledger
```

---

## 3. Running All Phases

To process an application through the complete 5-agent lifecycle (Document → Credit → Fraud → Compliance → Decision):

```powershell
# Run the full pipeline
uv run scripts/run_pipeline.py --application APEX-0001 --phase all

```

---

## 4. MCP Server Startup

The Ledger exposes its event store and projections via the Model Context Protocol (MCP).

**Start the server**:
```powershell
python -m src.mcp.server
```

### Server Capabilities:
- **Tools (8)**: `submit_application`, `start_agent_session`, `record_credit_analysis`, `record_fraud_screening`, `record_compliance_check`, `generate_decision`, `record_human_review`, `run_integrity_check`.
- **Resources (6)**: `ledger://applications/{id}`, `ledger://applications/{id}/compliance`, `ledger://applications/{id}/audit-trail`, etc.

---

## 5. Audit & Query Examples

### Temporal Query (As-Of)
Retrieve the state of compliance rules at a specific point in time:
```powershell
# Usage via MCP Resource
ledger://applications/APEX-0001/compliance?as_of=2026-03-25T14:45:00Z
```

### Integrity Verification
Verify the cryptographic chain for a entity's event stream:
```powershell
# Via MCP Tool
run_integrity_check(entity_type="loan", entity_id="APEX-0001")
```

---

## Testing
Run the verification suite for concurrency, upcasting, and core infrastructure:
```powershell
uv run pytest tests/
```
