# The Ledger — Agentic Event Store & Audit Infrastructure

## Quick Start
```bash
# 1. Install dependencies (using uv)
uv sync

# 2. Start PostgreSQL
docker run -d -e POSTGRES_PASSWORD=apex -e POSTGRES_DB=apex_ledger -p 5432:5432 postgres:16

# 3. Set environment
cp .env.example .env

# 4. Generate all data (companies + documents + seed events → DB)
python datagen/generate_all.py --db-url postgresql://postgres:apex@localhost/apex_ledger

# 5. Run migrations (Apply schema)
uv run python scripts/apply_schema.py

# 6. Execute test suite
uv run pytest tests/test_concurrency.py -v
```

## Structure
- `src/schema.sql`: PostgreSQL schema (events, event_streams, projection_checkpoints, outbox).
- `src/event_store.py`: `EventStore` class (appends, stream versions, load).
- `src/models/events.py`: Pydantic models (BaseEvent, StoredEvent, StreamMetadata) + Exceptions.
- `src/aggregates/`: Aggregate roots (loan_application, agent_session).
- `src/commands/`: Handlers following the load → validate → determine → append pattern.

## Verifying Concurrency
Run the double-decision test to verify optimistic concurrency:
```bash
uv run pytest tests/test_concurrency.py
```
This test asserts:
1. Exactly one of two racing tasks succeeds.
2. The loser raises `OptimisticConcurrencyError`.
3. Total stream length is correct (4).
