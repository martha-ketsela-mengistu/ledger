import pytest
import asyncio
import asyncpg
import time
from uuid import uuid4

from src.event_store import EventStore
from src.projections.daemon import ProjectionDaemon
from src.projections.application_summary import ApplicationSummary
from src.projections.agent_performance import AgentPerformanceLedger
from src.projections.compliance_audit import ComplianceAuditView

DB_URL = "postgresql://postgres:apex@localhost/apex_ledger"

@pytest.fixture
async def db_pool():
    pool = await asyncpg.create_pool(DB_URL, min_size=10, max_size=50)
    
    async with pool.acquire() as conn:
        # We need the foundational tables to exist for EventStore to append
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS event_streams (
                stream_id TEXT PRIMARY KEY,
                aggregate_type TEXT NOT NULL,
                current_version BIGINT NOT NULL,
                archived_at TIMESTAMPTZ
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS events (
                global_position BIGSERIAL PRIMARY KEY,
                event_id UUID UNIQUE NOT NULL,
                stream_id TEXT NOT NULL,
                stream_position BIGINT NOT NULL,
                event_type TEXT NOT NULL,
                event_version INT NOT NULL,
                payload JSONB NOT NULL,
                metadata JSONB NOT NULL,
                recorded_at TIMESTAMPTZ NOT NULL
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS outbox (
                event_id UUID PRIMARY KEY,
                destination TEXT NOT NULL,
                payload JSONB NOT NULL
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS projection_checkpoints (
                projection_name TEXT PRIMARY KEY,
                last_position BIGINT NOT NULL DEFAULT 0,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        ''')
        
    yield pool
    await pool.close()

@pytest.fixture
async def store():
    s = EventStore(DB_URL)
    await s.connect()
    # Boost pool size slightly for 50 concurrent tests
    if s._pool:
        await s._pool.close()
        s._pool = await asyncpg.create_pool(DB_URL, min_size=10, max_size=60)
    yield s
    await s.close()

@pytest.mark.asyncio
async def test_projection_lag_under_load(db_pool, store):
    """
    Test Projection lag SLO under simulated load of 50 concurrent command handlers.
    """
    projections = [ApplicationSummary(), AgentPerformanceLedger(), ComplianceAuditView()]
    
    async with db_pool.acquire() as conn:
        for p in projections:
            await p.initialize(conn)

    daemon = ProjectionDaemon(store, projections, db_pool=db_pool)
    daemon_task = asyncio.create_task(daemon.run_forever(poll_interval_ms=100))
    
    # 1. 50 concurrent command handlers writing 3 events each
    async def simulate_handler_worker(worker_id: int):
        app_id = f"APEX-LOAD-{uuid4().hex[:6]}"
        stream_id = f"loan-{app_id}"
        events = [
            {"event_type": "ApplicationSubmitted", "payload": {"application_id": app_id, "requested_amount_usd": 150000}},
            {"event_type": "CreditAnalysisCompleted", "payload": {"application_id": app_id, "duration_ms": 1200, "confidence": 0.9, "model_versions": {"credit": "v2.5"}}},
            {"event_type": "ApplicationApproved", "payload": {"application_id": app_id, "approved_amount_usd": 150000}}
        ]
        await store.append(stream_id, events, -1)
    
    # Fire all 50 concurrently
    start_time = time.time()
    workers = [simulate_handler_worker(i) for i in range(50)]
    await asyncio.gather(*workers)
    write_duration = time.time() - start_time
    
    # Allow daemon to catch up and measure lag
    max_wait = 10.0
    start_wait = time.time()
    
    while True:
        # Get the actual lag metric directly from the daemon mapping
        lag_summary = await daemon.get_lag("ApplicationSummary")
        lag_audit = await daemon.get_lag("ComplianceAuditView")
        
        if lag_summary == 0 and lag_audit == 0:
            break
            
        if time.time() - start_wait > max_wait:
            pytest.fail(f"SLO FAILED: Projections took too long to catch up! Lag summary: {lag_summary}")
        
        await asyncio.sleep(0.2)
        
    catch_up_time = time.time() - start_wait
    
    daemon._running = False
    daemon_task.cancel()
    
    # Assert successful processing
    async with db_pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM application_summary WHERE application_id LIKE 'APEX-LOAD-%'")
        assert count >= 50, f"Expected 50 state rows, but found {count}."


@pytest.mark.asyncio
async def test_rebuild_from_scratch(db_pool, store):
    """
    Tests that a projection can rebuild its state flawlessly from position 0 without downtime.
    """
    projection = ApplicationSummary()
    async with db_pool.acquire() as conn:
        await projection.initialize(conn)
        
        # Insert a seed event
        app_id = f"APEX-REBUILD-{uuid4().hex[:6]}"
        stream_id = f"loan-{app_id}"
        
        await store.append(stream_id, [{
            "event_type": "ApplicationSubmitted", 
            "payload": {"application_id": app_id, "requested_amount_usd": 500000}
        }], -1)
        
        # Corrupt table directly to prove rebuild works
        await conn.execute("DELETE FROM application_summary WHERE application_id = $1", app_id)
        
        # Run Rebuild
        await projection.rebuild_from_scratch(conn, store)
        
        # State should be re-materialized
        row = await conn.fetchrow("SELECT * FROM application_summary WHERE application_id = $1", app_id)
        assert row is not None, "Rebuild failed to recreate the missing row!"
        assert float(row["requested_amount_usd"]) == 500000
