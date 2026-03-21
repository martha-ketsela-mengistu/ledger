import pytest
import asyncio
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.event_store import EventStore
from src.models.events import OptimisticConcurrencyError
from tests.conftest import db_url  # reused from conftest

@pytest.fixture
async def store(db_url):
    s = EventStore(db_url)
    await s.connect()
    async with s._pool.acquire() as conn:
        await conn.execute("TRUNCATE events, event_streams, outbox CASCADE")
    yield s
    await s.close()

@pytest.mark.asyncio
async def test_double_decision_occ(store):
    stream_id = "loan-001"
    
    # 1. Setup: Stream at version 3 (events 1, 2, 3)
    # We append 3 events manually to reach version 3
    initial_events = [
        {"event_type": f"Event{i}", "payload": {"i": i}} 
        for i in range(1, 4)
    ]
    await store.append(stream_id, initial_events, expected_version=-1)
    assert await store.stream_version(stream_id) == 3
    
    # 2. Define the concurrent task
    async def try_append():
        event = {"event_type": "CreditAnalysisCompleted", "payload": {"agent": "AI"}}
        return await store.append(stream_id, [event], expected_version=3)

    # 3. Spawn two concurrent tasks
    # We use asyncio.gather and catch exceptions manually to verify (c)
    results = await asyncio.gather(
        try_append(),
        try_append(),
        return_exceptions=True
    )

    # 4. Assertions
    successes = [r for r in results if isinstance(r, list)]
    failures = [r for r in results if isinstance(r, OptimisticConcurrencyError)]
    
    # (a) Exactly one must succeed
    assert len(successes) == 1, f"Expected 1 success, got {len(successes)}"
    assert len(failures) == 1, f"Expected 1 OptimisticConcurrencyError, got {len(failures)}"
    
    # (b) Winning task's event has stream_position=4
    # success result is a list of positions: [4]
    assert successes[0] == [4]
    
    # (a) Total events appended to the stream = 4 (3 initial + 1 from winner)
    events = await store.load_stream(stream_id)
    assert len(events) == 4, f"Expected 4 events total, found {len(events)}"
    
    # Final check of stream version
    assert await store.stream_version(stream_id) == 4

if __name__ == "__main__":
    import os
    os.environ["TEST_DB_URL"] = "postgresql://postgres:apex@localhost:5432/apex_ledger"
    pytest.main([__file__, "-v"])
