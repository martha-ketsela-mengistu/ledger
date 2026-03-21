import pytest
import uuid
from src.event_store import EventStore
from src.models.events import StoredEvent

@pytest.fixture
async def store(db_url):
    s = EventStore(db_url)
    await s.connect()
    async with s._pool.acquire() as conn:
        await conn.execute("TRUNCATE events, event_streams, outbox CASCADE")
    yield s
    await s.close()

@pytest.mark.asyncio
async def test_causal_metadata_threading(store):
    stream_id = "loan-test-metadata"
    events = [{"event_type": "TestEvent", "payload": {"foo": "bar"}}]
    
    correlation_id = str(uuid.uuid4())
    causation_id = str(uuid.uuid4())
    
    # Append with metadata
    await store.append(
        stream_id, 
        events, 
        expected_version=-1,
        correlation_id=correlation_id,
        causation_id=causation_id
    )
    
    # Load and verify
    loaded_events = await store.load_stream(stream_id)
    assert len(loaded_events) == 1
    stored = loaded_events[0]
    
    assert isinstance(stored, StoredEvent)
    assert stored.metadata.get("correlation_id") == correlation_id
    assert stored.metadata.get("causation_id") == causation_id
    
    # Verify through load_all
    all_events = []
    async for e in store.load_all():
        all_events.append(e)
    
    assert len(all_events) >= 1
    found = next(e for e in all_events if e.stream_id == stream_id)
    assert found.metadata.get("correlation_id") == correlation_id
    assert found.metadata.get("causation_id") == causation_id

@pytest.mark.asyncio
async def test_stream_metadata_retrieval(store):
    stream_id = "loan-test-metadata-meta"
    events = [{"event_type": "TestEvent", "payload": {"foo": "bar"}}]
    
    await store.append(stream_id, events, expected_version=-1)
    
    meta = await store.get_stream_metadata(stream_id)
    assert meta is not None
    assert meta.stream_id == stream_id
    assert meta.current_version == 1 # First event at position 1
