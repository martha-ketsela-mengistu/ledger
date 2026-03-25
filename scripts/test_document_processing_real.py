import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import asyncio
import os
import json
from dotenv import load_dotenv
from unittest.mock import AsyncMock, MagicMock
from src.agents.document_agent import DocumentProcessingAgent
from src.models.events import DocumentType

load_dotenv()

async def run_real_test():
    app_id = "COMP-002"
    print(f"Testing DocumentProcessingAgent against {app_id}...")

    # Set documents directory to make sure it looks in the right place
    os.environ["DOCUMENTS_DIR"] = os.path.abspath(os.path.join(os.path.dirname(__file__), "../documents"))

    # Initializing a mock store
    store = MagicMock()
    store.load_stream = AsyncMock(return_value=[])
    store.append = AsyncMock(return_value=None)
    store.stream_version = AsyncMock(return_value=1)

    agent = DocumentProcessingAgent(
        agent_id="doc-agent-real-test",
        agent_type="document_processing",
        store=store,
        registry=MagicMock()
    )

    try:
        await agent.process_application(app_id)
        
        print("\n✅ DocumentProcessingAgent completed successfully!")
        
        # Checking append calls to see events
        print("\nEvents appended to store:")
        for call in store.append.call_args_list:
            args, kwargs = call
            stream_id = kwargs.get('stream_id')
            event = kwargs.get('events', [{}])[0]
            if not event:
                event = kwargs.get('event_dict', {})
            print(f"  Stream: {stream_id} | EventType: {event.get('event_type')}")
            if event.get('event_type') == 'ExtractionCompleted':
                facts = event.get('payload', {}).get('facts', {})
                print(f"    --> Extracted: {json.dumps(facts, indent=2)}")

    except Exception as e:
        print(f"\n❌ DocumentProcessingAgent failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(run_real_test())
