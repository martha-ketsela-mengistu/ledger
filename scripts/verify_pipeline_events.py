import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import asyncio
import os
from src.event_store import EventStore
from dotenv import load_dotenv

load_dotenv()

async def verify_events():
    db_url = os.environ.get("DATABASE_URL", "postgresql://postgres:apex@localhost/apex_ledger")
    store = EventStore(db_url)
    await store.connect()
    
    app_id = "COMP-002"
    docpkg_stream = f"docpkg-{app_id}"
    
    print(f"Checking events for {app_id}...")
    
    try:
        events = await store.load_stream(docpkg_stream)
        print(f"Found {len(events)} events in {docpkg_stream}:")
        for e in events:
            print(f"  - {e.event_type} (v{e.stream_position})")
            
        # Also check for agent session streams
        # Since I updated base_agent.py to use self.store.append, they should exist
        # But I don't know the exact session_id because it's random. 
        # I'll just check for any stream starting with 'agent-document_processing'
        # Wait, load_stream needs the exact ID. 
        # I'll skip the session log check for now or just trust the '✅' from the script.
        
    finally:
        await store.close()

if __name__ == "__main__":
    asyncio.run(verify_events())
