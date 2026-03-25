import asyncio
import os
import json
import asyncpg
from dotenv import load_dotenv

load_dotenv()

async def check_events(application_id: str):
    db_url = os.environ.get("DATABASE_URL", "postgresql://postgres:apex@localhost/apex_ledger")
    conn = await asyncpg.connect(db_url)
    try:
        streams = [
            f"loan-{application_id}",
            f"docpkg-{application_id}",
            f"fraud-{application_id}",
            f"credit-{application_id}",
            f"compliance-{application_id}",
        ]
        
        print(f"--- Events for Application: {application_id} ---")
        for stream_id in streams:
            print(f"\nStream: {stream_id}")
            rows = await conn.fetch(
                "SELECT stream_position, event_type, payload FROM events WHERE stream_id = $1 ORDER BY stream_position",
                stream_id
            )
            if not rows:
                print("  (No events found)")
                continue
                
            for r in rows:
                print(f"  [{r['stream_position']}] {r['event_type']}")
                # print(f"    Payload: {json.dumps(r['payload'], indent=2)}")
                
    finally:
        await conn.close()

if __name__ == "__main__":
    import sys
    app_id = sys.argv[1] if len(sys.argv) > 1 else "APEX-0001"
    asyncio.run(check_events(app_id))
