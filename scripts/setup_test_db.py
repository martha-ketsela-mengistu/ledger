import asyncio
import asyncpg
import os
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datagen.generate_all import REGISTRY_SQL, EVENT_STORE_SQL

async def setup_test_db():
    db_url = os.environ.get("TEST_DB_URL", "postgresql://postgres:apex@localhost:5432/apex_ledger")
    print(f"Setting up test database at {db_url}...")
    
    conn = await asyncpg.connect(db_url)
    try:
        # Apply Registry Schema
        print("Applying Applicant Registry schema...")
        await conn.execute(REGISTRY_SQL)
        
        # Apply Event Store Schema
        print("Applying Event Store schema...")
        await conn.execute(EVENT_STORE_SQL)
        
        # Seed mock company for tests if needed
        # (Alternatively, tests can do this)
        print("Database setup complete.")
    except Exception as e:
        print(f"Error during setup: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(setup_test_db())
