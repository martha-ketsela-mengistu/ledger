import asyncio
import os
import asyncpg
from dotenv import load_dotenv

load_dotenv()

async def apply_schema():
    db_url = os.environ.get("DATABASE_URL", "postgresql://postgres:apex@localhost/apex_ledger")
    schema_path = "g:/projects/ledger/src/schema.sql"
    
    with open(schema_path, "r") as f:
        schema_sql = f.read()
    
    print(f"Connecting to {db_url}...")
    conn = await asyncpg.connect(db_url)
    try:
        print("Applying schema...")
        # Split by ';' to execute potentially multiple statements if needed, 
        # but asyncpg.execute can usually handle multiple statements at once.
        await conn.execute(schema_sql)
        print("Schema applied successfully.")
        
        # Verify tables
        rows = await conn.fetch("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'")
        print("Tables in 'public' schema:")
        for row in rows:
            print(f"  - {row['tablename']}")
            
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(apply_schema())
