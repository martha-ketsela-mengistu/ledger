import asyncio
import asyncpg
import os

async def apply_schema():
    db_url = os.environ.get("TEST_DB_URL", "postgresql://postgres:apex@localhost:5432/apex_ledger")
    sql_path = "g:/projects/ledger/ledger/schema/event_store.sql"
    
    with open(sql_path, "r") as f:
        sql = f.read()
    
    conn = await asyncpg.connect(db_url)
    try:
        await conn.execute(sql)
        print("Schema applied successfully.")
    except Exception as e:
        print(f"Error applying schema: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(apply_schema())
