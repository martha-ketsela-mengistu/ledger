import asyncio
import os
import asyncpg
from dotenv import load_dotenv

load_dotenv()

async def check_registry():
    db_url = os.environ.get("DATABASE_URL", "postgresql://postgres:apex@localhost/apex_ledger")
    conn = await asyncpg.connect(db_url)
    try:
        # Check if schema exists
        schema_exists = await conn.fetchval(
            "SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'applicant_registry'"
        )
        if not schema_exists:
            print("Error: applicant_registry schema NOT found. Run datagen/generate_all.py first.")
            return

        print("--- Applicant Registry Data ---")
        
        print("\nCompanies:")
        rows = await conn.fetch("SELECT * FROM applicant_registry.companies LIMIT 10")
        for r in rows:
            print(dict(r))
            
        print("\nCompliance Flags:")
        rows = await conn.fetch("SELECT * FROM applicant_registry.compliance_flags LIMIT 10")
        for r in rows:
            print(dict(r))

    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(check_registry())
