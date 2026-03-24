"""
scripts/run_pipeline.py — Process one application through all agents.
Usage: python scripts/run_pipeline.py --application APEX-0007 [--phase all|document|credit|fraud|compliance|decision]
"""
import argparse, asyncio, os, sys
from pathlib import Path; sys.path.insert(0, str(Path(__file__).parent.parent))
from dotenv import load_dotenv; load_dotenv()

async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--application", required=True)
    p.add_argument("--phase", default="all")
    p.add_argument("--db-url", default=os.environ.get("DATABASE_URL","postgresql://postgres:apex@localhost/apex_ledger"))
    args = p.parse_args()

    # 1. Initialize Infrastructure
    from src.event_store import EventStore
    from src.registry.client import ApplicantRegistryClient
    from src.agents.document_agent import DocumentProcessingAgent
    import asyncpg

    print(f"--- Starting Pipeline for {args.application} (Phase: {args.phase}) ---")
    
    # Initialize EventStore
    store = EventStore(args.db_url)
    await store.connect()

    # Initialize Registry Client (needs a pool)
    pool = await asyncpg.create_pool(args.db_url)
    registry = ApplicantRegistryClient(pool)

    try:
        # 2. Route to appropriate agent(s)
        if args.phase in ["all", "document"]:
            print(f"Stage 1: Document Processing...")
            doc_agent = DocumentProcessingAgent(
                agent_id="pipeline-runner-doc",
                agent_type="document_processing",
                store=store,
                registry=registry
            )
            await doc_agent.process_application(args.application)
            print(f"✅ Document Processing complete.")

        # Future phases will be added here
        if args.phase in ["all", "credit"] and args.phase != "all": # Only if explicitly requested for now
             print(f"Stage 2: Credit Analysis (Not yet integrated)...")

        print(f"\n--- Pipeline finished for {args.application} ---")

    finally:
        await store.close()
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
