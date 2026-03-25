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
            from src.agents.document_agent import DocumentProcessingAgent
            doc_agent = DocumentProcessingAgent(
                agent_id="pipeline-runner-doc",
                agent_type="document_processing",
                store=store,
                registry=registry
            )
            await doc_agent.process_application(args.application)
            print(f"✅ Document Processing complete.")

        if args.phase in ["all", "credit"]:
            print(f"Stage 2: Credit Analysis...")
            from src.agents.credit_analysis_agent import CreditAnalysisAgent
            credit_agent = CreditAnalysisAgent(
                agent_id="pipeline-runner-credit",
                agent_type="credit_analysis",
                store=store,
                registry=registry
            )
            await credit_agent.process_application(args.application)
            print(f"✅ Credit Analysis complete.")

        if args.phase in ["all", "fraud"]:
            print(f"Stage 3: Fraud Detection...")
            from src.agents.fraud_detection_agent import FraudDetectionAgent
            fraud_agent = FraudDetectionAgent(
                agent_id="pipeline-runner-fraud",
                agent_type="fraud_detection",
                store=store,
                registry=registry
            )
            await fraud_agent.process_application(args.application)
            print(f"✅ Fraud Detection complete.")

        if args.phase in ["all", "compliance"]:
            print(f"Stage 4: Compliance Check...")
            from src.agents.compliance_agent import ComplianceAgent
            comp_agent = ComplianceAgent(
                agent_id="pipeline-runner-compliance",
                agent_type="compliance",
                store=store,
                registry=registry
            )
            await comp_agent.process_application(args.application)
            print(f"✅ Compliance Check complete.")

        if args.phase in ["all", "decision"]:
            print(f"Stage 5: Decision Orchestration...")
            from src.agents.orchestrator_agent import DecisionOrchestratorAgent
            orch_agent = DecisionOrchestratorAgent(
                agent_id="pipeline-runner-orchestrator",
                agent_type="decision_orchestration",
                store=store,
                registry=registry
            )
            await orch_agent.process_application(args.application)
            print(f"✅ Decision Orchestration complete.")

        print(f"\n--- Pipeline finished for {args.application} ---")

    finally:
        await store.close()
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
