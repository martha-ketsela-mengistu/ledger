import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class ApplicationSummary:
    name = "ApplicationSummary"

    async def initialize(self, conn) -> None:
        if not conn: return
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS application_summary (
                application_id TEXT PRIMARY KEY,
                applicant_id TEXT,
                company_id TEXT,
                status TEXT DEFAULT 'SUBMITTED',
                requested_amount_usd NUMERIC,
                approved_amount_usd NUMERIC,
                risk_tier TEXT,
                fraud_score NUMERIC,
                compliance_status TEXT,
                decision TEXT,
                agent_sessions_completed TEXT[],
                last_event_type TEXT,
                last_event_at TIMESTAMPTZ,
                human_reviewer_id TEXT,
                final_decision_at TIMESTAMPTZ,
                last_updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

    async def handle_event(self, conn, event) -> None:
        if not conn: return
        p = event.payload
        app_id = p.get("application_id")
        if not app_id: return
        
        # Upsert basic app info with defaults if new
        await conn.execute("""
            INSERT INTO application_summary (application_id, last_event_type, last_event_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (application_id) DO UPDATE 
            SET last_event_type = excluded.last_event_type, 
                last_event_at = excluded.last_event_at
        """, app_id, event.event_type, event.recorded_at or datetime.now())

        if event.event_type == "ApplicationSubmitted":
            await conn.execute("""
                UPDATE application_summary 
                SET applicant_id = $1, company_id = $2, status = 'SUBMITTED', requested_amount_usd = $3
                WHERE application_id = $4
            """, p.get("applicant_id"), p.get("applicant_id"), p.get("requested_amount_usd"), app_id)
            
        elif event.event_type == "ApplicationApproved":
            await conn.execute("""
                UPDATE application_summary 
                SET state = 'FINAL_APPROVED', approved_amount_usd = $1, final_decision_at = NOW()
                WHERE application_id = $2
            """, p.get("approved_amount_usd"), app_id)
            
        elif event.event_type == "ApplicationDeclined":
            await conn.execute("""
                UPDATE application_summary 
                SET state = 'FINAL_DECLINED', final_decision_at = NOW()
                WHERE application_id = $1
            """, app_id)
            
        elif event.event_type == "DecisionGenerated":
            await conn.execute("""
                UPDATE application_summary 
                SET decision = $1, state = 'PENDING_DECISION'
                WHERE application_id = $2
            """, p.get("recommendation"), app_id)
            
        elif event.event_type == "HumanReviewCompleted":
            await conn.execute("""
                UPDATE application_summary 
                SET human_reviewer_id = $1
                WHERE application_id = $2
            """, p.get("reviewer_id"), app_id)
            
        elif event.event_type == "AgentSessionCompleted":
            # Track agent sessions
            await conn.execute("""
                UPDATE application_summary 
                SET agent_sessions_completed = array_append(agent_sessions_completed, $1)
                WHERE application_id = $2
            """, p.get("agent_type"), app_id)

    async def rebuild_from_scratch(self, conn, store) -> None:
        await conn.execute("TRUNCATE application_summary")
        async for event in store.load_all(0):
            await self.handle_event(conn, event)
