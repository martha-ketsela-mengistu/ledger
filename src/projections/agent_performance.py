import json
import logging
logger = logging.getLogger(__name__)

class AgentPerformanceLedger:
    name = "AgentPerformanceLedger"

    async def initialize(self, conn) -> None:
        if not conn: return
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS agent_performance (
                agent_id TEXT,
                model_version TEXT,
                analyses_completed INT DEFAULT 0,
                decisions_generated INT DEFAULT 0,
                avg_confidence_score NUMERIC DEFAULT 0,
                avg_duration_ms NUMERIC DEFAULT 0,
                approve_rate NUMERIC DEFAULT 0,
                decline_rate NUMERIC DEFAULT 0,
                refer_rate NUMERIC DEFAULT 0,
                human_override_rate NUMERIC DEFAULT 0,
                first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                
                -- Accumulators for averages
                _total_confidence NUMERIC DEFAULT 0,
                _total_duration BIGINT DEFAULT 0,
                _total_approves INT DEFAULT 0,
                _total_declines INT DEFAULT 0,
                _total_refers   INT DEFAULT 0,
                _total_overrides INT DEFAULT 0,
                
                PRIMARY KEY (agent_id, model_version)
            )
        """)

    async def handle_event(self, conn, event) -> None:
        if not conn: return
        p = event.payload
        
        # Only process agent-related outputs
        et = event.event_type
        if et not in ("CreditAnalysisCompleted", "FraudScreeningCompleted", "ComplianceCheckCompleted", "DecisionGenerated", "HumanReviewCompleted"):
            return
            
        agent_id = getattr(event, "stream_id", "").split("-")[0] # approximate, typically we'd extract from payload or app stream context
        
        # We need the model version. It was added in upcasters
        model_ver = p.get("model_versions", {}).get("credit") or p.get("screening_model_version") or "unknown"
        if et == "DecisionGenerated":
           model_ver = p.get("model_versions", {}).get("orchestrator", "unknown")
           
        await conn.execute("""
            INSERT INTO agent_performance (agent_id, model_version, first_seen_at, last_seen_at)
            VALUES ($1, $2, NOW(), NOW())
            ON CONFLICT (agent_id, model_version) DO UPDATE SET last_seen_at = NOW()
        """, agent_id, model_ver)
        
        if et in ("CreditAnalysisCompleted"):
            await conn.execute("""
                UPDATE agent_performance 
                SET analyses_completed = analyses_completed + 1,
                    _total_duration = _total_duration + $1,
                    _total_confidence = _total_confidence + $2
                WHERE agent_id = $3 AND model_version = $4
            """, p.get("duration_ms", 0), p.get("confidence", 0), agent_id, model_ver)
            
        elif et == "DecisionGenerated":
            rec = p.get("recommendation", "REFER")
            appr = 1 if rec == "APPROVE" else 0
            decl = 1 if rec == "DECLINE" else 0
            ref = 1 if rec == "REFER" else 0
            
            await conn.execute("""
                UPDATE agent_performance 
                SET decisions_generated = decisions_generated + 1,
                    _total_approves = _total_approves + $1,
                    _total_declines = _total_declines + $2,
                    _total_refers = _total_refers + $3,
                    approve_rate = (_total_approves + $1)::numeric / (decisions_generated + 1),
                    decline_rate = (_total_declines + $2)::numeric / (decisions_generated + 1),
                    refer_rate = (_total_refers + $3)::numeric / (decisions_generated + 1)
                WHERE agent_id = $4 AND model_version = $5
            """, appr, decl, ref, "orchestrator", model_ver)
        
        # A trigger or periodic update can sync the avg_confidence_score = _total_confidence / analyses_completed

    async def rebuild_from_scratch(self, conn, store) -> None:
        await conn.execute("TRUNCATE agent_performance")
        async for event in store.load_all(0):
            await self.handle_event(conn, event)
