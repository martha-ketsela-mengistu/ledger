import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class ComplianceAuditView:
    name = "ComplianceAuditView"

    async def initialize(self, conn) -> None:
        if not conn: return
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS compliance_audit_events (
                id SERIAL PRIMARY KEY,
                application_id TEXT,
                rule_id TEXT,
                regulation_version TEXT,
                status TEXT,
                evaluated_at TIMESTAMPTZ,
                snapshot_data JSONB
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_comp_app_time 
            ON compliance_audit_events (application_id, evaluated_at)
        """)

    async def handle_event(self, conn, event) -> None:
        if not conn: return
        p = event.payload
        app_id = p.get("application_id")
        rule_id = p.get("rule_id")
        
        if not app_id or not rule_id: return
            
        status = None
        if event.event_type == "ComplianceRulePassed": status = "PASSED"
        elif event.event_type == "ComplianceRuleFailed": status = "FAILED"
        elif event.event_type == "ComplianceRuleNoted": status = "NOTED"
        
        if status:
            evaluated_at = p.get("evaluated_at")
            if isinstance(evaluated_at, str):
                try:
                    # Handle both Z and +00:00 or no timezone
                    import dateutil.parser
                    evaluated_at = dateutil.parser.isoparse(evaluated_at)
                except Exception:
                    evaluated_at = datetime.fromisoformat(evaluated_at.replace("Z", "+00:00"))
            
            await conn.execute("""
                INSERT INTO compliance_audit_events 
                (application_id, rule_id, regulation_version, status, evaluated_at, snapshot_data)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, app_id, rule_id, p.get("rule_version", "unknown"), status, 
               evaluated_at or datetime.now(), json.dumps(p))

    async def get_current_compliance(self, conn, application_id: str):
        # Fetch latest state per rule
        rows = await conn.fetch("""
            SELECT DISTINCT ON (rule_id) rule_id, status, regulation_version, evaluated_at 
            FROM compliance_audit_events 
            WHERE application_id = $1 
            ORDER BY rule_id, evaluated_at DESC
        """, application_id)
        return [dict(r) for r in rows]

    async def get_compliance_at(self, conn, application_id: str, timestamp: datetime):
        # Temporal Query: Get state at a specific moment
        rows = await conn.fetch("""
            SELECT DISTINCT ON (rule_id) rule_id, status, regulation_version, evaluated_at 
            FROM compliance_audit_events 
            WHERE application_id = $1 AND evaluated_at <= $2
            ORDER BY rule_id, evaluated_at DESC
        """, application_id, timestamp)
        return [dict(r) for r in rows]

    async def get_projection_lag(self, daemon) -> int:
        return await daemon.get_lag(self.name)

    async def rebuild_from_scratch(self, conn, store) -> None:
        # Must complete without downtime (ideally using a swap table, simplified here)
        await conn.execute("CREATE TABLE compliance_audit_events_new (LIKE compliance_audit_events INCLUDING ALL)")
        async for event in store.load_all(0):
            # Write to _new table logic here...
            pass
        await conn.execute("DROP TABLE compliance_audit_events")
        await conn.execute("ALTER TABLE compliance_audit_events_new RENAME TO compliance_audit_events")
