import json
from src.models.events import StoredEvent

class ApplicationSummaryProjection:
    name = "ApplicationSummaryProjection"

    async def initialize(self, conn) -> None:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS application_summary (
                application_id TEXT PRIMARY KEY,
                company_id TEXT,
                state TEXT,
                requested_amount_usd NUMERIC,
                approved_amount_usd NUMERIC,
                last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

    async def initialize_memory(self, memory_state: dict) -> None:
        memory_state.setdefault("application_summary", {})

    async def handle_event(self, conn, event: StoredEvent) -> None:
        p = event.payload
        app_id = p.get("application_id")
        if not app_id: return

        if event.event_type == "ApplicationSubmitted":
            await conn.execute("""
                INSERT INTO application_summary (application_id, company_id, state, requested_amount_usd, last_updated_at)
                VALUES ($1, $2, 'SUBMITTED', $3, NOW())
                ON CONFLICT (application_id) DO NOTHING
            """, app_id, p.get("company_id"), p.get("requested_amount_usd"))
            
        elif event.event_type == "ApplicationApproved":
            await conn.execute("""
                UPDATE application_summary 
                SET state = 'FINAL_APPROVED', approved_amount_usd = $1, last_updated_at = NOW()
                WHERE application_id = $2
            """, p.get("approved_amount_usd"), app_id)
            
        elif event.event_type == "ApplicationDeclined":
            await conn.execute("""
                UPDATE application_summary 
                SET state = 'FINAL_DECLINED', last_updated_at = NOW()
                WHERE application_id = $1
            """, app_id)
        else:
            state_map = {
                "DecisionGenerated": "PENDING_DECISION",
                "ComplianceCheckCompleted": "COMPLIANCE_CHECK_COMPLETED",
                "FraudScreeningCompleted": "FRAUD_SCREENING_COMPLETED",
                "CreditAnalysisCompleted": "CREDIT_ANALYSIS_COMPLETED",
            }
            if event.event_type in state_map:
                await conn.execute("""
                    UPDATE application_summary 
                    SET state = $1, last_updated_at = NOW()
                    WHERE application_id = $2
                """, state_map[event.event_type], app_id)

    async def handle_event_memory(self, memory_state: dict, event: StoredEvent) -> None:
        p = event.payload
        app_id = p.get("application_id")
        if not app_id: return
        store = memory_state["application_summary"]
        
        if event.event_type == "ApplicationSubmitted":
            store[app_id] = {
                "application_id": app_id,
                "company_id": p.get("company_id"),
                "state": "SUBMITTED",
                "requested_amount_usd": p.get("requested_amount_usd"),
                "approved_amount_usd": None
            }
        elif event.event_type == "ApplicationApproved":
            if app_id in store:
                store[app_id]["state"] = "FINAL_APPROVED"
                store[app_id]["approved_amount_usd"] = p.get("approved_amount_usd")
        elif event.event_type == "ApplicationDeclined":
            if app_id in store:
                store[app_id]["state"] = "FINAL_DECLINED"
        elif event.event_type in ["DecisionGenerated", "ComplianceCheckCompleted", "FraudScreeningCompleted"]:
            if app_id in store:
                store[app_id]["state"] = event.event_type.upper().replace("COMPLETED", "COMPLETE").replace("GENERATED", "DECISION")


class DocumentPackageProjection:
    name = "DocumentPackageProjection"

    async def initialize(self, conn) -> None:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS document_packages (
                docpkg_id TEXT PRIMARY KEY,
                application_id TEXT,
                status TEXT,
                quality_flags JSONB,
                extracted_facts JSONB,
                last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

    async def initialize_memory(self, memory_state: dict) -> None:
        memory_state.setdefault("document_packages", {})

    async def handle_event(self, conn, event: StoredEvent) -> None:
        p = event.payload
        docpkg_id = p.get("docpkg_id")
        if not docpkg_id: return

        if event.event_type == "PackageCreated":
            await conn.execute("""
                INSERT INTO document_packages (docpkg_id, application_id, status, last_updated_at)
                VALUES ($1, $2, 'CREATED', NOW())
                ON CONFLICT (docpkg_id) DO NOTHING
            """, docpkg_id, p.get("application_id"))
        elif event.event_type == "ExtractionStarted":
            await conn.execute("UPDATE document_packages SET status = 'EXTRACTION_STARTED', last_updated_at = NOW() WHERE docpkg_id = $1", docpkg_id)
        elif event.event_type == "ExtractionCompleted":
            await conn.execute("""
                UPDATE document_packages 
                SET status = 'EXTRACTION_COMPLETED', extracted_facts = COALESCE(extracted_facts, '{}'::jsonb) || $1::jsonb, last_updated_at = NOW()
                WHERE docpkg_id = $2
            """, json.dumps(p.get("facts", {})), docpkg_id)
        elif event.event_type == "QualityAssessmentCompleted":
            flags = {"overall_confidence": p.get("overall_confidence"), "is_coherent": p.get("is_coherent")}
            await conn.execute("""
                UPDATE document_packages 
                SET status = 'QUALITY_ASSESSED', quality_flags = $1::jsonb, last_updated_at = NOW()
                WHERE docpkg_id = $2
            """, json.dumps(flags), docpkg_id)
        elif event.event_type == "PackageReadyForAnalysis":
            await conn.execute("UPDATE document_packages SET status = 'READY_FOR_ANALYSIS', last_updated_at = NOW() WHERE docpkg_id = $1", docpkg_id)

    async def handle_event_memory(self, memory_state: dict, event: StoredEvent) -> None:
        p = event.payload
        docpkg_id = p.get("docpkg_id")
        if not docpkg_id: return
        store = memory_state["document_packages"]
        
        if event.event_type == "PackageCreated":
            store[docpkg_id] = {"application_id": p.get("application_id"), "status": "CREATED", "extracted_facts": {}, "quality_flags": {}}
        elif event.event_type == "ExtractionStarted" and docpkg_id in store:
            store[docpkg_id]["status"] = "EXTRACTION_STARTED"
        elif event.event_type == "ExtractionCompleted" and docpkg_id in store:
            store[docpkg_id]["status"] = "EXTRACTION_COMPLETED"
            store[docpkg_id]["extracted_facts"].update(p.get("facts", {}))
        elif event.event_type == "QualityAssessmentCompleted" and docpkg_id in store:
            store[docpkg_id]["status"] = "QUALITY_ASSESSED"
            store[docpkg_id]["quality_flags"] = {"overall_confidence": p.get("overall_confidence"), "is_coherent": p.get("is_coherent")}
        elif event.event_type == "PackageReadyForAnalysis" and docpkg_id in store:
            store[docpkg_id]["status"] = "READY_FOR_ANALYSIS"


class ComplianceAuditProjection:
    name = "ComplianceAuditProjection"

    async def initialize(self, conn) -> None:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS compliance_audit_view (
                application_id TEXT,
                rule_id TEXT,
                status TEXT,
                evaluated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (application_id, rule_id)
            )
        """)

    async def initialize_memory(self, memory_state: dict) -> None:
        memory_state.setdefault("compliance_audit_view", {})

    async def handle_event(self, conn, event: StoredEvent) -> None:
        p = event.payload
        app_id = p.get("application_id")
        rule_id = p.get("rule_id")
        if not app_id or not rule_id: return
            
        status = None
        if event.event_type == "ComplianceRulePassed": status = "PASSED"
        elif event.event_type == "ComplianceRuleFailed": status = "FAILED"
        elif event.event_type == "ComplianceRuleNoted": status = "NOTED"
            
        if status:
            await conn.execute("""
                INSERT INTO compliance_audit_view (application_id, rule_id, status, evaluated_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (application_id, rule_id) DO UPDATE 
                SET status = excluded.status, evaluated_at = NOW()
            """, app_id, rule_id, status)

    async def handle_event_memory(self, memory_state: dict, event: StoredEvent) -> None:
        p = event.payload
        app_id = p.get("application_id")
        rule_id = p.get("rule_id")
        if not app_id or not rule_id: return
        store = memory_state["compliance_audit_view"]
        key = f"{app_id}_{rule_id}"
        
        status = None
        if event.event_type == "ComplianceRulePassed": status = "PASSED"
        elif event.event_type == "ComplianceRuleFailed": status = "FAILED"
        elif event.event_type == "ComplianceRuleNoted": status = "NOTED"
        
        if status:
            store[key] = {
                "application_id": app_id,
                "rule_id": rule_id,
                "status": status
            }
