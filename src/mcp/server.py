"""
src/mcp/server.py
=================
MCP Server entry point for The Ledger.

Exposes 8 tools (command side) and 6 resources (query side).
Tools write events; Resources read from projections — structural CQRS.

Usage:
    python -m src.mcp.server
"""
from __future__ import annotations
import asyncio
import logging
from fastmcp import FastMCP

logger = logging.getLogger(__name__)

DB_URL = "postgresql://postgres:apex@localhost/apex_ledger"

mcp = FastMCP(
    "The Ledger",
    instructions=(
        "The Ledger is an enterprise-grade event-sourced loan application processing system. "
        "Use tools to write events (commands) and resources to read projections (queries). "
        "IMPORTANT: You must call start_agent_session before recording any analysis results. "
        "All tools return structured error types with suggested_action for autonomous recovery."
    ),
)

# Lazy-initialized shared state
_store = None
_pool = None


async def get_store():
    """Lazy-initialize the EventStore singleton and ProjectionDaemon."""
    global _store
    if _store is None:
        from src.event_store import EventStore
        from src.upcasting.upcasters import upcaster_registry
        _store = EventStore(DB_URL, upcaster_registry=upcaster_registry)
        await _store.connect()
        
        # Start ProjectionDaemon
        from src.projections.daemon import ProjectionDaemon
        from src.projections.projections import (
            ApplicationSummaryProjection,
            DocumentPackageProjection,
            ComplianceAuditProjection
        )
        # Correct instantiation based on daemon.py: (store, projections_list, db_pool)
        projections = [
            ApplicationSummaryProjection(),
            DocumentPackageProjection(),
            ComplianceAuditProjection()
        ]
        pool = await get_pool()
        daemon = ProjectionDaemon(_store, projections, db_pool=pool)
        
        asyncio.create_task(daemon.run_forever())
        logger.info("✅ ProjectionDaemon started in background via get_store()")

        # Start OutboxRelay
        from src.outbox_relay import OutboxRelay
        relay = OutboxRelay(DB_URL)
        await relay.start()
        logger.info("✅ OutboxRelay started in background via get_store()")
        
    return _store


async def get_pool():
    """Lazy-initialize the asyncpg pool for projection queries."""
    global _pool
    if _pool is None:
        import asyncpg
        _pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=5)
    return _pool


# ─── Import tools and resources to register them ───
import src.mcp.tools  # noqa: F401, E402
import src.mcp.resources  # noqa: F401, E402


if __name__ == "__main__":
    mcp.run()
