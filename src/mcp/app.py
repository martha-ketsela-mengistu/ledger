"""
src/mcp/app.py
==============
Shared FastMCP instance for The Ledger.
"""
from fastmcp import FastMCP

mcp = FastMCP(
    "The Ledger",
    instructions=(
        "The Ledger is an enterprise-grade event-sourced loan application processing system. "
        "Use tools to write events (commands) and resources to read projections (queries). "
        "IMPORTANT: You must call start_agent_session before recording any analysis results. "
        "All tools return structured error types with suggested_action for autonomous recovery."
    ),
)
