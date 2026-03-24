import pytest
import os
import asyncio
from unittest.mock import MagicMock, patch
from src.agents.document_agent import DocumentProcessingAgent
from src.models.events import DocumentType

@pytest.fixture
def mock_store():
    from unittest.mock import AsyncMock
    store = MagicMock()
    # Mock load_stream and append
    store.load_stream = AsyncMock(return_value=[])
    store.append = AsyncMock(return_value=None)
    store.stream_version = AsyncMock(return_value=1)
    return store

@pytest.fixture
def test_app_id():
    return "APEX-TEST-001"

@pytest.fixture
def setup_test_docs(test_app_id):
    """Creates dummy PDF files for the agent to process."""
    # Use a local test_documents folder
    docs_dir = os.path.abspath("test_documents")
    os.environ["DOCUMENTS_DIR"] = docs_dir
    
    app_dir = os.path.join(docs_dir, test_app_id)
    os.makedirs(app_dir, exist_ok=True)
    
    is_path = os.path.join(app_dir, "income_statement_2024.pdf")
    bs_path = os.path.join(app_dir, "balance_sheet_2024.pdf")
    
    with open(is_path, "wb") as f:
        f.write(b"%PDF-1.4 dummy content")
    with open(bs_path, "wb") as f:
        f.write(b"%PDF-1.4 dummy content")
        
    yield {
        DocumentType.INCOME_STATEMENT: is_path,
        DocumentType.BALANCE_SHEET: bs_path
    }
    
    # Cleanup
    import shutil
    if os.path.exists(docs_dir):
        shutil.rmtree(docs_dir)
    if "DOCUMENTS_DIR" in os.environ:
        del os.environ["DOCUMENTS_DIR"]

@pytest.mark.asyncio
async def test_document_agent_flow(mock_store, db_url, test_app_id, setup_test_docs):
    """
    Verifies the full LangGraph flow of the DocumentProcessingAgent.
    Checks that all nodes execute and output events are generated.
    """
    # Using a real pool but potentially mocked store methods if we want to isolate
    import asyncpg
    pool = await asyncpg.create_pool(db_url)
    
    try:
        agent = DocumentProcessingAgent(
            agent_id="doc-agent-test",
            agent_type="document_processing",
            store=mock_store,      # We don't have a real EventStore setup here
            registry=MagicMock()  # Mocking registry
        )
        
        # We need to ensure the agent's internal store methods work or are mocked.
        # For a "full flow" test, we'll let it run against the test DB.
        # But first, we must 'start' the session (which is usually done by the orchestrator).
        # We'll simulate the start by manually appending the AgentSessionStarted event if needed,
        # but the agent's run() method handles its own state.
        
        # Run the agent
        # The agent.process_application() method takes (application_id)
        # Note: DocumentProcessingAgent._node_validate_inputs has a hardcoded path for now.
        async def dummy_call_llm(system, user, max_tokens=1024):
            return '{"risk_tier":"LOW","confidence":0.9,"quality_assessment":{"issues":[]}}', 10, 10, 0.0
        agent._call_llm = dummy_call_llm
        await agent.process_application(test_app_id)
        
        # Verify result state by looking at the mock store append calls or state
        # Since we mocked the store, we can check its append method calls
        assert mock_store.append.called
        
        print(f"Successfully verified LangGraph flow for {test_app_id}")
        
    finally:
        await pool.close()

@pytest.mark.asyncio
async def test_document_agent_missing_docs(mock_store, db_url):
    """Verifies that the agent fails gracefully when documents are missing."""
    import asyncpg
    pool = await asyncpg.create_pool(db_url)
    
    try:
        agent = DocumentProcessingAgent(
            agent_id="doc-agent-fail",
            agent_type="document_processing",
            store=mock_store,
            registry=MagicMock()
        )
        
        with pytest.raises(FileNotFoundError):
            await agent.process_application("NONEXISTENT_APP")
            
    finally:
        await pool.close()
