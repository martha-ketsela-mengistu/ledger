import pytest
import asyncpg
import logging
from src.registry.client import ApplicantRegistryClient, FinancialYear, ComplianceFlag

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_registry_client_queries(db_url):
    """
    Verifies that the ApplicantRegistryClient can correctly query the seeded registry data.
    Uses the TEST_DB_URL from conftest.py.
    """
    pool = await asyncpg.create_pool(db_url)
    client = ApplicantRegistryClient(pool)
    
    try:
        # 1. Test get_company (using APEX-0012 as an example company from seed_events.jsonl)
        # Note: company_id in registry might be different from APEX-0012 (which is an app_id)
        # In this system, typically company_id matches the applicant_id in the submission.
        # Let's try to find a valid company_id from the database first or use a known one.
        
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT company_id FROM applicant_registry.companies LIMIT 1")
            if not row:
                pytest.fail("Applicant Registry is empty (no companies found).")
            company_id = row['company_id']

        company = await client.get_company(company_id)
        assert company is not None
        assert company.company_id == company_id
        logger.info(f"Verified company query for {company_id}")

        # 2. Test get_financial_history
        history = await client.get_financial_history(company_id)
        assert isinstance(history, list)
        if history:
            assert history[0].fiscal_year > 0
            logger.info(f"Verified financial history for {company_id} (count={len(history)})")

        # 3. Test get_compliance_flags
        flags = await client.get_compliance_flags(company_id)
        assert isinstance(flags, list)
        logger.info(f"Verified compliance flags for {company_id} (count={len(flags)})")

        # 4. Test get_loan_relationships
        loans = await client.get_loan_relationships(company_id)
        assert isinstance(loans, list)
        logger.info(f"Verified loan relationships for {company_id} (count={len(loans)})")

    finally:
        await pool.close()

@pytest.mark.asyncio
async def test_registry_client_nonexistent_company(db_url):
    """Verifies that the client handles missing companies gracefully."""
    pool = await asyncpg.create_pool(db_url)
    client = ApplicantRegistryClient(pool)
    
    try:
        company = await client.get_company("NONEXISTENT_ID")
        assert company is None
    finally:
        await pool.close()
