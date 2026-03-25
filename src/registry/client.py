"""
ledger/registry/client.py — Applicant Registry read-only client
===============================================================
COMPLETION STATUS: STUB — implement the query methods.

This client reads from the applicant_registry schema in PostgreSQL.
It is READ-ONLY. No agent or event store component ever writes here.
The Applicant Registry is the external CRM — seeded by datagen/generate_all.py.
"""
from __future__ import annotations
from dataclasses import dataclass
import logging
import asyncpg

logger = logging.getLogger(__name__)

@dataclass
class CompanyProfile:
    """Demographic and profile data for an applicant company."""
    company_id: str
    name: str
    industry: str
    naics: str
    jurisdiction: str
    legal_type: str
    founded_year: int
    employee_count: int
    risk_segment: str
    trajectory: str
    submission_channel: str
    ip_region: str
    ein: str = ""
    address_city: str = ""
    address_state: str = ""
    relationship_start: any = None
    account_manager: str = ""
    created_at: any = None

@dataclass
class FinancialYear:
    """Structured historical financial record for a single fiscal year."""
    fiscal_year: int
    total_revenue: float
    gross_profit: float
    operating_income: float
    ebitda: float
    net_income: float
    total_assets: float
    total_liabilities: float
    total_equity: float
    long_term_debt: float
    cash_and_equivalents: float
    current_assets: float
    current_liabilities: float
    accounts_receivable: float
    inventory: float
    debt_to_equity: float
    current_ratio: float
    debt_to_ebitda: float
    interest_coverage_ratio: float
    gross_margin: float
    ebitda_margin: float
    net_margin: float
    id: int = 0
    company_id: str = ""
    operating_expenses: float = 0.0
    depreciation_amortization: float = 0.0
    interest_expense: float = 0.0
    income_before_tax: float = 0.0
    tax_expense: float = 0.0
    operating_cash_flow: float = 0.0
    investing_cash_flow: float = 0.0
    financing_cash_flow: float = 0.0
    free_cash_flow: float = 0.0
    balance_sheet_check: bool = True

@dataclass
class ComplianceFlag:
    """Regulatory or risk flag associated with a company."""
    flag_type: str
    severity: str
    is_active: bool
    added_date: any
    note: str
    id: int = 0
    company_id: str = ""

class ApplicantRegistryClient:
    """
    READ-ONLY access to the Applicant Registry.
    
    This client provides a high-level interface to query company profiles,
    financial history, and compliance flags from the external CRM database.
    It should never be used to write data.
    """

    def __init__(self, pool: asyncpg.Pool):
        """Initializes the client with a PostgreSQL connection pool."""
        self._pool = pool

    async def get_company(self, company_id: str) -> CompanyProfile | None:
        """
        Reads company demographics from the registry.
        
        Args:
            company_id: The unique ID of the company to retrieve.
            
        Returns:
            A CompanyProfile object if found, otherwise None.
        """
        logger.debug(f"Querying company profile for {company_id}")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM applicant_registry.companies WHERE company_id = $1",
                company_id
            )
            if not row:
                logger.warning(f"Company {company_id} not found in registry")
                return None
            return CompanyProfile(**dict(row))

    async def get_financial_history(self, company_id: str,
                                     years: list[int] | None = None) -> list[FinancialYear]:
        """
        Reads historical financial data for trend analysis.
        
        Args:
            company_id: The company ID to query.
            years: Optional list of fiscal years to filter by.
            
        Returns:
            A list of FinancialYear records sorted by year.
        """
        logger.debug(f"Querying financial history for {company_id} years={years}")
        async with self._pool.acquire() as conn:
            query = "SELECT * FROM applicant_registry.financial_history WHERE company_id = $1"
            params = [company_id]
            if years:
                query += " AND fiscal_year = ANY($2)"
                params.append(years)
            query += " ORDER BY fiscal_year ASC"
            
            rows = await conn.fetch(query, *params)
            return [FinancialYear(**dict(row)) for row in rows]

    async def get_compliance_flags(self, company_id: str,
                                    active_only: bool = False) -> list[ComplianceFlag]:
        """
        Reads compliance and regulatory flags from the CRM.
        
        Args:
            company_id: The company ID to query.
            active_only: If True, returns only currently active flags.
            
        Returns:
            A list of ComplianceFlag records.
        """
        logger.debug(f"Querying compliance flags for {company_id} active_only={active_only}")
        async with self._pool.acquire() as conn:
            query = "SELECT * FROM applicant_registry.compliance_flags WHERE company_id = $1"
            if active_only:
                query += " AND is_active = TRUE"
            
            rows = await conn.fetch(query, company_id)
            return [ComplianceFlag(**dict(row)) for row in rows]

    async def get_loan_relationships(self, company_id: str) -> list[dict]:
        """
        Reads existing loan relationships and performance.
        
        Args:
            company_id: The company ID to query.
            
        Returns:
            A list of historical loan relationship records as dictionaries.
        """
        logger.debug(f"Querying loan relationships for {company_id}")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM applicant_registry.loan_relationships WHERE company_id = $1",
                company_id
            )
            return [dict(row) for row in rows]
