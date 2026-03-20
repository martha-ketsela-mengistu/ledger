# ledger/domain/errors.py

class DomainError(Exception):
    """Raised when a business rule or state machine invariant is violated."""
    def __init__(self, message: str, details: dict | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}
