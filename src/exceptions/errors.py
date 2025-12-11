"""Shared exception types for the Lambda domain."""
from __future__ import annotations


class ValidationError(Exception):
    """Raised when payload validation fails."""


class AuthorizationError(Exception):
    """Raised when authorization/ownership rules are broken."""

    def __init__(self, message: str, status_code: int = 403) -> None:
        super().__init__(message)
        self.status_code = status_code
