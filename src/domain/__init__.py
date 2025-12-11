"""Domain logic for DCe cancellation."""
from domain.models.payload import ValidatedPayload
from domain.services.validation import validate_payload

__all__ = ["validate_payload", "ValidatedPayload"]
