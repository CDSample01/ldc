"""Validation helpers for DCe cancellation payloads."""
from __future__ import annotations

import datetime as dt
from typing import Any, Dict

from exceptions import ValidationError
from domain.models.payload import ValidatedPayload


def validate_payload(
    payload: Dict[str, Any], cancellation_deadline_minutes: int | None = None
) -> ValidatedPayload:
    """Validate the incoming payload and enrich it with the current timestamp.

    ``eventCancelDate`` is no longer provided by the requester; it is populated at
    validation time using the current UTC timestamp.
    """

    if not isinstance(payload, dict):
        raise ValidationError("payload must be a JSON object")

    document_id = payload.get("id")
    cancel_reason = payload.get("cancelReason")

    if not document_id:
        raise ValidationError("id is required")
    if not cancel_reason:
        raise ValidationError("cancelReason is required")

    if not isinstance(cancel_reason, str):
        raise ValidationError("cancelReason must be a string")

    now_dt = dt.datetime.now(dt.timezone.utc)
    now_iso = now_dt.isoformat()

    return ValidatedPayload(
        document_id=str(document_id),
        event_cancel_date=now_iso,
        event_cancel_date_dt=now_dt,
        cancel_reason=str(cancel_reason),
        client_id="",  # populated after client validation in the handler
    )
