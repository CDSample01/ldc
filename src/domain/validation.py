"""Validation helpers for DCe cancellation payloads."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict

from exceptions import ValidationError


@dataclass
class ValidatedPayload:
    document_id: str
    event_cancel_date: str
    event_cancel_date_dt: dt.datetime
    cancel_reason: str
    client_id: str


def validate_payload(
    payload: Dict[str, Any], cancellation_deadline_minutes: int | None = None
) -> ValidatedPayload:
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
        client_id="",
    )
