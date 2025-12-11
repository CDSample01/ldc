"""Domain models for DCe cancellation payloads."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass


@dataclass
class ValidatedPayload:
    """Represents a payload that has passed domain validation rules."""

    document_id: str
    event_cancel_date: str
    event_cancel_date_dt: dt.datetime
    cancel_reason: str
    client_id: str
