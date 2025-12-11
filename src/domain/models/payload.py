"""Domain models for DCe cancellation payloads."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class ValidatedPayload:
    """Represents a payload that has passed domain validation rules."""

    dce_id: str
    event: Dict[str, Any]
    timestamp: str
    timestamp_dt: dt.datetime
    issuer: Dict[str, Any]
    metadata: Dict[str, Any]
    client_id: str
