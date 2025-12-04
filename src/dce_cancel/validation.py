"""Validation helpers for DCe cancellation payloads."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict


ALLOWED_EVENT_CODES = {"110111"}
ALLOWED_SCHEMA_VERSIONS = {"1.00", "1.01"}


class ValidationError(Exception):
    """Raised when payload validation fails."""


@dataclass
class ValidatedPayload:
    dce_id: str
    event: Dict[str, Any]
    timestamp: str
    issuer: Dict[str, Any]
    metadata: Dict[str, Any]


_ISO_FORMATS = ["%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ"]


def _ensure_isoformat(value: str) -> None:
    for fmt in _ISO_FORMATS:
        try:
            dt.datetime.strptime(value, fmt)
            return
        except ValueError:
            continue
    raise ValidationError("timestamp must be ISO 8601 with timezone information")


def validate_payload(payload: Dict[str, Any]) -> ValidatedPayload:
    if not isinstance(payload, dict):
        raise ValidationError("payload must be a JSON object")

    dce_id = payload.get("dceId")
    event = payload.get("event")
    timestamp = payload.get("timestamp")
    issuer = payload.get("issuer")

    if not dce_id:
        raise ValidationError("dceId is required")
    if not isinstance(event, dict):
        raise ValidationError("event must be an object")
    if not timestamp:
        raise ValidationError("timestamp is required")
    _ensure_isoformat(timestamp)

    event_code = str(event.get("code")) if event.get("code") is not None else None
    schema_version = event.get("schemaVersion")
    sequence_number = event.get("sequenceNumber")
    reason = event.get("reason")
    protocol = event.get("protocol")

    if event_code not in ALLOWED_EVENT_CODES:
        raise ValidationError(
            f"event.code must be one of {', '.join(sorted(ALLOWED_EVENT_CODES))}"
        )

    if schema_version not in ALLOWED_SCHEMA_VERSIONS:
        raise ValidationError(
            f"event.schemaVersion must be one of {', '.join(sorted(ALLOWED_SCHEMA_VERSIONS))}"
        )

    if not isinstance(sequence_number, int) or sequence_number < 1:
        raise ValidationError("event.sequenceNumber must be a positive integer")

    if not reason:
        raise ValidationError("event.reason is required")

    if not protocol:
        raise ValidationError("event.protocol is required")

    if not isinstance(issuer, dict):
        raise ValidationError("issuer must be an object")

    metadata = payload.get("metadata") or {}
    if not isinstance(metadata, dict):
        raise ValidationError("metadata, when provided, must be an object")

    return ValidatedPayload(
        dce_id=str(dce_id),
        event={
            "code": event_code,
            "schemaVersion": schema_version,
            "sequenceNumber": sequence_number,
            "reason": str(reason),
            "protocol": str(protocol),
        },
        timestamp=timestamp,
        issuer=issuer,
        metadata=metadata,
    )
