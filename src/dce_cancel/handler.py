"""AWS Lambda handler for DCe cancellation events."""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

from botocore.exceptions import BotoCoreError, ClientError

from .clients import dynamodb_client, sqs_client
from .config import EnvConfig
from .validation import ValidationError, validate_payload

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _parse_event(event: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(event, dict) and "body" in event:
        body = event.get("body")
        if isinstance(body, str):
            return json.loads(body or "{}")
        if isinstance(body, dict):
            return body
    return event if isinstance(event, dict) else {}


def _build_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def _enqueue_cancellation(config: EnvConfig, payload: Dict[str, Any], correlation_id: str) -> None:
    message_body = json.dumps({**payload, "correlationId": correlation_id})
    sqs_client().send_message(
        QueueUrl=config.sqs_queue_url,
        MessageBody=message_body,
        MessageAttributes={
            "CorrelationId": {
                "StringValue": correlation_id,
                "DataType": "String",
            }
        },
    )


def _upsert_cancellation_status(config: EnvConfig, payload: Dict[str, Any], correlation_id: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    client = dynamodb_client()
    client.update_item(
        TableName=config.dce_table_name,
        Key={
            config.partition_key: {"S": f"DCE#{payload['dceId']}"},
            config.sort_key: {"S": "LATEST"},
        },
        UpdateExpression=(
            "SET #status = :status, correlationId = :correlationId, "
            "eventCode = :eventCode, updatedAt = :updatedAt, eventTimestamp = :eventTimestamp"
        ),
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":status": {"S": "CANCELLED"},
            ":correlationId": {"S": correlation_id},
            ":eventCode": {"S": payload["event"]["code"]},
            ":updatedAt": {"S": now},
            ":eventTimestamp": {"S": payload["timestamp"]},
        },
    )


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    logger.info("Received event: %s", event)
    try:
        payload_dict = _parse_event(event)
        validated = validate_payload(payload_dict)
        config = EnvConfig.load()
        correlation_id = (
            event.get("headers", {}).get("X-Correlation-Id")
            if isinstance(event, dict)
            else None
        ) or payload_dict.get("correlationId") or str(uuid.uuid4())

        enqueue_payload = {
            "dceId": validated.dce_id,
            "event": validated.event,
            "timestamp": validated.timestamp,
            "issuer": validated.issuer,
            "metadata": validated.metadata,
        }

        _enqueue_cancellation(config, enqueue_payload, correlation_id)
        _upsert_cancellation_status(config, enqueue_payload, correlation_id)

        return _build_response(
            202,
            {
                "message": "Cancellation received",
                "dceId": validated.dce_id,
                "correlationId": correlation_id,
            },
        )
    except ValidationError as exc:
        logger.warning("Validation failed: %s", exc)
        return _build_response(400, {"error": str(exc)})
    except (ClientError, BotoCoreError) as exc:
        logger.error("AWS client error: %s", exc, exc_info=True)
        return _build_response(502, {"error": "Failed to dispatch cancellation event"})
    except Exception as exc:  # pragma: no cover
        logger.exception("Unexpected error while handling cancellation")
        return _build_response(500, {"error": str(exc)})
