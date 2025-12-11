"""AWS Lambda handler for DCe cancellation events."""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import BotoCoreError, ClientError

from adapters.clients import dynamodb_client, dynamodb_resource, sqs_client
from config.config import EnvConfig
from domain.services.validation import validate_payload
from exceptions import AuthorizationError, ValidationError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

EVENT_CODE = "110111"


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


def _authenticate_request(event: Dict[str, Any], config: EnvConfig) -> None:
    token = config.api_auth_token
    if not token:
        return

    headers = event.get("headers") if isinstance(event, dict) else None
    provided = None
    if isinstance(headers, dict):
        auth_header = headers.get("Authorization") or headers.get("authorization")
        if auth_header and auth_header.startswith("Bearer "):
            provided = auth_header.split(" ", 1)[1].strip()

    if provided != token:
        raise AuthorizationError("Unauthorized", status_code=401)


def _extract_client_id(event: Dict[str, Any], config: EnvConfig) -> str:
    headers = event.get("headers") if isinstance(event, dict) else None
    normalized_headers = {k.lower(): v for k, v in headers.items()} if isinstance(headers, dict) else {}

    client_id = None
    for key in ("client-id", "client_id", "clientid", "x-client-id"):
        if key in normalized_headers:
            client_id = normalized_headers.get(key)
            break

    if not client_id or not isinstance(client_id, str):
        raise AuthorizationError("clientId header is required", status_code=401)

    return client_id


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
            config.partition_key: {"S": f"DCE#{payload['id']}"},
            config.sort_key: {"S": "LATEST"},
        },
        UpdateExpression=(
            "SET #status = :status, correlationId = :correlationId, "
            "eventCode = :eventCode, updatedAt = :updatedAt, eventTimestamp = :eventTimestamp, "
            "requestedAt = :requestedAt, cancellationReason = :reason, operationStatus = :operationStatus, "
            "clientId = :clientId"
        ),
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":status": {"S": "CANCELLATION_REQUESTED"},
            ":correlationId": {"S": correlation_id},
            ":eventCode": {"S": EVENT_CODE},
            ":updatedAt": {"S": now},
            ":eventTimestamp": {"S": payload["eventCancelDate"]},
            ":requestedAt": {"S": now},
            ":reason": {"S": payload["cancelReason"]},
            ":operationStatus": {"S": "RECEIVED"},
            ":clientId": {"S": payload["clientId"]},
        },
    )


def _authorize_client(access_key: str, client_id: str, table_name: str) -> None:
    table = dynamodb_resource().Table(table_name)
    response = table.query(
        KeyConditionExpression=Key("accessKey").eq(access_key),
        FilterExpression=Attr("clientId").eq(client_id),
        Limit=1,
    )

    if not response.get("Items"):
        raise AuthorizationError("clientId is not authorized to cancel this DCe")


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    logger.info("Received event: %s", event)
    try:
        payload_dict = _parse_event(event)
        config = EnvConfig.load()
        _authenticate_request(event, config)
        client_id = _extract_client_id(event, config)
        validated = validate_payload(
            payload_dict, cancellation_deadline_minutes=config.cancellation_deadline_minutes
        )
        validated.client_id = client_id

        _authorize_client(validated.document_id, validated.client_id, config.log_dce_table_name)

        correlation_id = (
            event.get("headers", {}).get("X-Correlation-Id")
            if isinstance(event, dict)
            else None
        ) or payload_dict.get("correlationId") or str(uuid.uuid4())

        enqueue_payload = {
            "id": validated.document_id,
            "eventCancelDate": validated.event_cancel_date,
            "cancelReason": validated.cancel_reason,
            "clientId": validated.client_id,
            "eventCode": EVENT_CODE,
        }

        _enqueue_cancellation(config, enqueue_payload, correlation_id)
        _upsert_cancellation_status(config, enqueue_payload, correlation_id)

        logger.info(
            "Cancellation request recorded",
            extra={
                "dceId": validated.document_id,
                "correlationId": correlation_id,
                "clientId": validated.client_id,
            },
        )

        return _build_response(
            201,
            {
                "message": "Cancellation received",
                "dceId": validated.document_id,
                "correlationId": correlation_id,
            },
        )
    except AuthorizationError as exc:
        logger.warning("Authorization failed: %s", exc)
        return _build_response(getattr(exc, "status_code", 403), {"error": str(exc)})
    except ValidationError as exc:
        logger.warning("Validation failed: %s", exc)
        return _build_response(400, {"error": str(exc)})
    except (ClientError, BotoCoreError) as exc:
        logger.error("AWS client error: %s", exc, exc_info=True)
        return _build_response(502, {"error": "Failed to dispatch cancellation event"})
    except Exception as exc:  # pragma: no cover
        logger.exception("Unexpected error while handling cancellation")
        return _build_response(500, {"error": str(exc)})
