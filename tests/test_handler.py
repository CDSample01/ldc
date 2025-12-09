import json
from unittest.mock import MagicMock, patch

from datetime import datetime, timezone
import pytest
from botocore.exceptions import ClientError

from dce_cancel import handler as lambda_handler


VALID_PAYLOAD = {
    "dceId": "DCE123",
    "event": {
        "code": "110111",
        "schemaVersion": "1.00",
        "sequenceNumber": 1,
        "reason": "Cancelamento por duplicidade",
        "protocol": "123456789012345",
    },
    "timestamp": "2024-01-01T12:00:00+00:00",
    "issuer": {"cnpj": "12345678000199"},
    "metadata": {"state": "RS"},
    "clientId": "partner-123",
}


def _fresh_payload() -> dict:
    payload = json.loads(json.dumps(VALID_PAYLOAD))
    payload["timestamp"] = datetime.now(timezone.utc).isoformat()
    return payload


def _mock_client():
    return MagicMock()


def test_successful_enqueue_and_update(monkeypatch):
    monkeypatch.setenv("SQS_QUEUE_URL", "https://sqs.queue")
    monkeypatch.setenv("DCE_TABLE_NAME", "dce-table")
    monkeypatch.setenv("API_AUTH_TOKEN", "secret")
    monkeypatch.setenv("ALLOWED_CLIENT_IDS", "partner-123")
    monkeypatch.setenv("CANCELLATION_DEADLINE_MINUTES", "525600")

    sqs_mock = _mock_client()
    dynamodb_mock = _mock_client()
    payload = _fresh_payload()

    with patch("dce_cancel.handler.sqs_client", return_value=sqs_mock), patch(
        "dce_cancel.handler.dynamodb_client", return_value=dynamodb_mock
    ):
        response = lambda_handler.handler(
            {"body": json.dumps(payload), "headers": {"Authorization": "Bearer secret"}},
            None,
        )

    assert response["statusCode"] == 201

    sqs_mock.send_message.assert_called_once()
    call_kwargs = sqs_mock.send_message.call_args.kwargs
    assert call_kwargs["QueueUrl"] == "https://sqs.queue"
    body = json.loads(call_kwargs["MessageBody"])
    assert body["dceId"] == payload["dceId"]
    assert body["event"]["code"] == "110111"

    dynamodb_mock.update_item.assert_called_once()
    assert dynamodb_mock.update_item.call_args.kwargs["TableName"] == "dce-table"
    update_values = dynamodb_mock.update_item.call_args.kwargs["ExpressionAttributeValues"]
    assert update_values[":clientId"]["S"] == "partner-123"
    assert update_values[":status"]["S"] == "CANCELLATION_REQUESTED"


def test_validation_failure_returns_400(monkeypatch):
    monkeypatch.setenv("SQS_QUEUE_URL", "https://sqs.queue")
    monkeypatch.setenv("DCE_TABLE_NAME", "dce-table")
    monkeypatch.setenv("API_AUTH_TOKEN", "secret")
    monkeypatch.setenv("CANCELLATION_DEADLINE_MINUTES", "525600")

    with patch("dce_cancel.handler.sqs_client") as sqs_mock, patch(
        "dce_cancel.handler.dynamodb_client"
    ) as dynamodb_mock:
        response = lambda_handler.handler({"body": json.dumps({}), "headers": {"Authorization": "Bearer secret"}}, None)

    assert response["statusCode"] == 400
    sqs_mock.return_value.send_message.assert_not_called()
    dynamodb_mock.return_value.update_item.assert_not_called()


def test_sqs_failure_returns_502(monkeypatch):
    monkeypatch.setenv("SQS_QUEUE_URL", "https://sqs.queue")
    monkeypatch.setenv("DCE_TABLE_NAME", "dce-table")
    monkeypatch.setenv("API_AUTH_TOKEN", "secret")
    monkeypatch.setenv("CANCELLATION_DEADLINE_MINUTES", "525600")

    error_response = {"Error": {"Code": "500", "Message": "boom"}}
    sqs_mock = _mock_client()
    sqs_mock.send_message.side_effect = ClientError(error_response, "SendMessage")
    dynamodb_mock = _mock_client()
    payload = _fresh_payload()

    with patch("dce_cancel.handler.sqs_client", return_value=sqs_mock), patch(
        "dce_cancel.handler.dynamodb_client", return_value=dynamodb_mock
    ):
        response = lambda_handler.handler(
            {"body": json.dumps(payload), "headers": {"Authorization": "Bearer secret"}},
            None,
        )

    assert response["statusCode"] == 502
    dynamodb_mock.update_item.assert_not_called()


def test_rejects_expired_timestamp(monkeypatch):
    monkeypatch.setenv("SQS_QUEUE_URL", "https://sqs.queue")
    monkeypatch.setenv("DCE_TABLE_NAME", "dce-table")
    monkeypatch.setenv("API_AUTH_TOKEN", "secret")
    monkeypatch.setenv("CANCELLATION_DEADLINE_MINUTES", "60")
    expired_payload = {**_fresh_payload(), "timestamp": "2023-01-01T00:00:00+00:00"}

    response = lambda_handler.handler(
        {"body": json.dumps(expired_payload), "headers": {"Authorization": "Bearer secret"}},
        None,
    )

    assert response["statusCode"] == 400


def test_rejects_unauthorized_client(monkeypatch):
    monkeypatch.setenv("SQS_QUEUE_URL", "https://sqs.queue")
    monkeypatch.setenv("DCE_TABLE_NAME", "dce-table")
    monkeypatch.setenv("API_AUTH_TOKEN", "secret")
    monkeypatch.setenv("ALLOWED_CLIENT_IDS", "another-client")
    monkeypatch.setenv("CANCELLATION_DEADLINE_MINUTES", "525600")
    payload = _fresh_payload()

    response = lambda_handler.handler(
        {"body": json.dumps(payload), "headers": {"Authorization": "Bearer secret"}},
        None,
    )

    assert response["statusCode"] == 403


def test_requires_auth_header(monkeypatch):
    monkeypatch.setenv("SQS_QUEUE_URL", "https://sqs.queue")
    monkeypatch.setenv("DCE_TABLE_NAME", "dce-table")
    monkeypatch.setenv("API_AUTH_TOKEN", "secret")
    payload = _fresh_payload()

    response = lambda_handler.handler({"body": json.dumps(payload)}, None)

    assert response["statusCode"] == 401
