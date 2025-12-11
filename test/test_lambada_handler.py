import json
from unittest.mock import MagicMock, patch

from datetime import datetime, timezone
from botocore.exceptions import ClientError

import lambada_handler


VALID_PAYLOAD = {
    "id": "1234567890",
    "cancelReason": "Solicitação de cancelamento por duplicidade.",
}


def _fresh_payload() -> dict:
    return json.loads(json.dumps(VALID_PAYLOAD))


def _mock_client():
    return MagicMock()


def _mock_dynamodb_resource(items=None):
    resource = _mock_client()
    table = _mock_client()
    table.query.return_value = {"Items": items if items is not None else [{}]}
    resource.Table.return_value = table
    return resource, table


def test_successful_enqueue_and_update(monkeypatch):
    monkeypatch.setenv("SQS_QUEUE_URL", "https://sqs.queue")
    monkeypatch.setenv("DCE_TABLE_NAME", "dce-table")
    monkeypatch.setenv("API_AUTH_TOKEN", "secret")
    monkeypatch.setenv("LOG_DCE_TABLE_NAME", "logDce")
    monkeypatch.setenv("CANCELLATION_DEADLINE_MINUTES", "525600")

    sqs_mock = _mock_client()
    dynamodb_mock = _mock_client()
    dynamodb_resource_mock, log_table_mock = _mock_dynamodb_resource()
    payload = _fresh_payload()

    with patch("lambada_handler.sqs_client", return_value=sqs_mock), patch(
        "lambada_handler.dynamodb_client", return_value=dynamodb_mock
    ), patch("lambada_handler.dynamodb_resource", return_value=dynamodb_resource_mock):
        response = lambada_handler.handler(
            {
                "body": json.dumps(payload),
                "headers": {"Authorization": "Bearer secret", "Client-Id": "partner-123"},
            },
            None,
        )

    assert response["statusCode"] == 201

    log_table_mock.query.assert_called_once()

    sqs_mock.send_message.assert_called_once()
    call_kwargs = sqs_mock.send_message.call_args.kwargs
    assert call_kwargs["QueueUrl"] == "https://sqs.queue"
    body = json.loads(call_kwargs["MessageBody"])
    assert body["id"] == payload["id"]
    assert body["eventCode"] == "110111"

    dynamodb_mock.update_item.assert_called_once()
    assert dynamodb_mock.update_item.call_args.kwargs["TableName"] == "dce-table"
    update_values = dynamodb_mock.update_item.call_args.kwargs["ExpressionAttributeValues"]
    assert update_values[":clientId"]["S"] == "partner-123"
    assert update_values[":status"]["S"] == "CANCELLATION_REQUESTED"
    event_timestamp = update_values[":eventTimestamp"]["S"]
    parsed_event_timestamp = datetime.fromisoformat(event_timestamp.replace("Z", "+00:00"))
    assert (datetime.now(timezone.utc) - parsed_event_timestamp).total_seconds() < 30
    assert update_values[":reason"]["S"] == payload["cancelReason"]


def test_validation_failure_returns_400(monkeypatch):
    monkeypatch.setenv("SQS_QUEUE_URL", "https://sqs.queue")
    monkeypatch.setenv("DCE_TABLE_NAME", "dce-table")
    monkeypatch.setenv("API_AUTH_TOKEN", "secret")
    monkeypatch.setenv("CANCELLATION_DEADLINE_MINUTES", "525600")

    with patch("lambada_handler.sqs_client") as sqs_mock, patch(
        "lambada_handler.dynamodb_client"
    ) as dynamodb_mock:
        response = lambada_handler.handler(
            {
                "body": json.dumps({}),
                "headers": {"Authorization": "Bearer secret", "Client-Id": "partner-123"},
            },
            None,
        )

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
    dynamodb_resource_mock, _ = _mock_dynamodb_resource()
    payload = _fresh_payload()

    with patch("lambada_handler.sqs_client", return_value=sqs_mock), patch(
        "lambada_handler.dynamodb_client", return_value=dynamodb_mock
    ), patch("lambada_handler.dynamodb_resource", return_value=dynamodb_resource_mock):
        response = lambada_handler.handler(
            {
                "body": json.dumps(payload),
                "headers": {"Authorization": "Bearer secret", "Client-Id": "partner-123"},
            },
            None,
        )

    assert response["statusCode"] == 502
    dynamodb_mock.update_item.assert_not_called()


def test_rejects_unauthorized_client(monkeypatch):
    monkeypatch.setenv("SQS_QUEUE_URL", "https://sqs.queue")
    monkeypatch.setenv("DCE_TABLE_NAME", "dce-table")
    monkeypatch.setenv("API_AUTH_TOKEN", "secret")
    monkeypatch.setenv("LOG_DCE_TABLE_NAME", "logDce")
    monkeypatch.setenv("CANCELLATION_DEADLINE_MINUTES", "525600")
    payload = _fresh_payload()

    dynamodb_resource_mock, _ = _mock_dynamodb_resource(items=[])

    with patch("lambada_handler.dynamodb_resource", return_value=dynamodb_resource_mock):
        response = lambada_handler.handler(
            {
                "body": json.dumps(payload),
                "headers": {"Authorization": "Bearer secret", "Client-Id": "partner-123"},
            },
            None,
        )

    assert response["statusCode"] == 403


def test_requires_auth_header(monkeypatch):
    monkeypatch.setenv("SQS_QUEUE_URL", "https://sqs.queue")
    monkeypatch.setenv("DCE_TABLE_NAME", "dce-table")
    monkeypatch.setenv("API_AUTH_TOKEN", "secret")
    payload = _fresh_payload()

    response = lambada_handler.handler({"body": json.dumps(payload), "headers": {"Client-Id": "partner-123"}}, None)

    assert response["statusCode"] == 401


def test_requires_client_id_header(monkeypatch):
    monkeypatch.setenv("SQS_QUEUE_URL", "https://sqs.queue")
    monkeypatch.setenv("DCE_TABLE_NAME", "dce-table")
    monkeypatch.setenv("API_AUTH_TOKEN", "secret")
    payload = _fresh_payload()

    response = lambada_handler.handler({"body": json.dumps(payload), "headers": {"Authorization": "Bearer secret"}}, None)

    assert response["statusCode"] == 401
