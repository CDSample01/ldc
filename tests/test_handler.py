import json
from unittest.mock import MagicMock, patch

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
}


def _mock_client():
    return MagicMock()


def test_successful_enqueue_and_update(monkeypatch):
    monkeypatch.setenv("SQS_QUEUE_URL", "https://sqs.queue")
    monkeypatch.setenv("DCE_TABLE_NAME", "dce-table")

    sqs_mock = _mock_client()
    dynamodb_mock = _mock_client()

    with patch("dce_cancel.handler.sqs_client", return_value=sqs_mock), patch(
        "dce_cancel.handler.dynamodb_client", return_value=dynamodb_mock
    ):
        response = lambda_handler.handler({"body": json.dumps(VALID_PAYLOAD)}, None)

    assert response["statusCode"] == 202

    sqs_mock.send_message.assert_called_once()
    call_kwargs = sqs_mock.send_message.call_args.kwargs
    assert call_kwargs["QueueUrl"] == "https://sqs.queue"
    body = json.loads(call_kwargs["MessageBody"])
    assert body["dceId"] == VALID_PAYLOAD["dceId"]
    assert body["event"]["code"] == "110111"

    dynamodb_mock.update_item.assert_called_once()
    assert dynamodb_mock.update_item.call_args.kwargs["TableName"] == "dce-table"


def test_validation_failure_returns_400(monkeypatch):
    monkeypatch.setenv("SQS_QUEUE_URL", "https://sqs.queue")
    monkeypatch.setenv("DCE_TABLE_NAME", "dce-table")

    with patch("dce_cancel.handler.sqs_client") as sqs_mock, patch(
        "dce_cancel.handler.dynamodb_client"
    ) as dynamodb_mock:
        response = lambda_handler.handler({"body": json.dumps({})}, None)

    assert response["statusCode"] == 400
    sqs_mock.return_value.send_message.assert_not_called()
    dynamodb_mock.return_value.update_item.assert_not_called()


def test_sqs_failure_returns_502(monkeypatch):
    monkeypatch.setenv("SQS_QUEUE_URL", "https://sqs.queue")
    monkeypatch.setenv("DCE_TABLE_NAME", "dce-table")

    error_response = {"Error": {"Code": "500", "Message": "boom"}}
    sqs_mock = _mock_client()
    sqs_mock.send_message.side_effect = ClientError(error_response, "SendMessage")
    dynamodb_mock = _mock_client()

    with patch("dce_cancel.handler.sqs_client", return_value=sqs_mock), patch(
        "dce_cancel.handler.dynamodb_client", return_value=dynamodb_mock
    ):
        response = lambda_handler.handler({"body": json.dumps(VALID_PAYLOAD)}, None)

    assert response["statusCode"] == 502
    dynamodb_mock.update_item.assert_not_called()
