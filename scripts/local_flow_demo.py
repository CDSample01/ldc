import json
import os
from datetime import datetime, timezone

import boto3
from moto import mock_aws

import lambada_handler


PAYLOAD = {
    "dceId": "DCE123",
    "event": {
        "code": "110111",
        "schemaVersion": "1.00",
        "sequenceNumber": 1,
        "reason": "Cancelamento local de teste",
        "protocol": "123456789012345",
    },
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "issuer": {"cnpj": "12345678000199"},
    "metadata": {"state": "RS"},
    "clientId": "demo-client",
}


def main() -> None:
    region = os.getenv("AWS_REGION", "us-east-1")

    with mock_aws():
        # Bootstrap fake AWS resources
        sqs = boto3.client("sqs", region_name=region)
        queue_url = sqs.create_queue(QueueName="dce-cancel-queue")["QueueUrl"]

        dynamodb = boto3.client("dynamodb", region_name=region)
        table_name = "dce-cancel-table"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
            BillingMode="PAY_PER_REQUEST",
        )

        # Point the Lambda toward the mocked endpoints
        os.environ["SQS_QUEUE_URL"] = queue_url
        os.environ["DCE_TABLE_NAME"] = table_name
        os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
        os.environ.setdefault("AWS_SESSION_TOKEN", "testing")
        os.environ.setdefault("API_AUTH_TOKEN", "local-token")
        os.environ.setdefault("ALLOWED_CLIENT_IDS", PAYLOAD["clientId"])

        # Invoke the handler just like API Gateway would
        event = {"body": json.dumps(PAYLOAD), "headers": {"Authorization": "Bearer local-token"}}
        response = lambada_handler.handler(event, None)
        print("Lambda response:\n", json.dumps(response, indent=2))

        # Read back what went to SQS and DynamoDB
        messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1).get("Messages", [])
        if messages:
            print("\nMessage enqueued to SQS:\n", messages[0]["Body"])

        item = dynamodb.get_item(
            TableName=table_name,
            Key={"pk": {"S": "DCE#" + PAYLOAD["dceId"]}, "sk": {"S": "LATEST"}},
        ).get("Item")
        if item:
            printable = {k: list(v.values())[0] for k, v in item.items()}
            print("\nDynamoDB item:\n", json.dumps(printable, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
