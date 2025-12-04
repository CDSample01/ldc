"""Client factories for AWS services used by the Lambda."""
from __future__ import annotations

import boto3
from boto3.session import Session


def _get_session() -> Session:
    return boto3.session.Session()


def sqs_client():
    return _get_session().client("sqs")


def dynamodb_client():
    return _get_session().client("dynamodb")
