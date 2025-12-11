"""Adapter clients for external services."""
from .clients import dynamodb_client, sqs_client

__all__ = ["dynamodb_client", "sqs_client"]
