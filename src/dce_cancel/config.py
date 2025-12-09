"""Configuration helpers for the DCe cancellation Lambda."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class EnvConfig:
    sqs_queue_url: str
    dce_table_name: str
    partition_key: str = "pk"
    sort_key: str = "sk"
    api_auth_token: str | None = None
    allowed_client_ids: set[str] | None = None
    cancellation_deadline_minutes: int = 60 * 24

    @classmethod
    def load(cls) -> "EnvConfig":
        queue_url = os.getenv("SQS_QUEUE_URL")
        table_name = os.getenv("DCE_TABLE_NAME")
        partition_key = os.getenv("DCE_TABLE_PK", "pk")
        sort_key = os.getenv("DCE_TABLE_SK", "sk")
        api_auth_token = os.getenv("API_AUTH_TOKEN")
        allowed_client_ids_env = os.getenv("ALLOWED_CLIENT_IDS", "")
        cancellation_deadline_minutes = int(
            os.getenv("CANCELLATION_DEADLINE_MINUTES", cls.cancellation_deadline_minutes)
        )

        missing = [
            name
            for name, value in (
                ("SQS_QUEUE_URL", queue_url),
                ("DCE_TABLE_NAME", table_name),
            )
            if not value
        ]
        if missing:
            raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")

        return cls(
            sqs_queue_url=queue_url,
            dce_table_name=table_name,
            partition_key=partition_key,
            sort_key=sort_key,
            api_auth_token=api_auth_token,
            allowed_client_ids={c.strip() for c in allowed_client_ids_env.split(",") if c.strip()}
            or None,
            cancellation_deadline_minutes=cancellation_deadline_minutes,
        )
