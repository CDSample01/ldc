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

    @classmethod
    def load(cls) -> "EnvConfig":
        queue_url = os.getenv("SQS_QUEUE_URL")
        table_name = os.getenv("DCE_TABLE_NAME")
        partition_key = os.getenv("DCE_TABLE_PK", "pk")
        sort_key = os.getenv("DCE_TABLE_SK", "sk")

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
        )
