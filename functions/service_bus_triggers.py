
import os
import json
import logging
from datetime import datetime, timezone
from uuid import uuid4
from typing import List

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

# Reuse the single FunctionApp instance
from function_app import _app as app


# ---------- Storage configuration ----------
ACCOUNT_URL = os.environ["MESSAGE_STORAGE_ACCOUNT"]
CONTAINER   = os.environ["MESSAGE_STORAGE_CONTAINER"]

blob_service = BlobServiceClient(
    account_url=ACCOUNT_URL,
    credential=DefaultAzureCredential()
)


def register_batch_trigger(
    entity: str,
    topic_env: str,
    subscription_env: str,
    connection_setting: str = "ServiceBusConnection",
):
    """
    Registers a Service Bus Topic batch trigger for the given entity.

    - Receives messages in batches
    - Writes one JSON array per invocation to Blob Storage
    """

    topic_name = os.environ.get(topic_env)
    subscription_name = os.environ.get(subscription_env)

    if not topic_name or not subscription_name:
        raise RuntimeError(
            f"Missing required environment variables: "
            f"{topic_env}, {subscription_env}"
        )

    @app.function_name(name=f"{entity}_batch_trigger")
    @app.service_bus_topic_trigger(
        arg_name="messages",
        topic_name=topic_name,
        subscription_name=subscription_name,
        connection=connection_setting,
        data_type=func.DataType.STRING,
        cardinality=func.Cardinality.MANY
    )
    def _handler(messages: List[func.ServiceBusMessage]) -> None:
        logging.info(f"[{entity}] Service Bus batch trigger fired")

        batch: List[str] = []

        for msg in messages:
            try:
                batch.append(msg.get_body().decode("utf-8"))
            except Exception as e:
                logging.error(
                    f"[{entity}] Failed to decode message "
                    f"(message_id={msg.message_id}): {e}"
                )

        if not batch:
            logging.warning(f"[{entity}] Empty batch received")
            return

        # Blob path: <entity>/batch-<timestamp>-<uuid>.json
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        blob_name = f"{entity}/batch-{timestamp}-{uuid4().hex}.json"

        blob_client = blob_service.get_blob_client(
            container=CONTAINER,
            blob=blob_name
        )

        blob_client.upload_blob(
            json.dumps(batch),
            overwrite=False
        )

        logging.info(
            f"[{entity}] Successfully wrote {len(batch)} messages to blob {blob_name}"
        )
