
import os
import json
import logging
from datetime import datetime, timezone
from uuid import uuid4
from typing import List

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from function_app import _app as app
from servicebus_funcs import validate_trigger_messages
from schemas import SCHEMAS   # assuming your schemas live here


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
    schema_name: str,
    connection_setting: str = "ServiceBusConnection",
):
    """
    Registers a Service Bus Topic batch trigger for the given entity.

    - Receives messages in batches
    - Validates using trigger-safe logic
    - Writes one JSON array of objects per invocation
    """

    topic_name = os.environ.get(topic_env)
    subscription_name = os.environ.get(subscription_env)

    if not topic_name or not subscription_name:
        raise RuntimeError(
            f"Missing required environment variables: "
            f"{topic_env}, {subscription_env}"
        )

    schema = SCHEMAS[schema_name]

    @app.function_name(name=f"{entity}_batch_trigger")
    @app.service_bus_topic_trigger(
        arg_name="messages",
        topic_name=topic_name,
        subscription_name=subscription_name,
        connection=connection_setting,
        cardinality=func.Cardinality.MANY
    )
    def _handler(messages: List[func.ServiceBusMessage]) -> None:
        logging.info(f"[{entity}] Service Bus batch trigger fired")

        if not messages:
            logging.warning(f"[{entity}] Empty batch received")
            return

        try:
           
            data = validate_trigger_messages(messages, schema)

        except Exception:
            logging.exception(f"[{entity}] Batch validation failed")
            raise

        # Blob path: <entity>/batch-<timestamp>-<uuid>.json
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        blob_name = f"{entity}/batch-{timestamp}-{uuid4().hex}.json"

        blob_client = blob_service.get_blob_client(
            container=CONTAINER,
            blob=blob_name
        )

        blob_client.upload_blob(
            json.dumps(data),
            overwrite=False
        )

        logging.info(
            f"[{entity}] Successfully wrote {len(data)} messages to blob {blob_name}"
        )
