# generic_batch_trigger.py
import os
import json
import logging
from datetime import datetime, timezone
from uuid import uuid4
from typing import List

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

# Reuse the existing FunctionApp instance from function_app.py
from function_app import _app as app

# Blob client using Managed Identity
ACCOUNT_URL = os.environ["MESSAGE_STORAGE_ACCOUNT"]
CONTAINER   = os.environ["MESSAGE_STORAGE_CONTAINER"]
blob_service = BlobServiceClient(account_url=ACCOUNT_URL, credential=DefaultAzureCredential())

def register_batch_trigger(entity: str, topic_env: str, subscription_env: str, connection_setting: str = "ServiceBusConnection"):
    """
    Registers a batch Service Bus Topic Trigger for the given entity.
    Writes one JSON file per batch to Blob Storage.
    """
    TOPIC = os.environ[topic_env]
    SUB   = os.environ[subscription_env]

    @app.function_name(name=f"{entity}_batch_trigger")
    @app.service_bus_topic_trigger(
        arg_name="messages",
        topic_name=TOPIC,
        subscription_name=SUB,
        connection=connection_setting,
        data_type=func.DataType.STRING,
        cardinality=func.Cardinality.MANY
    )
    def _handler(messages: List[func.ServiceBusMessage]) -> None:
        batch = []
        for msg in messages:
            try:
                body = msg.get_body().decode("utf-8")
                batch.append(body)  # Keep raw message as string
            except Exception as e:
                logging.error(f"Failed to process message: {e}")

        # Create unique blob name
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        blob_name = f"{entity}/batch-{ts}-{uuid4().hex}.json"

        # Write batch as JSON array
        blob_client = blob_service.get_blob_client(container=CONTAINER, blob=blob_name)
        blob_client.upload_blob(json.dumps(batch), overwrite=False)

        logging.info(f"[{entity}] Wrote {len(batch)} messages to blob: {blob_name}")
