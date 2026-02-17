
import os
import json
import logging
from datetime import datetime, timezone
from uuid import uuid4
from typing import List, Tuple

import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


# ---------- Storage configuration ----------
ACCOUNT_URL = os.environ["MESSAGE_STORAGE_ACCOUNT"]
CONTAINER   = os.environ["MESSAGE_STORAGE_CONTAINER"]

blob_service = BlobServiceClient(
    account_url=ACCOUNT_URL,
    credential=DefaultAzureCredential()
)
topic_name = os.environ.get(topic_env)
subscription_name = os.environ.get(subscription_env)
@app.function_name(name=f"{entity}_batch_trigger")
@app.service_bus_topic_trigger(
        arg_name="message",
        topic_name=topic_name,
        subscription_name=subscription_name,
        connection=connection_setting,
        data_type=func.DataType.STRING,
        cardinality=func.Cardinality.ONE
        #func.Cardinality.MANY
        
    )
def _handler(message: List[func.ServiceBusMessage]) -> None:
        logging.info(f"[{entity}] Service Bus batch trigger fired: {len(message)} message(s)")

        decoded_payloads: List[str] = []
        failed: List[Tuple[str, str]] = []  # (message_id, error_text)# Decode all message first (no storage yet)
        for msg in message:
            mid = getattr(msg, "message_id", "<unknown>")
            try:
                decoded_payloads.append(msg.get_body().decode("utf-8"))
            except Exception as e:
                # Record failure; do NOT swallow it.
                err = f"Decode failed for message_id={mid}: {e}"
                logging.error(f"[{entity}] {err}")
                failed.append((mid, str(e)))

        # 2) If any decode failed -> raise to force retry/DLQ path
        if failed:
            # Raising ensures:
            # - With autoCompleteMessages=false, the batch is NOT completed
            # - Messages will be retried; after maxDeliveryCount they go to DLQ
            raise RuntimeError(
                f"[{entity}] {len(failed)} message(s) failed to decode. "
                f"First error: {failed[0][0]} -> {failed[0][1]}"
            )

        if not decoded_payloads:
            # If we get here, there were zero message or all failed, but the previous branch raises on any failure.
            # So this is only possible when batch is truly empty.
            logging.warning(f"[{entity}] Empty batch received")
            return

        # 3) Persist the decoded batch atomically (fail -> raise)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        blob_name = f"{entity}/batch-{timestamp}-{uuid4().hex}.json"

        try:
            blob_client = blob_service.get_blob_client(container=CONTAINER, blob=blob_name)
            blob_client.upload_blob(json.dumps(decoded_payloads), overwrite=False)
        except Exception as e:
            # CRITICAL: Do NOT swallow. RAISE so runtime retries and message are not completed.
            logging.exception(f"[{entity}] Blob upload failed for {blob_name}")
            raise RuntimeError(f"[{entity}] Blob upload failed: {e}") from e

        logging.info(f"[{entity}] Wrote {len(decoded_payloads)} message to blob {blob_name}")
