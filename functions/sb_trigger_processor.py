"""
Service Bus Trigger Processor

Implements the real-time drain pattern:
- Deserialize JSON message
- Enrich with metadata (message_id, enqueued_time, message_type, delivery_count)
- Validate against schema
- If valid: write to RAW storage
- If invalid: write to quarantine storage + raise so message DLQs via retry policy

Note: DLQ custom reason cannot be set directly via the Python trigger binding in most cases,
so we preserve failure details in Storage (quarantine) and logs.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import azure.functions as func

from validate_messages import validate_data
from servicebus_funcs import send_to_storage
from entity_registry import EntitySpec


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%z")


def process_servicebus_trigger_message(
    *,
    msg: func.ServiceBusMessage,
    entity: EntitySpec,
    schema: dict,
    storage_account_url: str,
    storage_container: str,
    credential: Any,
    validation_error_token: str,
) -> None:
    """
    Process a single Service Bus message for an entity.
    """

    # Deserialize
    body_raw = msg.get_body().decode("utf-8")
    try:
        message_body = json.loads(body_raw)
    except Exception as e:
        logging.error(
            "SB_TRIGGER JSON_DESERIALIZE_FAILED entity=%s message_id=%s delivery_count=%s body=%s error=%s",
            entity.key,
            getattr(msg, "message_id", None),
            getattr(msg, "delivery_count", None),
            body_raw[:2000],
            str(e),
        )

        # Quarantine invalid JSON (so we can inspect it later)
        failure_record = {
            "entity": entity.key,
            "schema": entity.schema_filename,
            "failure_type": "invalid_json",
            "message_id": getattr(msg, "message_id", None),
            "delivery_count": getattr(msg, "delivery_count", None),
            "enqueued_time_utc": getattr(msg, "enqueued_time_utc", None),
            "received_utc": _utc_now_iso(),
            "raw_body": body_raw,
            "error": str(e),
        }
        send_to_storage(
            account_url=storage_account_url,
            credential=credential,
            container=storage_container,
            entity=f"{entity.storage_entity}__invalid",
            data=[failure_record],
        )

        raise ValueError(f"{validation_error_token}: message body is not valid JSON") from e

    # Metadata enrichment
    user_props = getattr(msg, "user_properties", None) or {}
    msg_type = (
        user_props.get("type")
        or user_props.get(b"type")
        or user_props.get("Type")
        or user_props.get(b"Type")
    )
    if isinstance(msg_type, (bytes, bytearray)):
        msg_type = msg_type.decode("utf-8")

    enqueued_dt = getattr(msg, "enqueued_time_utc", None)
    if isinstance(enqueued_dt, datetime):
        enqueued_time_utc = enqueued_dt.strftime("%Y-%m-%dT%H:%M:%S.%f%z") or enqueued_dt.isoformat()
    else:
        enqueued_time_utc = _utc_now_iso()

    message_id = getattr(msg, "message_id", None)
    delivery_count = getattr(msg, "delivery_count", None)

    # Validate
    validation_errors = validate_data(message_body, schema)

    if validation_errors:
        logging.error(
            "SB_TRIGGER SCHEMA_VALIDATION_FAILED entity=%s message_id=%s delivery_count=%s errors=%s",
            entity.key,
            message_id,
            delivery_count,
            validation_errors,
        )

        # Quarantine record (this is where we store the real reason and the raw payload)
        failure_record = {
            "entity": entity.key,
            "schema": entity.schema_filename,
            "failure_type": "schema_validation",
            "message_id": message_id,
            "delivery_count": delivery_count,
            "message_type": msg_type,
            "message_enqueued_time_utc": enqueued_time_utc,
            "received_utc": _utc_now_iso(),
            "validation_errors": validation_errors,
            "body": message_body,
        }

        send_to_storage(
            account_url=storage_account_url,
            credential=credential,
            container=storage_container,
            entity=f"{entity.storage_entity}__invalid",
            data=[failure_record],
        )

        # Fail invocation so SB retries & DLQ once MaxDeliveryCount exceeded
        raise ValueError(f"{validation_error_token}: " + "; ".join(validation_errors))

    message_body["message_type"] = msg_type
    message_body["message_enqueued_time_utc"] = enqueued_time_utc
    message_body["message_id"] = message_id

    # Persist to RAW
    # For the pilot: 1 message > 1 blob write (simple, reliable and easy to debug)
    send_to_storage(
        account_url=storage_account_url,
        credential=credential,
        container=storage_container,
        entity=entity.storage_entity,
        data=[message_body],
    )

    logging.info(
        "SB_TRIGGER OK entity=%s message_id=%s delivery_count=%s",
        entity.key,
        message_id,
        delivery_count,
    )
