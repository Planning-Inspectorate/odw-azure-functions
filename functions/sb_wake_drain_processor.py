"""
Wake + Drain Processor

Pattern:
- A lightweight wake subscription triggers the Function when messages arrive
- The Function then drains the REAL subscription using the azure-servicebus SDK
- On validation failure, we explicitly dead letter messages on the REAL subscription
  so that DeadLetterReason and DeadLetterErrorDescription are populated in Service Bus

This avoids relying on Data Lake quarantine as the primary source of failure reasons which was the other options if staying fully with ServiceBusTrigger
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.servicebus.exceptions import ServiceBusError
import azure.functions as func

from entity_registry import EntitySpec
from validate_messages import validate_data
from servicebus_funcs import send_to_storage


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%z")


def _truncate(value: str, max_len: int) -> str:
    if value is None:
        return ""
    if len(value) <= max_len:
        return value
    return value[: max_len - 3] + "..."


def _extract_message_type(msg: ServiceBusMessage) -> Optional[str]:
    props = getattr(msg, "application_properties", None) or {}
    msg_type = (
        props.get("type")
        or props.get(b"type")
        or props.get("Type")
        or props.get(b"Type")
    )
    if isinstance(msg_type, (bytes, bytearray)):
        try:
            return msg_type.decode("utf-8")
        except Exception:
            return None
    if isinstance(msg_type, str):
        return msg_type
    return None


def _dead_letter(
    receiver,
    message: ServiceBusMessage,
    *,
    reason: str,
    description: str,
) -> None:
    """
    Explicit DLQ to show meaningful DeadLetterReason/Description
    Note: SB has limits on these fields, so we truncate
    """
    # Keep reasons short and stable
    reason = _truncate(reason, 128)
    description = _truncate(description, 4096)

    receiver.dead_letter_message(
        message,
        reason=reason,
        error_description=description,
    )


def process_wake_and_drain(
    *,
    wake_msg: func.ServiceBusMessage,
    entity: EntitySpec,
    schema: dict,
    storage_account_url: str,
    storage_container: str,
    credential: Any,
    namespace: str,
    max_message_count: int,
    max_wait_time_seconds: int,
) -> None:
    """
    Triggered by a wake subscription message.

    Important:
    - We do NOT drain the wake subscription.
    - We drain the REAL subscription (entity.subscription).
    - If the function returns successfully, the wake message is auto completed by Functions runtime.
    """
    logging.info(
        "WAKE_DRAIN TRIGGERED entity=%s topic=%s wake_sub=%s drain_sub=%s",
        entity.key,
        entity.topic,
        entity.trigger_subscription,
        entity.subscription,
    )

    if not namespace:
        raise ValueError(f"Missing Service Bus namespace env var for entity={entity.key}")

    drained_total = 0

    # Drain loop: receive batches until empty or hit max_message_count
    try:
        sb_client = ServiceBusClient(fully_qualified_namespace=namespace, credential=credential)

        with sb_client:
            receiver = sb_client.get_subscription_receiver(
                topic_name=entity.topic,
                subscription_name=entity.subscription,
                prefetch_count=100,
            )

            with receiver:
                while drained_total < max_message_count:
                    batch_size = min(50, max_message_count - drained_total)
                    messages = receiver.receive_messages(
                        max_message_count=batch_size,
                        max_wait_time=max_wait_time_seconds,
                    )

                    if not messages:
                        break

                    for msg in messages:
                        drained_total += 1
                        _process_one_message(
                            receiver=receiver,
                            msg=msg,
                            entity=entity,
                            schema=schema,
                            storage_account_url=storage_account_url,
                            storage_container=storage_container,
                            credential=credential,
                        )

    except ServiceBusError as e:
        logging.exception("WAKE_DRAIN SERVICEBUS_ERROR entity=%s error=%s", entity.key, str(e))
        raise
    except Exception as e:
        logging.exception("WAKE_DRAIN UNEXPECTED_ERROR entity=%s error=%s", entity.key, str(e))
        raise

    logging.info("WAKE_DRAIN DONE entity=%s drained_total=%s", entity.key, drained_total)


def _process_one_message(
    *,
    receiver,
    msg: ServiceBusMessage,
    entity: EntitySpec,
    schema: dict,
    storage_account_url: str,
    storage_container: str,
    credential: Any,
) -> None:
    """
    Process ONE message from the REAL subscription
    - JSON parse
    - schema validate
    - on fail > dead letter it to the real subscription DLQ
    - on success > write to storage + complete
    """
    message_id = getattr(msg, "message_id", None)
    delivery_count = getattr(msg, "delivery_count", None)

    try:
        body_raw = b"".join([b for b in msg.body])
        body_text = body_raw.decode("utf-8", errors="replace")
    except Exception:
        _dead_letter(
            receiver,
            msg,
            reason="BodyReadFailed",
            description="Could not read message body bytes/utf-8 decode.",
        )
        return

    try:
        payload = json.loads(body_text)
    except Exception as e:
        _dead_letter(
            receiver,
            msg,
            reason="InvalidJson",
            description=f"JSON deserialization failed: {str(e)}. Body(sample)={body_text[:500]}",
        )
        return

    errors = validate_data(payload, schema)
    if errors:
        errors_joined = "; ".join(errors)
        _dead_letter(
            receiver,
            msg,
            reason="SchemaValidationFailed",
            description=f"{errors_joined}",
        )
        return

    msg_type = _extract_message_type(msg)

    enqueued = getattr(msg, "enqueued_time_utc", None)
    if isinstance(enqueued, datetime):
        enqueued_time_utc = enqueued.strftime("%Y-%m-%dT%H:%M:%S.%f%z") or enqueued.isoformat()
    else:
        enqueued_time_utc = _utc_now_iso()

    payload["message_id"] = message_id
    payload["message_type"] = msg_type
    payload["message_enqueued_time_utc"] = enqueued_time_utc

    try:
        send_to_storage(
            account_url=storage_account_url,
            credential=credential,
            container=storage_container,
            entity=entity.storage_entity,
            data=[payload],
        )
        receiver.complete_message(msg)

        logging.info(
            "WAKE_DRAIN OK entity=%s message_id=%s delivery_count=%s",
            entity.key,
            message_id,
            delivery_count,
        )
    except Exception as e:
        # If storage write fails we don't complete
        # Let the message be retried we do not DLQ infra problems here
        logging.exception(
            "WAKE_DRAIN STORAGE_WRITE_FAILED entity=%s message_id=%s error=%s",
            entity.key,
            message_id,
            str(e),
        )
        raise
