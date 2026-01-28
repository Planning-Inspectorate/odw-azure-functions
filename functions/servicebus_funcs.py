
from __future__ import annotations
import json
import logging
from collections import OrderedDict
from typing import Any, Dict, List, NamedTuple,Optional
# Keep just this; don't also import ServiceBusMessage directly
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient,ContentSettings
from azure.servicebus import ServiceBusClient
# Your validator (adjust the module path if different)
from validate_messages import validate_data
def send_to_storage(
    account_url: str,
    credential: DefaultAzureCredential,
    container: str,
    entity: str,
    data: list[list | dict],
) -> int:
    """
    Upload data to Azure Blob Storage.

    Args:
        account_url (str): The URL of the Azure Blob Storage account.
        credential: The credential object for authentication.
        container (str): The name of the container in Azure Blob Storage.
        entity (str): The name of the entity, e.g. service-user, nsip-project
        data: The data to be uploaded.

    Returns:
        int: a count of messages processed. This is used in the http response body.
    """

    from var_funcs import current_date, current_time

    _CURRENT_DATE = current_date()
    _CURRENT_TIME = current_time()
    _FILENAME = f"{entity}/{_CURRENT_DATE}/{entity}_{_CURRENT_TIME}.json"

    try:
        if data:
            print("Creating blob service client...")
            blob_service_client = BlobServiceClient(account_url, credential)
            print("Blob service client created")
            blob_client = blob_service_client.get_blob_client(container, blob=_FILENAME)
            print("Converting data to json format...")
            json_data = json.dumps(data)
            print("Data converted to json")
            print("Uploading file to storage...")
            blob_client.upload_blob(json_data, overwrite=True)
            print(f"JSON file '{_FILENAME}' uploaded to Azure Blob Storage.")

        else:
            print("No messages to send to storage")

    except Exception as e:
        print(f"Error sending to storage account\n{e}")
        raise e

    return len(data)

def send_to_storage_trigger(
    account_url: str,
    credential: DefaultAzureCredential,
    container: str,
    entity: str,
    data: List[Dict[str, Any]],
) -> Optional[int]:
    """
    Safe blob uploader.

    Returns:
        - positive int: number of records uploaded (success)
        - 0: no valid payloads (nothing to upload)
        - None: upload attempt failed (transient/permanent storage error)

    Never raises to caller.
    """

    if not data:
        # Important: this is not a failure; just nothing to do.
        logging.info("No valid data to upload for entity '%s'; skipping blob write.", entity)
        return 0

    # Keep your date/time partitioning but ensure uniqueness with a GUID.
    from var_funcs import current_date, current_time

    guid = uuid.uuid4().hex  # 32 chars, no hyphens
    filename = f"{entity}/{current_date()}/{entity}_{current_time()}_{guid}.json"

    try:
        blob_service = BlobServiceClient(account_url=account_url, credential=credential)
        blob_client = blob_service.get_blob_client(container=container, blob=filename)

        blob_client.upload_blob(
            json.dumps(data, ensure_ascii=False),
            overwrite=False,  # with GUID we do not expect collisions; fail fast if it ever happens
            content_settings=ContentSettings(content_type="application/json; charset=utf-8"),
        )

        logging.info("Uploaded %d records to %s", len(data), filename)
        return len(data)

    except Exception:
        # This is a real failure and should cause the trigger to retry.
        logging.exception("Storage upload failed for blob %s", filename)
        return None
    
def get_messages_and_validate(
    namespace: str,
    credential: DefaultAzureCredential,
    topic: str,
    subscription: str,
    max_message_count: int,
    max_wait_time: int,
    schema: dict,
) -> list:
    """
    Retrieve messages from a Service Bus topic subscription.

    Args:
        namespace (str): The fully qualified namespace of the Service Bus.
        credential: The credential object for authentication.
        topic (str): The name of the topic.
        subscription (str): The name of the subscription.
        max_message_count (int): The maximum number of messages to retrieve.
        max_wait_time (int): The maximum wait time in seconds.
        schema: The json schema to validate against.

    Returns:
        list: A list of messages retrieved from the topic subscription.
    """

    message_type_mapping: dict = {
        "Create": [],
        "Update": [],
        "Delete": [],
        "Publish": [],
        "Unpublish": [],
    }
    other_message_types: list = []
    valid_messages: list = []
    invalid_messages: list = []
    valid_with_properties: list = []

    try:
        print("Creating Servicebus client...")

        servicebus_client: ServiceBusClient = ServiceBusClient(
            fully_qualified_namespace=namespace, credential=credential
        )

        print("Creating receiver object...")

        with servicebus_client:
            subscription_receiver = servicebus_client.get_subscription_receiver(
                topic_name=topic, subscription_name=subscription
            )

            print("Receiving messages...")

            with subscription_receiver:
                received_msgs: list = subscription_receiver.receive_messages(
                    max_message_count, max_wait_time
                )

                if received_msgs:
                    print(
                        f"{len(received_msgs)} messages received - processing them one by one..."
                    )

                    for message in received_msgs:
                        message_body = json.loads(str(message))
                        message_enqueued_time_utc = message.enqueued_time_utc.strftime(
                            "%Y-%m-%dT%H:%M:%S.%f%z"
                        )
                        message_id = message.message_id
                        properties = message.application_properties
                        message_type = properties.get(b"type", None)
                        if message_type is not None:
                            message_type: str = message_type.decode("utf-8")
                        if message_type in message_type_mapping:
                            message_type_mapping[message_type].append(message_body)
                        else:
                            other_message_types.append(message_body)

                        validation_errors = validate_data(message_body, schema)

                        if not validation_errors:
                            valid_messages.append(message_body)
                            subscription_receiver.complete_message(message)
                            message_body["message_type"] = message_type
                            message_body[
                                "message_enqueued_time_utc"
                            ] = message_enqueued_time_utc
                            message_body["message_id"] = message_id
                            valid_with_properties.append(message_body)
                        else:
                            invalid_messages.append({
                                "message_id": message_id,
                                "body": message_body,
                                "errors": validation_errors
                            })
                            logging.error(
                                f"Message ID {message_id} failed validation: {validation_errors}"
                            )
                            subscription_receiver.dead_letter_message(
                                message, reason="Failed validation against schema", error_description="; ".join(validation_errors)
                            )

                    print(
                        f"Valid: {len(valid_messages)}\nInvalid: {len(invalid_messages)}"
                    )

                else:
                    print("No messages received")

    except Exception as e:
        print(f"Error processing messages\n{e}")
        raise e

    return valid_with_properties

def get_payloads_and_validate(
    messages: List[Any],
    schema: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Trigger-safe validator (batch-friendly):

    - Never raises for per-message validation/parse errors.
    - Returns only valid payloads (enriched), skipping invalid ones.
    - Enrichment happens AFTER validation for the valid ones only.
    - Excludes delivery_count and content_type as requested.
    - Metadata fields appended at the END in this order:
        message_type, message_enqueued_time_utc, message_id

    Assumes a callable `validate_data(payload: Dict, schema: Dict) -> List[str]`
    is available; it should return [] when valid or a list of error strings when invalid.
    """
    valid_with_properties: List[Dict[str, Any]] = []

    for m in messages:
        message_id = getattr(m, "message_id", "<unknown>")
        raw_text: Optional[str] = None

        try:
            # ---- 1) Decode RAW payload
            body = m.get_body() if hasattr(m, "get_body") else getattr(m, "body", None)

            if isinstance(body, (bytes, bytearray)):
                raw_text = body.decode("utf-8")
                payload = json.loads(raw_text)

            elif isinstance(body, str):
                raw_text = body
                payload = json.loads(raw_text)

            elif isinstance(body, dict):
                payload = body
                raw_text = json.dumps(body, ensure_ascii=False)

            else:
                raise ValueError(f"Unsupported message body type: {type(body)}")

            # ---- 2) Validate RAW payload (no enrichment yet)
            errors = validate_data(payload, schema)
            if errors:
                logging.warning(
                    "[Validation Failed] message_id=%s errors=%s raw=%s",
                    message_id,
                    errors,
                    raw_text,
                )
                # Skip this message; do NOT raise
                continue

            # ---- 3) Extract metadata (robust type extraction)
            message_type = None
            props = getattr(m, "application_properties", None)
            if props:
                raw_type = None
                if b"type" in props:
                    raw_type = props.get(b"type")
                elif "type" in props:
                    raw_type = props.get("type")

                if raw_type is not None:
                    message_type = (
                        raw_type.decode("utf-8")
                        if isinstance(raw_type, (bytes, bytearray))
                        else str(raw_type)
                    )

            message_enqueued_time_utc = None
            enq = getattr(m, "enqueued_time_utc", None)
            if enq:
                try:
                    message_enqueued_time_utc = enq.strftime("%Y-%m-%dT%H:%M:%S.%f%z") or None
                except Exception:
                    message_enqueued_time_utc = str(enq)

            # ---- 4) Enrich AFTER validation; append metadata at the end
            enriched: Dict[str, Any] = dict(payload)  # preserve payload order
            enriched["message_type"] = message_type
            enriched["message_enqueued_time_utc"] = message_enqueued_time_utc
            enriched["message_id"] = message_id

            valid_with_properties.append(enriched)

        except Exception as ex:
            # Per-message processing error: capture & continue (no raise)
            logging.exception("[Processing Error] message_id=%s raw=%s", message_id, raw_text)
            # Skip this message
            continue

    return valid_with_properties





