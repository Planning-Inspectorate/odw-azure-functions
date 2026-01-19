"""
Module containing functions to read messages from Service Bus and send them to Azure Storage.

Functions:
- get_messages: Retrieve messages from a Service Bus topic subscription.
- send_to_storage: Upload data to Azure Blob Storage.
"""

import logging
from azure.servicebus import ServiceBusClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import json
from validate_messages import validate_data



from typing import List, Optional, Union
import json
import logging
from azure.identity import DefaultAzureCredential
from azure.servicebus import ServiceBusClient

def get_messages_and_validate(
    namespace: str,
    credential: DefaultAzureCredential,
    topic: str,
    subscription: str,
    max_message_count: int,
    max_wait_time: int,
    schema: dict,
    override_messages: Optional[List[Union[str, dict]]] = None,
) -> list:
    """
    Retrieve and validate messages from a Service Bus topic subscription (pull mode),
    OR validate already-received messages provided via `override_messages` (push mode).

    Returns a list of validated message bodies with selected properties attached.
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
        # -------------------------------
        # PUSH MODE (Service Bus trigger)
        # -------------------------------
        if override_messages is not None:
            logging.info("Processing overridden messages (SB trigger path) ...")

            for raw in override_messages:
                # raw can be a JSON string or a dict
                body = json.loads(raw) if isinstance(raw, str) else dict(raw)

                msg_type = body.get("type")  # best-effort; SB props not available here
                if msg_type in message_type_mapping:
                    message_type_mapping[msg_type].append(body)
                else:
                    other_message_types.append(body)

                errors = validate_data(body, schema)
                if not errors:
                    body["message_type"] = msg_type
                    valid_with_properties.append(body)
                else:
                    invalid_messages.append({"body": body, "errors": errors})
                    logging.error("Message failed validation: %s", errors)

            logging.info("Validated %d message(s) via override", len(valid_with_properties))
            return valid_with_properties

        # -------------------------------
        # PULL MODE (HTTP/manual path)
        # -------------------------------
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
                    print(f"{len(received_msgs)} messages received - processing them one by one...")

                    for message in received_msgs:
                        message_body = json.loads(str(message))
                        message_enqueued_time_utc = message.enqueued_time_utc.strftime(
                            "%Y-%m-%dT%H:%M:%S.%f%z"
                        )
                        message_id = message.message_id
                        properties = message.application_properties
                        message_type = properties.get(b"type", None)
                        if message_type is not None:
                            message_type = message_type.decode("utf-8")

                        if message_type in message_type_mapping:
                            message_type_mapping[message_type].append(message_body)
                        else:
                            other_message_types.append(message_body)

                        validation_errors = validate_data(message_body, schema)

                        if not validation_errors:
                            valid_messages.append(message_body)
                            subscription_receiver.complete_message(message)
                            message_body["message_type"] = message_type
                            message_body["message_enqueued_time_utc"] = message_enqueued_time_utc
                            message_body["message_id"] = message_id
                            valid_with_properties.append(message_body)
                        else:
                            invalid_messages.append({
                                "message_id": message_id,
                                "body": message_body,
                                "errors": validation_errors
                            })
                            logging.error(
                                "Message ID %s failed validation: %s",
                                message_id, validation_errors
                            )
                            subscription_receiver.dead_letter_message(
                                message,
                                reason="Failed validation against schema",
                                error_description="; ".join(validation_errors)
                            )
                    print(f"Valid: {len(valid_messages)}\nInvalid: {len(invalid_messages)}")
                else:
                    print("No messages received")

    except Exception as e:
        print(f"Error processing messages\n{e}")
        raise

    return valid_with_properties



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
