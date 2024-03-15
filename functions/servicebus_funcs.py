"""
Module containing functions to read messages from Service Bus and send them to Azure Storage.

Functions:
- get_messages: Retrieve messages from a Service Bus topic subscription.
- send_to_storage: Upload data to Azure Blob Storage.
"""

from azure.servicebus import ServiceBusClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import json
from validate_messages import validate_data


def get_messages_and_validate(
    namespace: str,
    credential: DefaultAzureCredential,
    topic: str,
    subscription: str,
    max_message_count: int,
    max_wait_time: int,
    schema,
) -> list | str:
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

    print("Creating Servicebus client...")

    messages = []

    servicebus_client = ServiceBusClient(
        fully_qualified_namespace=namespace, credential=credential
    )

    print("Servicebus client created")
    print("Creating receiver object...")

    with servicebus_client:
        subscription_receiver = servicebus_client.get_subscription_receiver(
            topic_name=topic, subscription_name=subscription
        )

        print("Receiving messages...")

        with subscription_receiver:
            received_msgs = subscription_receiver.receive_messages(
                max_message_count, max_wait_time
            )
            for message in received_msgs:
                message_body = json.loads(str(message))
                messages.append(message_body)

            try:
                if messages:
                    print("Validating message data...")
                    validate_data(messages, schema)
                    for message in received_msgs:
                        subscription_receiver.complete_message(message)
                    print("Messages validated and completed")
                else:
                    print("No messages to validate")
                return messages
            except Exception as e:
                for message in received_msgs:
                    subscription_receiver.abandon_message(message)
                print("Error - abandoning messages - sending to dead letter queue")
                raise e


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
        print("No messages to process")

    return len(data)
