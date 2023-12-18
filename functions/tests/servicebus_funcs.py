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


def get_messages(
    namespace: str,
    credential: DefaultAzureCredential,
    topic: str,
    subscription: str,
    max_message_count: int,
    max_wait_time: int,
) -> list:
    """
    Retrieve messages from a Service Bus topic subscription.

    Args:
        namespace (str): The fully qualified namespace of the Service Bus.
        credential: The credential object for authentication.
        topic (str): The name of the topic.
        subscription (str): The name of the subscription.
        max_message_count (int): The maximum number of messages to retrieve.

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
                max_message_count,
                max_wait_time
            )
            for message in received_msgs:
                message_body = json.loads(str(message))
                messages.append(message_body)
                subscription_receiver.complete_message(message)

    print(f"{len(messages)} messages received from topic")

    return messages


def send_to_storage(
    account_url: str,
    credential: DefaultAzureCredential,
    container: str,
    filename: str,
    data: list[list | dict],
) -> None:
    """
    Upload data to Azure Blob Storage.

    Args:
        account_url (str): The URL of the Azure Blob Storage account.
        credential: The credential object for authentication.
        container (str): The name of the container in Azure Blob Storage.
        filename (str): The name of the file to upload.
        data: The data to be uploaded.

    Returns:
        None
    """

    print("Creating blob service client...")

    blob_service_client = BlobServiceClient(account_url, credential)

    print("Blob service client created")

    blob_client = blob_service_client.get_blob_client(container, blob=filename)

    print("Converting data to json format...")

    json_data = json.dumps(data)

    print("Data converted to json")

    print("Uploading file to storage...")

    blob_client.upload_blob(json_data, overwrite=True)

    print(f"JSON file '{filename}' uploaded to Azure Blob Storage.")
