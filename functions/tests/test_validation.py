from set_environment import config
import pprint
from azure.servicebus import ServiceBusClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import json
from var_funcs import CREDENTIAL
from pins_data_model import load_schemas
from validate_messages import validate_data

_CREDENTIAL = CREDENTIAL
_NAMESPACE = config["preprod"]["servicebus_namespace_odt"]
_MAX_MESSAGE_COUNT = config["global"]["max_message_count"]
_SCHEMAS = load_schemas.load_all_schemas()["schemas"]
_ENTITY = "nsip-project"
_SCHEMA = _SCHEMAS["nsip-project.schema.json"]
_TOPIC = config["global"]["entities"][_ENTITY]["topic"]
_SUBSCRIPTION = config["global"]["entities"][_ENTITY]["subscription"]


def topics_dict():
    topic_config = {k: v for k, v in config["global"]["entities"].items()}
    return topic_config


def read_messages(
    namespace: str,
    credential: DefaultAzureCredential,
    topic: str,
    subscription: str,
    max_message_count: int,
) -> str | list:
    messages = []

    servicebus_client = ServiceBusClient(
        fully_qualified_namespace=namespace, credential=credential
    )

    with servicebus_client:
        subscription_receiver = servicebus_client.get_subscription_receiver(
            topic_name=topic, subscription_name=subscription, prefetch_count=5000
        )

        print(f"Reading messages from {topic}")

        with subscription_receiver:
            received_msgs = subscription_receiver.peek_messages(
                max_message_count,
            )

            sorted_messages = sorted(
                received_msgs, key=lambda x: x.enqueued_time_utc, reverse=True
            )
            if sorted_messages:
                latest_message = sorted_messages[0]
                message_id = latest_message.message_id
                enqueued_time = latest_message.enqueued_time_utc
                properties = latest_message.application_properties
                message_type = properties.get(b"type", None)
                if message_type is not None:
                    message_type = message_type.decode("utf-8")
                message_body = json.loads(str(latest_message))
                messages.append(message_body)
                return f"ID: {message_id} \n ENQUEUED TIME:{enqueued_time} \n TYPE: {message_type}"
                # return messages
            else:
                return []


# def test_read_messages():
#     result = read_messages(_NAMESPACE, _CREDENTIAL, _TOPIC, _SUBSCRIPTION, _MAX_MESSAGE_COUNT)
#     assert len(result) >= 1

# def main():
#     # pprint.pprint(_SCHEMA)
#     validate_data(data = read_messages(_NAMESPACE, _CREDENTIAL, _TOPIC, _SUBSCRIPTION, _MAX_MESSAGE_COUNT),
#                       schema = _SCHEMA)


def main():
    for k, v in topics_dict().items():
        topic = v["topic"]
        subscription = v["subscription"]
        message_data = read_messages(
            _NAMESPACE, _CREDENTIAL, topic, subscription, _MAX_MESSAGE_COUNT
        )
        print(f"{topic}: \n {message_data}")


if __name__ == "__main__":
    main()
