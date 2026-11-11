import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import function_app
import sb_wake_drain_processor
import servicebus_funcs
from entity_registry import build_entity_spec


class Message:
    application_properties = {b"type": b"Create"}
    enqueued_time_utc = datetime(2026, 10, 27, tzinfo=timezone.utc)
    message_id = "integration-message"

    def __str__(self):
        return json.dumps({"id": "record-1"})


class FakeReceiver:
    def __init__(self, messages):
        self.messages = messages
        self.completed = []
        self.dead_lettered = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def receive_messages(self, *_args, **_kwargs):
        messages, self.messages = self.messages, []
        return messages

    def complete_message(self, message):
        self.completed.append(message)

    def dead_letter_message(self, message, **kwargs):
        self.dead_lettered.append((message, kwargs))


class FakeServiceBusClient:
    def __init__(self, receiver):
        self.receiver = receiver

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def get_subscription_receiver(self, **kwargs):
        self.receiver_args = kwargs
        return self.receiver


def test_http_function_ingests_valid_message_and_uploads_json(monkeypatch):
    entity = build_entity_spec("service-user")
    message = Message()
    receiver = FakeReceiver([message])
    fake_blob = MagicMock()
    fake_blob_service = MagicMock()
    fake_blob_service.get_blob_client.return_value = fake_blob
    monkeypatch.setattr(
        function_app, "_SCHEMAS", {entity.schema_filename: {"type": "object", "required": ["id"]}}
    )
    monkeypatch.setenv(entity.http_namespace_env_var, "test-namespace")
    with (
        patch.object(
            servicebus_funcs,
            "ServiceBusClient",
            return_value=FakeServiceBusClient(receiver),
        ),
        patch.object(servicebus_funcs, "BlobServiceClient", return_value=fake_blob_service),
    ):
        response = function_app._make_http_pull_handler(entity)(MagicMock())

    assert response.status_code == 200
    assert json.loads(response.get_body())["count"] == 1
    assert receiver.completed == [message]
    assert receiver.dead_lettered == []
    uploaded_json = json.loads(fake_blob.upload_blob.call_args.args[0])
    assert uploaded_json[0]["id"] == "record-1"
    assert uploaded_json[0]["message_type"] == "Create"
    assert uploaded_json[0]["message_id"] == "integration-message"


def test_wake_drain_function_drains_valid_message_and_uploads_json(monkeypatch):
    entity = build_entity_spec("service-user")
    message = Message()
    message.body = [b'{"id":"record-2"}']
    message.message_id = "wake-drain-message"
    receiver = FakeReceiver([message])
    client = FakeServiceBusClient(receiver)
    fake_blob = MagicMock()
    fake_blob_service = MagicMock()
    fake_blob_service.get_blob_client.return_value = fake_blob
    monkeypatch.setattr(
        function_app,
        "_SCHEMAS",
        {entity.schema_filename: {"type": "object", "required": ["id"]}},
    )
    monkeypatch.setenv(entity.http_namespace_env_var, "test-namespace")
    with (
        patch.object(
            sb_wake_drain_processor, "ServiceBusClient", return_value=client
        ),
        patch.object(
            servicebus_funcs, "BlobServiceClient", return_value=fake_blob_service
        ),
    ):
        function_app._make_wake_drain_trigger_handler(entity)(MagicMock())

    assert client.receiver_args["topic_name"] == entity.topic
    assert client.receiver_args["subscription_name"] == entity.subscription
    assert receiver.completed == [message]
    assert receiver.dead_lettered == []
    uploaded_json = json.loads(fake_blob.upload_blob.call_args.args[0])
    assert uploaded_json[0]["id"] == "record-2"
    assert uploaded_json[0]["message_id"] == "wake-drain-message"
