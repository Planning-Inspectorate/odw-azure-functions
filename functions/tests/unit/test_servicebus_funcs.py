import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import servicebus_funcs


class ServiceBusContext:
    def __init__(self, messages):
        self.messages = messages
        self.receiver = MagicMock()
        self.receiver.receive_messages.return_value = messages

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def get_subscription_receiver(self, **_):
        return self.receiver


class Message:
    def __init__(self, body, properties=None, message_id="id-1"):
        self.body = body
        self.application_properties = properties or {}
        self.message_id = message_id
        self.enqueued_time_utc = datetime(2026, 10, 27, tzinfo=timezone.utc)

    def __str__(self):
        return json.dumps(self.body)


def test_get_messages_and_validate_completes_valid_messages_and_adds_metadata():
    message = Message({"name": "Ada"}, {b"type": b"Create"})
    client = ServiceBusContext([message])
    with patch.object(servicebus_funcs, "ServiceBusClient", return_value=client):
        result = servicebus_funcs.get_messages_and_validate(
            "namespace", object(), "topic", "subscription", 10, 2, {"type": "object"}
        )

    assert result == [
        {
            "name": "Ada",
            "message_type": "Create",
            "message_enqueued_time_utc": "2026-10-27T00:00:00.000000+0000",
            "message_id": "id-1",
        }
    ]
    client.receiver.complete_message.assert_called_once_with(message)
    client.receiver.dead_letter_message.assert_not_called()


def test_get_messages_and_validate_dead_letters_invalid_messages():
    message = Message({"name": "Ada"})
    client = ServiceBusContext([message])
    schema = {"type": "object", "required": ["required"]}
    with patch.object(servicebus_funcs, "ServiceBusClient", return_value=client):
        result = servicebus_funcs.get_messages_and_validate(
            "namespace", object(), "topic", "subscription", 10, 2, schema
        )

    assert result == []
    client.receiver.complete_message.assert_not_called()
    client.receiver.dead_letter_message.assert_called_once()
    assert client.receiver.dead_letter_message.call_args.kwargs["reason"] == (
        "Failed validation against schema"
    )


def test_get_messages_and_validate_returns_empty_when_no_messages():
    client = ServiceBusContext([])
    with patch.object(servicebus_funcs, "ServiceBusClient", return_value=client):
        result = servicebus_funcs.get_messages_and_validate(
            "namespace", object(), "topic", "subscription", 10, 2, {}
        )

    assert result == []


def test_get_messages_and_validate_reraises_service_bus_errors():
    client = ServiceBusContext([])
    error = RuntimeError("service unavailable")
    client.receiver.receive_messages.side_effect = error
    with (
        patch.object(servicebus_funcs, "ServiceBusClient", return_value=client),
        pytest.raises(RuntimeError, match="service unavailable"),
    ):
        servicebus_funcs.get_messages_and_validate(
            "namespace", object(), "topic", "subscription", 10, 2, {}
        )


def test_send_to_storage_uploads_json_with_deterministic_blob_name():
    blob = MagicMock()
    service = MagicMock()
    service.get_blob_client.return_value = blob
    data = [{"id": 1}]
    with (
        patch("var_funcs.current_date", return_value="2026-10-27"),
        patch("var_funcs.current_time", return_value="2026-10-27T12:34:56.000000+0000"),
        patch.object(servicebus_funcs, "BlobServiceClient", return_value=service) as client,
    ):
        count = servicebus_funcs.send_to_storage(
            "https://storage", object(), "container", "entity", data
        )

    assert count == 1
    client.assert_called_once()
    service.get_blob_client.assert_called_once_with(
        "container", blob="entity/2026-10-27/entity_2026-10-27T12_34_56.000000+0000.json"
    )
    blob.upload_blob.assert_called_once_with(json.dumps(data), overwrite=True)


def test_send_to_storage_skips_client_creation_for_empty_data():
    with patch.object(servicebus_funcs, "BlobServiceClient") as client:
        assert servicebus_funcs.send_to_storage(
            "https://storage", object(), "container", "entity", []
        ) == 0

    client.assert_not_called()


def test_send_to_storage_reraises_upload_errors():
    service = MagicMock()
    service.get_blob_client.side_effect = RuntimeError("upload failed")
    with (
        patch("var_funcs.current_date", return_value="2026-10-27"),
        patch("var_funcs.current_time", return_value="12:34:56"),
        patch.object(servicebus_funcs, "BlobServiceClient", return_value=service),
        pytest.raises(RuntimeError, match="upload failed"),
    ):
        servicebus_funcs.send_to_storage(
            "https://storage", object(), "container", "entity", [{"id": 1}]
        )
