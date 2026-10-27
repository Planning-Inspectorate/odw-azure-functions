import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest
from azure.servicebus.exceptions import ServiceBusError

import sb_wake_drain_processor as processor
from entity_registry import EntitySpec


ENTITY = EntitySpec(
    key="test-entity",
    topic="test-topic",
    subscription="real-subscription",
    schema_filename="test.schema.json",
    sb_connection="connection",
    http_namespace_env_var="NAMESPACE",
    storage_entity_override="storage-entity",
    wake_subscription="wake-subscription",
)
SCHEMA = {"type": "object", "required": ["name"]}


class Message:
    def __init__(self, payload, properties=None, message_id="message-id"):
        self.body = [json.dumps(payload).encode()]
        self.application_properties = properties or {}
        self.message_id = message_id
        self.delivery_count = 1
        self.enqueued_time_utc = datetime(2026, 10, 27, tzinfo=timezone.utc)


class InvalidJsonMessage(Message):
    def __init__(self):
        super().__init__({})
        self.body = [b"not json"]

    def __str__(self):
        return "not json"


class BadBodyMessage:
    message_id = "bad-body"
    delivery_count = 1

    @property
    def body(self):
        raise RuntimeError("unreadable body")


class EmptyFormattedDatetime(datetime):
    def strftime(self, _format):
        return ""


class ServiceBusClientContext:
    def __init__(self, receiver):
        self.receiver = receiver

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def get_subscription_receiver(self, **kwargs):
        self.receiver_args = kwargs
        return self.receiver


def test_truncate_handles_none_short_and_long_values():
    assert processor._truncate(None, 5) == ""
    assert processor._truncate("short", 5) == "short"
    assert processor._truncate("long-value", 7) == "long..."


def test_utc_now_iso_includes_utc_offset():
    assert processor._utc_now_iso().endswith("+0000")


@pytest.mark.parametrize(
    ("properties", "expected"),
    [
        ({"type": "Create"}, "Create"),
        ({b"type": b"Update"}, "Update"),
        ({"Type": "Delete"}, "Delete"),
        ({b"Type": b"Publish"}, "Publish"),
        ({b"type": b"\xff"}, None),
        ({}, None),
        (None, None),
    ],
)
def test_extract_message_type(properties, expected):
    message = MagicMock()
    message.application_properties = properties

    assert processor._extract_message_type(message) == expected


def test_process_one_valid_message_writes_metadata_then_completes():
    receiver = MagicMock()
    message = Message({"name": "Ada"}, {b"type": b"Create"})
    with patch.object(processor, "send_to_storage") as send:
        result = processor._process_one_message(
            receiver=receiver,
            msg=message,
            entity=ENTITY,
            schema=SCHEMA,
            storage_account_url="https://storage",
            storage_container="container",
            credential=object(),
            operation_id="operation",
        )

    assert result is True
    payload = send.call_args.kwargs["data"][0]
    assert payload["name"] == "Ada"
    assert payload["message_id"] == "message-id"
    assert payload["message_type"] == "Create"
    assert payload["message_enqueued_time_utc"] == "2026-10-27T00:00:00.000000+0000"
    assert send.call_args.kwargs["entity"] == "storage-entity"
    receiver.complete_message.assert_called_once_with(message)
    receiver.dead_letter_message.assert_not_called()


@pytest.mark.parametrize(
    ("message", "reason"),
    [
        (BadBodyMessage(), "BodyReadFailed"),
        (InvalidJsonMessage(), "InvalidJson"),
        (Message({}), "SchemaValidationFailed"),
    ],
)
def test_process_one_invalid_message_dead_letters(message, reason):
    receiver = MagicMock()
    with patch.object(processor, "send_to_storage") as send:
        result = processor._process_one_message(
            receiver=receiver,
            msg=message,
            entity=ENTITY,
            schema=SCHEMA,
            storage_account_url="https://storage",
            storage_container="container",
            credential=object(),
            operation_id="operation",
        )

    assert result is False
    assert receiver.dead_letter_message.call_args.kwargs["reason"] == reason
    receiver.complete_message.assert_not_called()
    send.assert_not_called()


def test_process_one_uses_current_time_when_message_timestamp_is_missing():
    receiver = MagicMock()
    message = Message({"name": "Ada"})
    message.enqueued_time_utc = None
    with (
        patch.object(processor, "_utc_now_iso", return_value="now") as utc_now,
        patch.object(processor, "send_to_storage") as send,
    ):
        processor._process_one_message(
            receiver=receiver,
            msg=message,
            entity=ENTITY,
            schema=SCHEMA,
            storage_account_url="https://storage",
            storage_container="container",
            credential=object(),
            operation_id="operation",
        )

    utc_now.assert_called_once()
    assert send.call_args.kwargs["data"][0]["message_enqueued_time_utc"] == "now"


def test_process_one_falls_back_to_isoformat_when_strftime_returns_empty():
    receiver = MagicMock()
    message = Message({"name": "Ada"})
    message.enqueued_time_utc = EmptyFormattedDatetime(2026, 10, 27, tzinfo=timezone.utc)
    with patch.object(processor, "send_to_storage") as send:
        processor._process_one_message(
            receiver=receiver,
            msg=message,
            entity=ENTITY,
            schema=SCHEMA,
            storage_account_url="https://storage",
            storage_container="container",
            credential=object(),
            operation_id="operation",
        )

    assert send.call_args.kwargs["data"][0]["message_enqueued_time_utc"] == (
        "2026-10-27T00:00:00+00:00"
    )

def test_process_one_does_not_complete_or_dead_letter_when_storage_fails():
    receiver = MagicMock()
    message = Message({"name": "Ada"})
    with (
        patch.object(processor, "send_to_storage", side_effect=RuntimeError("storage down")),
        pytest.raises(RuntimeError, match="storage down"),
    ):
        processor._process_one_message(
            receiver=receiver,
            msg=message,
            entity=ENTITY,
            schema=SCHEMA,
            storage_account_url="https://storage",
            storage_container="container",
            credential=object(),
            operation_id="operation",
        )

    receiver.complete_message.assert_not_called()
    receiver.dead_letter_message.assert_not_called()


def test_dead_letter_truncates_reason_and_description_to_service_bus_limits():
    receiver = MagicMock()
    message = Message({})
    processor._dead_letter(
        receiver, message, reason="r" * 200, description="d" * 5000
    )

    kwargs = receiver.dead_letter_message.call_args.kwargs
    assert len(kwargs["reason"]) == 128
    assert kwargs["reason"].endswith("...")
    assert len(kwargs["error_description"]) == 4096
    assert kwargs["error_description"].endswith("...")


def test_process_wake_and_drain_requires_a_namespace():
    with pytest.raises(ValueError, match="Missing Service Bus namespace"):
        processor.process_wake_and_drain(
            wake_msg=MagicMock(),
            entity=ENTITY,
            schema=SCHEMA,
            storage_account_url="https://storage",
            storage_container="container",
            credential=object(),
            namespace="",
            max_message_count=10,
            max_wait_time_seconds=1,
        )


def test_process_wake_and_drain_receives_from_real_subscription_and_stops_when_empty():
    receiver = MagicMock()
    receiver.receive_messages.side_effect = [[Message({"name": "Ada"})], []]
    client = ServiceBusClientContext(receiver)
    with (
        patch.object(processor, "ServiceBusClient", return_value=client) as client_factory,
        patch.object(processor, "_process_one_message", return_value=True) as process_one,
    ):
        processor.process_wake_and_drain(
            wake_msg=MagicMock(),
            entity=ENTITY,
            schema=SCHEMA,
            storage_account_url="https://storage",
            storage_container="container",
            credential="credential",
            namespace="namespace",
            max_message_count=100,
            max_wait_time_seconds=2,
        )

    client_factory.assert_called_once_with(
        fully_qualified_namespace="namespace", credential="credential"
    )
    assert client.receiver_args == {
        "topic_name": "test-topic",
        "subscription_name": "real-subscription",
        "prefetch_count": 100,
    }
    assert receiver.receive_messages.call_args_list == [
        call(max_message_count=50, max_wait_time=2),
        call(max_message_count=50, max_wait_time=2),
    ]
    process_one.assert_called_once()


def test_process_wake_and_drain_caps_last_batch_at_remaining_message_limit():
    receiver = MagicMock()
    receiver.receive_messages.side_effect = [[MagicMock(), MagicMock()], []]
    client = ServiceBusClientContext(receiver)
    with (
        patch.object(processor, "ServiceBusClient", return_value=client),
        patch.object(processor, "_process_one_message", return_value=True),
    ):
        processor.process_wake_and_drain(
            wake_msg=MagicMock(),
            entity=ENTITY,
            schema=SCHEMA,
            storage_account_url="https://storage",
            storage_container="container",
            credential="credential",
            namespace="namespace",
            max_message_count=2,
            max_wait_time_seconds=2,
        )

    receiver.receive_messages.assert_called_once_with(
        max_message_count=2, max_wait_time=2
    )


@pytest.mark.parametrize("error", [RuntimeError("failure"), ServiceBusError("failure")])
def test_process_wake_and_drain_reraises_service_bus_and_unexpected_errors(error):
    class FailedClient:
        def __enter__(self):
            raise error

        def __exit__(self, *_):
            return False

    with (
        patch.object(processor, "ServiceBusClient", return_value=FailedClient()),
        pytest.raises(type(error), match="failure"),
    ):
        processor.process_wake_and_drain(
            wake_msg=MagicMock(),
            entity=ENTITY,
            schema=SCHEMA,
            storage_account_url="https://storage",
            storage_container="container",
            credential="credential",
            namespace="namespace",
            max_message_count=10,
            max_wait_time_seconds=1,
        )
