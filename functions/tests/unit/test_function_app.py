import importlib
import json
from unittest.mock import MagicMock, patch

import pytest

import function_app
import entity_registry
from entity_registry import EntitySpec, all_entities


def registered_handler(function_name):
    function = next(
        function
        for function in function_app._app.get_functions()
        if function.get_function_name() == function_name
    )
    return function.get_user_function()


@pytest.mark.parametrize("entity", all_entities(), ids=lambda entity: entity.key)
def test_http_pull_handler_reads_validates_and_stores_for_each_entity(
    entity, monkeypatch
):
    monkeypatch.setattr(function_app, "_SCHEMAS", {entity.schema_filename: {"type": "object"}})
    monkeypatch.setenv(entity.http_namespace_env_var, "namespace")
    request = MagicMock()
    with (
        patch.object(function_app, "get_messages_and_validate", return_value=[{"id": 1}]) as get_messages,
        patch.object(function_app, "send_to_storage", return_value=1) as send,
    ):
        response = function_app._make_http_pull_handler(entity)(request)

    assert response.status_code == 200
    assert json.loads(response.get_body()) == {
        "message": "Function executed successfully - 1 messages sent to storage",
        "count": 1,
    }
    get_messages.assert_called_once_with(
        namespace="namespace",
        credential=function_app._CREDENTIAL,
        topic=entity.topic,
        subscription=entity.subscription,
        max_message_count=function_app._MAX_MESSAGE_COUNT,
        max_wait_time=function_app._MAX_WAIT_TIME,
        schema={"type": "object"},
    )
    send.assert_called_once_with(
        account_url=function_app._STORAGE,
        credential=function_app._CREDENTIAL,
        container=function_app._CONTAINER,
        entity=entity.storage_entity,
        data=[{"id": 1}],
    )


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (RuntimeError("Failed validating payload"), "Validation error: Failed validating payload"),
        (RuntimeError("service unavailable"), "Unknown error: service unavailable"),
    ],
)
def test_http_pull_handler_returns_500_for_processing_errors(error, expected, monkeypatch):
    entity = all_entities()[0]
    monkeypatch.setattr(function_app, "_SCHEMAS", {entity.schema_filename: {}})
    with patch.object(function_app, "get_messages_and_validate", side_effect=error):
        response = function_app._make_http_pull_handler(entity)(MagicMock())

    assert response.status_code == 500
    assert response.get_body().decode() == expected


def test_http_pull_handler_uses_empty_namespace_when_environment_is_missing(monkeypatch):
    entity = all_entities()[0]
    monkeypatch.setattr(function_app, "_SCHEMAS", {entity.schema_filename: {}})
    monkeypatch.delenv(entity.http_namespace_env_var, raising=False)
    with (
        patch.object(function_app, "get_messages_and_validate", return_value=[]) as get_messages,
        patch.object(function_app, "send_to_storage", return_value=0),
    ):
        function_app._make_http_pull_handler(entity)(MagicMock())

    assert get_messages.call_args.kwargs["namespace"] == ""


def test_http_pull_handler_propagates_missing_schema_error(monkeypatch):
    entity = all_entities()[0]
    monkeypatch.setattr(function_app, "_SCHEMAS", {})

    with pytest.raises(KeyError, match=entity.schema_filename):
        function_app._make_http_pull_handler(entity)(MagicMock())


@pytest.mark.parametrize("entity", all_entities(), ids=lambda entity: entity.key)
def test_wake_drain_handler_passes_entity_configuration(entity, monkeypatch):
    monkeypatch.setenv(entity.http_namespace_env_var, "namespace")
    schema = {"type": "object"}
    monkeypatch.setattr(function_app, "_SCHEMAS", {entity.schema_filename: schema})
    message = MagicMock()
    with patch.object(function_app, "process_wake_and_drain") as process:
        function_app._make_wake_drain_trigger_handler(entity)(message)

    process.assert_called_once_with(
        wake_msg=message,
        entity=entity,
        schema=schema,
        storage_account_url=function_app._STORAGE,
        storage_container=function_app._CONTAINER,
        credential=function_app._CREDENTIAL,
        namespace="namespace",
        max_message_count=function_app._MAX_MESSAGE_COUNT,
        max_wait_time_seconds=function_app._MAX_WAIT_TIME,
    )


def test_wake_drain_handler_uses_empty_namespace_when_environment_is_missing(
    monkeypatch,
):
    entity = next(entity for entity in all_entities() if entity.wake_subscription)
    monkeypatch.delenv(entity.http_namespace_env_var, raising=False)
    monkeypatch.setattr(function_app, "_SCHEMAS", {entity.schema_filename: {}})
    with patch.object(function_app, "process_wake_and_drain") as process:
        function_app._make_wake_drain_trigger_handler(entity)(MagicMock())

    assert process.call_args.kwargs["namespace"] == ""


class SqlRow:
    def __init__(self, payload):
        self.payload = payload

    def to_json(self):
        return json.dumps(self.payload)


def test_gettimesheets_serializes_rows_as_json():
    response = registered_handler("gettimesheets")(
        MagicMock(), [SqlRow({"id": 1})]
    )

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert json.loads(response.get_body()) == [{"id": 1}]


def test_gettimesheets_returns_500_when_row_serialization_fails():
    row = MagicMock()
    row.to_json.side_effect = RuntimeError("bad row")

    response = registered_handler("gettimesheets")(MagicMock(), [row])

    assert response.status_code == 500
    assert response.get_body().decode() == "Unknown error: bad row"


def test_getdart_parses_json_values_and_preserves_plain_strings():
    row = SqlRow(
        {
            "object": "{\"key\": \"value\"}",
            "list": "[1, 2]",
            "text": "plain",
            "count": 3,
            "empty": None,
        }
    )

    response = registered_handler("getDaRT")(MagicMock(), [row])

    assert response.status_code == 200
    assert json.loads(response.get_body()) == [
        {
            "object": {"key": "value"},
            "list": [1, 2],
            "text": "plain",
            "count": 3,
            "empty": None,
        }
    ]


def test_getdart_returns_500_when_row_serialization_fails():
    row = MagicMock()
    row.to_json.side_effect = RuntimeError("bad row")

    response = registered_handler("getDaRT")(MagicMock(), [row])

    assert response.status_code == 500
    assert response.get_body().decode() == "Unknown error: bad row"


def test_test_function_serializes_rows_as_json():
    response = registered_handler("testFunction")(
        MagicMock(), [SqlRow({"id": 1})]
    )

    assert response.status_code == 200
    assert response.mimetype == "application/json"
    assert json.loads(response.get_body()) == [{"id": 1}]


def test_test_function_returns_500_when_row_serialization_fails():
    row = MagicMock()
    row.to_json.side_effect = RuntimeError("bad row")

    response = registered_handler("testFunction")(MagicMock(), [row])

    assert response.status_code == 500
    assert response.get_body().decode() == "Unknown error: bad row"


def test_function_app_registers_all_http_and_wake_drain_functions():
    registered_names = {
        function.get_function_name() for function in function_app._app.get_functions()
    }
    expected_names = {entity.route for entity in all_entities()}
    expected_names.update(
        f"{entity.route}_wake_drain"
        for entity in all_entities()
        if entity.wake_subscription
    )
    expected_names.update({"gettimesheets", "getDaRT", "testFunction"})

    assert registered_names == expected_names


def test_function_app_reads_storage_settings_when_present(monkeypatch):
    monkeypatch.setenv("MESSAGE_STORAGE_ACCOUNT", "https://storage.example")
    monkeypatch.setenv("MESSAGE_STORAGE_CONTAINER", "raw")

    reloaded = importlib.reload(function_app)

    assert reloaded._STORAGE == "https://storage.example"
    assert reloaded._CONTAINER == "raw"


def test_function_app_rejects_wake_drain_entity_without_wake_subscription(monkeypatch):
    entity = EntitySpec(
        key="invalid-wake-entity",
        topic="topic",
        subscription="subscription",
        schema_filename="schema.json",
        sb_connection="connection",
        http_namespace_env_var="namespace",
    )
    monkeypatch.setattr(entity_registry, "all_entities", lambda: [entity])
    monkeypatch.setattr(
        entity_registry, "_WAKE_SUBSCRIPTION_OVERRIDES", {entity.key: "wake"}
    )

    try:
        with pytest.raises(ValueError, match="no wake_subscription is configured"):
            importlib.reload(function_app)
    finally:
        monkeypatch.undo()
        importlib.reload(function_app)
