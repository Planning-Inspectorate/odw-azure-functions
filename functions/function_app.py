"""
ODW Azure Functions - Service Bus ingestion

This module is intentionally thin:
- Load env + config + schemas once
- Register all HTTP (pull) functions in a loop (as before)
- Register Wake & Drain trigger functions in a loop (kind of real time with proper DLQ reasons)
"""

from __future__ import annotations

import json
import os
from typing import Callable

import azure.functions as func

from pins_data_model import load_schemas
from var_funcs import CREDENTIAL
from set_environment import config
from servicebus_funcs import get_messages_and_validate, send_to_storage
from entity_registry import EntitySpec, all_entities
from sb_wake_drain_processor import process_wake_and_drain

# Environment
try:
    _STORAGE = os.environ["MESSAGE_STORAGE_ACCOUNT"]
    _CONTAINER = os.environ["MESSAGE_STORAGE_CONTAINER"]
except Exception:
    # Keep the same behaviour we had before
    print("Warning: Missing Environment Variables")
    _STORAGE = ""
    _CONTAINER = ""

_CREDENTIAL = CREDENTIAL
_MAX_MESSAGE_COUNT = config["global"]["max_message_count"]
_MAX_WAIT_TIME = config["global"]["max_wait_time"]
_SUCCESS_RESPONSE = config["global"]["success_response"]
_VALIDATION_ERROR = config["global"]["validation_error"]

_SCHEMAS = load_schemas.load_all_schemas()["schemas"]

_app = func.FunctionApp()

# Pilot toggle Wake & Drain
# Later: set to all entities OR via env var (which would be better tbh)
_WAKE_DRAIN_ENABLED_ENTITY_KEYS = {"appeal-document","appeal-has"}


def _make_http_pull_handler(entity: EntitySpec) -> Callable[[func.HttpRequest], func.HttpResponse]:
    """
    Builds an HTTP GET handler for an entity
    """
    def _handler(req: func.HttpRequest) -> func.HttpResponse:
        schema = _SCHEMAS[entity.schema_filename]
        topic = entity.topic
        subscription = entity.subscription

        namespace = os.environ.get(entity.http_namespace_env_var, "")

        try:
            data = get_messages_and_validate(
                namespace=namespace,
                credential=_CREDENTIAL,
                topic=topic,
                subscription=subscription,
                max_message_count=_MAX_MESSAGE_COUNT,
                max_wait_time=_MAX_WAIT_TIME,
                schema=schema,
            )

            message_count = send_to_storage(
                account_url=_STORAGE,
                credential=_CREDENTIAL,
                container=_CONTAINER,
                entity=entity.storage_entity,
                data=data,
            )

            response = json.dumps({
                "message": f"{_SUCCESS_RESPONSE} - {message_count} messages sent to storage",
                "count": message_count
            })

            return func.HttpResponse(response, status_code=200)

        except Exception as e:
            return (
                func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
                if f"{_VALIDATION_ERROR}" in str(e)
                else func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
            )

    return _handler


def _make_wake_drain_trigger_handler(entity: EntitySpec) -> Callable[[func.ServiceBusMessage], None]:
    """
    Builds a Service Bus trigger handler for an entity
    The trigger listens to entity.trigger_subscription (the wake subscription)
    The draining is performed against entity.subscription (the real subscription for e.g appeal-document-odw-sub)
    """
    def _handler(msg: func.ServiceBusMessage) -> None:
        schema = _SCHEMAS[entity.schema_filename]

        namespace = os.environ.get(entity.http_namespace_env_var, "")

        process_wake_and_drain(
            wake_msg=msg,
            entity=entity,
            schema=schema,
            storage_account_url=_STORAGE,
            storage_container=_CONTAINER,
            credential=_CREDENTIAL,
            namespace=namespace,
            max_message_count=_MAX_MESSAGE_COUNT,
            max_wait_time_seconds=_MAX_WAIT_TIME,
        )

    return _handler

for entity in all_entities():
    http_fn_name = entity.route
    http_route = entity.route

    _app.function_name(http_fn_name)(
        _app.route(route=http_route, methods=["get"], auth_level=func.AuthLevel.FUNCTION)(
            _make_http_pull_handler(entity)
        )
    )

    if entity.key in _WAKE_DRAIN_ENABLED_ENTITY_KEYS:
        if not entity.wake_subscription:
            raise ValueError(
                f"Wake&Drain enabled for {entity.key} but no wake_subscription is configured"
            )

        sb_fn_name = f"{entity.route}_wake_drain"

        _app.function_name(sb_fn_name)(
            _app.service_bus_topic_trigger(
                arg_name="msg",
                topic_name=entity.topic,
                subscription_name=entity.trigger_subscription,
                connection=entity.sb_connection,
            )(
                _make_wake_drain_trigger_handler(entity)
            )
        )

@_app.function_name(name="gettimesheets")
@_app.route(route="gettimesheets", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
@_app.sql_input(arg_name="timesheet",
                command_text="SELECT [caseReference], [applicationReference], [siteAddressLine1], [siteAddressLine2], [siteAddressTown], [siteAddressCounty], [siteAddressPostcode] FROM [odw_curated_db].[dbo].[appeal_has] WHERE UPPER([caseReference]) LIKE Concat(Char(37), UPPER(@searchCriteria), Char(37)) OR UPPER([applicationReference]) LIKE Concat(Char(37), UPPER(@searchCriteria), Char(37)) OR UPPER([siteAddressLine1]) LIKE Concat(Char(37), UPPER(@searchCriteria), Char(37)) OR UPPER([siteAddressLine2]) LIKE Concat(Char(37), UPPER(@searchCriteria), Char(37)) OR UPPER([siteAddressTown]) LIKE Concat(Char(37), UPPER(@searchCriteria), Char(37)) OR UPPER([siteAddressCounty]) LIKE Concat(Char(37), UPPER(@searchCriteria), Char(37)) OR UPPER([siteAddressPostcode]) LIKE Concat(Char(37), UPPER(@searchCriteria), Char(37))",
                command_type="Text",
                parameters="@searchCriteria={searchCriteria}",
                connection_string_setting="SqlConnectionString")
def gettimesheets(req: func.HttpRequest, timesheet: func.SqlRowList) -> func.HttpResponse:
    """
    We need to use Char(37) to escape the %
    https://stackoverflow.com/questions/71914897/how-do-i-use-sql-like-value-operator-with-azure-functions-sql-binding
    """
    try:
        rows = list(map(lambda r: json.loads(r.to_json()), timesheet))
        return func.HttpResponse(
            json.dumps(rows),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        return (
            func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
        )


@_app.function_name(name="getDaRT")
@_app.route(route="getDaRT", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
@_app.sql_input(arg_name="dart",
                command_text="""
                SELECT *
                FROM odw_curated_db.dbo.dart_api
                WHERE UPPER([applicationReference]) = UPPER(@applicationReference) 
                OR UPPER([caseReference]) = UPPER(@caseReference)
                """,
                command_type="Text",
                parameters="@caseReference={caseReference},@applicationReference={applicationReference}",
                connection_string_setting="SqlConnectionString"
                )
def getDaRT(req: func.HttpRequest, dart: func.SqlRowList) -> func.HttpResponse:
    try:
        rows = []
        for r in dart:
            row = json.loads(r.to_json())
            for key, value in row.items():
                if isinstance(value, str):
                    try:
                        parsed_value = json.loads(value)
                        row[key] = parsed_value
                    except json.JSONDecodeError as e:
                        row[key] = value
            rows.append(row)
        return func.HttpResponse(
            json.dumps(rows),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)


@_app.function_name(name="testFunction")
@_app.route(route="testFunction", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
@_app.sql_input(
    arg_name="logs",
    command_text="""
    SELECT TOP (1000) [file_ID],
                    [ingested_datetime],
                    [ingested_by_process_name],
                    [input_file],
                    [modified_datetime],
                    [modified_by_process_name],
                    [entity_name],
                    [rows_raw],
                    [rows_new]
    FROM logging.dbo.tables_logs
    """,
    command_type="Text",
    connection_string_setting="SqlConnectionString"
)
def test_function(req: func.HttpRequest, logs: func.SqlRowList) -> func.HttpResponse:
    try:
        rows = []
        for r in logs:
            rows.append(json.loads(r.to_json()))

        return func.HttpResponse(
            json.dumps(rows),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)