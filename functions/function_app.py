"""
ODW Azure Functions - Service Bus ingestion

This module is intentionally thin:
- Load env + config + schemas once
- Register all HTTP (pull) functions in a loop (Same as we had)
- Register Service Bus trigger functions in a loop (new pattern)
"""

from __future__ import annotations

import json
import logging
import os
from typing import Callable

import azure.functions as func

from pins_data_model import load_schemas
from var_funcs import CREDENTIAL
from set_environment import config
from servicebus_funcs import get_messages_and_validate, send_to_storage
from entity_registry import EntitySpec, all_entities
from sb_trigger_processor import process_servicebus_trigger_message

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

# Toggle: which entities are enabled as Service Bus triggers
# Later: set this to all entity keys OR control with an env var
_SB_TRIGGER_ENABLED_ENTITY_KEYS = {"appeal-document"}

# HTTP pull handler factory (keeps the approach that we had)

def _make_http_pull_handler(entity: EntitySpec) -> Callable[[func.HttpRequest], func.HttpResponse]:
    """
    Builds an HTTP GET handler for an entity.
    """
    def _handler(req: func.HttpRequest) -> func.HttpResponse:
        schema = _SCHEMAS[entity.schema_filename]
        topic = entity.topic
        subscription = entity.subscription

        # HTTP pull (the way it was)
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


# Service Bus trigger handler factory (new)

def _make_servicebus_trigger_handler(entity: EntitySpec) -> Callable[[func.ServiceBusMessage], None]:
    """
    Builds a Service Bus trigger handler for an entity.
    """
    def _handler(msg: func.ServiceBusMessage) -> None:
        schema = _SCHEMAS[entity.schema_filename]

        process_servicebus_trigger_message(
            msg=msg,
            entity=entity,
            schema=schema,
            storage_account_url=_STORAGE,
            storage_container=_CONTAINER,
            credential=_CREDENTIAL,
            validation_error_token=_VALIDATION_ERROR,
        )

    return _handler

# Register the functions here

for entity in all_entities():
    # Register HTTP pull endpoint
    http_fn_name = entity.route
    http_route = entity.route

    _app.function_name(http_fn_name)(
        _app.route(route=http_route, methods=["get"], auth_level=func.AuthLevel.FUNCTION)(
            _make_http_pull_handler(entity)
        )
    )

    # Register Service Bus trigger
    if entity.key in _SB_TRIGGER_ENABLED_ENTITY_KEYS:
        sb_fn_name = f"{entity.route}_sb"

        _app.function_name(sb_fn_name)(
            _app.service_bus_topic_trigger(
                arg_name="msg",
                topic_name=entity.topic,
                subscription_name=entity.subscription,
                connection=entity.sb_connection,
            )(
                _make_servicebus_trigger_handler(entity)
            )
        )
