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
_WAKE_DRAIN_ENABLED_ENTITY_KEYS = {"appeal-document"}


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
