"""
Azure Function code to read messages from Azure Service Bus and send them to Azure Storage
"""

import azure.functions as func
import logging
from servicebus_funcs import get_messages_and_validate, send_to_storage
from set_environment import current_config, config
from var_funcs import CREDENTIAL

_STORAGE = current_config["storage_account"]
_CONTAINER = current_config["storage_container"]
_CREDENTIAL = CREDENTIAL
_NAMESPACE = current_config["servicebus_namespace_odt"]
_MAX_MESSAGE_COUNT = config["global"]["max_message_count"]
_MAX_WAIT_TIME = config["global"]["max_wait_time"]

_app = func.FunctionApp()


@_app.function_name("serviceuser")
@_app.route(route="serviceuser", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
def serviceuser(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint for handling HTTP requests.

    Args:
        req: An instance of `func.HttpRequest` representing the HTTP request.

    Returns:
        An instance of `func.HttpResponse` representing the HTTP response.
    """

    from model_service_user import ServiceUser

    logging.info("FUNCTION STARTED...")

    _ENTITY = config["global"]["service-user-entity"]
    _MODEL = ServiceUser

    try:
        _data = get_messages_and_validate(
            namespace=_NAMESPACE,
            credential=_CREDENTIAL,
            topic=_ENTITY,
            subscription=_ENTITY,
            max_message_count=_MAX_MESSAGE_COUNT,
            max_wait_time=_MAX_WAIT_TIME,
            model=_MODEL,
        )
        send_to_storage(
            account_url=_STORAGE,
            credential=_CREDENTIAL,
            container=_CONTAINER,
            entity=_ENTITY,
            data=_data,
        )
        return func.HttpResponse("Function executed successfully", status_code=200)

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if "MessageInstances" in str(e)
            else func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
        )

@_app.function_name("nsipproject")
@_app.route(route="nsipproject", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
def nsipproject(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint for handling HTTP requests.

    Args:
        req: An instance of `func.HttpRequest` representing the HTTP request.

    Returns:
        An instance of `func.HttpResponse` representing the HTTP response.
    """

    from model_nsip_project import NsipProject

    logging.info("FUNCTION STARTED...")

    _ENTITY = config["global"]["nsip-project-entity"]
    _MODEL = NsipProject

    try:
        _data = get_messages_and_validate(
            namespace=_NAMESPACE,
            credential=_CREDENTIAL,
            topic=_ENTITY,
            subscription=_ENTITY,
            max_message_count=_MAX_MESSAGE_COUNT,
            max_wait_time=_MAX_WAIT_TIME,
            model=_MODEL,
        )
        send_to_storage(
            account_url=_STORAGE,
            credential=_CREDENTIAL,
            container=_CONTAINER,
            entity=_ENTITY,
            data=_data,
        )
        return func.HttpResponse("Function executed successfully", status_code=200)

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if "MessageInstances" in str(e)
            else func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
        )
