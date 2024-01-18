"""
Azure Function code to read messages from Azure Service Bus and send them to Azure Storage
"""

import azure.functions as func
from servicebus_funcs import get_messages_and_validate, send_to_storage
from set_environment import current_config, config
from var_funcs import CREDENTIAL
from pins_data_model import load_schemas

_STORAGE = current_config["storage_account"]
_CONTAINER = current_config["storage_container"]
_CREDENTIAL = CREDENTIAL
_NAMESPACE = current_config["servicebus_namespace_odt"]
_MAX_MESSAGE_COUNT = config["global"]["max_message_count"]
_MAX_WAIT_TIME = config["global"]["max_wait_time"]
_SUCCESS_RESPONSE = config["global"]["success_response"]
_VALIDATION_ERROR = config["global"]["validation_error"]
_SCHEMAS = load_schemas.load_all_schemas()["schemas"]

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

    _SCHEMA = _SCHEMAS["service-user.schema.json"]
    _TOPIC = config["global"]["entities"]["service-user"]["topic"]
    _SUBSCRIPTION = config["global"]["entities"]["service-user"]["subscription"]

    try:
        _data = get_messages_and_validate(
            namespace=_NAMESPACE,
            credential=_CREDENTIAL,
            topic=_TOPIC,
            subscription=_SUBSCRIPTION,
            max_message_count=_MAX_MESSAGE_COUNT,
            max_wait_time=_MAX_WAIT_TIME,
            schema=_SCHEMA,
        )
        _message_count = send_to_storage(
            account_url=_STORAGE,
            credential=_CREDENTIAL,
            container=_CONTAINER,
            entity=_TOPIC,
            data=_data,
        )
        return func.HttpResponse(f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", status_code=200)

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if f"{_VALIDATION_ERROR}" in str(e)
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

    _SCHEMA = _SCHEMAS["nsip-project.schema.json"]
    _TOPIC = config["global"]["entities"]["nsip-project"]["topic"]
    _SUBSCRIPTION = config["global"]["entities"]["nsip-project"]["subscription"]

    try:
        _data = get_messages_and_validate(
            namespace=_NAMESPACE,
            credential=_CREDENTIAL,
            topic=_TOPIC,
            subscription=_SUBSCRIPTION,
            max_message_count=_MAX_MESSAGE_COUNT,
            max_wait_time=_MAX_WAIT_TIME,
            schema=_SCHEMA,
        )
        _message_count = send_to_storage(
            account_url=_STORAGE,
            credential=_CREDENTIAL,
            container=_CONTAINER,
            entity=_TOPIC,
            data=_data,
        )
        return func.HttpResponse(f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", status_code=200)

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if f"{_VALIDATION_ERROR}" in str(e)
            else func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
        )


@_app.function_name("employee")
@_app.route(route="employee", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
def employee(req: func.HttpRequest) -> func.HttpResponse:

    """
    Azure Function endpoint for handling HTTP requests.

    Args:
        req: An instance of `func.HttpRequest` representing the HTTP request.

    Returns:
        An instance of `func.HttpResponse` representing the HTTP response.
    """

    _SCHEMA = _SCHEMAS["employee.schema.json"]
    _TOPIC = config["global"]["entities"]["employee"]["topic"]
    _SUBSCRIPTION = config["global"]["entities"]["employee"]["subscription"]

    try:
        _data = get_messages_and_validate(
            namespace=_NAMESPACE,
            credential=_CREDENTIAL,
            topic=_TOPIC,
            subscription=_SUBSCRIPTION,
            max_message_count=_MAX_MESSAGE_COUNT,
            max_wait_time=_MAX_WAIT_TIME,
            schema=_SCHEMA,
        )
        _message_count = send_to_storage(
            account_url=_STORAGE,
            credential=_CREDENTIAL,
            container=_CONTAINER,
            entity=_TOPIC,
            data=_data,
        )
        return func.HttpResponse(f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", status_code=200)

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if f"{_VALIDATION_ERROR}" in str(e)
            else func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
        )

@_app.function_name("nsipdocument")
@_app.route(route="nsipdocument", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
def nsipdocument(req: func.HttpRequest) -> func.HttpResponse:

    """
    Azure Function endpoint for handling HTTP requests.

    Args:
        req: An instance of `func.HttpRequest` representing the HTTP request.

    Returns:
        An instance of `func.HttpResponse` representing the HTTP response.
    """

    _SCHEMA = _SCHEMAS["nsip-document.schema.json"]
    _TOPIC = config["global"]["entities"]["nsip-document"]["topic"]
    _SUBSCRIPTION = config["global"]["entities"]["nsip-document"]["subscription"]

    try:
        _data = get_messages_and_validate(
            namespace=_NAMESPACE,
            credential=_CREDENTIAL,
            topic=_TOPIC,
            subscription=_SUBSCRIPTION,
            max_message_count=_MAX_MESSAGE_COUNT,
            max_wait_time=_MAX_WAIT_TIME,
            schema=_SCHEMA,
        )
        _message_count = send_to_storage(
            account_url=_STORAGE,
            credential=_CREDENTIAL,
            container=_CONTAINER,
            entity=_TOPIC,
            data=_data,
        )
        return func.HttpResponse(f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", status_code=200)

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if f"{_VALIDATION_ERROR}" in str(e)
            else func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
        )

@_app.function_name("nsipexamtimetable")
@_app.route(
    route="nsipexamtimetable", methods=["get"], auth_level=func.AuthLevel.FUNCTION
)
def nsipexamtimetable(req: func.HttpRequest) -> func.HttpResponse:

    """
    Azure Function endpoint for handling HTTP requests.

    Args:
        req: An instance of `func.HttpRequest` representing the HTTP request.

    Returns:
        An instance of `func.HttpResponse` representing the HTTP response.
    """

    _SCHEMA = _SCHEMAS["nsip-exam-timetable.schema.json"]
    _TOPIC = config["global"]["entities"]["nsip-exam-timetable"]["topic"]
    _SUBSCRIPTION = config["global"]["entities"]["nsip-exam-timetable"]["subscription"]

    try:
        _data = get_messages_and_validate(
            namespace=_NAMESPACE,
            credential=_CREDENTIAL,
            topic=_TOPIC,
            subscription=_SUBSCRIPTION,
            max_message_count=_MAX_MESSAGE_COUNT,
            max_wait_time=_MAX_WAIT_TIME,
            schema=_SCHEMA,
        )
        _message_count = send_to_storage(
            account_url=_STORAGE,
            credential=_CREDENTIAL,
            container=_CONTAINER,
            entity=_TOPIC,
            data=_data,
        )
        return func.HttpResponse(f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", status_code=200)

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if f"{_VALIDATION_ERROR}" in str(e)
            else func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
        )


@_app.function_name("nsipprojectupdate")
@_app.route(
    route="nsipprojectupdate", methods=["get"], auth_level=func.AuthLevel.FUNCTION
)
def nsipprojectupdate(req: func.HttpRequest) -> func.HttpResponse:

    """
    Azure Function endpoint for handling HTTP requests.

    Args:
        req: An instance of `func.HttpRequest` representing the HTTP request.

    Returns:
        An instance of `func.HttpResponse` representing the HTTP response.
    """

    _SCHEMA = _SCHEMAS["nsip-project-update.schema.json"]
    _TOPIC = config["global"]["entities"]["nsip-project-update"]["topic"]
    _SUBSCRIPTION = config["global"]["entities"]["nsip-project-update"]["subscription"]

    try:
        _data = get_messages_and_validate(
            namespace=_NAMESPACE,
            credential=_CREDENTIAL,
            topic=_TOPIC,
            subscription=_SUBSCRIPTION,
            max_message_count=_MAX_MESSAGE_COUNT,
            max_wait_time=_MAX_WAIT_TIME,
            schema=_SCHEMA,
        )
        _message_count = send_to_storage(
            account_url=_STORAGE,
            credential=_CREDENTIAL,
            container=_CONTAINER,
            entity=_TOPIC,
            data=_data,
        )
        return func.HttpResponse(f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", status_code=200)

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if f"{_VALIDATION_ERROR}" in str(e)
            else func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
        )


@_app.function_name("nsiprepresentation")
@_app.route(
    route="nsiprepresentation", methods=["get"], auth_level=func.AuthLevel.FUNCTION
)
def nsiprepresentation(req: func.HttpRequest) -> func.HttpResponse:

    """
    Azure Function endpoint for handling HTTP requests.

    Args:
        req: An instance of `func.HttpRequest` representing the HTTP request.

    Returns:
        An instance of `func.HttpResponse` representing the HTTP response.
    """

    _SCHEMA = _SCHEMAS["nsip-representation.schema.json"]
    _TOPIC = config["global"]["entities"]["nsip-representation"]["topic"]
    _SUBSCRIPTION = config["global"]["entities"]["nsip-representation"]["subscription"]

    try:
        _data = get_messages_and_validate(
            namespace=_NAMESPACE,
            credential=_CREDENTIAL,
            topic=_TOPIC,
            subscription=_SUBSCRIPTION,
            max_message_count=_MAX_MESSAGE_COUNT,
            max_wait_time=_MAX_WAIT_TIME,
            schema=_SCHEMA,
        )
        _message_count = send_to_storage(
            account_url=_STORAGE,
            credential=_CREDENTIAL,
            container=_CONTAINER,
            entity=_TOPIC,
            data=_data,
        )
        return func.HttpResponse(f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", status_code=200)

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if f"{_VALIDATION_ERROR}" in str(e)
            else func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
        )


@_app.function_name("nsipsubscription")
@_app.route(
    route="nsipsubscription", methods=["get"], auth_level=func.AuthLevel.FUNCTION
)
def nsipsubscription(req: func.HttpRequest) -> func.HttpResponse:

    """
    Azure Function endpoint for handling HTTP requests.

    Args:
        req: An instance of `func.HttpRequest` representing the HTTP request.

    Returns:
        An instance of `func.HttpResponse` representing the HTTP response.
    """

    _SCHEMA = _SCHEMAS["nsip-subscription.schema.json"]
    _TOPIC = config["global"]["entities"]["nsip-subscription"]["topic"]
    _SUBSCRIPTION = config["global"]["entities"]["nsip-subscription"]["subscription"]

    try:
        _data = get_messages_and_validate(
            namespace=_NAMESPACE,
            credential=_CREDENTIAL,
            topic=_TOPIC,
            subscription=_SUBSCRIPTION,
            max_message_count=_MAX_MESSAGE_COUNT,
            max_wait_time=_MAX_WAIT_TIME,
            schema=_SCHEMA,
        )
        _message_count = send_to_storage(
            account_url=_STORAGE,
            credential=_CREDENTIAL,
            container=_CONTAINER,
            entity=_TOPIC,
            data=_data,
        )
        return func.HttpResponse(f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", status_code=200)

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if f"{_VALIDATION_ERROR}" in str(e)
            else func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
        )


@_app.function_name("nsips51advice")
@_app.route(route="nsips51advice", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
def nsips51advice(req: func.HttpRequest) -> func.HttpResponse:

    """
    Azure Function endpoint for handling HTTP requests.

    Args:
        req: An instance of `func.HttpRequest` representing the HTTP request.

    Returns:
        An instance of `func.HttpResponse` representing the HTTP response.
    """

    _SCHEMA = _SCHEMAS["s51-advice.schema.json"]
    _TOPIC = config["global"]["entities"]["nsip-s51-advice"]["topic"]
    _SUBSCRIPTION = config["global"]["entities"]["nsip-s51-advice"]["subscription"]

    try:
        _data = get_messages_and_validate(
            namespace=_NAMESPACE,
            credential=_CREDENTIAL,
            topic=_TOPIC,
            subscription=_SUBSCRIPTION,
            max_message_count=_MAX_MESSAGE_COUNT,
            max_wait_time=_MAX_WAIT_TIME,
            schema=_SCHEMA,
        )
        _message_count = send_to_storage(
            account_url=_STORAGE,
            credential=_CREDENTIAL,
            container=_CONTAINER,
            entity=_TOPIC,
            data=_data,
        )
        return func.HttpResponse(f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", status_code=200)

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if f"{_VALIDATION_ERROR}" in str(e)
            else func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
        )


@_app.function_name("caseschedule")
@_app.route(route="caseschedule", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
def caseschedule(req: func.HttpRequest) -> func.HttpResponse:

    """
    Azure Function endpoint for handling HTTP requests.

    Args:
        req: An instance of `func.HttpRequest` representing the HTTP request.

    Returns:
        An instance of `func.HttpResponse` representing the HTTP response.
    """

    _SCHEMA = _SCHEMAS["case-schedule.schema.json"]
    _TOPIC = config["global"]["entities"]["case-schedule"]["topic"]
    _SUBSCRIPTION = config["global"]["entities"]["case-schedule"]["subscription"]

    try:
        _data = get_messages_and_validate(
            namespace=_NAMESPACE,
            credential=_CREDENTIAL,
            topic=_TOPIC,
            subscription=_SUBSCRIPTION,
            max_message_count=_MAX_MESSAGE_COUNT,
            max_wait_time=_MAX_WAIT_TIME,
            schema=_SCHEMA,
        )
        _message_count = send_to_storage(
            account_url=_STORAGE,
            credential=_CREDENTIAL,
            container=_CONTAINER,
            entity=_TOPIC,
            data=_data,
        )
        return func.HttpResponse(f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", status_code=200)

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if f"{_VALIDATION_ERROR}" in str(e)
            else func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
        )
