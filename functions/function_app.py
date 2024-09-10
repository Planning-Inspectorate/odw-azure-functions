"""
Azure Function code to read messages from Azure Service Bus and send them to Azure Storage
One function for each Service us topic
"""

import azure.functions as func
from servicebus_funcs import get_messages_and_validate, send_to_storage
from set_environment import current_config, config
from var_funcs import CREDENTIAL
from pins_data_model import load_schemas
from azure.functions.decorators.core import DataType
import json
import os


try:
    _STORAGE = os.environ["MESSAGE_STORAGE_ACCOUNT"]
    _CONTAINER = os.environ["MESSAGE_STORAGE_CONTAINER"]
    _NAMESPACE = os.environ["ServiceBusConnection__fullyQualifiedNamespace"]
    _NAMESPACE_APPEALS = os.environ["SERVICEBUS_NAMESPACE_APPEALS"]
except:
    print("Warning: Missing Environment Variables")
    _STORAGE = ""
    _CONTAINER = ""
    _NAMESPACE = ""
    _NAMESPACE_APPEALS = ""


_CREDENTIAL = CREDENTIAL
_MAX_MESSAGE_COUNT = config["global"]["max_message_count"]
_MAX_WAIT_TIME = config["global"]["max_wait_time"]
_SUCCESS_RESPONSE = config["global"]["success_response"]
_VALIDATION_ERROR = config["global"]["validation_error"]
_SCHEMAS = load_schemas.load_all_schemas()["schemas"]

_app = func.FunctionApp()


@_app.function_name("folder")
@_app.route(route="folder", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
def folder(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint for handling HTTP requests.

    Args:
        req: An instance of `func.HttpRequest` representing the HTTP request.

    Returns:
        An instance of `func.HttpResponse` representing the HTTP response.
    """

    _SCHEMA = _SCHEMAS["folder.schema.json"]
    _TOPIC = config["global"]["entities"]["folder"]["topic"]
    _SUBSCRIPTION = config["global"]["entities"]["folder"]["subscription"]

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

        response = json.dumps({"message" : f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", "count": _message_count})

        return func.HttpResponse(
            response,
            status_code=200
        )

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

        response = json.dumps({"message" : f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", "count": _message_count})

        return func.HttpResponse(
            response,
            status_code=200
        )

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

        response = json.dumps({"message" : f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", "count": _message_count})

        return func.HttpResponse(
            response,
            status_code=200
        )

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

        response = json.dumps({"message" : f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", "count": _message_count})

        return func.HttpResponse(
            response,
            status_code=200
        )

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

        response = json.dumps({"message" : f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", "count": _message_count})

        return func.HttpResponse(
            response,
            status_code=200
        )

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

        response = json.dumps({"message" : f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", "count": _message_count})

        return func.HttpResponse(
            response,
            status_code=200
        )

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

        response = json.dumps({"message" : f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", "count": _message_count})

        return func.HttpResponse(
            response,
            status_code=200
        )

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

        response = json.dumps({"message" : f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", "count": _message_count})

        return func.HttpResponse(
            response,
            status_code=200
        )

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if f"{_VALIDATION_ERROR}" in str(e)
            else func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
        )


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
        
        response = json.dumps({"message" : f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", "count": _message_count})

        return func.HttpResponse(
            response,
            status_code=200
        )

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if f"{_VALIDATION_ERROR}" in str(e)
            else func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
        )

@_app.function_name("appealdocument")
@_app.route(route="appealdocument", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
def appealdocument(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint for handling HTTP requests.

    Args:
        req: An instance of `func.HttpRequest` representing the HTTP request.

    Returns:
        An instance of `func.HttpResponse` representing the HTTP response.
    """

    _SCHEMA = _SCHEMAS["appeal-document.schema.json"]
    _TOPIC = config["global"]["entities"]["appeal-document"]["topic"]
    _SUBSCRIPTION = config["global"]["entities"]["appeal-document"]["subscription"]

    try:
        _data = get_messages_and_validate(
            namespace=_NAMESPACE_APPEALS,
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
        
        response = json.dumps({"message" : f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", "count": _message_count})

        return func.HttpResponse(
            response,
            status_code=200
        )

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if f"{_VALIDATION_ERROR}" in str(e)
            else func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
        )

@_app.function_name("appealhas")
@_app.route(route="appealhas", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
def appeal(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint for handling HTTP requests.

    Args:
        req: An instance of `func.HttpRequest` representing the HTTP request.

    Returns:
        An instance of `func.HttpResponse` representing the HTTP response.
    """

    _SCHEMA = _SCHEMAS["appeal-has.schema.json"]
    _TOPIC = config["global"]["entities"]["appeal-has"]["topic"]
    _SUBSCRIPTION = config["global"]["entities"]["appeal-has"]["subscription"]

    try:
        _data = get_messages_and_validate(
            namespace=_NAMESPACE_APPEALS,
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
        
        response = json.dumps({"message" : f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", "count": _message_count})

        return func.HttpResponse(
            response,
            status_code=200
        )

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if f"{_VALIDATION_ERROR}" in str(e)
            else func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
        )

@_app.function_name("appealevent")
@_app.route(route="appealevent", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
def appealevent(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint for handling HTTP requests.

    Args:
        req: An instance of `func.HttpRequest` representing the HTTP request.

    Returns:
        An instance of `func.HttpResponse` representing the HTTP response.
    """

    _SCHEMA = _SCHEMAS["appeal-event.schema.json"]
    _TOPIC = config["global"]["entities"]["appeal-event"]["topic"]
    _SUBSCRIPTION = config["global"]["entities"]["appeal-event"]["subscription"]

    try:
        _data = get_messages_and_validate(
            namespace=_NAMESPACE_APPEALS,
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
        
        response = json.dumps({"message" : f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", "count": _message_count})

        return func.HttpResponse(
            response,
            status_code=200
        )

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if f"{_VALIDATION_ERROR}" in str(e)
            else func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
        )
    
@_app.function_name("appealserviceuser")
@_app.route(route="appealserviceuser", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
def appealserviceuser(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint for handling HTTP requests.

    Args:
        req: An instance of `func.HttpRequest` representing the HTTP request.

    Returns:
        An instance of `func.HttpResponse` representing the HTTP response.
    """

    _SCHEMA = _SCHEMAS["service-user.schema.json"]
    _TOPIC = config["global"]["entities"]["appeal-service-user"]["topic"]
    _SUBSCRIPTION = config["global"]["entities"]["appeal-service-user"]["subscription"]

    try:
        _data = get_messages_and_validate(
            namespace=_NAMESPACE_APPEALS,
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
            entity="service-user",
            data=_data,
        )
        
        response = json.dumps({"message" : f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", "count": _message_count})

        return func.HttpResponse(
            response,
            status_code=200
        )

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if f"{_VALIDATION_ERROR}" in str(e)
            else func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
        )
    
@_app.function_name(name="get_timesheets")
@_app.route(route="timesheets/{caseType}/{searchCriteria}", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
@_app.sql_input(arg_name="timesheet",
                command_text="SELECT TOP (100) * FROM [odw_harmonised_db].[dbo].[s62a_view_cases_dim] WHERE [Name] LIKE '@searchCriteria'",
                command_type="Text",
                parameters="@caseType={caseType},@searchCriteria={searchCriteria}",
                connection_string_setting="SqlConnectionString")
def get_timesheets(req: func.HttpRequest, timesheet: func.SqlRowList) -> func.HttpResponse:
    try:
        with open("timesheets_sql.sql", 'r') as timesheets_query:
            timesheets_sql = timesheets_query.read()
            timesheet.set('CommandText', timesheets_sql)
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