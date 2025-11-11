"""
Azure Function code to read messages from Azure Service Bus and send them to Azure Storage
One function for each Service us topic
"""

import logging
import azure.functions as func
from servicebus_funcs import get_messages_and_validate, send_to_storage
from set_environment import current_config, config
from var_funcs import CREDENTIAL
from pins_data_model import load_schemas
from azure.functions.decorators.core import DataType
import json
import os

_STORAGE = ""
_CONTAINER = ""
_NAMESPACE = ""
_NAMESPACE_APPEALS = ""

try:
    _STORAGE = os.environ["MESSAGE_STORAGE_ACCOUNT"]
    _CONTAINER = os.environ["MESSAGE_STORAGE_CONTAINER"]
    _NAMESPACE = os.environ["ServiceBusConnection__fullyQualifiedNamespace"]
    _NAMESPACE_APPEALS = os.environ["SERVICEBUS_NAMESPACE_APPEALS"]
except:
    print("Warning: Missing Environment Variables")

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


@_app.function_name("s51advice")
@_app.route(route="s51advice", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
def s51advice(req: func.HttpRequest) -> func.HttpResponse:
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
            entity='s51-advice',
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

    # Log the schema being used
    logging.info(f"Using schema for appealhas: {_SCHEMA}")

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
        # Log the error and extract validation details if possible
        logging.error(f"Error processing appealhas: {str(e)}")
        if hasattr(e, 'errors'):
            logging.error(f"Validation details: {e.errors}")
            # Log the schema being used
            logging.error("Schema used: %s", _SCHEMA)
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

    
@_app.function_name("appeals78")
@_app.route(route="appeals78", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
def appeals78(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint for handling HTTP requests.

    Args:
        req: An instance of `func.HttpRequest` representing the HTTP request.

    Returns:
        An instance of `func.HttpResponse` representing the HTTP response.
    """

    _SCHEMA = _SCHEMAS["appeal-s78.schema.json"]
    _TOPIC = config["global"]["entities"]["appeal-s78"]["topic"]
    _SUBSCRIPTION = config["global"]["entities"]["appeal-s78"]["subscription"]

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
            entity="appeal-s78",
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
    

@_app.function_name("appealrepresentation")
@_app.route(route="appealrepresentation", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
def appealrepresentation(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint for handling HTTP requests.

    Args:
        req: An instance of `func.HttpRequest` representing the HTTP request.

    Returns:
        An instance of `func.HttpResponse` representing the HTTP response.
    """

    _SCHEMA = _SCHEMAS["appeal-representation.schema.json"]
    _TOPIC = config["global"]["entities"]["appeal-representation"]["topic"]
    _SUBSCRIPTION = config["global"]["entities"]["appeal-representation"]["subscription"]

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
            entity="appeal-representation",
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


@_app.function_name(name="hbtTestFunction")
@_app.route(route="testFunction", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
def hbt_test_function(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps(
            {
                "message": "Hello world"
            }
        ),
        status_code=200,
        mimetype="application/json"
    )


@_app.function_name("appealeventestimate")
@_app.route(route="appealeventestimate", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
def appealeventestimate(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function endpoint for handling appeal-event-estimate service bus messages.
    """
    _SCHEMA = _SCHEMAS["appeal-event-estimate.schema.json"]
    _TOPIC = config["global"]["entities"]["appeal-event-estimate"]["topic"]
    _SUBSCRIPTION = config["global"]["entities"]["appeal-event-estimate"]["subscription"]

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

        response = json.dumps(
            {"message": f"{_SUCCESS_RESPONSE} - {_message_count} messages sent to storage", "count": _message_count})

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