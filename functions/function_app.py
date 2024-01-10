"""
Azure Function code to read messages from Azure Service Bus and send them to Azure Storage
"""

import azure.functions as func
from servicebus_funcs import get_messages_and_validate, send_to_storage
from set_environment import current_config, config
from var_funcs import CREDENTIAL

_STORAGE = current_config["storage_account"]
_CONTAINER = current_config["storage_container"]
_CREDENTIAL = CREDENTIAL
_NAMESPACE = current_config["servicebus_namespace_odt"]
_MAX_MESSAGE_COUNT = config["global"]["max_message_count"]
_MAX_WAIT_TIME = config["global"]["max_wait_time"]
_SUCCESS_RESPONSE = config["global"]["success_response"]

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

    from pins_data_model.models.model_service_user import ServiceUser

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
        return func.HttpResponse(f"{_SUCCESS_RESPONSE}", status_code=200)

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

    from pins_data_model.models.model_nsip_project import NsipProject

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
        return func.HttpResponse(f"{_SUCCESS_RESPONSE}", status_code=200)

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if "MessageInstances" in str(e)
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

    from pins_data_model.models.model_employee import Employee

    _ENTITY = config["global"]["employee-entity"]
    _MODEL = Employee

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
        return func.HttpResponse(f"{_SUCCESS_RESPONSE}", status_code=200)

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if "MessageInstances" in str(e)
            else func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
        )


@_app.function_name("interestedparty")
@_app.route(
    route="interestedparty", methods=["get"], auth_level=func.AuthLevel.FUNCTION
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

    from pins_data_model.models.model_nsip_document import NsipDocument

    _ENTITY = config["global"]["nsip-document-entity"]
    _MODEL = NsipDocument

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
        return func.HttpResponse(f"{_SUCCESS_RESPONSE}", status_code=200)

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if "MessageInstances" in str(e)
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

    from pins_data_model.models.model_nsip_exam_timetable import ExaminationTimetable

    _ENTITY = config["global"]["nsip-exam-timetable-entity"]
    _MODEL = ExaminationTimetable

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
        return func.HttpResponse(f"{_SUCCESS_RESPONSE}", status_code=200)

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if "MessageInstances" in str(e)
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

    from pins_data_model.models.model_nsip_project_update import NsipProjectUpdate

    _ENTITY = config["global"]["nsip-project-update-entity"]
    _MODEL = NsipProjectUpdate

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
        return func.HttpResponse(f"{_SUCCESS_RESPONSE}", status_code=200)

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if "MessageInstances" in str(e)
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

    from pins_data_model.models.model_nsip_representation import Representation

    _ENTITY = config["global"]["nsip-representation-entity"]
    _MODEL = Representation

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
        return func.HttpResponse(f"{_SUCCESS_RESPONSE}", status_code=200)

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if "MessageInstances" in str(e)
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

    from pins_data_model.models.model_nsip_subscription import NsipSubscription

    _ENTITY = config["global"]["nsip-subscription-entity"]
    _MODEL = NsipSubscription

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
        return func.HttpResponse(f"{_SUCCESS_RESPONSE}", status_code=200)

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if "MessageInstances" in str(e)
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

    from pins_data_model.models.model_s51_advice import S51Advice

    _ENTITY = config["global"]["nsip-s51-advice-entity"]
    _MODEL = S51Advice

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
        return func.HttpResponse(f"{_SUCCESS_RESPONSE}", status_code=200)

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if "MessageInstances" in str(e)
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

    from pins_data_model.models.model_case_schedule import CaseSchedule

    _ENTITY = config["global"]["case-schedule-entity"]
    _MODEL = CaseSchedule

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
        return func.HttpResponse(f"{_SUCCESS_RESPONSE}", status_code=200)

    except Exception as e:
        return (
            func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
            if "MessageInstances" in str(e)
            else func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
        )
