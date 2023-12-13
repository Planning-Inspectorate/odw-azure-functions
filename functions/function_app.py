"""
Azure Function code to read messages from Azure Service Bus and send them to Azure Storage
"""

import azure.functions as func
import logging
from servicebus_funcs import send_to_storage
import validation_nsip_project, validation_service_user
import config

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

    logging.info("FUNCTION STARTED...")

    _CURRENT_DATE = config.CURRENT_DATE
    _UTC_TIMESTAMP = config.UTC_TIMESTAMP
    _FILENAME = f"{config.SERVICE_USER_TOPIC}/{_CURRENT_DATE}/{config.SERVICE_USER_TOPIC}_{_UTC_TIMESTAMP}.json"

    try:
        send_to_storage(
            account_url=config.STORAGE_DEV,
            credential=config.CREDENTIAL,
            container=config.CONTAINER,
            filename=_FILENAME,
            data=validation_service_user.validate(),
        )

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        raise e

    return func.HttpResponse()


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

    logging.info("FUNCTION STARTED...")

    _CURRENT_DATE = config.CURRENT_DATE
    _UTC_TIMESTAMP = config.UTC_TIMESTAMP
    _FILENAME = f"{config.NSIP_PROJECT_TOPIC}/{_CURRENT_DATE}/{config.NSIP_PROJECT_TOPIC}_{_UTC_TIMESTAMP}.json"

    try:
        send_to_storage(
            account_url=config.STORAGE_DEV,
            credential=config.CREDENTIAL,
            container=config.CONTAINER,
            filename=_FILENAME,
            data=validation_nsip_project.validate(),
        )

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        raise e

    return func.HttpResponse()
