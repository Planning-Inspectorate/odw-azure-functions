"""
Azure Function code to read messages from Azure Service Bus and send them to Azure Storage
"""

import azure.functions as func
import logging
from servicebus_funcs import send_to_storage
import validation_nsip_project, validation_service_user
from set_environment import current_config, config
import var_funcs

_STORAGE = current_config['storage_account']
_CONTAINER = current_config['storage_container']
_CREDENTIAL = var_funcs.CREDENTIAL

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

    _ENTITY = config['global']['service-user-entity']
    _CURRENT_DATE = var_funcs.CURRENT_DATE
    _UTC_TIMESTAMP = var_funcs.UTC_TIMESTAMP
    _FILENAME = f"{_ENTITY}/{_CURRENT_DATE}/{_ENTITY}_{_UTC_TIMESTAMP}.json"

    try:
        send_to_storage(
            account_url=_STORAGE,
            credential=_CREDENTIAL,
            container=_CONTAINER,
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

    _ENTITY = config['global']['nsip-project-entity']
    _CURRENT_DATE = var_funcs.CURRENT_DATE
    _UTC_TIMESTAMP = var_funcs.UTC_TIMESTAMP
    _FILENAME = f"{_ENTITY}/{_CURRENT_DATE}/{_ENTITY}_{_UTC_TIMESTAMP}.json"

    try:
        send_to_storage(
            account_url=_STORAGE,
            credential=_CREDENTIAL,
            container=_CONTAINER,
            filename=_FILENAME,
            data=validation_nsip_project.validate(),
        )

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        raise e

    return func.HttpResponse()
