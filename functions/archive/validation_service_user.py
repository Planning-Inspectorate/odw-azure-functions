"""
Module containing a validate function to be called to run validation
of a list of sevricebus messages
"""

import model_service_user

from pydantic import BaseModel, ValidationError
from servicebus_funcs import get_messages
import var_funcs
from set_environment import current_config, config

_NAMESPACE = current_config["servicebus_namespace_odt"]
_SUBSCRIPTION = config["global"]["service-user-entity"]
_TOPIC = config["global"]["service-user-entity"]
_CREDENTIAL = var_funcs.CREDENTIAL
_MESSAGES = model_service_user.ServiceUser
_MAX_MESSAGE_COUNT = config["global"]["max_message_count"]
_MAX_WAIT_TIME = config["global"]["max_wait_time"]


def validate() -> list:
    """
    Function to validate a list of servicebus messages
    """

    _data = get_messages(
        _NAMESPACE,
        _CREDENTIAL,
        _TOPIC,
        _SUBSCRIPTION,
        _MAX_MESSAGE_COUNT,
        _MAX_WAIT_TIME,
    )

    class MessageInstances(BaseModel):

        """
        Represents a pydantic model for a list of ServiceUser instances.

        Args:
            messagedata (list[ServiceUser]): The list of ServiceUser instances.

        Attributes:
            messagedata (list[ServiceUser]): The list of ServiceUser instances.
        """

        messagedata: list[_MESSAGES]

    try:
        MessageInstances(messagedata=_data)
        print("VALIDATION SUCCEEDED!")
        print(f"{len(_data)} MESSAGES PROCESSED")
        return _data
    except ValidationError as e:
        print(e)
        raise e
