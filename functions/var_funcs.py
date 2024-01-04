"""
Config file to hold variables used by function apps and other functions
"""

import datetime
from azure.identity import DefaultAzureCredential
from pydantic import ConfigDict


def current_time():
    return (
        datetime.datetime.now(datetime.timezone.utc)
        .replace(tzinfo=datetime.timezone.utc)
        .isoformat()
    )


def current_date():
    return datetime.datetime.now().strftime("%Y-%m-%d")


CREDENTIAL = DefaultAzureCredential()
MODEL_CONFIG = ConfigDict(validate_assignment=True, extra="forbid")
