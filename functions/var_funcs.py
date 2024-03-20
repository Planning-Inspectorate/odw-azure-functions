"""
Config file to hold variables used by function apps and other functions
"""

import datetime
from azure.identity import DefaultAzureCredential


def current_time():
    return datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%f%z"
    )


def current_date():
    return datetime.datetime.now().strftime("%Y-%m-%d")


CREDENTIAL = DefaultAzureCredential()
