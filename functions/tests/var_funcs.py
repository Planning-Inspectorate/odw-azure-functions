"""
Config file to hold variables used by function apps and other functions
"""

import datetime
from azure.identity import DefaultAzureCredential
# from pydantic import ConfigDict

UTC_TIMESTAMP = (
    datetime.datetime.now(datetime.timezone.utc)
    .replace(tzinfo=datetime.timezone.utc)
    .isoformat()
)
CURRENT_DATE = datetime.datetime.now().strftime("%Y-%m-%d")
CREDENTIAL = DefaultAzureCredential()
# MODEL_CONFIG = ConfigDict(validate_assignment=True, extra="forbid")
