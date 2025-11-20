"""
Purge a topic subscription's Dead Letter Queue (DLQ) in Azure Service Bus.

Auth modes:
1) SAS connection string (if SERVICE_BUS_CONNECTION_STR is set)
2) Microsoft Entra ID (RBAC)

Prereqs:
  - For SAS : Create venv and install deps
  python3 -m venv .venv
  source .venv/bin/activate
  pip install azure-servicebus==7.13.0 python-dotenv==1.0.1

  - For Entra ID mode (when SAS is disabled)
  python3 -m venv .venv
  source .venv/bin/activate
  pip install azure-servicebus==7.13.0 python-dotenv==1.0.1 azure-identity==1.17.1
  az login  # Choose the correct subscription that contains the namespace

  If you need to change to another subscription you dont need to az login again you can:
  az account set --subscription "<subscription-name>"

  To check if you're in the right subscription:
  az account show --output table

  And to see all the subscriptions:
  az account list --output table

Environment (.env):
  # SAS mode (preferred if available)
  # SERVICE_BUS_CONNECTION_STR=Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...

  # Entra ID mode (no SAS)
  # SERVICE_BUS_NAMESPACE_FQDN=<namespace>.servicebus.windows.net

  SERVICE_BUS_TOPIC=<topic-name>
  SERVICE_BUS_SUBSCRIPTION=<subscription-name>
  DLQ_BATCH=1000
  DLQ_WAIT=5

IMPORTANT: Make sure you have reprocessed any DLQ messages you want to keep
before running this script. RECEIVE_AND_DELETE is irreversible.
"""

import os
from azure.servicebus import ServiceBusClient, ServiceBusReceiveMode, ServiceBusSubQueue
from dotenv import load_dotenv

# Try to enable Entra ID auth if set in .env
USE_AAD = True
try:
    from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential
except Exception:
    USE_AAD = False

load_dotenv()

# Config use .env
SERVICE_BUS_CONNECTION_STR = os.getenv("SERVICE_BUS_CONNECTION_STR")  # SAS path
SERVICE_BUS_NAMESPACE_FQDN = os.getenv("SERVICE_BUS_NAMESPACE_FQDN") or os.getenv("SERVICE_BUS_NAMESPACE")  # AAD path
SERVICE_BUS_TOPIC          = os.getenv("SERVICE_BUS_TOPIC")
SERVICE_BUS_SUBSCRIPTION   = os.getenv("SERVICE_BUS_SUBSCRIPTION")
BATCH_SIZE                 = int(os.getenv("DLQ_BATCH", "1000"))
WAIT_SECONDS               = int(os.getenv("DLQ_WAIT", "5"))

if not SERVICE_BUS_TOPIC:
    raise SystemExit("ERROR: Set SERVICE_BUS_TOPIC in your .env")
if not SERVICE_BUS_SUBSCRIPTION:
    raise SystemExit("ERROR: Set SERVICE_BUS_SUBSCRIPTION in your .env")


def extract_namespace_from_connstr(conn_str: str) -> str:
    for part in conn_str.split(";"):
        if part.strip().lower().startswith("endpoint="):
            endpoint = part.split("=", 1)[1].strip()
            endpoint = endpoint.replace("sb://", "").rstrip("/")
            return endpoint.split(".")[0]
    raise ValueError("Invalid Service Bus connection string Endpoint missing")


def to_fqdn(namespace: str) -> str:
    namespace = (namespace or "").strip()
    if not namespace:
        raise SystemExit(
            "ERROR: Using Entra ID auth Set SERVICE_BUS_NAMESPACE_FQDN or SERVICE_BUS_NAMESPACE in .env"
        )
    return namespace if "." in namespace else f"{namespace}.servicebus.windows.net"


def get_client() -> ServiceBusClient:
    if SERVICE_BUS_CONNECTION_STR:
        namespace = extract_namespace_from_connstr(SERVICE_BUS_CONNECTION_STR)
        print(f"Auth: SAS connection string | Namespace: {namespace}")
        return ServiceBusClient.from_connection_string(SERVICE_BUS_CONNECTION_STR)

    if not USE_AAD:
        raise SystemExit(
            "ERROR: No SERVICE_BUS_CONNECTION_STR and azure-identity is not installed"
            "Install azure-identity or provide a SAS connection string"
        )

    fqdn = to_fqdn(SERVICE_BUS_NAMESPACE_FQDN or "")
    try:
        credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)
        print(f"Auth: Entra ID (DefaultAzureCredential) | Namespace: {fqdn}")
        return ServiceBusClient(fqdn, credential=credential)
    except Exception:
        credential = InteractiveBrowserCredential()
        print(f"Auth: Entra ID (InteractiveBrowserCredential) | Namespace: {fqdn}")
        return ServiceBusClient(fqdn, credential=credential)


def namespace_label() -> str:
    if SERVICE_BUS_CONNECTION_STR:
        return extract_namespace_from_connstr(SERVICE_BUS_CONNECTION_STR)
    return to_fqdn(SERVICE_BUS_NAMESPACE_FQDN or "").split(".")[0]

def purge_dlq() -> None:
    namespace = namespace_label()
    print(f"Connecting to namespace: {namespace}")
    print(f"Purging DLQ for topic: {SERVICE_BUS_TOPIC}, subscription: {SERVICE_BUS_SUBSCRIPTION}")

    total = 0
    client = get_client()

    with client:
        receiver = client.get_subscription_receiver(
            topic_name=SERVICE_BUS_TOPIC,
            subscription_name=SERVICE_BUS_SUBSCRIPTION,
            sub_queue=ServiceBusSubQueue.DEAD_LETTER,
            receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE,
        )

        with receiver:
            while True:
                msgs = receiver.receive_messages(
                    max_message_count=BATCH_SIZE,
                    max_wait_time=WAIT_SECONDS,
                )
                if not msgs:
                    break

                total += len(msgs)
                if total % 100 == 0:
                    print(f"Deleted {total} messages...")

    print(f"Finished. Deleted {total} DLQ messages from {namespace}/{SERVICE_BUS_TOPIC}/{SERVICE_BUS_SUBSCRIPTION}")


if __name__ == "__main__":
    purge_dlq()