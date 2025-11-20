"""
Purge messages from a topic subscription in Azure Service Bus.

Supports:
  Purging the Dead Letter Queue (DLQ)
  Purging ACTIVE subscription messages
  Optional limit for active purge (delete first N only)

Auth modes:
  1) SAS connection string (if SERVICE_BUS_CONNECTION_STR is set)
  2) Microsoft Entra ID / RBAC (azure-identity)

Prereqs (SAS auth):
  python3 -m venv .venv
  source .venv/bin/activate
  pip install azure-servicebus==7.13.0 python-dotenv==1.0.1

Prereqs (AAD auth):
  python3 -m venv .venv
  source .venv/bin/activate
  pip install azure-servicebus==7.13.0 python-dotenv==1.0.1 azure-identity==1.17.1
  az login

Environment (.env):
  # SAS mode (preferred)
  # SERVICE_BUS_CONNECTION_STR=Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...

  # Entra ID mode
  # SERVICE_BUS_NAMESPACE_FQDN=<namespace>.servicebus.windows.net

  SERVICE_BUS_TOPIC=<topic-name>
  SERVICE_BUS_SUBSCRIPTION=<subscription-name>
  DLQ_BATCH=1000
  DLQ_WAIT=5

  --- How to run the script ---

  - To Purge all DLQ messages:
  python purge.py --dlq

  - To Purge all active subscription messages:
  python purge.py --active

  - To purge only a specific number of active subscription messages:
  python purge.py --active --limit [number]

  - If you run the script with the default command it will run only the DLQ
  python purge.py
"""

import os
import argparse
from azure.servicebus import ServiceBusClient, ServiceBusReceiveMode, ServiceBusSubQueue
from dotenv import load_dotenv

# Optional AAD auth
USE_AAD = True
try:
    from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential
except Exception:
    USE_AAD = False

load_dotenv()

# Load config
SERVICE_BUS_CONNECTION_STR = os.getenv("SERVICE_BUS_CONNECTION_STR")
SERVICE_BUS_NAMESPACE_FQDN = os.getenv("SERVICE_BUS_NAMESPACE_FQDN") or os.getenv("SERVICE_BUS_NAMESPACE")
SERVICE_BUS_TOPIC = os.getenv("SERVICE_BUS_TOPIC")
SERVICE_BUS_SUBSCRIPTION = os.getenv("SERVICE_BUS_SUBSCRIPTION")
BATCH_SIZE = int(os.getenv("DLQ_BATCH", "1000"))
WAIT_SECONDS = int(os.getenv("DLQ_WAIT", "5"))

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
    raise ValueError("Invalid connection string (Missing Endpoint)")


def to_fqdn(namespace: str) -> str:
    namespace = (namespace or "").strip()
    if not namespace:
        raise SystemExit("ERROR: Set SERVICE_BUS_NAMESPACE_FQDN in your .env for AAD auth")
    return namespace if "." in namespace else f"{namespace}.servicebus.windows.net"


def get_client() -> ServiceBusClient:
    if SERVICE_BUS_CONNECTION_STR:
        ns = extract_namespace_from_connstr(SERVICE_BUS_CONNECTION_STR)
        print(f"Auth: SAS | Namespace: {ns}")
        return ServiceBusClient.from_connection_string(SERVICE_BUS_CONNECTION_STR)

    if not USE_AAD:
        raise SystemExit("ERROR: No SAS connection string and azure-identity not available")

    fqdn = to_fqdn(SERVICE_BUS_NAMESPACE_FQDN or "")
    try:
        credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)
        print(f"Auth: AAD (DefaultAzureCredential) | Namespace: {fqdn}")
        return ServiceBusClient(fqdn, credential=credential)
    except Exception:
        credential = InteractiveBrowserCredential()
        print(f"Auth: AAD (InteractiveBrowserCredential) | Namespace: {fqdn}")
        return ServiceBusClient(fqdn, credential=credential)


def namespace_label() -> str:
    if SERVICE_BUS_CONNECTION_STR:
        return extract_namespace_from_connstr(SERVICE_BUS_CONNECTION_STR)
    return to_fqdn(SERVICE_BUS_NAMESPACE_FQDN or "").split(".")[0]


# DELETE FROM DLQ
def purge_dlq():
    ns = namespace_label()
    print(f"Purging DLQ: {ns}/{SERVICE_BUS_TOPIC}/{SERVICE_BUS_SUBSCRIPTION}")

    client = get_client()
    total = 0

    with client:
        receiver = client.get_subscription_receiver(
            topic_name=SERVICE_BUS_TOPIC,
            subscription_name=SERVICE_BUS_SUBSCRIPTION,
            sub_queue=ServiceBusSubQueue.DEAD_LETTER,
            receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE,
        )
        with receiver:
            while True:
                msgs = receiver.receive_messages(max_message_count=BATCH_SIZE, max_wait_time=WAIT_SECONDS)
                if not msgs:
                    break
                total += len(msgs)
                print(f"Deleted {total} DLQ messages...")

    print(f"DONE: Deleted {total} DLQ messages.")


# DELETE FROM ACTIVE SUBSCRIPTION
def purge_active(limit: int | None = None):
    ns = namespace_label()
    print(f"Purging ACTIVE messages: {ns}/{SERVICE_BUS_TOPIC}/{SERVICE_BUS_SUBSCRIPTION}")
    if limit:
        print(f"Limit: Will delete at most {limit} messages.")

    client = get_client()
    total = 0

    with client:
        receiver = client.get_subscription_receiver(
            topic_name=SERVICE_BUS_TOPIC,
            subscription_name=SERVICE_BUS_SUBSCRIPTION,
            receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE,
        )
        with receiver:
            while True:
                if limit and total >= limit:
                    break

                batch = BATCH_SIZE if not limit else min(BATCH_SIZE, limit - total)
                msgs = receiver.receive_messages(max_message_count=batch, max_wait_time=WAIT_SECONDS)
                if not msgs:
                    break

                total += len(msgs)
                print(f"Deleted {total} active messages...")

    print(f"DONE: Deleted {total} ACTIVE messages.")


# MAIN ENTRYPOINT
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Purge Azure Service Bus subscription messages")
    parser.add_argument("--dlq", action="store_true", help="Purge Dead Letter Queue messages")
    parser.add_argument("--active", action="store_true", help="Purge ACTIVE subscription messages")
    parser.add_argument("--limit", type=int, help="Limit active purge to N messages")

    args = parser.parse_args()

    if args.active:
        purge_active(limit=args.limit)
    else:
        purge_dlq()
