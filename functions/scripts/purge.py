"""
Purge messages from a topic subscription in Azure Service Bus.

Supports:
  - Purging the Dead Letter Queue (DLQ)
  - Purging ACTIVE subscription messages
  - Optional limit for active purge (delete first N only)

Auth modes:
  1) SAS connection string (if SERVICE_BUS_CONNECTION_STR is set)
  2) Microsoft Entra ID / RBAC via DefaultAzureCredential (azure-identity)

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
  # SAS mode (preferred if available)
  # SERVICE_BUS_CONNECTION_STR=Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=...

  # Entra ID mode (no SAS)
  # SERVICE_BUS_NAMESPACE_FQDN=<namespace>.servicebus.windows.net

  # Optional defaults (can be overridden by CLI args):
  # SERVICE_BUS_TOPIC=<topic-name>
  # SERVICE_BUS_SUBSCRIPTION=<subscription-name>

  DLQ_BATCH=1000
  DLQ_WAIT=5

  --- How to run the script ---

  - To Purge all DLQ messages (default behaviour):
    python purge.py

  - To Purge all active subscription messages:
    python purge.py --active

  - To purge only a specific number of active subscription messages:
    python purge.py --active --limit [number]

  - Optional: override topic/subscription without editing .env
    python purge.py --topic nsip-project --subscription odw-nsip-project-sub --active --limit 7
"""

import os
import argparse
from azure.servicebus import ServiceBusClient, ServiceBusReceiveMode, ServiceBusSubQueue
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv

load_dotenv()

# Load config
SERVICE_BUS_CONNECTION_STR = os.getenv("SERVICE_BUS_CONNECTION_STR")
SERVICE_BUS_NAMESPACE_FQDN = os.getenv("SERVICE_BUS_NAMESPACE_FQDN") or os.getenv("SERVICE_BUS_NAMESPACE")
ENV_TOPIC = os.getenv("SERVICE_BUS_TOPIC")
ENV_SUBSCRIPTION = os.getenv("SERVICE_BUS_SUBSCRIPTION")
BATCH_SIZE = int(os.getenv("DLQ_BATCH", "1000"))
WAIT_SECONDS = int(os.getenv("DLQ_WAIT", "5"))


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
        raise SystemExit("ERROR: Set SERVICE_BUS_NAMESPACE_FQDN or SERVICE_BUS_NAMESPACE in your .env")
    return namespace if "." in namespace else f"{namespace}.servicebus.windows.net"


def get_client() -> ServiceBusClient:
    # Prefer SAS if provided (keeps backwards compatibility)
    if SERVICE_BUS_CONNECTION_STR:
        ns = extract_namespace_from_connstr(SERVICE_BUS_CONNECTION_STR)
        print(f"Auth: SAS | Namespace: {ns}")
        return ServiceBusClient.from_connection_string(SERVICE_BUS_CONNECTION_STR)

    # Otherwise, use AAD via DefaultAzureCredential (matches other scripts)
    fqdn = to_fqdn(SERVICE_BUS_NAMESPACE_FQDN or "")
    credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)
    print(f"Auth: AAD (DefaultAzureCredential) | Namespace: {fqdn}")
    return ServiceBusClient(fqdn, credential=credential)


def namespace_label() -> str:
    if SERVICE_BUS_CONNECTION_STR:
        return extract_namespace_from_connstr(SERVICE_BUS_CONNECTION_STR)
    return to_fqdn(SERVICE_BUS_NAMESPACE_FQDN or "").split(".")[0]


# DELETE FROM DLQ
def purge_dlq(topic: str, subscription: str) -> None:
    ns = namespace_label()
    print(f"Purging DLQ: {ns}/{topic}/{subscription}")

    client = get_client()
    total = 0

    with client:
        receiver = client.get_subscription_receiver(
            topic_name=topic,
            subscription_name=subscription,
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
                print(f"Deleted {total} DLQ messages...")

    print(f"DONE: Deleted {total} DLQ messages.")


# DELETE FROM ACTIVE SUBSCRIPTION
def purge_active(topic: str, subscription: str, limit: int | None = None) -> None:
    ns = namespace_label()
    print(f"Purging ACTIVE messages: {ns}/{topic}/{subscription}")
    if limit:
        print(f"Limit: Will delete at most {limit} messages.")

    client = get_client()
    total = 0

    with client:
        receiver = client.get_subscription_receiver(
            topic_name=topic,
            subscription_name=subscription,
            receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE,
        )
        with receiver:
            while True:
                if limit and total >= limit:
                    break

                batch = BATCH_SIZE if not limit else min(BATCH_SIZE, limit - total)
                if batch <= 0:
                    break

                msgs = receiver.receive_messages(
                    max_message_count=batch,
                    max_wait_time=WAIT_SECONDS,
                )
                if not msgs:
                    break

                total += len(msgs)
                print(f"Deleted {total} active messages...")

    print(f"DONE: Deleted {total} ACTIVE messages.")


# MAIN ENTRYPOINT
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Purge Azure Service Bus subscription messages")
    parser.add_argument(
        "--topic",
        help="Topic name (overrides SERVICE_BUS_TOPIC if set)",
    )
    parser.add_argument(
        "--subscription",
        help="Subscription name (overrides SERVICE_BUS_SUBSCRIPTION if set)",
    )
    parser.add_argument(
        "--active",
        action="store_true",
        help="Purge ACTIVE subscription messages (default is DLQ)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit active purge to N messages (only used with --active)",
    )

    args = parser.parse_args()

    topic = args.topic or ENV_TOPIC
    subscription = args.subscription or ENV_SUBSCRIPTION

    if not topic:
        raise SystemExit("ERROR: Provide --topic or set SERVICE_BUS_TOPIC in .env")
    if not subscription:
        raise SystemExit("ERROR: Provide --subscription or set SERVICE_BUS_SUBSCRIPTION in .env")

    if args.active:
        purge_active(topic, subscription, limit=args.limit)
    else:
        # Default behaviour: purge DLQ
        purge_dlq(topic, subscription)
