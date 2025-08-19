import os
from azure.servicebus import ServiceBusClient, ServiceBusReceiveMode, ServiceBusSubQueue
from dotenv import load_dotenv
# Before running the script run this in your terminal
# python3 -m venv .venv
# source .venv/bin/activate
# pip install azure-servicebus==7.13.0 python-dotenv==1.0.1
# Then you can run the script: python3 purge_dlq.py

load_dotenv()

# Config use .env
SERVICE_BUS_CONNECTION_STR = os.getenv("SERVICE_BUS_CONNECTION_STR")
SERVICE_BUS_TOPIC          = os.getenv("SERVICE_BUS_TOPIC")
SERVICE_BUS_SUBSCRIPTION   = os.getenv("SERVICE_BUS_SUBSCRIPTION")
BATCH_SIZE                 = int(os.getenv("DLQ_BATCH", "1000"))
WAIT_SECONDS               = int(os.getenv("DLQ_WAIT", "5"))

if not SERVICE_BUS_CONNECTION_STR:
    raise SystemExit("ERROR: Set SERVICE_BUS_CONNECTION_STR in your .env")

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
    return "<unknown>"

def purge_dlq():
    namespace = extract_namespace_from_connstr(SERVICE_BUS_CONNECTION_STR)

    print(f"Connecting to namespace: {namespace}")
    print(f"Purging DLQ for topic: {SERVICE_BUS_TOPIC}, subscription: {SERVICE_BUS_SUBSCRIPTION}")

    total = 0
    with ServiceBusClient.from_connection_string(SERVICE_BUS_CONNECTION_STR) as client:
        receiver = client.get_subscription_receiver(
            topic_name=SERVICE_BUS_TOPIC,
            subscription_name=SERVICE_BUS_SUBSCRIPTION,
            sub_queue=ServiceBusSubQueue.DEAD_LETTER,
            receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE
        )
        with receiver:
            while True:
                msgs = receiver.receive_messages(max_message_count=BATCH_SIZE, max_wait_time=WAIT_SECONDS)
                if not msgs:
                    break
                total += len(msgs)
                if total % 100 == 0:
                    print(f"Deleted {total} messages...")

    print(f"Finished. Deleted {total} DLQ messages from {namespace}/{SERVICE_BUS_TOPIC}/{SERVICE_BUS_SUBSCRIPTION}")

if __name__ == "__main__":
    purge_dlq()
