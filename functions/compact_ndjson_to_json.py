from datetime import datetime, timezone
import json
import logging

import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

from functions.archive.function_app_old import _CONTAINER  # kept for parity with your environment

# Assumes these are provided by your app's bootstrap/module:
#   config: Dict[str, Any]
#   _STORAGE: str  # account_url
#   _CREDENTIAL: DefaultAzureCredential
#   _CONTAINER: str
#   _app: azure.functions.FunctionApp


@_app.function_name(name="compact_appeal_document")
@_app.schedule(schedule="0 0 0,6,12,18 * * *", arg_name="timer", run_on_startup=False)
def compact_appeal_document(timer: func.TimerRequest) -> None:
    """
    Compacts the current hour's NDJSON into a single JSON array blob, then archives
    the original NDJSON and deletes the source.

    Schedule: at minute 0 of hours 0,6,12,18 UTC (cron: "0 0 0,6,12,18 * * *").
    """

    entity = config["global"]["entities"]["appeal-document"]["topic"]

    # Current UTC date/hour for partition path and source file
    now_utc = datetime.now(timezone.utc)
    today = now_utc.strftime("%Y-%m-%d")
    hour = now_utc.strftime("%H")

    ndjson_blob = f"{entity}/{today}/{entity}_{hour}.ndjson"

    # Output blob named with a timestamp for uniqueness
    ts = now_utc.strftime("%Y%m%dT%H%M%SZ")
    out_blob = f"{entity}/{today}/{entity}_{ts}.json"

    blob_service = BlobServiceClient(account_url=_STORAGE, credential=_CREDENTIAL)
    container_client = blob_service.get_container_client(_CONTAINER)

    try:
        in_blob_client = container_client.get_blob_client(ndjson_blob)
        ndjson_bytes = in_blob_client.download_blob().readall()
    except Exception:
        logging.info("[compact_appeal_document] No NDJSON blob found: %s", ndjson_blob)
        return

    lines = ndjson_bytes.decode("utf-8").splitlines()
    records = []
    for line in lines:
        if not line.strip():
            continue
        records.append(json.loads(line))

    if not records:
        logging.info("[compact_appeal_document] NDJSON empty: %s", ndjson_blob)
        return

    out_blob_client = container_client.get_blob_client(out_blob)
    out_blob_client.upload_blob(json.dumps(records, ensure_ascii=False), overwrite=True)

    logging.info(
        "[compact_appeal_document] Wrote %d records to %s",
        len(records),
        out_blob,
    )

    # Archive then delete the source NDJSON
    archive_blob = f"{entity}/{today}/archive/{entity}_{hour}_{ts}.ndjson"
    archive_client = container_client.get_blob_client(archive_blob)
    archive_client.upload_blob(ndjson_bytes, overwrite=True)

    in_blob_client.delete_blob()
