"""
Drain messages by repeatedly calling our HTTP-triggered Azure Function,
and STOP only when the Service Bus subscription is actually drained
(active_message_count == 0 for N stable reads).

Auth for runtime-count checks (mirrors purge.py):
  1) SAS (SERVICE_BUS_CONNECTION_STR) if set
  2) AAD/RBAC via DefaultAzureCredential (az login)

Prereqs:
  python3 -m venv .venv
  source .venv/bin/activate
  pip install python-dotenv==1.0.1 requests==2.32.3 azure-servicebus==7.13.0 azure-identity==1.17.1
  az login
"""

from __future__ import annotations

import json
import os
import random
import sys
import time
from dataclasses import dataclass
from urllib.parse import urlencode, urljoin, urlparse, parse_qs, urlunparse

import requests
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    function_base_url: str
    function_route: str
    function_key: str
    sleep_seconds: float
    timeout_seconds: float
    max_iterations: int
    max_retries: int
    backoff_base: float
    backoff_max: float
    use_runtime_count: bool
    runtime_poll_seconds: float
    runtime_stable_reads: int
    sb_namespace_fqdn: str
    sb_topic: str
    sb_subscription: str


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or str(default)).strip().lower()
    return raw in ("1", "true", "yes", "y", "on")


def _required_env(name: str) -> str:
    val = (os.getenv(name) or "").strip()
    if not val:
        raise SystemExit(f"ERROR: Missing required env var: {name}")
    return val


def _safe_url_for_logs(url: str) -> str:
    u = urlparse(url)
    qs = parse_qs(u.query)
    if "code" in qs:
        qs["code"] = ["***"]
    new_query = urlencode(qs, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))


def load_config() -> Config:
    base_url = _required_env("FUNCTION_BASE_URL").rstrip("/") + "/"
    route = _required_env("FUNCTION_ROUTE").lstrip("/")
    key = (os.getenv("FUNCTION_KEY") or "").strip()

    return Config(
        function_base_url=base_url,
        function_route=route,
        function_key=key,
        sleep_seconds=float(os.getenv("DRAIN_SLEEP", "1.5")),
        timeout_seconds=float(os.getenv("DRAIN_TIMEOUT", "180")),
        max_iterations=int(os.getenv("DRAIN_MAX_ITERATIONS", "200000")),
        max_retries=int(os.getenv("DRAIN_MAX_RETRIES", "6")),
        backoff_base=float(os.getenv("DRAIN_BACKOFF_BASE_SECONDS", "1.0")),
        backoff_max=float(os.getenv("DRAIN_BACKOFF_MAX_SECONDS", "30")),
        use_runtime_count=_env_bool("DRAIN_USE_RUNTIME_COUNT", False),
        runtime_poll_seconds=float(os.getenv("DRAIN_RUNTIME_POLL_SECONDS", "10")),
        runtime_stable_reads=int(os.getenv("DRAIN_RUNTIME_STABLE_READS", "3")),
        sb_namespace_fqdn=(os.getenv("SERVICE_BUS_NAMESPACE_FQDN") or os.getenv("SERVICE_BUS_NAMESPACE") or "").strip(),
        sb_topic=(os.getenv("SERVICE_BUS_TOPIC") or "").strip(),
        sb_subscription=(os.getenv("SERVICE_BUS_SUBSCRIPTION") or "").strip(),
    )


def build_url(cfg: Config) -> str:
    endpoint = urljoin(cfg.function_base_url, cfg.function_route)
    if cfg.function_key:
        endpoint = endpoint + "?" + urlencode({"code": cfg.function_key})
    return endpoint


def parse_count(body: str) -> tuple[int, str]:
    payload = json.loads(body)
    count = int(payload.get("count", 0))
    msg = str(payload.get("message", "")).strip()
    return count, msg


def should_retry(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code <= 599


def backoff_sleep(attempt: int, base: float, cap: float) -> float:
    raw = min(cap, base * (2 ** (attempt - 1)))
    jitter = raw * random.uniform(-0.2, 0.2)
    return max(0.0, raw + jitter)


def get_with_retries(
    session: requests.Session,
    url: str,
    timeout: float,
    max_retries: int,
    backoff_base: float,
    backoff_max: float,
) -> requests.Response:
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp

            if should_retry(resp.status_code) and attempt < max_retries:
                sleep_s = backoff_sleep(attempt, backoff_base, backoff_max)
                print(f"HTTP {resp.status_code}; retrying in {sleep_s:.1f}s...", flush=True)
                time.sleep(sleep_s)
                continue

            return resp

        except requests.RequestException as e:
            if attempt >= max_retries:
                raise
            sleep_s = backoff_sleep(attempt, backoff_base, backoff_max)
            print(f"HTTP error ({e}); retrying in {sleep_s:.1f}s...", flush=True)
            time.sleep(sleep_s)

    raise RuntimeError("unexpected retry loop exit")

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


def get_admin_client(cfg: Config):
    from azure.identity import DefaultAzureCredential
    from azure.servicebus.management import ServiceBusAdministrationClient

    conn_str = (os.getenv("SERVICE_BUS_CONNECTION_STR") or "").strip()
    if conn_str:
        ns = extract_namespace_from_connstr(conn_str)
        print(f"Auth: SAS | Namespace: {ns}", flush=True)
        return ServiceBusAdministrationClient.from_connection_string(conn_str)

    fqdn = to_fqdn(cfg.sb_namespace_fqdn)
    credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)

    print(f"Auth: AAD (DefaultAzureCredential) | Namespace: {fqdn}", flush=True)
    return ServiceBusAdministrationClient(fqdn, credential=credential)


def get_runtime_counts(cfg: Config) -> tuple[int, int]:
    if not (cfg.sb_namespace_fqdn and cfg.sb_topic and cfg.sb_subscription):
        raise RuntimeError("Missing SERVICE_BUS_NAMESPACE_FQDN / SERVICE_BUS_TOPIC / SERVICE_BUS_SUBSCRIPTION in .env")

    admin = get_admin_client(cfg)
    props = admin.get_subscription_runtime_properties(cfg.sb_topic, cfg.sb_subscription)
    return int(props.active_message_count), int(props.dead_letter_message_count)


def wait_until_drained(cfg: Config) -> None:
    stable = 0
    while True:
        active, dlq = get_runtime_counts(cfg)
        print(f"Runtime counts: active={active} dlq={dlq}", flush=True)

        if active == 0:
            stable += 1
            if stable >= cfg.runtime_stable_reads:
                print(f"Drained confirmed: active==0 for {stable} consecutive checks.", flush=True)
                return
        else:
            stable = 0

        time.sleep(cfg.runtime_poll_seconds)


def main() -> int:
    cfg = load_config()
    url = build_url(cfg)

    print("Starting drain...", flush=True)
    print(f"Endpoint: {_safe_url_for_logs(url)}", flush=True)
    print(
        f"Config: sleep={cfg.sleep_seconds}s timeout={cfg.timeout_seconds}s "
        f"max_retries={cfg.max_retries} use_runtime_count={cfg.use_runtime_count}",
        flush=True,
    )

    if cfg.use_runtime_count:
        print(
            f"Runtime check: namespace={cfg.sb_namespace_fqdn} topic={cfg.sb_topic} sub={cfg.sb_subscription} "
            f"poll={cfg.runtime_poll_seconds}s stable_reads={cfg.runtime_stable_reads}",
            flush=True,
        )

    session = requests.Session()
    total = 0

    try:
        for i in range(1, cfg.max_iterations + 1):
            resp = get_with_retries(
                session=session,
                url=url,
                timeout=cfg.timeout_seconds,
                max_retries=cfg.max_retries,
                backoff_base=cfg.backoff_base,
                backoff_max=cfg.backoff_max,
            )

            body = (resp.text or "").strip()

            if resp.status_code != 200:
                print(f"[{i}] ERROR HTTP {resp.status_code}: {body[:800]}", flush=True)
                return 2

            try:
                count, msg = parse_count(body)
            except Exception as e:
                print(f"[{i}] ERROR parsing response JSON: {e}", flush=True)
                print(f"Body: {body[:800]}", flush=True)
                return 3

            total += count
            print(f"[{i}] processed={count} total={total} | {msg}", flush=True)

            if cfg.use_runtime_count:
                active, dlq = get_runtime_counts(cfg)
                print(f"Runtime counts after batch: active={active} dlq={dlq}", flush=True)

                if active == 0:
                    wait_until_drained(cfg)
                    print(f"DONE: drained confirmed. Total processed={total}", flush=True)
                    return 0

            time.sleep(cfg.sleep_seconds)

        print(f"Stopped: reached max_iterations={cfg.max_iterations}. Total processed={total}", flush=True)
        return 4

    except KeyboardInterrupt:
        print("\nInterrupted (Ctrl+C). Exiting cleanly.", flush=True)
        return 130
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
