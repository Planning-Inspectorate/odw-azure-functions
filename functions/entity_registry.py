"""
Entity registry for ODW Azure Functions

Goal:
- Keep entity definitions in one place
- Make adding a new entity a 10liner or less
- Support both:
  - HTTP pull (existing pattern for now before roll out)
  - Wake + Drain pattern (near real time without ServiceBusTrigger business logic to have our own DLQ handling)

Namespace / Connection rules:
- "appeal-*" entities use the Appeals namespace (connection prefix ServiceBusConnectionAppeals).
- Everything else uses the default ODW namespace (connection prefix ServiceBusConnection).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from set_environment import config


@dataclass(frozen=True)
class EntitySpec:
    """
    Defines everything needed to process an entity end to end
    """
    key: str  # e.g. "appeal-document"
    topic: str
    subscription: str  # the REAL subscription we drain (e.g. appeal-document-odw-sub)
    schema_filename: str  # e.g. "appeal-document.schema.json"

    # Service Bus connection prefix (identity based) for trigger binding
    sb_connection: str

    # HTTP pull uses explicit namespace env vars
    http_namespace_env_var: str

    # Storage folder name override (rare cases - I think like nsip-project)
    storage_entity_override: Optional[str] = None

    # HTTP route override (normally key without hyphens)
    http_route: Optional[str] = None

    # Optional wake subscription that triggers the function (wake only, we do NOT drain this)
    wake_subscription: Optional[str] = None

    @property
    def storage_entity(self) -> str:
        return self.storage_entity_override or self.topic

    @property
    def route(self) -> str:
        return self.http_route or self.key.replace("-", "")

    @property
    def trigger_subscription(self) -> str:
        return self.wake_subscription or self.subscription


def _is_appeals_entity(entity_key: str) -> bool:
    return entity_key.startswith("appeal-")


def build_entity_spec(entity_key: str) -> EntitySpec:
    """
    Build EntitySpec from config.yaml
    """
    entity_cfg = config["global"]["entities"][entity_key]
    topic = entity_cfg["topic"]
    subscription = entity_cfg["subscription"]

    schema_filename = f"{entity_key}.schema.json"

    # overriding schemas here for entity_key naming that don't match their schema
    SCHEMA_OVERRIDES = {
        "appeal-service-user": "service-user.schema.json",
        "nsip-s51-advice": "s51-advice.schema.json",
    }

    schema_filename = SCHEMA_OVERRIDES.get(entity_key, schema_filename)

    if _is_appeals_entity(entity_key):
        sb_connection = "ServiceBusConnectionAppeals"
        http_namespace_env_var = "SERVICEBUS_NAMESPACE_APPEALS"
    else:
        sb_connection = "ServiceBusConnection"
        http_namespace_env_var = "ServiceBusConnection__fullyQualifiedNamespace"

    http_route = entity_key.replace("-", "")
    if entity_key == "nsip-s51-advice":
        http_route = "s51advice"

    storage_override = None
    if entity_key == "nsip-s51-advice":
        storage_override = "s51-advice"
    if entity_key == "appeal-service-user":
        storage_override = "service-user"
    if entity_key == "appeal-s78":
        storage_override = "appeal-s78"
    if entity_key == "appeal-representation":
        storage_override = "appeal-representation"

    # Wake subscription (pilot only for now)
    wake_subscription = None
    if entity_key == "appeal-document":
        wake_subscription = "appeal-document-odw-wake-sub"

    return EntitySpec(
        key=entity_key,
        topic=topic,
        subscription=subscription,
        schema_filename=schema_filename,
        sb_connection=sb_connection,
        http_namespace_env_var=http_namespace_env_var,
        storage_entity_override=storage_override,
        http_route=http_route,
        wake_subscription=wake_subscription,
    )


def all_entities() -> list[EntitySpec]:
    """
    Create specs for all entities declared in config.yaml
    """
    return [build_entity_spec(k) for k in config["global"]["entities"].keys()]
