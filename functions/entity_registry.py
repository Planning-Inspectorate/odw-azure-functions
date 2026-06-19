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

# overriding schemas here for entity_key naming that don't match their schema
_SCHEMA_OVERRIDES = {
    "appeal-service-user": "service-user.schema.json",
    "nsip-s51-advice": "s51-advice.schema.json",
}

_STORAGE_OVERRIDES = {
    "nsip-s51-advice": "s51-advice",
    "appeal-service-user": "service-user",
}

# Wake subscriptions used by SB trigger
# Entities not listed here remain HTTP pull only
_WAKE_SUBSCRIPTION_OVERRIDES = {
    "nsip-document": "odw-nsip-document-wake-sub",
    "nsip-exam-timetable": "odw-nsip-exam-timetable-wake-sub",
    "nsip-project": "odw-nsip-project-wake-sub",
    "nsip-project-update": "odw-nsip-project-update-wake-sub",
    "nsip-representation": "odw-nsip-representation-wake-sub",
    "nsip-s51-advice": "odw-nsip-s51-advice-wake-sub",
    "nsip-subscription": "odw-nsip-subscription-wake-sub",
    "service-user": "odw-service-user-wake-sub",
    "application-update": "application-update-odw-wake-sub",

    "appeal-document": "appeal-document-odw-wake-sub",
    "appeal-has": "appeal-has-odw-wake-sub",
    "appeal-event": "appeal-event-odw-wake-sub",
    "appeal-event-estimate": "appeal-event-estimate-odw-wake-sub",
    "appeal-service-user": "appeal-service-user-odw-wake-sub",
    "appeal-s78": "appeal-s78-odw-wake-sub",
    "appeal-representation": "appeal-representation-odw-wake-sub",
}


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

    # Storage folder name override for rare cases where raw folder differs from topic
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


def _uses_odw_service_bus_namespace(entity_key: str) -> bool:
    return entity_key == "application-update"


def build_entity_spec(entity_key: str) -> EntitySpec:
    """
    Build EntitySpec from config.yaml
    """
    entity_cfg = config["global"]["entities"][entity_key]
    topic = entity_cfg["topic"]
    subscription = entity_cfg["subscription"]

    schema_filename = f"{entity_key}.schema.json"

    schema_filename = _SCHEMA_OVERRIDES.get(entity_key, schema_filename)

    if _is_appeals_entity(entity_key):
        sb_connection = "ServiceBusConnectionAppeals"
        http_namespace_env_var = "SERVICEBUS_NAMESPACE_APPEALS"
    elif _uses_odw_service_bus_namespace(entity_key):
        sb_connection = "ServiceBusConnectionOdw"
        http_namespace_env_var = "ServiceBusConnectionOdw__fullyQualifiedNamespace"
    else:
        sb_connection = "ServiceBusConnection"
        http_namespace_env_var = "ServiceBusConnection__fullyQualifiedNamespace"

    http_route = entity_key.replace("-", "")
    if entity_key == "nsip-s51-advice":
        http_route = "s51advice"

    storage_override = _STORAGE_OVERRIDES.get(entity_key)

    wake_subscription = _WAKE_SUBSCRIPTION_OVERRIDES.get(entity_key)

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
