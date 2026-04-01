
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


# -----------------------------
# Centralised config & overrides
# -----------------------------

# Wake & Drain: entities that get a wake subscription
WAKE_DRAIN_ENABLED_ENTITY_KEYS = {
    "appeal-document",
    "appeal-has",
    "appeal-event",
    "appeal-event-estimate",
    "appeal-service-user",
    "appeal-s78",
    "appeal-representation",
    "folder",
    "service-user",
    "nsip-document",
    "nsip-exam-timetable",
    "nsip-project",
    "nsip-project-update",
    "nsip-representation",
    "nsip-s51-advice",
    "nsip-subscription",
}

# Some entities have schema filenames that don't match the entity key pattern
SCHEMA_OVERRIDES = {
    "appeal-service-user": "service-user.schema.json",
    "nsip-s51-advice": "s51-advice.schema.json",
}

# HTTP route overrides (defaults to key with hyphens removed)
HTTP_ROUTE_OVERRIDES = {
    "nsip-s51-advice": "s51advice",
}

# Storage folder overrides (defaults to topic)
STORAGE_ENTITY_OVERRIDES = {
    "nsip-s51-advice": "s51-advice",
    "appeal-service-user": "service-user",
    "appeal-s78": "appeal-s78",
    "appeal-representation": "appeal-representation",
}


@dataclass(frozen=True)
class EntitySpec:
    """
    Defines everything needed to process an entity end to end
    """
    key: str  # e.g. "appeal-document"
    topic: str
    # The REAL subscription we drain (e.g. appeal-document-odw-sub)
    subscription: str
    # e.g. "appeal-document.schema.json"
    schema_filename: str

    # Service Bus connection prefix (identity based) for trigger binding
    sb_connection: str

    # HTTP pull uses explicit namespace env vars
    http_namespace_env_var: str

    # Storage folder name override (rare cases - e.g. nsip-project)
    storage_entity_override: Optional[str] = None

    # HTTP route override (normally key without hyphens)
    http_route: Optional[str] = None

    # Optional wake subscription that triggers the function (wake only; we do NOT drain this)
    wake_subscription: Optional[str] = None

    @property
    def storage_entity(self) -> str:
        return self.storage_entity_override or self.topic

    @property
    def route(self) -> str:
        return self.http_route or self.key.replace("-", "")

    @property
    def trigger_subscription(self) -> Optional[str]:
        # IMPORTANT: Do NOT fall back to the real subscription here.
        # This field is specifically the WAKE (trigger) subscription or None.
        return self.wake_subscription


def _is_appeals_entity(entity_key: str) -> bool:
    return entity_key.startswith("appeal-")


def build_entity_spec(entity_key: str) -> EntitySpec:
    """
    Build EntitySpec from config.yaml
    """
    entity_cfg = config["global"]["entities"][entity_key]
    topic = entity_cfg["topic"]
    subscription = entity_cfg["subscription"]

    # schema (default follows the "<entity_key>.schema.json" convention)
    schema_filename = SCHEMA_OVERRIDES.get(
        entity_key, f"{entity_key}.schema.json"
    )

    # connection prefixes / http namespace env var
    if _is_appeals_entity(entity_key):
        sb_connection = "ServiceBusConnectionAppeals"
        http_namespace_env_var = "SERVICEBUS_NAMESPACE_APPEALS"
    else:
        sb_connection = "ServiceBusConnection"
        # this is what you already use elsewhere for non-appeals http namespace
        http_namespace_env_var = "ServiceBusConnection__fullyQualifiedNamespace"

    # http route (default: strip hyphens) with override
    http_route = HTTP_ROUTE_OVERRIDES.get(
        entity_key, entity_key.replace("-", "")
    )

    # storage entity override if any (default later: topic)
    storage_override = STORAGE_ENTITY_OVERRIDES.get(entity_key)

    # Wake subscription (centralised logic derived from the enabled set)
    wake_subscription = (
        f"{entity_key}-odw-wake-sub"
        if entity_key in WAKE_DRAIN_ENABLED_ENTITY_KEYS
        else None
    )

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
