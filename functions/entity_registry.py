"""
Entity registry for ODW Azure Functions.

Goal:
- Keep entity definitions in one place.
- Make adding a new entity a ~10-liner (or less).
- Support both:
  - HTTP pull (existing pattern)
  - Service Bus trigger (new real time drain)

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
    Defines everything needed to process an entity end-to-end.
    """
    key: str  # e.g. "appeal-document"
    topic: str
    subscription: str
    schema_filename: str  # e.g. "appeal-document.schema.json"

    # Service Bus trigger binding connection prefix (identity-based)
    sb_connection: str  # e.g. "ServiceBusConnection" or "ServiceBusConnectionAppeals"

    # HTTP pull uses explicit namespace env vars; we keep the existing pattern:
    http_namespace_env_var: str

    # Storage folder name override (rare cases like s51-advice/service-user)
    storage_entity_override: Optional[str] = None

    # HTTP route override (normally same as key but our code uses some route names)
    http_route: Optional[str] = None

    @property
    def storage_entity(self) -> str:
        return self.storage_entity_override or self.topic

    @property
    def route(self) -> str:
        return self.http_route or self.key.replace("-", "")


def _is_appeals_entity(entity_key: str) -> bool:
    return entity_key.startswith("appeal-")


def build_entity_spec(entity_key: str) -> EntitySpec:
    """
    Build EntitySpec from config.yaml + naming conventions
    """
    entity_cfg = config["global"]["entities"][entity_key]
    topic = entity_cfg["topic"]
    subscription = entity_cfg["subscription"]

    # schema filename convention matches our schemas mapping
    schema_filename = f"{entity_key}.schema.json"

    if _is_appeals_entity(entity_key):
        sb_connection = "ServiceBusConnectionAppeals"
        http_namespace_env_var = "SERVICEBUS_NAMESPACE_APPEALS"
    else:
        sb_connection = "ServiceBusConnection"
        http_namespace_env_var = "ServiceBusConnection__fullyQualifiedNamespace"

    # Route naming: we currently use for example "nsipdocument" not "nsip-document"
    # Our existing routes are mostly the "key without hyphens"
    http_route = entity_key.replace("-", "")

    # Known storage overrides from our current implementation
    storage_override = None
    if entity_key == "nsip-s51-advice":
        storage_override = "s51-advice"
    if entity_key == "appeal-service-user":
        storage_override = "service-user"
    if entity_key == "appeal-s78":
        storage_override = "appeal-s78"
    if entity_key == "appeal-representation":
        storage_override = "appeal-representation"

    return EntitySpec(
        key=entity_key,
        topic=topic,
        subscription=subscription,
        schema_filename=schema_filename,
        sb_connection=sb_connection,
        http_namespace_env_var=http_namespace_env_var,
        storage_entity_override=storage_override,
        http_route=http_route,
    )


def all_entities() -> list[EntitySpec]:
    """
    Create specs for all entities declared in config.yaml
    """
    return [build_entity_spec(k) for k in config["global"]["entities"].keys()]
