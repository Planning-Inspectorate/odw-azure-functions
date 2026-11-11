import pytest

from entity_registry import (
    EntitySpec,
    _ODW_NAMESPACE_ENTITIES,
    _SCHEMA_OVERRIDES,
    _STORAGE_OVERRIDES,
    _WAKE_SUBSCRIPTION_OVERRIDES,
    all_entities,
    build_entity_spec,
    config,
)


def test_all_configured_entities_have_unique_routes_and_expected_specs():
    specs = all_entities()
    routes = [spec.route for spec in specs]

    assert len(specs) == len(config["global"]["entities"])
    assert len(routes) == len(set(routes))
    assert all(spec.topic and spec.subscription for spec in specs)


@pytest.mark.parametrize(
    ("entity_key", "connection", "namespace_env"),
    [
        ("appeal-document", "ServiceBusConnectionAppeals", "SERVICEBUS_NAMESPACE_APPEALS"),
        (
            "applications-representation",
            "ServiceBusConnectionOdw",
            "ServiceBusConnectionOdw__fullyQualifiedNamespace",
        ),
        (
            "nsip-document",
            "ServiceBusConnection",
            "ServiceBusConnection__fullyQualifiedNamespace",
        ),
    ],
)
def test_build_entity_spec_selects_service_bus_namespace(
    entity_key, connection, namespace_env
):
    spec = build_entity_spec(entity_key)

    assert spec.sb_connection == connection
    assert spec.http_namespace_env_var == namespace_env


@pytest.mark.parametrize(
    ("entity_key", "schema_filename"),
    list(_SCHEMA_OVERRIDES.items()),
)
def test_build_entity_spec_applies_schema_overrides(entity_key, schema_filename):
    assert build_entity_spec(entity_key).schema_filename == schema_filename


@pytest.mark.parametrize(
    ("entity_key", "storage_entity"),
    list(_STORAGE_OVERRIDES.items()),
)
def test_build_entity_spec_applies_storage_overrides(entity_key, storage_entity):
    assert build_entity_spec(entity_key).storage_entity == storage_entity


def test_build_entity_spec_applies_s51_route_override():
    assert build_entity_spec("nsip-s51-advice").route == "s51advice"


@pytest.mark.parametrize("entity_key", sorted(_ODW_NAMESPACE_ENTITIES))
def test_odw_entities_use_the_odw_namespace(entity_key):
    assert build_entity_spec(entity_key).sb_connection == "ServiceBusConnectionOdw"


def test_wake_subscription_defaults_to_real_subscription_when_not_configured():
    entity_key = next(
        key
        for key in config["global"]["entities"]
        if key not in _WAKE_SUBSCRIPTION_OVERRIDES
    )
    spec = build_entity_spec(entity_key)

    assert spec.wake_subscription is None
    assert spec.trigger_subscription == spec.subscription


@pytest.mark.parametrize(
    ("storage_override", "http_route", "wake_subscription", "expected_storage", "expected_route", "expected_trigger"),
    [
        (None, None, None, "topic", "entitykey", "subscription"),
        ("folder", "custom", "wake", "folder", "custom", "wake"),
    ],
)
def test_entity_spec_properties_apply_optional_overrides(
    storage_override,
    http_route,
    wake_subscription,
    expected_storage,
    expected_route,
    expected_trigger,
):
    spec = EntitySpec(
        key="entity-key",
        topic="topic",
        subscription="subscription",
        schema_filename="schema.json",
        sb_connection="connection",
        http_namespace_env_var="namespace",
        storage_entity_override=storage_override,
        http_route=http_route,
        wake_subscription=wake_subscription,
    )

    assert spec.storage_entity == expected_storage
    assert spec.route == expected_route
    assert spec.trigger_subscription == expected_trigger
