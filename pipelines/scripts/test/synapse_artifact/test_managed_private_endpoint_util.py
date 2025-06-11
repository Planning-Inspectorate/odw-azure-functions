from pipelines.scripts.synapse_artifact.synapse_managed_private_endpoint_util import SynapseManagedPrivateEndpointUtil
from copy import deepcopy


def test__synapse_synapse_managed_private_endpoint_util__replace_env_strings():
    source_env = "dev"
    target_env = "test"
    artifact = {
        "name": "some mpe",
        "properties": {
            "privateLinkResourceId": f"/subscriptions/ff442a29-fc06-4a13-8e3e-65fd5da513b3/resourceGroups/pins-rg-data-odw-{source_env}-uks/providers/Microsoft.Synapse/workspaces/pins-synw-odw-{source_env}-uks",
            "groupId": "sql",
            "fqdns": [
                f"pins-synw-odw-{source_env}-uks.9f8d10ea-bfcb-46a2-9943-9bf5b8f34290.sql.azuresynapse.net"
            ]
        }
    }
    expected_cleaned_artifact = {
        "name": "some mpe",
        "properties": {
            "privateLinkResourceId": f"/subscriptions/ff442a29-fc06-4a13-8e3e-65fd5da513b3/resourceGroups/pins-rg-data-odw-{target_env}-uks/providers/Microsoft.Synapse/workspaces/pins-synw-odw-{target_env}-uks",
            "groupId": "sql",
            "fqdns": [
                f"pins-synw-odw-{target_env}-uks.9f8d10ea-bfcb-46a2-9943-9bf5b8f34290.sql.azuresynapse.net"
            ]
        }
    }
    cleaned_artifact = SynapseManagedPrivateEndpointUtil("some_workspace").replace_env_strings(deepcopy(artifact), source_env, target_env)
    assert cleaned_artifact == expected_cleaned_artifact


def test__synapse_synapse_managed_private_endpoint_util__compare__match():
    artifact = {
        "name": "some mpe",
        "properties": {
            "privateLinkResourceId": "/subscriptions/ff442a29-fc06-4a13-8e3e-65fd5da513b3/resourceGroups/pins-rg-data-odw-dev-uks/providers/Microsoft.Synapse/workspaces/pins-synw-odw-dev-uks",
            "groupId": "sql",
            "fqdns": [
                "pins-synw-odw-dev-uks.9f8d10ea-bfcb-46a2-9943-9bf5b8f34290.sql.azuresynapse.net"
            ]
        }
    }
    artifact_copy = deepcopy(artifact)
    assert SynapseManagedPrivateEndpointUtil("some_workspace").compare(artifact, artifact_copy)


def test__synapse_synapse_managed_private_endpoint_util__compare__mismatch():
    artifact = {
        "name": "some mpe",
        "properties": {
            "privateLinkResourceId": "/subscriptions/ff442a29-fc06-4a13-8e3e-65fd5da513b3/resourceGroups/pins-rg-data-odw-dev-uks/providers/Microsoft.Synapse/workspaces/pins-synw-odw-dev-uks",
            "groupId": "sql",
            "fqdns": [
                "pins-synw-odw-dev-uks.9f8d10ea-bfcb-46a2-9943-9bf5b8f34290.sql.azuresynapse.net"
            ]
        }
    }
    different_attributes = {
        "properties": {
            "description": "some description"
        }
    }
    artifact_copy = {**artifact, **different_attributes}
    assert not SynapseManagedPrivateEndpointUtil("some_workspace").compare(artifact, artifact_copy)
