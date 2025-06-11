from pipelines.scripts.synapse_artifact.synapse_linked_service_util import SynapseLinkedServiceUtil
from copy import deepcopy


def test__synapse_linked_service_util__replace_env_strings():
    source_env = "dev"
    target_env = "test"
    artifact = {
        "name": "test_ls",
        "properties": {
            "annotations": [],
            "type": "AzureKeyVault",
            "typeProperties": {
                "url": "https://somedevkeyvault.vault.azure.net/",
                "baseUrl": "https://somedevkeyvault.vault.azure.net/"
            }
        },
        "type": "Microsoft.Synapse/workspaces/linkedservices"
    }
    expected_cleaned_artifact = {
        "name": "test_ls",
        "properties": {
            "annotations": [],
            "type": "AzureKeyVault",
            "typeProperties": {
                "url": f"https://some{target_env}keyvault.vault.azure.net/",
                "baseUrl": f"https://some{target_env}keyvault.vault.azure.net/"
            }
        },
        "type": "Microsoft.Synapse/workspaces/linkedservices"
    }
    cleaned_artifact = SynapseLinkedServiceUtil("some_workspace").replace_env_strings(deepcopy(artifact), source_env, target_env)
    assert cleaned_artifact == expected_cleaned_artifact


def test__synapse_linked_service_util__compare__match():
    artifact = {
        "name": "test_ls",
        "properties": {
            "annotations": [],
            "type": "AzureKeyVault",
            "typeProperties": {
                "baseUrl": "https://somedevkeyvault.vault.azure.net/"
            }
        },
        "type": "Microsoft.Synapse/workspaces/linkedservices"
    }
    artifact_copy = deepcopy(artifact)
    assert SynapseLinkedServiceUtil("some_workspace").compare(artifact, artifact_copy)


def test__synapse_linked_service_util__compare__mismatch():
    artifact = {
        "name": "test_ls",
        "properties": {
            "annotations": [],
            "type": "AzureKeyVault",
            "typeProperties": {
                "baseUrl": "https://somedevkeyvault.vault.azure.net/"
            }
        },
        "type": "Microsoft.Synapse/workspaces/linkedservices"
    }
    different_attributes = {
        "properties": {
            "description": "some description"
        }
    }
    artifact_copy = {**artifact, **different_attributes}
    assert not SynapseLinkedServiceUtil("some_workspace").compare(artifact, artifact_copy)
