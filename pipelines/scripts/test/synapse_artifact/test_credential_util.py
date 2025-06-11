from pipelines.scripts.synapse_artifact.synapse_credential_util import SynapseCredentialUtil
from copy import deepcopy


def test__synapse_credential_util__compare__match():
    artifact = {
        "name": "WorkspaceSystemIdentity",
        "properties": {
            "type": "ManagedIdentity"
        }
    }
    artifact_copy = deepcopy(artifact)
    assert SynapseCredentialUtil("some_workspace").compare(artifact, artifact_copy)


def test__synapse_credential_util__compare__mismatch():
    artifact = {
        "name": "WorkspaceSystemIdentity",
        "properties": {
            "type": "ManagedIdentity"
        }
    }
    different_attributes = {
        "properties": {
            "description": "some description"
        }
    }
    artifact_copy = {**artifact, **different_attributes}
    assert not SynapseCredentialUtil("some_workspace").compare(artifact, artifact_copy)
