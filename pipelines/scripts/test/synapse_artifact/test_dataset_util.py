from pipelines.scripts.synapse_artifact.synapse_dataset_util import SynapseDatasetUtil
from copy import deepcopy


def test__synapse_dataset_util__compare__match():
    artifact = {
        "name": "test_dataset",
        "properties": {
            "linkedServiceName": {
                "referenceName": "some linked service",
                "type": "LinkedServiceReference"
            },
            "parameters": {
                "FileName": {
                    "type": "string"
                }
            },
            "annotations": [],
            "type": "DelimitedText",
            "typeProperties": {},
            "schema": [
                {
                    "name": "colA",
                    "type": "String"
                },
                {
                    "name": "colB",
                    "type": "String"
                }
            ]
        }
    }
    artifact_copy = deepcopy(artifact)
    assert SynapseDatasetUtil("some_workspace").compare(artifact, artifact_copy)


def test__synapse_dataset_util__compare__mismatch():
    artifact = {
        "name": "test_dataset",
        "properties": {
            "linkedServiceName": {
                "referenceName": "some linked service",
                "type": "LinkedServiceReference"
            },
            "parameters": {
                "FileName": {
                    "type": "string"
                }
            },
            "annotations": [],
            "type": "DelimitedText",
            "typeProperties": {},
            "schema": [
                {
                    "name": "colA",
                    "type": "String"
                },
                {
                    "name": "colB",
                    "type": "String"
                }
            ]
        }
    }
    different_attributes = {
        "properties": {
            "schema": [
                {
                    "name": "colA",
                    "type": "String"
                },
                {
                    "name": "colB",
                    "type": "String"
                },
                {
                    "name": "colC",
                    "type": "String"
                }
            ]
        }
    }
    artifact_copy = {**artifact, **different_attributes}
    assert not SynapseDatasetUtil("some_workspace").compare(artifact, artifact_copy)
