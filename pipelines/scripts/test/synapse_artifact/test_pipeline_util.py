from pipelines.scripts.synapse_artifact.synapse_pipeline_util import SynapsePipelineUtil
from copy import deepcopy


def test__synapse_pipeline_util__compare__match():
    artifact = {
        "name": "test_pipeline",
        "properties": {
            "description": "some description",
            "activities": [
                {
                    "name": "some activity",
                    "description": "a description",
                    "type": "Copy",
                    "dependsOn": [],
                    "policy": {
                        "timeout": "0.12:00:00",
                        "retry": 0,
                        "retryIntervalInSeconds": 30,
                        "secureOutput": False,
                        "secureInput": False
                    },
                    "userProperties": [
                        {
                            "name": "Source",
                            "value": "some.sourceTable"
                        },
                        {
                            "name": "Destination",
                            "value": "some/destination"
                        }
                    ],
                    "typeProperties": {
                        "source": {
                            "type": "SqlServerSource",
                            "queryTimeout": "02:00:00",
                            "isolationLevel": "ReadCommitted",
                            "partitionOption": "None"
                        },
                        "sink": {
                            "type": "DelimitedTextSink",
                            "storeSettings": {
                                "type": "AzureBlobFSWriteSettings"
                            },
                            "formatSettings": {
                                "type": "DelimitedTextWriteSettings",
                                "quoteAllText": True,
                                "fileExtension": ".txt"
                            }
                        },
                        "enableStaging": False,
                        "translator": {
                            "type": "TabularTranslator",
                            "typeConversion": True,
                            "typeConversionSettings": {
                                "allowDataTruncation": True,
                                "treatBooleanAsNumber": False
                            }
                        }
                    },
                    "inputs": [
                        {
                            "referenceName": "some input",
                            "type": "DatasetReference"
                        }
                    ],
                    "outputs": [
                        {
                            "referenceName": "some output",
                            "type": "DatasetReference",
                            "parameters": {
                                "FileName": "someFile.csv"
                            }
                        }
                    ]
                }
            ],
            "folder": {
                "name": "some/folder"
            },
            "annotations": [],
            "lastPublishTime": "2024-01-08T18:03:43Z"
        },
        "type": "Microsoft.Synapse/workspaces/pipelines"
    }
    artifact_copy = deepcopy(artifact)
    assert SynapsePipelineUtil("some_workspace").compare(artifact, artifact_copy)


def test__synapse_pipeline_util__compare__mismatch():
    artifact = {
        "name": "test_pipeline",
        "properties": {
            "description": "some description",
            "activities": [
                {
                    "name": "some activity",
                    "description": "a description",
                    "type": "Copy",
                    "dependsOn": [],
                    "policy": {
                        "timeout": "0.12:00:00",
                        "retry": 0,
                        "retryIntervalInSeconds": 30,
                        "secureOutput": False,
                        "secureInput": False
                    },
                    "userProperties": [
                        {
                            "name": "Source",
                            "value": "some.sourceTable"
                        },
                        {
                            "name": "Destination",
                            "value": "some/destination"
                        }
                    ],
                    "typeProperties": {
                        "source": {
                            "type": "SqlServerSource",
                            "queryTimeout": "02:00:00",
                            "isolationLevel": "ReadCommitted",
                            "partitionOption": "None"
                        },
                        "sink": {
                            "type": "DelimitedTextSink",
                            "storeSettings": {
                                "type": "AzureBlobFSWriteSettings"
                            },
                            "formatSettings": {
                                "type": "DelimitedTextWriteSettings",
                                "quoteAllText": True,
                                "fileExtension": ".txt"
                            }
                        },
                        "enableStaging": False,
                        "translator": {
                            "type": "TabularTranslator",
                            "typeConversion": True,
                            "typeConversionSettings": {
                                "allowDataTruncation": True,
                                "treatBooleanAsNumber": False
                            }
                        }
                    },
                    "inputs": [
                        {
                            "referenceName": "some input",
                            "type": "DatasetReference"
                        }
                    ],
                    "outputs": [
                        {
                            "referenceName": "some output",
                            "type": "DatasetReference",
                            "parameters": {
                                "FileName": "someFile.csv"
                            }
                        }
                    ]
                }
            ],
            "folder": {
                "name": "some/folder"
            },
            "annotations": [],
            "lastPublishTime": "2024-01-08T18:03:43Z"
        },
        "type": "Microsoft.Synapse/workspaces/pipelines"
    }
    different_attributes = {
        "properties": {
            "description": "another description"
        }
    }
    artifact_copy = {**artifact, **different_attributes}
    assert not SynapsePipelineUtil("some_workspace").compare(artifact, artifact_copy)
