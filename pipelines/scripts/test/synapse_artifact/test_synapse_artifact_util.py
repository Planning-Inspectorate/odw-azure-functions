from pipelines.scripts.synapse_artifact.synapse_artifact_util import SynapseArtifactUtil
import pytest
from copy import deepcopy


@pytest.mark.parametrize(
    "test_case",
    [
        ("a", 1),
        ("b", 2),
        ("c", [3, 4, {"sublistdir": {"a": [42]}}]),
        ("c.0", 3),
        ("c.1", 4),
        ("d.0.0", 5),
        ("e.0.f", 6),
        ("g.h", 7),
        ("g.i.j", 8),
        ("c.2.sublistdir.a.0", 42),  # A complex test case,
        ("g.k.l.m", 9),
        ("c.2.sublistdir", {"a": [42]})
    ]
)
def test_synapse_artifact_util__get_by_attribute(test_case):
    artifact = {
        "a": 1,
        "b": 2,
        "c": [
            3,
            4,
            {
                "sublistdir": {
                    "a": [
                        42
                    ]
                }
            }
        ],
        "d": [
            [
                5
            ]
        ],
        "e": [
            {
                "f": 6
            }
        ],
        "g": {
            "h": 7,
            "i": {
                "j": 8
            },
            "k.l.m": 9
        }
    }
    attribute_string = test_case[0]
    expected_value = test_case[1]
    assert SynapseArtifactUtil.get_by_attribute(artifact, attribute_string) == expected_value


@pytest.mark.parametrize(
    "test_case",
    [
        ("a", "Jean-Luc Picard", {"a": "Jean-Luc Picard"}),
        ("b", "James T Kirk", {"b": "James T Kirk"}),
        ("c", 42, {"c": 42}),
        ("c.0", 42, {"c": [42, 4]}),
        ("c.1", 42, {"c": [3, 42]}),
        ("d.0", 10, {"d": [10]}),
        ("e.0.f", "bob", {"e": [{"f": "bob"}]}),
        ("g.h", 66, {"g": {"h": 66, "i": {"j": 8}}}),
        ("g.i.j", None, {"g": {"h": 7, "i": {"j": None}}})
    ]
)
def test_synapse_artifact_util__set_by_attribute(test_case):
    artifact = {
        "a": 1,
        "b": 2,
        "c": [
            3,
            4
        ],
        "d": [
            [
                5
            ]
        ],
        "e": [
            {
                "f": 6
            }
        ],
        "g": {
            "h": 7,
            "i": {
                "j": 8
            }
        }
    }
    attribute_string = test_case[0]
    new_value = test_case[1]
    new_artifact_properties = test_case[2]
    expected_artifact = {**artifact, **new_artifact_properties}
    artifact_copy = deepcopy(artifact)
    assert SynapseArtifactUtil.set_by_attribute(artifact_copy, attribute_string, new_value) == expected_artifact


def test_synapse_artifact_util__get_all_attributes():
    sample_json = {
        "some": 1,
        "attributes": 2,
        "list_type": [
            "a"
        ],
        "dict_type": {
            "b": 3,
            "c": 4
        }
    }
    expected_attributes = {
        "some",
        "attributes",
        "list_type.0",
        "dict_type.b",
        "dict_type.c"
    }
    assert SynapseArtifactUtil.get_all_attributes(sample_json) == expected_attributes


def test__synapse_artifact_util__dependent_artifacts():
    # This is not a real artifact, it's an amalgamation of all possible references that could exist across all artifact types
    artifact = {
        "name": "test_pipeline",
        "properties": {
            "linkedServiceName": {
                "referenceName": "test_linked_service",
                "type": "LinkedServiceReference"
            },
            "activities": [
                {
                    "type": "SynapseNotebook",
                    "typeProperties": {
                        "notebook": {
                            "referenceName": "test_notebook_a",
                            "type": "NotebookReference"
                        }
                    }
                },
                {
                    "type": "SynapseNotebook",
                    "userProperties": [],
                    "typeProperties": {
                        "notebook": {
                            "referenceName": "test_notebook_b",
                            "type": "NotebookReference"
                        }
                    }
                },
                {
                    "type": "Copy",
                    "userProperties": [],
                    "typeProperties": {
                        "source": {
                            "type": "AzureSqlSource",
                            "queryTimeout": "02:00:00",
                            "partitionOption": "None"
                        },
                        "sink": {
                            "type": "AzureSqlSink",
                            "preCopyScript": "",
                            "writeBehavior": "insert",
                            "sqlWriterUseTableLock": False,
                            "tableOption": "autoCreate",
                            "disableMetricsCollection": False
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
                            "referenceName": "test_dataset",
                            "type": "DatasetReference"
                        }
                    ],
                    "outputs": []
                },
            ],
            "pipelines": [
                {
                    "pipelineReference": {
                        "referenceName": "test_pipeline_a",
                        "type": "PipelineReference"
                    }
                },
                {
                    "pipelineReference": {
                        "referenceName": "test_pipeline_b",
                        "type": "PipelineReference"
                    }
                },
                {
                    "pipelineReference": {
                        "referenceName": "test_pipeline_c",
                        "type": "PipelineReference"
                    }
                }
            ]
        }
    }
    expected_dependent_artifacts = {
        "linkedService/test_linked_service.json",
        "notebook/test_notebook_a.json",
        "notebook/test_notebook_b.json",
        "dataset/test_dataset.json",
        "pipeline/test_pipeline_a.json",
        "pipeline/test_pipeline_b.json",
        "pipeline/test_pipeline_c.json"
    }
    assert SynapseArtifactUtil.dependent_artifacts(artifact) == expected_dependent_artifacts
