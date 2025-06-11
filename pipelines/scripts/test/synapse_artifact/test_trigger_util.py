from pipelines.scripts.synapse_artifact.synapse_trigger_util import SynapseTriggerUtil
from copy import deepcopy


def test__synapse_synapse_trigger_util__compare__match():
    artifact = {
        "name": "some trigger",
        "properties": {
            "description": "some description",
            "annotations": [],
            "runtimeState": "Started",
            "pipelines": [
                {
                    "pipelineReference": {
                        "referenceName": "some pipeline",
                        "type": "PipelineReference"
                    }
                }
            ],
            "type": "ScheduleTrigger",
            "typeProperties": {
                "recurrence": {
                    "frequency": "Hour",
                    "interval": 24,
                    "startTime": "2025-04-10T09:00:00Z",
                    "timeZone": "UTC"
                }
            }
        }
    }
    artifact_copy = deepcopy(artifact)
    assert SynapseTriggerUtil("some_workspace").compare(artifact, artifact_copy)


def test__synapse_synapse_trigger_util__compare__mismatch():
    artifact = {
        "name": "some trigger",
        "properties": {
            "description": "some description",
            "annotations": [],
            "runtimeState": "Started",
            "pipelines": [
                {
                    "pipelineReference": {
                        "referenceName": "some pipeline",
                        "type": "PipelineReference"
                    }
                }
            ],
            "type": "ScheduleTrigger",
            "typeProperties": {
                "recurrence": {
                    "frequency": "Hour",
                    "interval": 24,
                    "startTime": "2025-04-10T09:00:00Z",
                    "timeZone": "UTC"
                }
            }
        }
    }
    different_attributes = {
        "properties": {
            "description": "another description"
        }
    }
    artifact_copy = {**artifact, **different_attributes}
    assert not SynapseTriggerUtil("some_workspace").compare(artifact, artifact_copy)
