from pipelines.scripts.synapse_artifact.synapse_spark_configuration_util import SynapseSparkConfigurationUtil
from copy import deepcopy


def test__synapse_synapse_spark_configuration_util__compare__match():
    artifact = {
        "name": "someSparkConfiguration",
        "properties": {
            "configs": {
                "park.executorEnv.dataLakeAccountName": "somestorageaccount",
                "spark.executorEnv.keyVaultName": f"somekeyvault",
                "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
                "spark.sql.constraintPropagation.enabled": "false"
            },
            "created": "2025-04-11T14:34:14.9160000+01:00",
            "createdBy": "bob@pinso365.onmicrosoft.com",
            "annotations": [],
            "configMergeRule": {
                "artifact.currentOperation.park.executorEnv.dataLakeAccountName": "replace",
                "artifact.currentOperation.spark.executorEnv.keyVaultName": "replace",
                "artifact.currentOperation.spark.sql.parquet.int96RebaseModeInWrite": "replace",
                "artifact.currentOperation.spark.sql.constraintPropagation.enabled": "replace"
            }
        }
    }
    artifact_copy = deepcopy(artifact)
    assert SynapseSparkConfigurationUtil("some_workspace").compare(artifact, artifact_copy)


def test__synapse_linked_service_util__compare__mismatch():
    artifact = {
        "name": "someSparkConfiguration",
        "properties": {
            "configs": {
                "park.executorEnv.dataLakeAccountName": "somestorageaccount",
                "spark.executorEnv.keyVaultName": f"somekeyvault",
                "spark.sql.parquet.int96RebaseModeInWrite": "CORRECTED",
                "spark.sql.constraintPropagation.enabled": "false"
            },
            "created": "2025-04-11T14:34:14.9160000+01:00",
            "createdBy": "bob@pinso365.onmicrosoft.com",
            "annotations": [],
            "configMergeRule": {
                "artifact.currentOperation.park.executorEnv.dataLakeAccountName": "replace",
                "artifact.currentOperation.spark.executorEnv.keyVaultName": "replace",
                "artifact.currentOperation.spark.sql.parquet.int96RebaseModeInWrite": "replace",
                "artifact.currentOperation.spark.sql.constraintPropagation.enabled": "replace"
            }
        }
    }
    different_attributes = {
        "properties": {
            "description": "some description"
        }
    }
    artifact_copy = {**artifact, **different_attributes}
    assert not SynapseSparkConfigurationUtil("some_workspace").compare(artifact, artifact_copy)
