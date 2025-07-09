from pipelines.scripts.synapse_artifact.synapse_artifact_util import SynapseArtifactUtil
from typing import List, Dict, Any


class SynapsePipelineUtil(SynapseArtifactUtil):
    """
        Class for managing the retrieval and analysis of Synapse Pipeline artifacts
    """
    @classmethod
    def get_type_name(cls) -> str:
        return "pipeline"

    def get(self, artifact_name: str, **kwargs: Dict[str, Any]) -> Dict[str, Any]:
        return self._web_request(
            f"{self.synapse_endpoint}/pipelines/{artifact_name}?api-version=2020-12-01",
        ).json()

    def get_all(self, **kwargs: Dict[str, Any]) -> List[Dict[str, Any]]:
        response = self._web_request(
            f"{self.synapse_endpoint}/pipelines?api-version=2020-12-01",
        ).json()
        all_pipelines = response["value"]
        while "nextLink" in response:
            next_link = response["nextLink"]
            response = self._web_request(next_link,).json()
            all_pipelines.extend(response["value"])
        return all_pipelines

    def get_uncomparable_attributes(self) -> List[str]:
        return [
            r"^id$",
            r"^type$",
            r"^etag$",
            r"^properties.lastPublishTime"
        ]

    def get_nullable_attributes(self) -> List[str]:
        return [
            r"^properties.activities.\d+.inputs.\d+.parameters$",
            r"^properties.activities.\d+.outputs.\d+.parameters$",
            r"^properties.activities.\d+.policy$",
            r"^properties.activities.\d+.policy.secureInput$",
            r"^properties.activities.\d+.typeProperties.parameters$",
            r"^properties.policy$",
            r"^properties.policy.elapsedTimeMetric$",
            r"properties.activities.\d+.typeProperties.conf.spark.dynamicAllocation.minExecutors",
            r"properties.activities.\d+.typeProperties.numExecutors",
            r"properties.activities.\d+.typeProperties.conf.spark.dynamicAllocation.enabled",
            r"properties.activities.\d+.typeProperties.conf.spark.dynamicAllocation.maxExecutors",
            r"properties.activities.\d+.typeProperties.ifTrueActivities.\d+.typeProperties.conf.spark.dynamicAllocation.minExecutors",
            r"properties.activities.\d+.typeProperties.ifTrueActivities.\d+.typeProperties.conf.spark.dynamicAllocation.enabled",
            r"properties.activities.\d+.typeProperties.ifTrueActivities.\d+.typeProperties.conf.spark.dynamicAllocation.maxExecutors",
            r"properties.activities.\d+.typeProperties.ifTrueActivities.\d+.typeProperties.numExecutors",
            r"properties.activities.\d+.typeProperties.ifFalseActivities.\d+.policy.secureInput",
            r"properties.activities.\d+.typeProperties.activities.\d+.typeProperties.conf.spark.dynamicAllocation.maxExecutors",
            r"properties.activities.\d+.typeProperties.activities.\d+.typeProperties.numExecutors",
            r"properties.activities.\d+.typeProperties.activities.\d+.typeProperties.conf.spark.dynamicAllocation.enabled",
            r"properties.activities.\d+.typeProperties.activities.\d+.typeProperties.conf.spark.dynamicAllocation.minExecutors",
            r"properties.activities.\d+.typeProperties.activities.\d+.policy.secureInput",
            r"properties.activities.\d+.typeProperties.ifTrueActivities.\d+.policy.secureInput"
        ]

    def get_env_attributes_to_replace(self) -> List[str]:
        return []

    @classmethod
    def can_be_archived(cls) -> bool:
        return True

    @classmethod
    def archive(cls, artifact: Dict[str, Any]) -> Dict[str, Any]:
        existing_folder = artifact["properties"].get("folder", dict())
        existing_folder_name = existing_folder.get("name", "")
        existing_folder.update(
            {
                "name": "/".join(["archive", existing_folder_name])
            }
        )
        artifact["properties"]["folder"] = existing_folder
        return artifact
