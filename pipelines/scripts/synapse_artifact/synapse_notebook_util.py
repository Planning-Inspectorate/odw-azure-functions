from pipelines.scripts.synapse_artifact.synapse_artifact_util import SynapseArtifactUtil
from typing import List, Dict, Any


class SynapseNotebookUtil(SynapseArtifactUtil):
    """
        Class for managing the retrieval and analysis of Synapse Notebook artifacts
    """
    @classmethod
    def get_type_name(cls) -> str:
        return "notebook"

    def get(self, artifact_name: str, **kwargs: Dict[str, Any]) -> Dict[str, Any]:
        return self._web_request(
            f"{self.synapse_endpoint}/notebooks/{artifact_name}?api-version=2020-12-01",
        ).json()

    def get_all(self, **kwargs: Dict[str, Any]) -> List[Dict[str, Any]]:
        response = self._web_request(
            f"{self.synapse_endpoint}/notebooks?api-version=2020-12-01",
        ).json()
        all_notebooks = response["value"]
        while "nextLink" in response:
            next_link = response["nextLink"]
            response = self._web_request(next_link,).json()
            all_notebooks.extend(response["value"])
        return all_notebooks

    def get_uncomparable_attributes(self) -> List[str]:
        return [
            r"id",
            r"type",
            r"etag",
            r"properties.entityState",
            r"properties.renameOperationDetails",
            r"properties.targetSparkConfiguration",
            r"properties.description",
            r"properties.sessionProperties.runAsWorkspaceSystemIdentity",
            r"properties.metadata.a365ComputeOptions.extraHeader",
            r"properties.metadata.a365ComputeOptions.auth.authHeader",
            r"properties.cells.\d+.execution_count",
            r"properties.cells.\d+.outputs",
            r"properties.metadata.kernelspec",
            r"properties.metadata.kernelspec.name",
            r"properties.metadata.kernelspec.display_name",
        ]

    def get_nullable_attributes(self) -> List[str]:
        return [
            r"properties.metadata.a365ComputeOptions.automaticScaleJobs",
            r"properties.cells.\d+.metadata",
            r"properties.folder",
            r"properties.bigDataPool",
            r"properties.metadata.a365ComputeOptions"
        ]
    
    def compare(self, artifact_a, artifact_b):
        def _add_missing_attributes(artifact: Dict[str, Any]) -> Dict[str, Any]:
            # Set default values for optional properties. This is the only artifact where this is required
            properties = artifact["properties"]
            metadata = properties["metadata"]
            if "a365ComputeOptions" not in metadata or metadata["a365ComputeOptions"] is None:
                metadata["a365ComputeOptions"] = {
                    "automaticScaleJobs": True
                }
            compute_options = metadata["a365ComputeOptions"]
            if "automaticScaleJobs" not in compute_options:
                compute_options["automaticScaleJobs"] = True
            return artifact
        artifact_a = _add_missing_attributes(artifact_a)
        artifact_b = _add_missing_attributes(artifact_b)
        return super().compare(artifact_a, artifact_b)

    def get_env_attributes_to_replace(self) -> List[str]:
        return [
            "properties.metadata.a365ComputeOptions.id",
            "properties.metadata.a365ComputeOptions.endpoint"
        ]
