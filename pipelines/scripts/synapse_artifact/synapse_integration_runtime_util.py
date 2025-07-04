from pipelines.scripts.synapse_artifact.synapse_artifact_util import SynapseArtifactUtil
from typing import List, Dict, Any


class SynapseIntegrationRuntimeUtil(SynapseArtifactUtil):
    """
        Class for managing the retrieval and analysis of Synapse Integration Runtime artifacts
    """
    @classmethod
    def get_type_name(cls) -> str:
        return "integrationRuntime"

    def get(self, artifact_name: str, **kwargs: Dict[str, Any]) -> Dict[str, Any]:
        return self._web_request(
            f"{self.synapse_endpoint}/integrationRuntimes/{artifact_name}?api-version=2020-12-01",
        ).json()

    def get_all(self, **kwargs: Dict[str, Any]) -> List[Dict[str, Any]]:
        response = self._web_request(
            f"{self.synapse_endpoint}/integrationRuntimes?api-version=2020-12-01",
        ).json()
        all_runtimes = response["value"]
        while "nextLink" in response:
            next_link = response["nextLink"]
            response = self._web_request(next_link,).json()
            all_runtimes.extend(response["value"])
        return all_runtimes

    def get_uncomparable_attributes(self) -> List[str]:
        return []

    def get_nullable_attributes(self) -> List[str]:
        return []
    
    def get_env_attributes_to_replace(self) -> List[str]:
        return []
