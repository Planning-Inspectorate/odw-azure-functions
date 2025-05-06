from pipelines.scripts.util import Util
from pipelines.scripts.synapse_artifact.synapse_artifact_util import SynapseArtifactUtil
from typing import List, Dict, Any


class SynapseSparkPoolUtil(SynapseArtifactUtil):
    """
        Class for managing the retrieval and analysis of Synapse Spark Pool artifacts
    """
    @classmethod
    def get_type_name(cls) -> str:
        return "sparkPool"

    def get(self, artifact_name: str, **kwargs: Dict[str, Any]) -> Dict[str, Any]:
        return self._web_request(
            f"{self.synapse_endpoint}/bigDataPools/{artifact_name}?api-version=2020-12-01",
        ).json()

    def get_all(self, **kwargs: Dict[str, Any]) -> List[Dict[str, Any]]:
        response = self._web_request(
            f"{self.synapse_endpoint}/bigDataPools?api-version=2020-12-01",
        ).json()
        all_pools = response["value"]
        while "nextLink" in response:
            next_link = response["nextLink"]
            response = self._web_request(next_link,).json()
            all_pools.extend(response["value"])
        return all_pools

    def get_uncomparable_attributes(self) -> List[str]:
        return []

    def get_nullable_attributes(self) -> List[str]:
        return []
