from pipelines.scripts.synapse_artifact.synapse_artifact_util import SynapseArtifactUtil
from typing import List, Dict, Any


class SynapseCredentialUtil(SynapseArtifactUtil):
    """
        Class for managing the retrieval and analysis of Synapse Credential artifacts
    """
    @classmethod
    def get_type_name(cls) -> str:
        return "credential"

    def get(self, artifact_name: str, **kwargs: Dict[str, Any]) -> Dict[str, Any]:
        return self._web_request(
            f"{self.synapse_endpoint}/credentials/{artifact_name}?api-version=2020-12-01",
        ).json()

    def get_all(self, **kwargs: Dict[str, Any]) -> List[Dict[str, Any]]:
        response = self._web_request(
            f"{self.synapse_endpoint}/credentials?api-version=2020-12-01",
        ).json()
        all_credentials = response["value"]
        while "nextLink" in response:
            next_link = response["nextLink"]
            response = self._web_request(next_link,).json()
            all_credentials.extend(response["value"])
        return all_credentials

    def get_uncomparable_attributes(self) -> List[str]:
        return [
            r"^id$",
            r"^etag$",
            r"^type$"
        ]

    def get_nullable_attributes(self) -> List[str]:
        return [
            r"^properties.typeProperties$"
        ]
