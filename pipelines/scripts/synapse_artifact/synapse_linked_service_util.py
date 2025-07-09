from pipelines.scripts.synapse_artifact.synapse_artifact_util import SynapseArtifactUtil
from typing import List, Dict, Any


class SynapseLinkedServiceUtil(SynapseArtifactUtil):
    """
        Class for managing the retrieval and analysis of Synapse Linked Service artifacts
    """
    @classmethod
    def get_type_name(cls) -> str:
        return "linkedService"

    def get(self, artifact_name: str, **kwargs: Dict[str, Any]) -> Dict[str, Any]:
        return self._web_request(
            f"{self.synapse_endpoint}/linkedservices/{artifact_name}?api-version=2020-12-01",
        ).json()

    def get_all(self, **kwargs: Dict[str, Any]) -> List[Dict[str, Any]]:
        response = self._web_request(
            f"{self.synapse_endpoint}/linkedservices?api-version=2020-12-01",
        ).json()
        all_linked_services = response["value"]
        while "nextLink" in response:
            next_link = response["nextLink"]
            response = self._web_request(next_link,).json()
            all_linked_services.extend(response["value"])
        return all_linked_services

    def get_uncomparable_attributes(self) -> List[str]:
        return [
            r"^id$",
            r"^etag$",
            r"^type$"
        ]

    def get_nullable_attributes(self) -> List[str]:
        return []

    def get_env_attributes_to_replace(self) -> List[str]:
        return [
            "properties.typeProperties.url",
            "properties.typeProperties.baseUrl"
        ]
