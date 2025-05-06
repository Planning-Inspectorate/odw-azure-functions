from pipelines.scripts.synapse_artifact.synapse_artifact_util import SynapseArtifactUtil
from typing import List, Dict, Any
import os
import json
import logging


class SynapseManagedPrivateEndpointUtil(SynapseArtifactUtil):
    """
        Class for managing the retrieval and analysis of Synapse Managed Private Endpoint artifacts
    """
    def __init__(self, workspace_name):
        super().__init__(workspace_name)

    @classmethod
    def get_type_name(cls) -> str:
        return "managedVirtualNetwork"

    def get(self, artifact_name: str, **kwargs: Dict[str, Any]) -> Dict[str, Any]:
        vnet = kwargs.get("vnet")
        return self._web_request(
            f"{self.synapse_endpoint}/managedVirtualNetworks/{vnet}/managedPrivateEndpoints/{artifact_name}?api-version=2020-12-01",
        ).json()

    def get_all(self, **kwargs: Dict[str, Any]) -> List[Dict[str, Any]]:
        vnet = kwargs.get("vnet")
        response = self._web_request(
            f"{self.synapse_endpoint}/managedVirtualNetworks/{vnet}/managedPrivateEndpoints?api-version=2020-12-01",
        ).json()
        all_mpes = response["value"]
        while "nextLink" in response:
            next_link = response["nextLink"]
            response = self._web_request(next_link,).json()
            all_mpes.extend(response["value"])
        return all_mpes

    def download_live_workspace(self, local_folder: str):
        """
            Download all artifacts to a local folder
            
            :param local_folder: Where to store
        """
        virtual_networks = [
            f.replace(".json", "")
            for f in os.listdir("workspace/managedVirtualNetwork")
            if os.path.isfile(os.path.join("workspace/managedVirtualNetwork", f))
        ]
        for vnet in virtual_networks:
            artifacts = self.get_all(vnet=vnet)
            logging.info(f"Downloaded {len(artifacts)} artifacts of type '{self.get_type_name()}'")
            base_folder = f"{local_folder}/{self.get_type_name()}/{vnet}/managedPrivateEndpoint"
            os.makedirs(base_folder)
            for artifact in artifacts:
                artifact_name = artifact["name"]
                with open(f"{base_folder}/{artifact_name}.json", "w") as f:
                    json.dump(artifact, f, indent=4)

    def get_uncomparable_attributes(self) -> List[str]:
        return [
            r"^id$",
            r"^etag$",
            r"^type$",
            # Ignore these because they are auto-generated after creation, or are not relevant to the MPE's config
            r"properties.connectionState",
            r"^properties.resourceId$",
            r"^properties.isCompliant$",
            r"^properties.isReserved$",
            r"^properties.ipAddress$",
            r"^properties.provisioningState$"
        ]

    def get_nullable_attributes(self) -> List[str]:
        return []
