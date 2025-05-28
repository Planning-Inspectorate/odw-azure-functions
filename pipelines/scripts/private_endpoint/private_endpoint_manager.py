from typing import List, Dict, Any, Iterable
from abc import ABC, abstractmethod
from pipelines.scripts.util import Util
import logging
import json


class PrivateEndpointManager(ABC):

    def get(self, private_endpoint_id: str) -> Dict[str, Any]:
        """
            Return the details of the given private endpoint

            :param private_endpoint_id: The private endpoint to fetch the details for
        """
        command_to_run = (
            "az network private-endpoint-connection show"
        )
        command_args = {
            "--id": private_endpoint_id,
            "--output": "json"
        }
        return json.loads(
            Util.run_az_cli_command(
                command_to_run.split(" ") + [
                    elem
                    for kv_pair in command_args.items()
                    for elem in kv_pair
                ]
            )
        )

    def approve(self, private_endpoint_id: str):
        """
            Approve a private endpoint that is pending on the given Azure resource type

            :param private_endpoint_id: The private endpoint to approve
        """
        logging.info(f"Approving private endpoint with id '{private_endpoint_id}'")
        existing_endpoint = self.get(private_endpoint_id)
        if existing_endpoint.get("properties", dict()).get("privateLinkServiceConnectionState", dict()).get("status", None) == "Approved":
            logging.info("    Private endpoint already approved")
            return
        command_to_run = (
            "az network private-endpoint-connection approve"
        )
        command_args = {
            "--description": f"Auto-Approved by {Util.get_current_user()}",
            "--id": private_endpoint_id,
            "--output": "json"
        }
        Util.run_az_cli_command(
            command_to_run.split(" ") + [
                elem
                for kv_pair in command_args.items()
                for elem in kv_pair
            ]
        )

    def get_all(self, resource_group_name: str, resource_name: str) -> List[Dict[str, Any]]:
        """
            Return all private endpoints for the resource type class

            :param resource_group_name: The resource group the target resource belongs to
            :param resource_name: The target resource to approve private endpoints for
        """
        command_to_run = (
            "az network private-endpoint-connection list"
        )
        command_args = {
            "--name": resource_name,
            "--resource-group": resource_group_name,
            "--type": self.get_resource_type(),
            "--output": "json"
        }
        return json.loads(
            Util.run_az_cli_command(
                command_to_run.split(" ") + [
                    elem
                    for kv_pair in command_args.items()
                    for elem in kv_pair
                ]
            )
        )

    def get_all_ids(self, resource_group_name: str, resource_name: str) -> List[Dict[str, Any]]:
        """
            Return all private endpoint ids for the resource type class

            :param resource_group_name: The resource group the target resource belongs to
            :param resource_name: The target resource to approve private endpoints for
        """
        return [x["id"] for x in self.get_all(resource_group_name, resource_name)]

    def approve_all(self, resource_group_name: str, resource_name: str, endpoints_to_exclude: Iterable[str]):
        """
            Approve all pending private endpoints on the given resource

            :param resource_group_name: The resource group the target resource belongs to
            :param resource_name: The target resource to approve private endpoints for
            :param endpoints_to_exclude: A group of strings to use to skip approving endpoints
        """
        endpoints = [
            endpoint
            for endpoint in self.get_all_ids(resource_group_name, resource_name)
            if not any(x in endpoint for x in endpoints_to_exclude)
        ]
        for endpoint in endpoints:
            self.approve(endpoint)

    @abstractmethod
    def get_resource_type(self) -> str:
        pass
