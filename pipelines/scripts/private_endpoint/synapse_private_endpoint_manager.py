from pipelines.scripts.private_endpoint.private_endpoint_manager import PrivateEndpointManager


class SynapsePrivateEndpointManager(PrivateEndpointManager):
    def get_resource_type(self):
        return "Microsoft.Synapse/workspaces"
