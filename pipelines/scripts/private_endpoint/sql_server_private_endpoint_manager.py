from pipelines.scripts.private_endpoint.private_endpoint_manager import PrivateEndpointManager


class SSQLServerPrivateEndpointManager(PrivateEndpointManager):
    def get_resource_type(self):
        return "Microsoft.Sql/servers"
