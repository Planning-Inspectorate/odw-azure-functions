from pipelines.scripts.private_endpoint.private_endpoint_manager import PrivateEndpointManager


class ServiceBusPrivateEndpointManager(PrivateEndpointManager):
    def get_resource_type(self):
        return "Microsoft.ServiceBus/namespaces"
