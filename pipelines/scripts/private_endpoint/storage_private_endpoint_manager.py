from pipelines.scripts.private_endpoint.private_endpoint_manager import PrivateEndpointManager


class StoragePrivateEndpointManager(PrivateEndpointManager):
    def get_resource_type(self):
        return "Microsoft.Storage/storageAccounts"
