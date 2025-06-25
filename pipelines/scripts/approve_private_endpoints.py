from pipelines.scripts.private_endpoint.synapse_private_endpoint_manager import SynapsePrivateEndpointManager
from pipelines.scripts.private_endpoint.key_vault_private_endpoint_manager import KeyVaultPrivateEndpointManager
from pipelines.scripts.private_endpoint.storage_private_endpoint_manager import StoragePrivateEndpointManager
from pipelines.scripts.private_endpoint.service_bus_private_endpoint_manager import ServiceBusPrivateEndpointManager
from pipelines.scripts.util import Util
import argparse
import logging
import os


logging.basicConfig(level=logging.INFO)


ENDPOINTS_TO_EXCLUDE = {
    "synapse-mpe-appeals-bo--odw-prod-uks",
    "synapse-mpe-kv--odw-prod-uks"
}


def approve_private_endpoints(env: str):
    """
        Approve all ODW private endpoints
    """
    initial_subscription = Util.get_subscription()
    synapse_private_endpoint_manager = SynapsePrivateEndpointManager()
    synapse_private_endpoint_manager.approve_all(
        f"pins-rg-data-odw-{env}-uks",
        f"pins-synw-odw-{env}-uks",
        ENDPOINTS_TO_EXCLUDE
    )
    key_vault_private_endpoint_manager = KeyVaultPrivateEndpointManager()
    key_vault_private_endpoint_manager.approve_all(
        f"pins-rg-data-odw-{env}-uks",
        f"pinskvsynwodw{env}uks",
        ENDPOINTS_TO_EXCLUDE
    )
    storage_private_endpoint_manager = StoragePrivateEndpointManager()
    odw_storage_accounts = Util.get_odw_storage_account_names(env)
    for storage_account in odw_storage_accounts:
        storage_private_endpoint_manager.approve_all(
            f"pins-rg-data-odw-{env}-uks",
            storage_account,
            ENDPOINTS_TO_EXCLUDE
        )
    # Approve pending Synapse MPEs pointing to the Appeals Back Office service bus
    # Switch to the appeals bo subscription
    if env != "build":
        exception = None
        Util.set_subscription(os.environ.get("ODT_SUBSCRIPTION_ID"))
        try:
            service_bus_private_endpoint_manager = ServiceBusPrivateEndpointManager()
            appeals_bo_resource_group = f"pins-rg-appeals-bo-{env}"
            appeals_bo_service_bus_name = f"pins-sb-appeals-bo-{env}"
            all_private_endpoints = service_bus_private_endpoint_manager.get_all(
                appeals_bo_resource_group,
                appeals_bo_service_bus_name
            )
            synapse_mpes = [x for x in all_private_endpoints if f"synapse-mpe-appeals-bo--odw-{env}-uks" in x["properties"]["privateEndpoint"]["id"]]
            for synapse_mpe in synapse_mpes:
                service_bus_private_endpoint_manager.approve(synapse_mpe["id"])
        except Exception as e:
            exception = e
        finally:
            Util.set_subscription(initial_subscription)
        if exception:
            raise exception


if __name__ == "__main__":
    # Load command line arguments
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-e", "--env", help="Environment to run against", required=True)
    args = parser.parse_args()
    env = args.env
    approve_private_endpoints(env)
