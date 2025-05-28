from pipelines.scripts.private_endpoint.synapse_private_endpoint_manager import SynapsePrivateEndpointManager
from pipelines.scripts.private_endpoint.key_vault_private_endpoint_manager import KeyVaultPrivateEndpointManager
from pipelines.scripts.private_endpoint.storage_private_endpoint_manager import StoragePrivateEndpointManager
from pipelines.scripts.util import Util
import argparse
import logging


logging.basicConfig(level=logging.INFO)


ENDPOINTS_TO_EXCLUDE = {
    "synapse-mpe-appeals-bo--odw-prod-uks",
    "synapse-mpe-kv--odw-prod-uks"
}


def approve_private_endpoints(env: str):
    """
        Approve all ODW private endpoints
    """
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


if __name__ == "__main__":
    # Load command line arguments
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-e", "--env", help="Environment to run against", required=True)
    args = parser.parse_args()
    env = args.env
    approve_private_endpoints(env)
