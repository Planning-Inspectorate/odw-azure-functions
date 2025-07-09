from azure.identity import AzureCliCredential
from azure.keyvault.secrets import SecretClient
import argparse
from typing import Set
import logging


logging.basicConfig(level=logging.INFO)


class KeyVaultManager():
    """
        Wrapper class to handle the interaction with a key vault
    """
    def __init__(self, vault_name: str):
        credential = AzureCliCredential()
        self.client = SecretClient(vault_url=f"https://{vault_name}.vault.azure.net", credential=credential)
    
    def create_secret(self, secret_name: str, secret_value: str):
        return self.client.set_secret(secret_name, secret_value)

    def get_secret(self, secret_name: str):
        return self.client.get_secret(secret_name)
    
    def get_secrets(self) -> Set[str]:
        return {x.name for x in self.client.list_properties_of_secrets()}


def copy_secrets_between_key_vaults(source_vault: str, target_vault: str):
    """
        Copy secrets from one key vault to another - only copies secrets that will be new to the target vault

        :param source_vault: The key vault to copy secrets from
        :param target_vault: The key vault to copy secrets to
    """
    source_kv_manager = KeyVaultManager(source_vault)
    target_kv_manager = KeyVaultManager(target_vault)

    source_secrets = source_kv_manager.get_secrets()
    target_secrets = target_kv_manager.get_secrets()
    secrets_to_copy = source_secrets - target_secrets
    for secret_name in secrets_to_copy:
        logging.info(f"Copying secret from {source_vault} to {target_vault}")
        secret_value = source_kv_manager.get_secret(secret_name).value
        target_kv_manager.create_secret(secret_name, secret_value)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-skv", "--source_key_vault", required=True, help="The key vault to copy from")
    parser.add_argument("-tkv", "--target_key_vault", required=True, help="The key vault to copy to")
    args = parser.parse_args()
    source_key_vault = args.source_key_vault
    target_key_vault = args.target_key_vault
    copy_secrets_between_key_vaults(source_key_vault, target_key_vault)
