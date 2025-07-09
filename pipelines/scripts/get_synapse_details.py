import subprocess
import json
import argparse
from typing import List, Dict, Any, Union


class NameFactory():
    """
        Class for handling switching the names of resources depending on if the deployment
        is for disaster recovery
    """
    @classmethod
    def get(cls, env: str, failover_deployment: bool = False):
        base_names = {
            "keyvault_resource_group": f"pins-rg-data-odw-{env}-uks"
        }
        if failover_deployment:
            return {
                **base_names,
                **{
                    "data_lake_resource_group": f"pins-rg-data-odw-{env}-ukw",
                    "data_lake_resource_group_backup": f"pins-rg-data-odw-{env}-uks",
                    "data_lake_name": f"pinsstodw{env}ukw",
                    "data_lake_name_backup": f"pinsstodw{env}uks",
                    "service_bus_resource_group": f"pins-rg-ingestion-odw-{env}-ukw",
                    "devops_agent_pool_resource_group_name": f"pins-rg-devops-odw-{env}-ukw"
                }
            }
        return {
            **base_names,
            **{
                "data_lake_resource_group": f"pins-rg-data-odw-{env}-uks",
                "data_lake_resource_group_backup": f"pins-rg-data-odw-{env}-ukw",
                "data_lake_name": f"pinsstodw{env}uks",
                "data_lake_name_backup": f"pinsstodw{env}ukw",
                "service_bus_resource_group": f"pins-rg-ingestion-odw-{env}-uks",
                "devops_agent_pool_resource_group_name": f"pins-rg-devops-odw-{env}-uks"
            }
        }


def run_az_cli_command(args: List[str]):
    """
        Run an az cli command. Raises a `RuntimeException` if something goes wrong
    """
    print(f"Running command: '{' '.join(args)}'")
    try:
        return subprocess.check_output(args)
    except subprocess.CalledProcessError as e:
        exception = e
    raise RuntimeError(f"Exception raised when running the above command: {exception.output.decode('utf-8')}")


def get_storage_accounts(resource_group_name: str) -> List[Dict[str, Any]]:
    """
        Return all storage accounts under the given resource group
    """
    return json.loads(run_az_cli_command(["az", "storage", "account", "list", "--resource-group", resource_group_name]))


def get_key_vaults(resource_group_name: str) -> List[Dict[str, Any]]:
    """
        Return all key vaults under the given resource group
    """
    return json.loads(run_az_cli_command(["az", "keyvault", "list", "--resource-group", resource_group_name]))


def get_services_buses(resource_group_name: str) -> List[Dict[str, Any]]:
    """
        Return all service buses under the given resource group
    """
    return json.loads(run_az_cli_command(["az", "servicebus", "namespace", "list", "--resource-group", resource_group_name]))


def get_synapse_workspace(resource_group_name: str) -> List[Dict[str, Any]]:
    """
        Return all synapse workspaces under the given resource group
    """
    return json.loads(run_az_cli_command(["az", "synapse", "workspace", "list", "--resource-group", resource_group_name]))


def get_service_bus_connection_string(resource_group_name: str, service_bus_name: str) -> str:
    """
        Return the primary connection string of the given service bus under the specified resource group
    """
    args = [
        "--resource-group",
        resource_group_name,
        "--namespace-name",
        service_bus_name,
        "-n",
        "RootManageSharedAccessKey",
        "--query",
        "primaryConnectionString",
        "-o",
        "tsv"
    ]
    command_to_run = ["az", "servicebus", "namespace", "authorization-rule", "keys", "list"] + args

    return run_az_cli_command(command_to_run).decode("ascii")


def get_resource(resource_type: str, resource_group_name: str, resource_name_prefix: str, optional: bool = False) -> Union[Dict[str, Any], None]:
    """
        Extracts a single resource of specified resource_type under a particular resource group, by selecting the resource that
        matches the given prefix. If more than one resource is identified with the prefix (or if none are found), then raises an exception.
        If the resource is optional, then if it is not found then None is returned
    """
    resource_type_function_map = {
        "Blob Storage": get_storage_accounts,
        "Key Vault": get_key_vaults,
        "Service Bus": get_services_buses,
        "Synapse Workspace": get_synapse_workspace
    }
    if resource_type not in resource_type_function_map:
        raise ValueError(f"No resource getter defined for resource_type {resource_type}")
    all_resources = resource_type_function_map[resource_type](resource_group_name)
    candidate_resources = [x for x in all_resources if x["name"].startswith(resource_name_prefix)]
    if len(candidate_resources) != 1:
        if optional and not candidate_resources:
            return None
        raise ValueError(
            f"Expected only a single {resource_type} resource to match the prefix {resource_name_prefix}, but there were {len(candidate_resources)}"
        )
    return candidate_resources[0]


if __name__ == "__main__":
    # Load command line arguments
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-e", "--env", help="Environment to run against")
    parser.add_argument("-f", "--failover_deployment", help="If this is a backup deployment", default=False)
    args = parser.parse_args()
    env = args.env
    failover_deployment = str(args.failover_deployment).lower() == "true"

    # Set resource group during processing based on Terraform logic
    names = NameFactory.get(env, failover_deployment)
    data_lake_resource_group = names["data_lake_resource_group"]
    data_lake_resource_group_backup = names["data_lake_resource_group_backup"]
    data_lake_name = names["data_lake_name"]
    data_lake_name_backup = names["data_lake_name_backup"]
    keyvault_resource_group = names["keyvault_resource_group"]
    service_bus_resource_group = names["service_bus_resource_group"]
    devops_agent_pool_resource_group_name = names["devops_agent_pool_resource_group_name"]

    # Extract relevant Azure resources
    main_datalake = get_resource("Blob Storage", data_lake_resource_group, data_lake_name)
    backup_datalake = get_resource("Blob Storage", data_lake_resource_group_backup, data_lake_name_backup, optional=True)
    key_vault = get_resource("Key Vault", keyvault_resource_group, f"pinskvsynwodw{env}uks")
    service_bus = get_resource("Service Bus", service_bus_resource_group, f"pins-sb-odw-{env}")
    synapse_workspace = get_resource("Synapse Workspace", data_lake_resource_group, f"pins-synw-odw-{env}")
    service_bus_primary_connection_string = get_service_bus_connection_string(service_bus_resource_group, service_bus["name"])
    
    # Save variables to Azure Pipeline
    variables = {
        "data_lake_account_id": main_datalake["id"],
        "data_lake_account_id_failover": backup_datalake["id"],
        "data_lake_account_name": main_datalake["name"],
        "data_lake_dfs_endpoint": main_datalake["primaryEndpoints"]["dfs"],
        "data_lake_dfs_endpoint_failover": backup_datalake["primaryEndpoints"]["dfs"],
        "data_resource_group_name": f"pins-rg-data-odw-{env}-uks",
        "devops_agent_pool_resource_group_name": devops_agent_pool_resource_group_name,
        "key_vault_uri": key_vault["properties"]["vaultUri"],
        "service_bus_namespace_name": service_bus["name"],
        "service_bus_primary_connection_string": service_bus_primary_connection_string,
        "synapse_dev_endpoint": synapse_workspace["connectivityEndpoints"]["dev"],
        "synapse_dsql_endpoint": synapse_workspace["connectivityEndpoints"]["sql"],
        "synapse_ssql_endpoint": synapse_workspace["connectivityEndpoints"]["sqlOnDemand"],
        "synapse_workspace_id": synapse_workspace["id"],
        "synapse_workspace_name": synapse_workspace["name"]
    }
    print("Setting the following variables")
    print(json.dumps(variables, indent=4))
    for variable_name, variable_value in variables.items():
        print(f"Saving variable '{variable_name}'")
        print(f'##vso[task.setvariable variable={variable_name};]{variable_value}')
