from azure.identity import DefaultAzureCredential
from azure.mgmt.web import WebSiteManagementClient
from azure.keyvault.secrets import SecretClient


# Dev
#subscription_id = "ff442a29-fc06-4a13-8e3e-65fd5da513b3"
#resource_group_name = "pins-rg-function-app-odw-dev-uks"
#DB_resource_group_name = "pins-rg-data-odw-dev-uks"
#function_app_name = "pins-fnapp01-odw-dev-uks"
#keyvault_name = "pinskvsynwodwdevuks"
#vault_uri = "https://pinskvsynwodwdevuks.vault.azure.net/"

# Pre-Prod
subscription_id = "6b18ba9d-2399-48b5-a834-e0f267be122d"
resource_group_name = "pins-rg-function-app-odw-test-uks"
DB_resource_group_name = "pins-rg-data-odw-test-uks"
function_app_name = "pins-fnapp01-odw-test-uks"
keyvault_name = "pinskvsynwodwtestuks"
vault_uri = "https://pinskvsynwodwtestuks.vault.azure.net/"

# Prod
# subscription_id = "a82fd28d-5989-4e06-a0bb-1a5d859f9e0c"
# resource_group_name = "pins-rg-function-app-odw-prod-uks"
# DB_resource_group_name = "pins-rg-data-odw-prod-uks"
# function_app_name = "pins-fnapp01-odw-prod-uks"
# keyvault_name = "pinskvsynwodwproduks"
# vault_uri = "https://pinskvsynwodwproduks.vault.azure.net/"

# Build
#subscription_id = "12806449-ae7c-4754-b104-65bcdc7b28c8"
#resource_group_name = "pins-rg-function-app-odw-build-uks"
#DB_resource_group_name = "pins-rg-data-odw-build-uks"
#function_app_name = "pins-fnapp01-odw-build-uks"
#keyvault_name = "pinskvsynwodwbuilduks"
#vault_uri = "https://pinskvsynwodwbuilduks.vault.azure.net/"

# Authenticate using DefaultAzureCredential
credential = DefaultAzureCredential()

# Create the WebSiteManagementClient
web_client = WebSiteManagementClient(credential, subscription_id)

# Create a keyvault secret client
secret_client = SecretClient(vault_url=vault_uri, credential=credential)


def listfunctions(resource_group_name: str, function_app_name: str) -> list:
    functions_list = []
    functions = web_client.web_apps.list_functions(
        resource_group_name, function_app_name
    )
    for function in functions:
        functions_list.append(function.name)
    return functions_list


def getfunctionkey(
    resource_group_name: str, function_app_name: str, function_name: str
) -> str:
    function_key = web_client.web_apps.list_function_keys(
        resource_group_name, function_app_name, function_name
    )
    return function_key


def getfunctionurl(
    function_app_name: str, function_name: str, function_key: str
) -> str:
    keys_dict = eval(str(function_key).replace("'", '"'))
    code = keys_dict["additional_properties"]["default"]
    function_url = (
        f"https://{function_app_name}.azurewebsites.net/api/{function_name}?code={code}"
    )
    return function_url


def set_secret(secret_name: str, secret_value: str) -> None:
    secret_client.set_secret(secret_name, secret_value)
    return print(f"{secret_name} created")


def listfunctionurls() -> None:
    function_list = listfunctions(resource_group_name, function_app_name)
    for function in function_list:
        name = function.split("/")[1]
        function_key = getfunctionkey(resource_group_name, function_app_name, name)
        function_url = getfunctionurl(function_app_name, name, function_key)
        print(function_url)


def setkeyvaultsecrets() -> None:
    function_list = listfunctions(resource_group_name, function_app_name)
    for function in function_list:
        name = function.split("/")[1]
        function_key = getfunctionkey(resource_group_name, function_app_name, name)
        function_url = getfunctionurl(function_app_name, name, function_key)
        secret_name = f"function-url-{name}"
        secret_value = function_url
        set_secret(secret_name, secret_value)
    print("All secrets added to KeyVault")


# select the function you want to call

def main() -> None:
    set_secret("SubscriptionId", subscription_id)
    set_secret("DBResourceGroup", DB_resource_group_name)
    listfunctionurls()
    setkeyvaultsecrets()

if __name__ == "__main__":
    main()