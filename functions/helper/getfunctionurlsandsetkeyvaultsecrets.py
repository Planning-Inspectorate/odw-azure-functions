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
#Teams_Webhook = "https://pinso365.webhook.office.com/webhookb2/c7088ab3-5530-4007-b222-3044f604a848@5878df98-6f88-48ab-9322-998ce557088d/IncomingWebhook/0ef53e5faadc4e8d8fd9508d324d1f48/e7b7b154-3e75-46ae-8332-a534cb30d38e"

# Pre-Prod
subscription_id = "6b18ba9d-2399-48b5-a834-e0f267be122d"
resource_group_name = "pins-rg-function-app-odw-test-uks"
DB_resource_group_name = "pins-rg-data-odw-test-uks"
function_app_name = "pins-fnapp01-odw-test-uks"
keyvault_name = "pinskvsynwodwtestuks"
vault_uri = "https://pinskvsynwodwtestuks.vault.azure.net/"
Teams_Webhook = "https://pinso365.webhook.office.com/webhookb2/c7088ab3-5530-4007-b222-3044f604a848@5878df98-6f88-48ab-9322-998ce557088d/IncomingWebhook/418ef22183d243c2b6466f8aeb024000/e7b7b154-3e75-46ae-8332-a534cb30d38e"

# Prod
# subscription_id = "a82fd28d-5989-4e06-a0bb-1a5d859f9e0c"
# resource_group_name = "pins-rg-function-app-odw-prod-uks"
# DB_resource_group_name = "pins-rg-data-odw-prod-uks"
# function_app_name = "pins-fnapp01-odw-prod-uks"
# keyvault_name = "pinskvsynwodwproduks"
# vault_uri = "https://pinskvsynwodwproduks.vault.azure.net/"
# Teams_Webhook = "https://pinso365.webhook.office.com/webhookb2/c7088ab3-5530-4007-b222-3044f604a848@5878df98-6f88-48ab-9322-998ce557088d/IncomingWebhook/40955bbd63f14bfebc7049080e52d00d/e7b7b154-3e75-46ae-8332-a534cb30d38e"

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
    set_secret("TeamsWebhook", Teams_Webhook)
    set_secret("SubscriptionId", subscription_id)
    set_secret("DBResourceGroup", DB_resource_group_name)
    listfunctionurls()
    setkeyvaultsecrets()

if __name__ == "__main__":
    main()