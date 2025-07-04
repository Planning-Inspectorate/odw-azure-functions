import subprocess
from typing import List, Dict, Any
import json
import os
import logging


class Util:
    """
        Class that has useful utility functions
    """
    @classmethod
    def run_az_cli_command(cls, args: List[str]):
        """
            Run an az cli command. Raises a `RuntimeException` if something goes wrong
        """
        logging.info(f"Running command {' '.join(args)}")
        try:
            return subprocess.check_output(args)
        except subprocess.CalledProcessError as e:
            exception = e
        raise RuntimeError(f"Exception raised when running the above command: {exception.output.decode('utf-8')}")

    @classmethod
    def get_current_subscription_details(cls) -> Dict[str, Any]:
        """
            Return the output of `az account show`
        """
        cmd = "az account show"
        exception = None
        try:
            return json.loads(os.popen(cmd).read())
        except json.JSONDecodeError as e:
            exception = e
        raise RuntimeError(f"Failed to decode output from `{cmd}` - please check you are signed in. Raised exception: {exception}")

    @classmethod
    def get_subscription(cls) -> str:
        return cls.get_current_subscription_details()["name"]

    @classmethod
    def set_subscription(cls, subscription_name: str):
        cmd = f'az account set --subscription {subscription_name}'
        logging.info(f"Switching subscription to '{subscription_name}' via command '{cmd}'")
        os.popen(cmd).read()
        logging.info(f"Current subscription is '{cls.get_subscription()}'")

    @classmethod
    def get_current_user(cls) -> str:
        """
            Return the name of the current user
        """
        return cls.get_current_subscription_details()["user"]["name"]

    @classmethod
    def get_subscription_id(cls, subscription_name: str) -> str:
        """
            Return the id of the current subscription
        """
        subscriptions = json.loads(cls.run_az_cli_command(["az", "account", "subscription", "list", "--output", "json"]))
        for subscription in subscriptions:
            if subscription["displayName"] == subscription_name:
                return subscription["id"].replace("/subscriptions/", "")
        raise ValueError(f"Could not find a subscription with name '{subscription_name}'")

    @classmethod
    def get_odw_storage_accounts(cls, env: str) -> Dict[str, Any]:
        """
            Return the details of all ODW storage accounts
        """
        resource_group = f"pins-rg-data-odw-{env}-uks"
        command = f"az storage account list -g {resource_group}"
        return json.loads(cls.run_az_cli_command(command.split(" ")))

    @classmethod
    def get_odw_storage_account_names(cls, env: str) -> List[str]:
        """
            Return the names of all ODW storage accounts
        """
        return [
            storage_account["name"]
            for storage_account in cls.get_odw_storage_accounts(env)
        ]
    
    @classmethod
    def get_all_artifact_paths(cls, artifact_paths: List[str]) -> List[str]:
        """
            Return the paths to all artifacts under the given paths

            :param artifact_paths: Local artifact folder to include. e.g: `workspace/notebook`
        """
        return [
            os.path.join(path, name)
            for path, subdirs, files in os.walk("workspace")
            for name in files
            if any(
                os.path.join(path, name).startswith(x)
                for x in artifact_paths
            ) and name.endswith(".json")
        ]
    
    @classmethod
    def _convert_to_json(cls, obj: object) -> Dict[str, Any]:
        '''
        if isinstance(obj, list):
            return [
                cls._convert_to_json(elem)
                for elem in obj
            ]
        if isinstance(obj, dict):
            return {
                k: cls._convert_to_json(v)
                for k, v in obj.items()
            }
        if getattr(obj, "__dict__", None):
            return {
                k: cls._convert_to_json(v)
                for k, v in obj.__dict__.items()
            }
        return str()
        '''
        return json.loads(
            json.dumps(obj, default=lambda o: getattr(o, "__dict__", str(o)))
        )
