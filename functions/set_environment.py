"""
Module to set the current environment.
Change CURRENT_ENVIRONMENT to the environment you want to deploy to.
Accepted values are:
    dev
    preprod
    prod

This is used to make manual deployments easier so switching between
environments is done in one place.

Functions:
- load_config: Loads a configuration file and returns the parsed configuration.
- get_environment_config: Retrieves the configuration for a specific environment from the parsed configuration.
"""

import yaml

CURRENT_ENVIRONMENT = "dev"
ENV_CONFIG_FILE = "config.yaml"


def load_config(file_path: str) -> dict:
    """
    Loads a configuration file and returns the parsed configuration.

    Args:
        file_path (str): The path to the configuration file.

    Returns:
        dict: The parsed configuration.
    """

    with open(file_path, "r") as file:
        config = yaml.safe_load(file)
    return config


def get_environment_config(config: dict, environment: str) -> dict:
    """
    Retrieves the configuration for a specific environment from the parsed configuration.

    Args:
        config (dict): The parsed configuration.
        environment (str): The name of the environment.

    Returns:
        dict: The configuration for the specified environment.

    Raises:
        ValueError: If the specified environment is not found in the configuration file.

    """

    if environment in config:
        return config[environment]
    else:
        raise ValueError(
            f"Environment '{environment}' not found in the configuration file."
        )


config = load_config(ENV_CONFIG_FILE)

current_config = get_environment_config(config, CURRENT_ENVIRONMENT)
