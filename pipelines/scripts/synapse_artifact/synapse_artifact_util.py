from azure.identity import AzureCliCredential, ChainedTokenCredential, ManagedIdentityCredential
import requests
from abc import ABC, abstractmethod
from typing import Union, List, Dict, Any, Set
from dataclasses import dataclass
import re
import logging
import json
import os
import re

logging.basicConfig(level=logging.INFO)


class AttributeNotFoundException(Exception):
    pass


@dataclass
class SynapseArtifactsPropertyIteratorResult():
    parent_collection: Union[Dict[str, Any], List[Any]]
    attribute: str
    value: Union[Dict[str, Any], List[Any], Any]
    remaining_attribute: str


class SynapseArtifactsPropertyIterator():
    """
        Class to iterate through the properties of a json object through dot notation
    """
    def __init__(self, dictionary: Dict[str, Any], attribute: str):
        self.parent_attribute_collection: Union[Dict[str, Any], List[Any]] = None
        self.attribute_collection: Union[Dict[str, Any], List[Any]] = dictionary
        self.attribute_split = [x for x in attribute.split(".") if x]
        if not self.attribute_split:
            raise ValueError(f"There is no attribute to evaluate")
        self.last_evaluated_attribute = None

    def __iter__(self):
        return self

    def __next__(self):
        """
            :return: The most recently-accessed collection
            :return: The most recently-accessed property
            :return: The value associated with the most recent property in the most recent collection
            :return: The remaining attribute string

        """
        if not self.attribute_split:
            #print()
            raise StopIteration
        # Imagine everything below is wrapped in a while loop as long as self.attribute_split has value
        self.last_evaluated_attribute = self.attribute_split.pop(0)
        if isinstance(self.attribute_collection, list):
            if not self.last_evaluated_attribute.isdigit():
                raise AttributeNotFoundException(f"Trying to access sub property '{self.last_evaluated_attribute}' on a list collection")
            self.last_evaluated_attribute = int(self.last_evaluated_attribute)
            if not (0 <= self.last_evaluated_attribute < len(self.attribute_collection)):
                raise AttributeNotFoundException(f"List index out of range for subproperty '{self.last_evaluated_attribute}' in list collection")
            self.parent_attribute_collection = self.attribute_collection
            self.attribute_collection = self.attribute_collection[self.last_evaluated_attribute]
        elif isinstance(self.attribute_collection, dict):
            if self.last_evaluated_attribute not in self.attribute_collection:
                if not self.attribute_split:
                    raise AttributeNotFoundException(
                        f"Sub attribute '{self.last_evaluated_attribute}' not in dictionary collection {self.attribute_collection}"
                    )
                # Special case where the key contains a period
                while self.attribute_split:
                    next_attribute = self.attribute_split.pop(0)
                    self.last_evaluated_attribute += f".{next_attribute}"
                    if self.last_evaluated_attribute in self.attribute_collection:
                        self.parent_attribute_collection = self.attribute_collection
                        self.attribute_collection = self.attribute_collection[self.last_evaluated_attribute]
            else:
                self.parent_attribute_collection = self.attribute_collection
                self.attribute_collection = self.attribute_collection[self.last_evaluated_attribute]
        else:
            if len(self.attribute_split) > 0:
                raise ValueError(
                    f"Trying to access a leaf property of a collection, but the remaining sub properties still need to be expanded: {self.attribute_split}"
                )
        #print(f"Last evaluated: '{self.last_evaluated_attribute}'")
        return SynapseArtifactsPropertyIteratorResult(
            self.parent_attribute_collection,
            self.last_evaluated_attribute,
            self.attribute_collection,
            ".".join(self.attribute_split)
        )


class SynapseArtifactUtil(ABC):
    """
        Abstract class for managing the retrieval and analysis of Synapse artifacts
    """
    # By default leave these unset, so that non-azure aspects of the code do not require
    # az cli to be set up
    credential = None
    _token = None

    def __init__(self, workspace_name: str):
        """
            :param workspace_name: The name of the Synapse workspace
        """
        self.workspace_name = workspace_name
        self.synapse_endpoint = f"https://{self.workspace_name}.dev.azuresynapse.net"

    @classmethod
    def _get_token(cls) -> str:
        if not (cls.credential and cls._token):
            cls.credential = ChainedTokenCredential(
                # ManagedIdentityCredential(),
                AzureCliCredential()
            )
            cls._token = cls.credential.get_token("https://dev.azuresynapse.net").token
        return cls._token

    @classmethod
    @abstractmethod
    def get_type_name(cls) -> str:
        pass

    def _web_request(self, endpoint: str) -> requests.Response:
        """
            Submit a http request against the specified endpoint

            :param endpooint: The url to send the request to
            :return: The http response
        """
        api_call_headers = {'Authorization': 'Bearer ' + self._get_token()}
        return requests.get(endpoint, headers=api_call_headers)

    @abstractmethod
    def get(self, artifact_name: str, **kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """
            Return the attributes for the given artifact

            :param artifact_name: The name of the artifact to fetch from the Synapse REST API
            :param kwargs: Any additional arguments that are required
            :return: The json object retrieved by the Synapse REST API
        """
        pass

    @abstractmethod
    def get_all(self, **kwargs: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
            Return all artifacts

            :param kwargs: Any additional arguments that are required
            :return: A list of json objects retrieved by the Synapse REST API
        """
        pass

    def download_live_workspace(self, local_folder: str):
        """
            Download all artifacts to a local folder
            
            :param local_folder: Where to store
        """
        artifacts = self.get_all()
        logging.info(f"Downloaded {len(artifacts)} artifacts of type '{self.get_type_name()}'")
        base_folder = f"{local_folder}/{self.get_type_name()}"
        os.makedirs(base_folder)
        for artifact in artifacts:
            artifact_name = artifact["name"]
            with open(f"{base_folder}/{artifact_name}.json", "w") as f:
                json.dump(artifact, f, indent=4)

    @abstractmethod
    def get_uncomparable_attributes(self) -> List[str]:
        """
            :return: Regex patterns of attributes that should not be compared for this kind of artifact
        """
        pass

    @abstractmethod
    def get_nullable_attributes(self) -> List[str]:
        """
            :return: Regex patterns of attributes that should be dropped if null for this kind of artifact
        """
        pass

    @abstractmethod
    def get_env_attributes_to_replace(self) -> List[str]:
        """
            :return: Attributes for specific environments that should be replaced
        """
        pass

    def compare(self, artifact_a: Dict[str, Any], artifact_b: Dict[str, Any]) -> bool:
        """
            Compare two artifacts.

            :param artifact_a: The json for the first artifact to compare
            :param artifact_b: The json for the second artifact to compare
            :return: True if the artifacts match, False otherwise
        """
        uncomparable_attributes = self.get_uncomparable_attributes()
        nullable_attributes = self.get_nullable_attributes()
        artifact_a_attributes = self._extract_dict_attributes(artifact_a)
        artifact_b_attributes = self._extract_dict_attributes(artifact_b)
        artifact_a_attributes_cleaned = self.clean_attributes(
            artifact_a_attributes,
            artifact_a,
            uncomparable_attributes,
            nullable_attributes
        )
        artifact_b_attributes_cleaned = self.clean_attributes(
            artifact_b_attributes,
            artifact_b,
            uncomparable_attributes,
            nullable_attributes
        )
        if artifact_a_attributes_cleaned != artifact_b_attributes_cleaned:
            '''
            # This block should be uncommented to help identify any extra regex patterns that could be added to the util classes
            old = [x for x in artifact_a_attributes_cleaned if x not in artifact_b_attributes_cleaned]
            new = [x for x in artifact_b_attributes_cleaned if x not in artifact_a_attributes_cleaned]
            if new:
                print("name: ", artifact_a["name"])
                print(json.dumps(list(old), indent=4))
                print(json.dumps(list(new), indent=4))
                raise Exception("Stopping code execution so that the difference can be analysed")
            '''
            extra_in_left = artifact_a_attributes_cleaned - artifact_b_attributes_cleaned
            extra_in_right = artifact_b_attributes_cleaned - artifact_a_attributes_cleaned
            logging.info("Left arfifact had the following extra mandatory/filled nullable attributes")
            logging.info(json.dumps(list(extra_in_left), indent=4))
            logging.info("Right artifact had the following extra mandatory/filled nullable attributes")
            logging.info(json.dumps(list(extra_in_right), indent=4))
            return False
        return self._compare_dictionaries_by_attributes(artifact_a_attributes_cleaned, artifact_a, artifact_b)

    def is_attribute_value_empty(self, attribute_value: Union[Dict[str, Any], List[Any], Any]) -> bool:
        """
            Return if the given attribute_value is empty, return false otherwise.

            :param attribute_value: The attribute_value to check
            :return: True if the bool(attribute_value) evaluates to False or if all of its children are empty. False otherwise
        """
        if isinstance(attribute_value, dict):
            if not attribute_value:
                return True
            for value in attribute_value.values():
                if not self.is_attribute_value_empty(value):
                    return False
            return True
        if isinstance(attribute_value, list):
            if not attribute_value:
                return True
            for elem in attribute_value:
                if elem:
                    return False
            return True
        return not bool(attribute_value)

    @classmethod
    def get_all_attributes(cls, artifact_json: Dict[str, Any]) -> Set[str]:
        """
            Return all attributes from the given json in dot notation

            :param artifact_json: The json object to get attributes for
            :return: The attributes of the json object, in dot-notation. e.g: some.attribute.of.the.json.object

            e.g
            ```
            my_json = {
                "some": 1,
                "attributes": 2,
                "list_type": [
                    "a"
                ],
                "dict_type": {
                    "b": 3,
                    "c": 4
                }
            }
            # Then
            get_all_attributes(my_json)
            # Will return
            {
                "some",
                "attributes",
                "list_type.0",
                "dict_type.b",
                "dict_type.c"
            }
            ```
        """
        return set(cls._extract_dict_attributes(artifact_json, current_level="").keys())

    @classmethod
    def _extract_list_attributes(cls, target_list: List[Any], current_level: str) -> Dict[str, None]:
        """
            Extract attribute names from the given list in dot-notation

            :param target_list: The list to extract attributes from
            :param current_level: String used to prefix any new attributes with. Used to identify the depth of the analysis so far
            :return: Extracted attributes as a dict of the form <attribute: None>. (A dict is used to preserve the order of the attributes)

            e.g.
            ```
            my_list = ["a", {"list": 1, "of": 2, "attributes": 3}]
            _extract_list_attributes(my_list, "some_prefix")
            # Returns
            {
                "some_prefix.0": None,
                "some_prefix.1.list": None,
                "some_prefix.1.of": None,
                "some_prefix.1.attributes": None
            }
            ```
        """
        current_level_prefix = f"{current_level}." if current_level else ""
        dict_keys = dict()
        for i, val in enumerate(target_list):
            new_level = f"{current_level_prefix}{i}"
            if isinstance(val, dict):
                dict_keys = dict(dict_keys, **cls._extract_dict_attributes(val, new_level))
            elif isinstance(val, list):
                dict_keys = dict(dict_keys, **cls._extract_list_attributes(val, new_level))
            else:
                dict_keys[new_level] = None
        return dict_keys

    @classmethod
    def _extract_dict_attributes(cls, target_dict: Dict[str, Any], current_level: str = "") -> Dict[str, None]:
        """
            Extract attribute names from the given dictionary

            :param target_dict: The dictionary to extract attributes from
            :param current_level: String used to prefix any new attributes with. Used to identify the depth of the analysis so far
            :return: Extracted attributes as a dict of the form <attribute: None>. (A dict is used to preserve the order of the attributes)

            e.g.
            ```
            my_dict = {"group": 1, "of": 2, "attributes": 3, "a_list": {"a", "b", "c}, "nested": {"a": 1, "b"; 2}}
            _extract_list_attributes(my_dict, "some_prefix")
            # Returns
            {
                "some_prefix": None,
                "some_prefix.group": None,
                "some_prefix.of": None,
                "some_prefix.attributes": None,
                "some_prefix.a_list.0": None,
                "some_prefix.a_list.1": None,
                "some_prefix.a_list.2": None,
                "some_prefix.nested.a": None,
                "some_prefix.nested.b": None
            }
            ```
        """
        current_level_prefix = f"{current_level}." if current_level else ""
        dict_keys = dict()
        for key, val in target_dict.items():
            new_level = f"{current_level_prefix}{key}"
            if isinstance(val, dict):
                dict_keys = dict(dict_keys, **cls._extract_dict_attributes(val, new_level))
            elif isinstance(val, list):
                dict_keys = dict(dict_keys, **cls._extract_list_attributes(val, new_level))
            else:
                dict_keys[new_level] = None
        return dict_keys
    
    def clean_attributes(
        self,
        attributes: Set[str],
        artifact_json: Dict[str, Any],
        uncomparable_attributes: Set[str],
        nullable_attributes: Set[str]
    ) -> Set[str]:
        """
            Return the attributes that can be compared, and are nullable but with non-null values

            :param artifact_json: The artifact the attributes belong to
            :param attributes: The attributes to clean, in dot-notation
            :param uncomparable_attributes: The attributes that should be ignored and removed from the output
            :param nullable_attributes: The attributes that should be removed only if their value is null in the artifact_json
            :return: All attributes with concrete values that can be compared against another attribute set
        """
        return {
            attribute
            for attribute in attributes
            if (
                (not any(re.match(pattern, attribute) for pattern in uncomparable_attributes)) and
                (
                    (not any(re.match(pattern, attribute) for pattern in nullable_attributes)) or
                    (
                        any(re.match(pattern, attribute) for pattern in nullable_attributes) and
                        not self.is_attribute_value_empty(self.get_by_attribute(artifact_json, attribute))
                    )
                )
            )
        }

    @classmethod
    def get_by_attribute(cls, dictionary: Dict[str, Any], attribute: str) -> Any:
        """
            Access json attributes by a dot-notation string.
            e.g `get_by_attribute({"a": {"b": 2}}, "a.b") -> 2`

            :param dictionary: The json artifact to extract the attribute from
            :param attribute: The attribute to extract, in dot-notation
            :return: The attribute value
            :raises: An exception is raised if the given inputs are invalid
        """
        property_details = [x for x in SynapseArtifactsPropertyIterator(dictionary, attribute)]
        return property_details.pop().value

    @classmethod
    def set_by_attribute(cls, dictionary: Dict[str, Any], attribute: str, new_value: Any):
        """
            Set a json attribute by a dot-notation string.
            e.g `set_by_attribute({"a": {"b": 2}}, "a.b", 5) -> {"a": {"b": 5}}

            :param dictionary: The json artifact to extract the attribute from
            :param attribute: The attribute to extract, in dot-notation
            :return: The same dictionary with the updated value
        """
        property_details = [x for x in SynapseArtifactsPropertyIterator(dictionary, attribute)]
        last_entry = property_details.pop()
        last_entry.parent_collection[last_entry.attribute] = new_value
        return dictionary

    def _compare_dictionaries_by_attributes(self, attributes: Set[str], dict_a: Dict[str, Any], dict_b: Dict[str, Any]) -> Dict[str, bool]:
        """
            Compare each attribute between the two dictionaries

            :param attributes: The attributes to compare, in dot-notation
            :param dict_a: First dictionary to compare
            :param dict_b: Second dictionary to compare
            :return: True if the dictionaries match, False otherwise.
        """
        for attribute in attributes:
            val_a = self.get_by_attribute(dict_a, attribute)
            val_b = self.get_by_attribute(dict_b, attribute)
            if val_a != val_b:
                logging.info(
                    (
                        f"Mismatch between artifacts for attribute '{attribute}'. {json.dumps(val_a, indent=4)} != {json.dumps(val_b, indent=4)}"
                        ". Ending comparison of artifacts"
                    )
                )
                return False
        return True

    def replace_env_strings(self, artifact: Dict[str, Any], base_env: str, new_env: str) -> Dict[str, Any]:
        """
            Immutably replace any hardcoded environment strings that get replaced during Synapse deployment.
            These strings all point to the `dev` environment by default

            :param artifact: The artifact to clean
            :param base_env: The default environment the artifacts are developed in (i.e. dev)
            :param new_env: The environment being deployed to (to replace the base_env with)
            :return: The cleaned artifact
        """
        regex_pattern = fr"{base_env}(?!\.)"
        for property_to_replace in self.get_env_attributes_to_replace():
            property_value = None
            try:
                property_value = self.get_by_attribute(artifact, property_to_replace)
            except AttributeNotFoundException:
                logging.info(f"Couldn't find property by dot-notation string '{property_to_replace}'")
            if property_value:
                cleaned_property_value = None
                if isinstance(property_value, str):
                    cleaned_property_value = re.sub(regex_pattern, new_env, property_value)
                elif isinstance(property_value, list):
                    cleaned_property_value = [
                        re.sub(regex_pattern, new_env, x) if isinstance(x, str) else x
                        for x in property_value
                    ]
                if cleaned_property_value:
                    self.set_by_attribute(artifact, property_to_replace, cleaned_property_value)
        return artifact

    @classmethod
    def dependent_artifacts(cls, artifact: Dict[str, Any]) -> Set[str]:
        """
            Return all dependent artifacts for the given artifact json

            :param artifact: The artifact to analyse
            :return: Set of paths to synapse artifacts under the `workspace` folder
        """
        reference_map = {
            "LinkedServiceReference": "linkedService",
            "DatasetReference": "dataset",
            "NotebookReference": "notebook",
            "PipelineReference": "pipeline",
            "ManagedVirtualNetworkReference": "managedVirtualNetwork",
            "IntegrationRuntimeReference": "integrationRuntime",
            "SparkConfigurationReference": "sparkConfiguration"
        }
        reference_types_to_ignore = {
            "BigDataPoolReference"
        }
        attributes = cls._extract_dict_attributes(artifact).keys()
        reference_attributes = {
            attribute: f"{'.'.join(attribute.split('.')[:-1])}.type"
            for attribute in attributes
            if attribute.endswith("referenceName")
        }
        reference_type_attributes = {k: v for k, v in reference_attributes.items() if v in attributes}
        reference_type_attributes = {
            k: v
            for k, v in reference_type_attributes.items()
            if not isinstance(cls.get_by_attribute(artifact, k), dict)
        }
        reference_values = {
            k: cls.get_by_attribute(artifact, k)
            for k in reference_type_attributes.keys()
        }
        reference_type_values = {
            v: cls.get_by_attribute(artifact, v)
            for v in reference_type_attributes.values()
        }
        return {
            f"{reference_map[reference_type_values[attribute_type]]}/{reference_values[attribute]}.json"
            for attribute, attribute_type in reference_attributes.items()
            if reference_type_values[attribute_type] not in reference_types_to_ignore
        }
    
    @classmethod
    def can_be_archived(cls) -> bool:
        """
            Return True if the artifact kind can be archived, false otherwise. An artifact can be archived if it has a 'folder' property
        """
        return False
    
    @classmethod
    def archive(cls, artifact: Dict[str, Any]) -> Dict[str, Any]:
        """
            Archive the given artifact. Must be explicitly implemented for each individual base artifact util class. This prepends
            the current folder property with 'archive/'
        """
        return artifact
    
    @classmethod
    def is_archived(cls, artifact: Dict[str, Any]) -> bool:
        """
            Return True if the artifact has already been archived. If a 'folder' property exists and already starts with 'archive/' then return True,
            False otherwise
        """
        existing_folder = artifact["properties"].get("folder", dict())
        if not existing_folder:
            return False
        return existing_folder.get("name", "").startswith("archive")
