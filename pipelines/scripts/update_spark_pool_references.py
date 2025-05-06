from pipelines.scripts.synapse_artifact.synapse_spark_pool_util import SynapseSparkPoolUtil
from typing import Dict, List, Any
import argparse
import json
import os
from copy import deepcopy
import logging


class SparkPoolReferenceUpdater():
    """
        Class for managing updating references to spark pools
    """
    def __init__(self, synapse_workspace_name: str, old_pool_name: str, new_pool_name: str):
        """
            Class for managing updating references to spark pools

            :param synapse_workspace_name: The name of the synapse workspace to target
            :param old_pool_name: The old spark pool we should replace reference to
            :param new_pool_name: The spark pool we should use to replace references
        """
        self._synapse_workspace_name = synapse_workspace_name
        self._spark_pool_cache = dict()
        self._old_pool_name = old_pool_name
        self._new_pool_name = new_pool_name
    
    def update_all_spark_pool_references(self):
        """
            Update all references to the old spark pool with the new spark pool
        """
        logging.info("Starting to replace spark pool references")
        artifact_names = self.get_all_relevant_artifact_names()
        logging.info("Performing replacement on the below files")
        logging.info(json.dumps(artifact_names, indent=4))
        logging.info("\nBegin replacement")
        cleaned_artifacts = {
            artifact_name: self.replace_spark_pool_references_in_artifact(artifact_name)
            for artifact_name in artifact_names
        }
        logging.info("\nSaving changes\n")
        for artifact_path, artifact_json in cleaned_artifacts.items():
            logging.info(f"Overwriting artifact '{artifact_path}'")
            with open(artifact_path, "w") as f:
                json.dump(artifact_json, f, indent="\t", ensure_ascii=False)

    def get_all_relevant_artifact_names(self) -> List[str]:
        """
            Return all local synapse artifact paths that should be updated
        """
        return [
            os.path.join(path, name)
            for path, subdirs, files in os.walk("workspace")
            for name in files
            if any(
                os.path.join(path, name).startswith(x)
                for x in [
                    "workspace/notebook",
                    "workspace/pipeline"
                ]
            )
        ]

    def get_spark_pool_details(self, spark_pool_name: str) -> Dict[str, Any]:
        """
            Get the details of the given spark pool

            :param spark_pool_name: The name of the spark pool to get details for
            :return: The details of the spark pool from the Synapse REST API
        """
        if spark_pool_name not in self._spark_pool_cache:
            self._spark_pool_cache[spark_pool_name] = SynapseSparkPoolUtil(self._synapse_workspace_name).get(spark_pool_name)
        return self._spark_pool_cache[spark_pool_name]

    def replace_spark_pool_references_in_artifact(self, artifact_path: str) -> Dict[str, Any]:
        """
            Replace all references to the old spark pool with the new spark pool for the given artifact

            :param artifact_path: The path of the artifact to update
            :return: The artifact with updated references
        """
        if artifact_path.startswith("workspace/notebook"):
            is_notebook = True
        elif artifact_path.startswith("workspace/pipeline"):
            is_notebook = False
        else:
            raise ValueError(f"Artifact '{artifact_path}' is not a path to a Synapse notebook or pipeline")
        with open(artifact_path, "r") as f:
            read_exception = None
            try:
                artifact = json.load(f)
            except json.JSONDecodeError as e:
                read_exception = e
            if read_exception:
                logging.error(f"Failed to parse JSON in {artifact_path}: {read_exception}")
        if is_notebook:
            return self._update_spark_pool_references_in_notebook(artifact)
        return self._update_spark_pool_references_in_pipeline(artifact)
    
    def _update_spark_pool_references_in_notebook(self, notebook: Dict[str, Any]) -> Dict[str, Any]:
        """
            Update references to spark pools in the given synape notebook json

            :param notebook: Notebook json
            :return: The notebook json with updated references
        """
        new_pool_details = self.get_spark_pool_details(self._new_pool_name)

        node_size_details_map = {
            "Small": {
                "vCores": 4,
                "memory": 32
            },
            "Medium": {
                "vCores": 8,
                "memory": 64
            },
            "Large": {
                "vCores": 16,
                "memory": 128
            },
            "XLarge": {
                "vCores": 32,
                "memory": 256
            },
            "XXLarge": {
                "vCores": 64,
                "memory": 432
            },
            "XXX Large (Isolated Compute)": {
                "vCores": 80,
                "memory": 504
            }
        }

        node_size_details = node_size_details_map[new_pool_details["properties"]["nodeSize"]]

        pool_name = new_pool_details["name"]
        driver_and_executor_memory = node_size_details["memory"] - node_size_details["vCores"] # This is an assumption, based on inspecting the json

        properties_to_overwrite = {
            "properties": {
                "sessionProperties": {
                    "driverMemory": f"{driver_and_executor_memory}g",
                    "driverCores": node_size_details["vCores"],
                    "executorMemory": f"{driver_and_executor_memory}g",
                    "executorCores": node_size_details["vCores"],
                    #"numExecutors": 1,  # Doesn't seem to be needed, can't see where this comes from
                    "conf": {
                        "spark.dynamicAllocation.enabled": str(new_pool_details["properties"]["dynamicExecutorAllocation"]["enabled"]).lower(),
                        #"spark.dynamicAllocation.minExecutors": "1",  # Doesn't seem to be needed, can't see where this comes from
                        #"spark.dynamicAllocation.maxExecutors": "4"  # Doesn't seem to be needed, can't see where this comes from
                    }
                }
            }
        }
        metadata = {
            "a365ComputeOptions": {
                "id": new_pool_details["id"],
                "name": pool_name,
                "endpoint": f"https://{self._synapse_workspace_name}.dev.azuresynapse.net/livyApi/versions/2019-11-01-preview/sparkPools/{pool_name}",
                "sparkVersion": new_pool_details["properties"]["sparkVersion"],
                #"nodeCount": 10,  # Doesn't seem to be needed, can't see where this comes from
                "cores": node_size_details["vCores"],
                "memory": node_size_details["memory"],
                "automaticScaleJobs": new_pool_details["properties"]["autoScale"]["enabled"]
            }
        }
        logging.info(f"    Replacing references for notebook {notebook['name']}")
        if "bigDataPool" in notebook["properties"]:
            properties_to_overwrite["properties"]["bigDataPool"] = {
                "referenceName": pool_name
            }
        if "metadata" in notebook["properties"] and "a365ComputeOptions" in notebook["properties"]["metadata"]:
            properties_to_overwrite["properties"]["metadata"] = metadata
        return self._merge_dictionaries(notebook, properties_to_overwrite)

    def _update_spark_pool_references_in_pipeline(self, pipeline: Dict[str, Any]) -> Dict[str, Any]:
        """
            Update references to spark pools in the given synape pipeline json

            :param pipeline: Pipeline json
            :return: The pipeline json with updated references
        """
        pipeline_copy = deepcopy(pipeline)
        new_pool_details = self.get_spark_pool_details(self._new_pool_name)
        sub_attributes_to_update = self._search_for_dict_attribute(pipeline_copy, "sparkPool")
        if sub_attributes_to_update:
            logging.info(f"   Replacing references for pipeline {pipeline['name']}")
        else:
            logging.info(f"    Skipping pipeline {pipeline['name']}")
        for attribute in sub_attributes_to_update:
            if attribute["referenceName"] == self._old_pool_name:
                attribute["referenceName"] = new_pool_details["name"]
        return pipeline_copy

    def _merge_dictionaries(self, dict_a: Dict[str, Any], dict_b: Dict[str, Any]) -> Dict[str, Any]:
        """
            Deeply update the contents of dict_a with the contents of dict_b

            :param dict_a: The first dictionary to use as a base
            :param dict_b: The dictionary to augment dict_a with
            :return: dict_a with all properties from dict_b added. This includes nested properties
        """
        dict_a_copy = deepcopy(dict_a)
        for key, dict_b_value in dict_b.items():
            if key in dict_a_copy:
                dict_a_value = dict_a_copy[key]
                if isinstance(dict_a_value, dict):
                    dict_a_copy[key] = self._merge_dictionaries(dict_a_value, dict_b_value)
                else:
                    dict_a_copy[key] = dict_b_value
            else:
                dict_a_copy[key] = dict_b_value
        return dict_a_copy

    def _search_for_dict_attribute(self, dictionary: Dict[str, Any], attribute: str) -> List[Any]:
        """
            Deeply return all values found in the dictionary for a key with name `attribute`. This traverses nested data structures

            :param dictionary: The dictionary to analyse
            :param attribute: The key in the dictionary to search for
            :return: All values found in the dictionary (and any nested dictionaries) that are for a key with the given attribute name
        """
        found_entries = []
        for key, value in dictionary.items():
            if key == attribute:
                found_entries.append(value)
            else:
                if isinstance(value, dict):
                    found_entries.extend(self._search_for_dict_attribute(value, attribute))
                elif isinstance(value, list):
                    found_entries.extend(self._search_for_list_attribute(value, attribute))
        return found_entries

    def _search_for_list_attribute(self, a_list: List[Any], attribute: str) -> List[Any]:
        """
            Deeply return all values found in any nested dictionaries in the list with the key name `attribute`.
            This traverses nested data structures

            :param a_list: The list to analyse
            :param attribute: The dictionary key to search for
            :return: All dictionary values found in the list (and any nested dictionaries) that are for a key with the given attribute name
        """
        found_entries = []
        for elem in a_list:
            if isinstance(elem, dict):
                found_entries.extend(self._search_for_dict_attribute(elem, attribute))
            elif isinstance(elem, list):
                found_entries.extend(self._search_for_list_attribute(elem, attribute))
        return found_entries


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-sw", "--synapse_workspace", required=True, help="The synapse workspace to check against")
    parser.add_argument("-o", "--old_pool", required=True, help="The old pool to replace")
    parser.add_argument("-n", "--new_pool", required=True, help="The pool to replace with")
    args = parser.parse_args()
    synapse_workspace = args.synapse_workspace
    old_pool_name = args.old_pool
    new_pool_name = args.new_pool
    SparkPoolReferenceUpdater(synapse_workspace, old_pool_name, new_pool_name).update_all_spark_pool_references()
