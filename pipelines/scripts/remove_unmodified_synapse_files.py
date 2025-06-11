from pipelines.scripts.synapse_artifact.synapse_artifact_util_factory import SynapseArtifactUtilFactory
from pipelines.scripts.synapse_artifact.synapse_artifact_util import SynapseArtifactUtil
from pipelines.scripts.synapse_artifact.synapse_workspace_util import SynapseWorkspaceUtil
from typing import Set, Iterable
import argparse
import json
import os
import logging
import shutil


logging.basicConfig(level=logging.INFO)


"""
    Script to compare local Synapse workspace files against the specified live workspace, and to delete unmodified files
    Example usage: `python3 pipelines/scripts/remove_unmodified_synapse_files.py -sw "SYNAPSE_WORKSPACE_NAME_HERE"`
"""


class Util():
    def __init__(self, workspace_name: str, target_env: str):
        self.workspace_name = workspace_name
        self.local_workspace = "my_local_workspace"
        self.target_env = target_env
    
    def remove_unmodified_files(self):
        SynapseWorkspaceUtil().download_workspace(self.workspace_name, self.local_workspace)
        modified_files = self._get_modified_files(self.local_workspace)
        dependencies = self._get_dependencies_for_files(modified_files)
        files_to_keep = modified_files.union(dependencies)
        files_to_remove = self._get_all_files_under_folder("workspace").difference(files_to_keep)
        logging.info(f"Total modified files: {len(modified_files)}")
        logging.info(f"Total dependencies: {len(dependencies)}")
        logging.info(f"Total files to keep: {len(files_to_keep)}")
        logging.info(f"Total files to remove: {len(files_to_remove)}")
        self._delete_files(files_to_remove)
        shutil.rmtree(self.local_workspace)

    def _get_dependencies_for_files(self, files_to_analyse: Set[str]) -> Set[str]:
        """
            Deeply return all dependencies for the given synapse artifact names

            :param files_to_analyse: The files to detect dependencies for
            :return: The paths to artifacts that are dependencies, including nested dependencies
        """
        files_to_ignore = {
            "template-parameters-definition.json",
            "publish_config.json"
        }
        analysed_dependencies = set()
        dependencies = {file for file in files_to_analyse if file not in files_to_ignore}
        while dependencies:
            file = dependencies.pop()
            analysed_dependencies.add(file)
            if file.endswith(".json"):
                local_artifact_name = f"workspace/{file}"
                if not os.path.exists(local_artifact_name):
                    # This case is ok to skip, since Synapse just ignores it. Raise a warning just for awareness
                    logging.warning(f"The artifact '{local_artifact_name}' is referenced by other artifacts but it does not exist")
                else:
                    local_workspace_file = json.load(open(local_artifact_name, "r"))
                    dependencies = dependencies.union(SynapseArtifactUtil.dependent_artifacts(local_workspace_file))
        logging.info("The below files have been identified as dependencies of the modified files")
        logging.info(json.dumps(list(analysed_dependencies), indent=4))
        return analysed_dependencies


    def _get_all_files_under_folder(self, folder: str):
        """
            Return all leaf files in under the target folder

            :param folder: Folder to start from
        """
        return {
            os.path.join(path, name).replace(f"{folder}/", "", 1)
            for path, subdirs, files in os.walk(folder)
            for name in files
            if ".DS_Store" not in name
        }

    def _compare_live_and_local_artifacts(self, artifact_name: str):
        """
            Compare the local artifacts to the locally-downloaded live workspace

            :param artifact_name: Name of the artifact to compare
            :return: True if the artifacts match and both exist, False otherwise
        """
        local_artifact_name = f"workspace/{artifact_name}"
        live_artifact_name = f"{self.local_workspace}/{artifact_name}"
        if not os.path.exists(local_artifact_name):
            return False
        if not os.path.exists(live_artifact_name):
            return False
        local_workspace_file = json.load(open(local_artifact_name, "r"))
        live_workspace_file = json.load(open(live_artifact_name, "r"))
        artifact_type = artifact_name.replace("workspace/", "").split("/")[0]
        artifact_util = SynapseArtifactUtilFactory.get(artifact_type)(self.workspace_name)
        local_workspace_file = artifact_util.replace_env_strings(local_workspace_file, "dev", self.target_env)
        logging.info(f"Analysing artifact '{artifact_name}'")
        return artifact_util.compare(local_workspace_file, live_workspace_file)

    def _get_modified_files(self, live_workspace_local_download_folder: str) -> Set[str]:
        """
            Return all Synapse workspace files modified between the `workspace` folder and the `live_workspace_local_download_folder` folder

            :param live_workspace_local_download_folder: The folder where the live Synapse workspace was downloaded to locally
        """
        live_file_names = self._get_all_files_under_folder(live_workspace_local_download_folder)
        workspace_file_names = self._get_all_files_under_folder("workspace")

        new_files = workspace_file_names - live_file_names
        deleted_files = live_file_names - workspace_file_names
        common_files = (workspace_file_names - deleted_files) - new_files

        modified_files = {
            file
            for file in common_files
            if not self._compare_live_and_local_artifacts(file)
        }
        diff = modified_files.union(new_files)
        logging.info(f"Total files in local workspace: {len(workspace_file_names)}")
        logging.info(f"Total files in the live workspace {len(live_file_names)}")
        logging.info(f"Total files common between local and live workspace {len(common_files)}")
        logging.info(f"Total common files that have modifications: {len(modified_files)}")
        logging.info(f"Total number of files that have been modified or created: {len(diff)}")
        logging.info("The below files do not exist in the live workspace")
        logging.info(json.dumps(list(new_files), indent=4))
        logging.info("The below files exist in the live workspace but do not exist locally")
        logging.info(json.dumps(list(deleted_files), indent=4))
        logging.info("The below files have been modified in the local workspace")
        logging.info(json.dumps(list(modified_files), indent=4))
        return diff

    def _delete_files(self, files_to_delete: Iterable[str]):
        """
            Delete all specified files

            :param files_to_delete: The file names to delete
        """
        for file in files_to_delete:
            logging.info(f"    Deleting file '{file}'")
            os.remove(f"workspace/{file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-sw", "--synapse_workspace", help="The synapse workspace to check against")
    args = parser.parse_args()
    synapse_workspace = args.synapse_workspace
    env = synapse_workspace.split("-")[3]  # The third element of the odw synapse name is the environment
    Util(synapse_workspace, env).remove_unmodified_files()
