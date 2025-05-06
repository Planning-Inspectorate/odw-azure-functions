from pipelines.scripts.synapse_artifact.synapse_artifact_util_factory import SynapseArtifactUtilFactory
from concurrent.futures import ThreadPoolExecutor
import os
import shutil


class SynapseWorkspaceUtil:
    """
        Tool to handle using the Synapse REST API to query a Synapse workspace
    """
    def download_workspace(self, workspace_name: str, local_folder: str):
        """
            Download the full json content of the live Synapse workspace, and save it under `local_folder/`

            :param workspace_name: Name of the Synapse workspace to download from
            :param local_folder: Where to save the workspace contents. Should be different to the workspace folder in this repo
        """
        if os.path.exists(local_folder):
            shutil.rmtree(local_folder)
        synapse_artifact_names = [
            f
            for f in os.listdir("workspace")
            if os.path.isdir(os.path.join("workspace", f))
        ]
        artifact_util_classes = {
            type_name: SynapseArtifactUtilFactory.get(type_name)(workspace_name)
            for type_name in synapse_artifact_names
            if SynapseArtifactUtilFactory.is_valid_type_name(type_name)
        }
        # Filter out any artifacts that had no associated util class
        synapse_artifact_names = [x for x in synapse_artifact_names if x in artifact_util_classes]
        get_artifacts = lambda type_name: artifact_util_classes[type_name].download_live_workspace(local_folder)
        os.makedirs(local_folder)
        with ThreadPoolExecutor() as tpe:
            # Download all artifacts in parallel
            [
                thread_response
                for thread_response in tpe.map(get_artifacts, synapse_artifact_names)
                if thread_response
            ]
