from pipelines.scripts.synapse_artifact.synapse_workspace_util import SynapseWorkspaceUtil
from pipelines.scripts.remove_unmodified_synapse_files import Util
from pipelines.scripts.test.testing_utility import TestingUtility
import shutil
import mock
import pytest
import os
import json


class MockSynapseWorkspaceUtil():
    def download_workspace(self, workspace_name: str, local_folder: str):
        workspace_name = "workspace_copy"
        shutil.copytree(workspace_name, local_folder, dirs_exist_ok=True)


def copy_workspace():
    shutil.copytree("workspace_copy", "my_local_workspace", dirs_exist_ok=True)
    shutil.rmtree("my_local_workspace/integrationRuntime")
    os.remove("my_local_workspace/template-parameters-definition.json")
    os.remove("my_local_workspace/publish_config.json")


def initialise_workspace_folder_for_testing():
    shutil.copytree("workspace_copy", "workspace", dirs_exist_ok=True)
    shutil.rmtree("workspace/integrationRuntime")
    os.remove("workspace/template-parameters-definition.json")
    os.remove("workspace/publish_config.json")
    all_files = TestingUtility.get_all_files_under_directory("workspace")
    ds_store_files = [x for x in all_files if ".DS_Store" in x]
    for file in ds_store_files:
        os.remove(file)


@pytest.fixture(scope="session", autouse=True)
def setup():
    if not os.path.exists("workspace_copy"):
        os.mkdir("workspace_copy")
    shutil.copytree("workspace", "workspace_copy", dirs_exist_ok=True)


@pytest.fixture(scope="session", autouse=True)
def teardown():
    yield
    shutil.copytree("workspace_copy", "workspace", dirs_exist_ok=True)
    shutil.rmtree("workspace_copy")


def test__util__remove_unmodified_files__without_changes():
    initialise_workspace_folder_for_testing()
    with mock.patch.object(SynapseWorkspaceUtil, "download_workspace", return_value=copy_workspace()):
        Util("some_workspace", "dev").remove_unmodified_files()
        remaining_files = {x.replace("workspace/", "") for x in TestingUtility.get_all_files_under_directory("workspace")}
        assert not remaining_files, f"The following files were not deleted when they were expected to have been: {remaining_files}"


def test__util__remove_unmodified_files__with_change():
    initialise_workspace_folder_for_testing()
    with mock.patch.object(SynapseWorkspaceUtil, "download_workspace", return_value=copy_workspace()):
        modified_file = "workspace/pipeline/0_Horizon_Appeals_Data_Transfer_Raw.json"
        # Modify 1 file here
        with open(modified_file, "r") as f:
            artifact = json.load(f)
            artifact["properties"]["description"] = "A brand new description"
        with open(modified_file, "w") as f:
            json.dump(artifact, f, indent="\t", ensure_ascii=False)
        expected_remaining_dependencies = {
            modified_file,
            # Immediate dependencies
            "workspace/pipeline/0_Raw_Horizon_NSIP_Data.json",
            "workspace/pipeline/0_Raw_Horizon_NSIP_Exam_Timetable.json",
            "workspace/dataset/BIS_HZN_AppealsAddtionalData.json",
            "workspace/dataset/HZN_EnforcementGroundsForAppeal.json",
            "workspace/dataset/HZNSpecialCircumstance.json",
            "workspace/dataset/HZN_ViewData.json",
            "workspace/notebook/py_fail_activity_logging.json",
            "workspace/dataset/Hzn_AdvertDetails.json",
            "workspace/dataset/BIS_AddAdditionalData.json",
            "workspace/dataset/HZN_BIS_DecisionIssues.json",
            "workspace/dataset/HZN_Right_Of_Way.json",
            "workspace/dataset/HZN_RightOfWay.json",
            "workspace/dataset/HZNSpecialism.json",
            "workspace/pipeline/0_Raw_Horizon_NSIP_Relevant_Reps.json",
            "workspace/dataset/HorizonData_BIS_AllAppeals.json",
            "workspace/dataset/HZN_LPARResponsability.json",
            # Nested dependencies
            "workspace/dataset/NSIP_Data.json",
            "workspace/dataset/HZN_NSIP_Query.json",
            "workspace/dataset/ExaminationTimeTable.json",
            "workspace/linkedService/ls_sql_hzn.json",
            "workspace/linkedService/ls_storage.json",
            #"pinssynspodw34.json",
            "workspace/linkedService/ls_sql_mipins.json",
            "workspace/dataset/NSIP_ReleventRepresentation.json",
            #"workspace/integrationRuntime/PinsIntegrationRuntime.json",
            "workspace/linkedService/ls_kv.json"
        }
        Util("some_workspace", "dev").remove_unmodified_files()
        remaining_files = TestingUtility.get_all_files_under_directory("workspace")
        missing_files = expected_remaining_dependencies - remaining_files
        extra_files = remaining_files - expected_remaining_dependencies
        assert not missing_files, f"The following files were expected to be present but were missing: {missing_files}"
        assert not extra_files, f"The following files were not expected but were present: {extra_files}"
