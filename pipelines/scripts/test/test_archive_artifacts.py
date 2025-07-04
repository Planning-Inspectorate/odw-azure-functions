from pipelines.scripts.archive_artifacts import ArtifactArchiver
from pipelines.scripts.synapse_artifact.synapse_artifact_util_factory import SynapseArtifactUtilFactory
from pipelines.scripts.synapse_artifact.synapse_artifact_util import SynapseArtifactUtil
from pipelines.scripts.synapse_artifact.synapse_notebook_util import SynapseNotebookUtil
from pipelines.scripts.synapse_artifact.synapse_pipeline_util import SynapsePipelineUtil
from pipelines.scripts.synapse_artifact.synapse_dataset_util import SynapseDatasetUtil
from pipelines.scripts.synapse_artifact.synapse_sql_script_util import SynapseSQLScriptUtil
from pipelines.scripts.util import Util
import pytest
import mock
import json
import os


def test__artifact_archiver__init():
    def _mock_get_json(inst, path: str) -> str:
        json_map = {
            "workspace/notebook/notebook_artifact_a.json": "a",
            "workspace/notebook/notebook_artifact_b.json": "b",
            "workspace/pipeline/pipeline_artifact.json": "c",
            "workspace/dataset/dataset_artifact.json": "d",
            "workspace/sqlscript/sql_artifact.json": "e",
            "workspace/trigger/trigger_artifact.json": "f",
            "workspace/integrationRuntime/ir_artifact.json": "g",
            "workspace/linkedService/ls_artifact.json": "h"
        }
        return json_map[path]
    
    def _mock_is_archived(artifact: str) -> bool:
        archive_map = {
            "a": True,
            "b": False,
            "c": True,
            "d": True,
            "e": False
        }
        return archive_map[artifact]

    all_artifacts = [
        "workspace/notebook/notebook_artifact_a.json",
        "workspace/notebook/notebook_artifact_b.json",
        "workspace/pipeline/pipeline_artifact.json",
        "workspace/dataset/dataset_artifact.json",
        "workspace/sqlscript/sql_artifact.json",
        "workspace/trigger/trigger_artifact.json",
        "workspace/integrationRuntime/ir_artifact.json",
        "workspace/linkedService/ls_artifact.json"
    ]
    expected_artifact_json = {
        "workspace/notebook/notebook_artifact_a.json": "a",
        "workspace/notebook/notebook_artifact_b.json": "b",
        "workspace/pipeline/pipeline_artifact.json": "c",
        "workspace/dataset/dataset_artifact.json": "d",
        "workspace/sqlscript/sql_artifact.json": "e",
        "workspace/trigger/trigger_artifact.json": "f",
        "workspace/integrationRuntime/ir_artifact.json": "g",
        "workspace/linkedService/ls_artifact.json": "h"
    }
    expected_archiveable_artifacts = {
        "workspace/notebook/notebook_artifact_a.json",
        "workspace/notebook/notebook_artifact_b.json",
        "workspace/pipeline/pipeline_artifact.json",
        "workspace/dataset/dataset_artifact.json",
        "workspace/sqlscript/sql_artifact.json"
    }
    expected_unarchiveable_artifacts = {
        "workspace/trigger/trigger_artifact.json",
        "workspace/integrationRuntime/ir_artifact.json",
        "workspace/linkedService/ls_artifact.json"
    }
    expected_existing_archived_artifacts = {
        "workspace/notebook/notebook_artifact_a.json",
        "workspace/pipeline/pipeline_artifact.json",
        "workspace/dataset/dataset_artifact.json",
    }
    with mock.patch.object(Util, "get_all_artifact_paths", return_value=all_artifacts):
        with mock.patch.object(ArtifactArchiver, "_get_artifact_json", _mock_get_json):
            with mock.patch.object(SynapseArtifactUtil, "is_archived", _mock_is_archived):
                archiver = ArtifactArchiver()
                assert "pipeline/pln_master.json" in archiver.ROOT_ARTIFACTS, "ROOT_ARTIFACTS must contain 'pipeline/pln_master.json'"
                assert archiver.ALL_ARTIFACT_NAMES == set(all_artifacts)
                assert archiver.ALL_ARTIFACTS == expected_artifact_json
                assert archiver.ALL_ARCHIVEABLE_ARTIFACTS == expected_archiveable_artifacts
                assert archiver.ALL_UNARCHIVEABLE_ARTIFACTS == expected_unarchiveable_artifacts
                assert archiver.EXISTING_ARCHIVED_ARTIFACTS == expected_existing_archived_artifacts


def test__artifact_archiver__get_artifact__successful():
    archiveable_artifacts = {"workspace/notebook/artifact_a.json": "a", "workspace/pipeline/artifact_b.json": "b"}
    with mock.patch.object(Util, "get_all_artifact_paths", return_value=[]):
        archiver = ArtifactArchiver()
        with mock.patch.object(archiver, "ALL_ARTIFACTS", new=archiveable_artifacts):
            assert archiver.get_artifact("workspace/notebook/artifact_a.json") == "a"


def test__artifact_archiver__get_artifact__failed():
    archiveable_artifacts = {"workspace/notebook/artifact_a.json": "a", "workspace/pipeline/artifact_b.json": "b"}
    with mock.patch.object(Util, "get_all_artifact_paths", return_value=[]):
        archiver = ArtifactArchiver()
        with mock.patch.object(archiver, "ALL_ARTIFACTS", new=archiveable_artifacts):
            with pytest.raises(ValueError):
                assert archiver.get_artifact("who asked?") == "a"


def test__artifact_archiver__get_dependencies():
    def _mock_get_artifact(inst, artifact_name: str):
        artifact_json_map = {
            "workspace/notebook/artifact_a.json": {"name": "a"},
            "workspace/pipeline/artifact_b.json": {"name": "b"},
            "workspace/pipeline/artifact_c.json": {"name": "c"},
            "workspace/pipeline/artifact_d.json": {"name": "d"},
            "workspace/pipeline/artifact_e.json": {"name": "e"},
            "workspace/pipeline/artifact_f.json": {"name": "f"},
            "workspace/notebook/artifact_g.json": {"name": "g"},
            "workspace/pipeline/artifact_h.json": {"name": "h"},
            "workspace/pipeline/artifact_i.json": {"name": "i"},
            "workspace/pipeline/artifact_j.json": {"name": "j"},
            "workspace/pipeline/artifact_k.json": {"name": "k"},
            "workspace/pipeline/artifact_l.json": {"name": "l"}
        }
        return artifact_json_map[artifact_name]

    all_artifacts = [
        "workspace/notebook/artifact_a.json",
        "workspace/pipeline/artifact_b.json",
        "workspace/pipeline/artifact_c.json",
        "workspace/pipeline/artifact_d.json",
        "workspace/pipeline/artifact_e.json",
        "workspace/pipeline/artifact_f.json",
        "workspace/notebook/artifact_g.json",
        "workspace/pipeline/artifact_h.json",
        "workspace/pipeline/artifact_i.json",
        "workspace/pipeline/artifact_j.json",
        "workspace/pipeline/artifact_k.json",
        "workspace/pipeline/artifact_l.json"
    ]
    root_artifact = "notebook/artifact_a.json"
    dependent_artifacts_side_effects = [
        {
            "notebook/artifact_a.json",
            "pipeline/artifact_b.json"
        },
        {
            "notebook/artifact_g.json"
        },
        {
            "pipeline/artifact_j.json"
        },
        {
            "pipeline/artifact_j.json",
            "pipeline/artifact_k.json"
        },
        {
            "pipeline/artifact_l.json"
        },
        {}, # No more dependencies found for the artifact being analysed
    ]
    expected_dependencies = {
        "workspace/notebook/artifact_a.json",
        "workspace/pipeline/artifact_b.json",
        "workspace/notebook/artifact_g.json",
        "workspace/pipeline/artifact_j.json",
        "workspace/pipeline/artifact_j.json",
        "workspace/pipeline/artifact_k.json",
        "workspace/pipeline/artifact_l.json"
    }
    mock_artifact_util = mock.MagicMock()
    with mock.patch.object(Util, "get_all_artifact_paths", return_value=all_artifacts):
        with mock.patch.object(ArtifactArchiver, "_get_artifact_json", _mock_get_artifact):
            with mock.patch.object(SynapseArtifactUtilFactory, "get", return_value=mock_artifact_util):
                with mock.patch.object(mock_artifact_util, "dependent_artifacts", side_effect=dependent_artifacts_side_effects):
                    with mock.patch.object(SynapseArtifactUtil, "is_archived", return_value=False):
                        archiver = ArtifactArchiver()
                        dependencies = archiver.get_dependencies(root_artifact)
                        assert expected_dependencies == dependencies
                        assert 6 == mock_artifact_util.dependent_artifacts.call_count


def test__artifact_archiver__get_artifacts_to_archive():
    all_artifacts = {
        "workspace/notebook/artifact_a.json",
        "workspace/pipeline/artifact_b.json",
        "workspace/pipeline/artifact_c.json",
        "workspace/pipeline/artifact_d.json",
        "workspace/pipeline/artifact_e.json",
        "workspace/pipeline/artifact_f.json"
    }
    dependencies = {
        "workspace/pipeline/artifact_b.json",
        "workspace/pipeline/artifact_d.json"

    }
    root_artifacts = {
        "workspace/pipeline/artifact_f.json"
    }
    expected_artifacts_to_archive = {
        "workspace/notebook/artifact_a.json",
        "workspace/pipeline/artifact_c.json",
        "workspace/pipeline/artifact_e.json"
    }
    with mock.patch.object(Util, "get_all_artifact_paths", return_value=[]):
        archiver = ArtifactArchiver()
        with mock.patch.object(archiver, "ALL_ARTIFACT_NAMES", new=all_artifacts):
            with mock.patch.object(archiver, "ROOT_ARTIFACTS", new=root_artifacts):
                actual_artifacts_to_archice = archiver.get_artifacts_to_archive(dependencies)
                assert expected_artifacts_to_archive == actual_artifacts_to_archice


def test__artifact_archiver__get_artifacts_that_cannot_be_archived():
    def _mock_is_artifact_archiveable(inst, artifact: str) -> bool:
        artifact_map = {
            "workspace/notebook/artifact_a.json": True,
            "workspace/pipeline/artifact_c.json": False,
            "workspace/pipeline/artifact_e.json": True
        }
        return artifact_map[artifact]

    artifacts_to_archive = {
        "workspace/notebook/artifact_a.json",
        "workspace/pipeline/artifact_c.json",
        "workspace/pipeline/artifact_e.json"
    }
    expected_unarchivable_artifacts_to_archive = {
        "workspace/pipeline/artifact_c.json"
    }
    with mock.patch.object(Util, "get_all_artifact_paths", return_value=[]):
        with mock.patch.object(ArtifactArchiver, "is_artifact_archiveable", _mock_is_artifact_archiveable):
            archiver = ArtifactArchiver()
            actual_unarchivable_artifacts_to_archive = archiver.get_artifacts_that_cannot_be_archived(artifacts_to_archive)
            assert expected_unarchivable_artifacts_to_archive == actual_unarchivable_artifacts_to_archive


def test_artifact_archiver__get_artifacts_to_delete():
    """
        Out of a set of artifacts to archive, only the artifacts that have already been archived should be marked for deletion
    """
    artifacts_to_archive = {
        "workspace/notebook/artifact_a.json",
        "workspace/pipeline/artifact_b.json",
        "workspace/pipeline/artifact_c.json",
        "workspace/pipeline/artifact_d.json",
        "workspace/pipeline/artifact_e.json",
        "workspace/pipeline/artifact_f.json"
    }
    existing_archived_artifacts = {
        "workspace/pipeline/artifact_b.json",
        "workspace/pipeline/artifact_d.json",
        "workspace/pipeline/artifact_g.json",
        "workspace/pipeline/artifact_h.json"
    }
    expected_artifacts_to_delete = {
        "workspace/pipeline/artifact_b.json",
        "workspace/pipeline/artifact_d.json"
    }
    with mock.patch.object(Util, "get_all_artifact_paths", return_value=[]):
        archiver = ArtifactArchiver()
        with mock.patch.object(archiver, "EXISTING_ARCHIVED_ARTIFACTS", new=existing_archived_artifacts):
            actual_artifacts_to_delete = archiver.get_artifacts_to_delete(artifacts_to_archive)
            assert expected_artifacts_to_delete == actual_artifacts_to_delete


def test_artifact_archiver__is_artifact_archiveable():
    mock_synapse_artifact_util = mock.MagicMock()
    with mock.patch.object(Util, "get_all_artifact_paths", return_value=[]):
        with mock.patch.object(SynapseArtifactUtilFactory, "get", return_value=mock_synapse_artifact_util):
            with mock.patch.object(mock_synapse_artifact_util, "can_be_archived", return_value=True):
                archiver = ArtifactArchiver()
                archiver.is_artifact_archiveable("workspace/somekind/artifact.json")
                SynapseArtifactUtilFactory.get.assert_called_once_with("somekind")
                mock_synapse_artifact_util.can_be_archived.assert_called_once()


def test__artifact_archiver__archive_artifacts():
    def _mock_get_artifact(inst, artifact: str):
        artifact_map = {
            "workspace/notebook/notebook_artifact_a.json": {"name": "a"},
            "workspace/notebook/notebook_artifact_b.json": {"name": "b"},
            "workspace/pipeline/pipeline_artifact.json": {"name": "c"},
            "workspace/dataset/dataset_artifact.json": {"name": "d"},
            "workspace/sqlscript/sql_artifact.json": {"name": "e"}
        }
        return artifact_map[artifact]

    artifacts_to_archive = {
        "workspace/notebook/notebook_artifact_a.json",
        "workspace/notebook/notebook_artifact_b.json",
        "workspace/pipeline/pipeline_artifact.json",
        "workspace/dataset/dataset_artifact.json",
        "workspace/sqlscript/sql_artifact.json"
    }
    expected_write_calls = [
        mock.call("workspace/notebook/notebook_artifact_a.json", {"name": "notebook_archive"}),
        mock.call("workspace/notebook/notebook_artifact_b.json", {"name": "notebook_archive"}),
        mock.call("workspace/pipeline/pipeline_artifact.json", {"name": "c_archive"}),
        mock.call("workspace/dataset/dataset_artifact.json", {"name": "d_archive"}),
        mock.call("workspace/sqlscript/sql_artifact.json", {"name": "e_archive"})
    ]
    with mock.patch.object(Util, "get_all_artifact_paths", return_value=[]):
        with mock.patch.object(ArtifactArchiver, "_write_artifact", return_value=""):
            with mock.patch.object(ArtifactArchiver, "get_artifact", _mock_get_artifact):
                with mock.patch.object(SynapseNotebookUtil, "archive", return_value={"name": "notebook_archive"}):
                    with mock.patch.object(SynapsePipelineUtil, "archive", return_value={"name": "c_archive"}):
                        with mock.patch.object(SynapseDatasetUtil, "archive", return_value={"name": "d_archive"}):
                            with mock.patch.object(SynapseSQLScriptUtil, "archive", return_value={"name": "e_archive"}):
                                archiver = ArtifactArchiver()
                                archiver.archive_artifacts(artifacts_to_archive)
                                # These can be in any order, so check the order with custom logic
                                SynapseNotebookUtil.archive.assert_has_calls([mock.call({"name": "a"}), mock.call({"name": "b"})], any_order=True)
                                SynapsePipelineUtil.archive.assert_has_calls([mock.call({"name": "c"})])
                                SynapseDatasetUtil.archive.assert_has_calls([mock.call({"name": "d"})])
                                SynapseSQLScriptUtil.archive.assert_has_calls([mock.call({"name": "e"})])
                                ArtifactArchiver._write_artifact.assert_has_calls(expected_write_calls, any_order=True)


def test_artifact_archiver__delete_artifacts():
    artifact_to_delete = {
        "workspace/notebook/notebook_artifact_a.json",
        "workspace/notebook/notebook_artifact_b.json",
        "workspace/pipeline/pipeline_artifact.json",
        "workspace/dataset/dataset_artifact.json",
        "workspace/sqlscript/sql_artifact.json"
    }
    expected_delete_calls = [
        mock.call(x)
        for x in artifact_to_delete
    ]
    with mock.patch.object(Util, "get_all_artifact_paths", return_value=[]):
        with mock.patch.object(os, "remove", return_value=True):
            archiver = ArtifactArchiver()
            archiver.delete_artifacts(artifact_to_delete)
            os.remove.assert_has_calls(expected_delete_calls)


def test_artifact_archiver__main():
    def _mock_is_artifact_archiveable(inst, artifact: str) -> bool:
        archiveable = [
            "notebook",
            "pipeline",
            "dataset",
            "sqlscript"
        ]
        return any(artifact.startswith(f"workspace/{x}") for x in archiveable)
    
    def _mock_get_artifact_json(inst, artifact: str):
        artifact_map = {
            "workspace/notebook/notebook_artifact_a.json": {"name": "a"},
            "workspace/notebook/notebook_artifact_b.json": {"name": "b"},
            "workspace/pipeline/pipeline_artifact.json": {"name": "c"},
            "workspace/dataset/dataset_artifact.json": {"name": "d"},
            "workspace/sqlscript/sql_artifact.json": {"name": "e"},
            "workspace/trigger/trigger_artifact.json": {"name": "e"},
            "workspace/integrationRuntime/ir_artifact.json": {"name": "f"},
            "workspace/linkedService/ls_artifact.json": {"name": "g"},
            "workspace/linkedService/ls_artifact_b.json": {"name": "h"}
        }
        return artifact_map[artifact]
    
    def _mock_is_archived(artifact: str):
        already_archived_artifacts = [
            {"name": "d"}
        ]
        return artifact in already_archived_artifacts

    all_artifacts = [
        "workspace/notebook/notebook_artifact_a.json", # Dependency
        "workspace/notebook/notebook_artifact_b.json",
        "workspace/pipeline/pipeline_artifact.json",
        "workspace/dataset/dataset_artifact.json",
        "workspace/sqlscript/sql_artifact.json", # Dependency
        "workspace/trigger/trigger_artifact.json", # Dependency
        "workspace/integrationRuntime/ir_artifact.json", # Dependency
        "workspace/linkedService/ls_artifact.json",  # Dependency
        "workspace/linkedService/ls_artifact_b.json"
    ]
    dependency_side_effects = [
        {"workspace/sqlscript/sql_artifact.json"},
        {
            "workspace/trigger/trigger_artifact.json",
            "workspace/linkedService/ls_artifact.json"
        },
        {
            "workspace/integrationRuntime/ir_artifact.json"
        }
    ]
    root_artifacts = {
        "workspace/notebook/notebook_artifact_a.json",
        "workspace/integrationRuntime/ir_artifact.json",
        "workspace/linkedService/ls_artifact.json"
    }
    expected_artifacts_to_archive = {
        "workspace/notebook/notebook_artifact_b.json",  # Can be archived
        "workspace/pipeline/pipeline_artifact.json"  # Can be archived
    }
    expected_artifacts_to_delete = {
        "workspace/dataset/dataset_artifact.json",  # Already deleted
        "workspace/linkedService/ls_artifact_b.json"  # Cannot be archived
    }
    with mock.patch.object(Util, "get_all_artifact_paths", return_value=all_artifacts):
        with mock.patch.object(ArtifactArchiver, "_get_artifact_json", _mock_get_artifact_json):
            with mock.patch.object(ArtifactArchiver, "is_artifact_archiveable", _mock_is_artifact_archiveable):
                with mock.patch.object(SynapseArtifactUtil, "is_archived", _mock_is_archived):
                    archiver = ArtifactArchiver()
                    with mock.patch.object(archiver, "ROOT_ARTIFACTS", new=root_artifacts):
                        with mock.patch.object(ArtifactArchiver, "get_dependencies", side_effect=dependency_side_effects):
                            with mock.patch.object(ArtifactArchiver, "archive_artifacts", return_value=True):
                                with mock.patch.object(ArtifactArchiver, "delete_artifacts", return_value=True):
                                    archiver.main()
                                    ArtifactArchiver.archive_artifacts.assert_called_once_with(expected_artifacts_to_archive)
                                    ArtifactArchiver.delete_artifacts.assert_called_once_with(expected_artifacts_to_delete)
