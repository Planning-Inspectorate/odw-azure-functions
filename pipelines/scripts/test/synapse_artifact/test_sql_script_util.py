from pipelines.scripts.synapse_artifact.synapse_sql_script_util import SynapseSQLScriptUtil
from copy import deepcopy


def test__synapse_sql_script_util__compare__match():
    artifact = {
        "name": "some sql script",
        "properties": {
            "folder": {
                "name": "archive"
            },
            "content": {
                "query": "SELECT * FROM SOME_TABLE",
                "metadata": {
                    "language": "sql"
                },
                "currentConnection": {
                    "databaseName": "master",
                    "poolName": "Built-in"
                },
                "resultLimit": 5000
            },
            "type": "SqlQuery"
        }
    }
    artifact_copy = deepcopy(artifact)
    assert SynapseSQLScriptUtil("some_workspace").compare(artifact, artifact_copy)


def test__synapse_sql_script_util__compare__mismatch():
    artifact = {
        "name": "some sql script",
        "properties": {
            "folder": {
                "name": "archive"
            },
            "content": {
                "query": "SELECT * FROM SOME_TABLE",
                "metadata": {
                    "language": "sql"
                },
                "currentConnection": {
                    "databaseName": "master",
                    "poolName": "Built-in"
                },
                "resultLimit": 5000
            },
            "type": "SqlQuery"
        }
    }
    different_attributes = {
        "properties": {
            "description": "some description"
        }
    }
    artifact_copy = {**artifact, **different_attributes}
    assert not SynapseSQLScriptUtil("some_workspace").compare(artifact, artifact_copy)
