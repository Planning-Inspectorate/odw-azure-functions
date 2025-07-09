from pipelines.scripts.util import Util
import json
from typing import Dict, Any
import logging


"""
    Script to check if notebooks are valid.
    Run using `python3 pipelines/scripts/check_notebooks_valid.py`
    You can use `pipelines/scripts/clean_notebooks.py` to clean notebook errors identified by this script
"""

logging.basicConfig(level=logging.INFO)


class InvalidNotebookException(Exception):
    pass


def is_notebook_execution_count_valid(notebook: Dict[str, Any]) -> bool:
    """
        Check that the notebook execution count is valid

        :param notebook: The notebook json to analyse

        :return: `True` if the execution count is null, `False` otherwise
    """
    return all(
        [
            x.get("execution_count", None) is None
            for x in notebook["properties"]["cells"]
        ]
    )


def validate_notebooks(notebooks_to_check: Dict[str, Any]):
    """
        Run validation checks against all notebooks

        :param notebooks_to_check: A dictionary of notebook paths mapped to the underlying json - i.e. the notebooks to analyse
        :raises: A InvalidNotebookException if there are any invalid notebooks
    """
    invalid_notebooks = {
        k: v
        for k, v in notebooks_to_check.items()
        if not is_notebook_execution_count_valid(v)
    }
    for notebook_name in invalid_notebooks.keys():
        logging.info(f"The notebook '{notebook_name}' has a non-null execution count. Please clear this value")
    if invalid_notebooks:
        raise InvalidNotebookException(f"There are {len(invalid_notebooks)} invalid notebooks. Please check the above log to resolve them")


if __name__ == "__main__":
    notebook_artifacts = {
        x: json.load(open(x))
        for x in Util.get_all_artifact_paths(["workspace/notebook"])
    }
    validate_notebooks(notebook_artifacts)
