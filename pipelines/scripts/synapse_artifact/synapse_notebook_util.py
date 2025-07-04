from pipelines.scripts.synapse_artifact.synapse_artifact_util import SynapseArtifactUtil
from typing import List, Dict, Any, Set
from pipelines.scripts.util import Util
import ast
from ast2json import ast2json
import re
import json


class NotAPythonNotebookException(Exception):
    pass


class SynapseNotebookUtil(SynapseArtifactUtil):
    """
        Class for managing the retrieval and analysis of Synapse Notebook artifacts
    """
    PYTHON_REFERENCE_PATTERNS = [
        r"%run",
        r"mssparkutils.notebook.run",
        r"mssparkutils.credentials.getFullConnectionString"
    ]

    @classmethod
    def get_type_name(cls) -> str:
        return "notebook"

    def get(self, artifact_name: str, **kwargs: Dict[str, Any]) -> Dict[str, Any]:
        return self._web_request(
            f"{self.synapse_endpoint}/notebooks/{artifact_name}?api-version=2020-12-01",
        ).json()

    def get_all(self, **kwargs: Dict[str, Any]) -> List[Dict[str, Any]]:
        response = self._web_request(
            f"{self.synapse_endpoint}/notebooks?api-version=2020-12-01",
        ).json()
        all_notebooks = response["value"]
        while "nextLink" in response:
            next_link = response["nextLink"]
            response = self._web_request(next_link,).json()
            all_notebooks.extend(response["value"])
        return all_notebooks

    def get_uncomparable_attributes(self) -> List[str]:
        return [
            r"id",
            r"type",
            r"etag",
            r"properties.entityState",
            r"properties.renameOperationDetails",
            r"properties.targetSparkConfiguration",
            r"properties.description",
            r"properties.sessionProperties.runAsWorkspaceSystemIdentity",
            r"properties.metadata.a365ComputeOptions.extraHeader",
            r"properties.metadata.a365ComputeOptions.auth.authHeader",
            r"properties.cells.\d+.execution_count",
            r"properties.cells.\d+.outputs",
            r"properties.metadata.kernelspec",
            r"properties.metadata.kernelspec.name",
            r"properties.metadata.kernelspec.display_name",
        ]

    def get_nullable_attributes(self) -> List[str]:
        return [
            r"properties.metadata.a365ComputeOptions.automaticScaleJobs",
            r"properties.cells.\d+.metadata",
            r"properties.folder",
            r"properties.bigDataPool",
            r"properties.metadata.a365ComputeOptions"
        ]
    
    def compare(self, artifact_a, artifact_b):
        def _add_missing_attributes(artifact: Dict[str, Any]) -> Dict[str, Any]:
            # Set default values for optional properties. This is the only artifact where this is required
            properties = artifact["properties"]
            metadata = properties["metadata"]
            if "a365ComputeOptions" not in metadata or metadata["a365ComputeOptions"] is None:
                metadata["a365ComputeOptions"] = {
                    "automaticScaleJobs": True
                }
            compute_options = metadata["a365ComputeOptions"]
            if "automaticScaleJobs" not in compute_options:
                compute_options["automaticScaleJobs"] = True
            return artifact
        artifact_a = _add_missing_attributes(artifact_a)
        artifact_b = _add_missing_attributes(artifact_b)
        return super().compare(artifact_a, artifact_b)

    def get_env_attributes_to_replace(self) -> List[str]:
        return [
            "properties.metadata.a365ComputeOptions.id",
            "properties.metadata.a365ComputeOptions.endpoint"
        ]

    @classmethod
    def convert_to_python(cls, artifact: Dict[str, Any]) -> str:
        """
            Convert the given notebook artifact to python

            Note: This function does not fully account for all iPython magic commands. The implemented logic attempts to convert
                  magic commands into comments, except for %run, which instead gets converted to mmsparkutils.notebook.run().
                  Extending the logic to account for all magic commands/input combinations will take some time
                  (and may involve importing the ipython package) however for all ODW notebooks seen as of 2025-07-03,
                  none of these other cases occur or negatively impact the usage of this function, since it's only used for detecting dependencies.
                  In the interest of time the functionality will be left as-is, unless there is a requirement to expand on this functionality
                  (in which case please update this comment as appropriate)
        """
        if artifact.get("properties", dict()).get("metadata", dict()).get("language_info", dict()).get("name") != "python":
            raise NotAPythonNotebookException(f"The given notebook artifact is not a python notebook")
        def _process_cell(cell: Dict[str, Any]) -> List[str]:
            """
                Convert the given cell to python, commenting out magic commands/converting %run to mssparkutils.notebook.run
                This is done on a cell-by-cell basis because Synapse notebooks mostly only support a single magic command in a cell
                (except for the special magic commands starting with %%)
            """
            if cell["cell_type"] != "code":
                # If the cell is not a code cell, then it can be dropped to simplify processing
                return ""
            # If there are no non-whitespace lines in the cell, the just return an empty string and skip the rest of the processing
            first_meaningful_line = next((line for line in cell["source"] if line.strip()), None)
            if not first_meaningful_line:
                return ""
            is_magic_cell = first_meaningful_line.lstrip().startswith("%")
            if is_magic_cell:
                # Merge magic cells into one line to simplify processing
                cleaned_cells = [line.strip() for line in cell["source"] if line.strip()]
                cleaned_cell_line = " ".join(cleaned_cells).lstrip()
                # %run is a special magic command that can be converted to a real python command, which is important for detecting dependencies
                # Only 1 magic command can exist in a cell - this is assumed to be true
                if cleaned_cell_line.startswith("%run "):
                    # Clean the arguments to be a single space between each arg, and convert to be a list of arguments
                    run_arguments = " ".join(cleaned_cell_line.lstrip()[cleaned_cell_line.rindex("%run ")+5:].split())
                    # The %run command either expects a quoted or unquoted path. The below logic only adds quotes if they are missing
                    quote = "\"" if not (run_arguments.startswith('"') or run_arguments.startswith("'") or run_arguments.startswith("`")) else ""
                    run_arguments_split = run_arguments.split("{")
                    run_target_path = run_arguments_split[0].strip()
                    cleaned_line = f"mssparkutils.notebook.run({quote}{run_target_path}{quote})"
                    return cleaned_line
                else:
                    # Comment out all other magic commands, since their operation is not important in the grant scheme of things
                    return f"# {cleaned_cell_line}"   
            # Combine all lines into a single string, separated by the newline character
            return "\n".join([line.rstrip() for line in cell["source"]])

        cell_code = [
            _process_cell(cell)
            for cell in artifact["properties"]["cells"]
        ]
        return "\n".join([line for line in cell_code if line])

    @classmethod
    def get_dependencies_in_notebook_code(cls, notebook_python: str):
        """
            Return the dependencies of the given Synapse notebook code

            Note: This assumes the usage of mssparkutils is inline with Microsoft documentation. If mssparkutils has been explicitly imported
                  as a different identifier then this will not work (i.e. import mssparkutils as something_else).
                  As of 2025-07-03 all ODW notebooks follow the Microsoft documentation
        """
        def get_dependencies(function_call: Dict[str, Any]) -> Set[str]:
            """
                Identify the dependencies of the given function call json.

                Note: this only accounts for mssparkutils.notebook.run and mssparkutils.credentials.getFullConnectionString , since
                      these are the only two function calls that can refer to external artifacts
            """
            found_dependencies = set()
            # Process kwargs first (the args passed like arg_name=arg_value)
            function_kwargs_map = {
                "run": ["path"],
                "getFullConnectionString": []
            }
            mssparkutils_function_call = function_call["func"]["attr"]
            kwargs_to_look_for = function_kwargs_map[mssparkutils_function_call]
            keyword_dependencies = [
                kwarg
                for kwarg in function_call.get("keywords", [])
                if kwarg["arg"] in kwargs_to_look_for
            ]
            found_dependencies.update(
                kwarg["value"]["value"]
                for kwarg in keyword_dependencies
            )
            args = function_call.get("args", [])
            # Process just the first loose argument (the argument passed without a keyword name)
            if args:
                found_dependencies.add(args[0]["value"])
            # Prepare the dependencies depending on the underlying mssparkutils function being called
            cleaned_found_dependencies = {
                f"notebook/{dependency.split('/')[-1]}.json" if mssparkutils_function_call == "run" else f"linkedService/{dependency}.json"
                for dependency in found_dependencies
            }
            return cleaned_found_dependencies

        # Convert the python code into abstract syntax tree json that can be analysed
        abstract_syntax_tree = ast.parse(notebook_python)
        abstract_syntax_tree_json = ast2json(abstract_syntax_tree)
        abstract_syntax_tree_attributes = cls.get_all_attributes(abstract_syntax_tree_json)

        abstract_syntax_tree_funtion_attribute_names = [
            x[0:-10]
            for x in abstract_syntax_tree_attributes
            if ".func." in x and x.endswith("func.attr")
        ]
        # Retrives all function objects from the AST
        abstract_syntax_tree_functions = [cls.get_by_attribute(abstract_syntax_tree_json, x) for x in abstract_syntax_tree_funtion_attribute_names]
        # Filter down the function objects to only be the ones related to mssparkutils.notebook.run and mssparkutils.credentials.getFullConnectionString
        abstract_syntax_tree_functions = [
            function_json
            for function_json in abstract_syntax_tree_functions
            if (
                (
                    function_json["func"]["attr"] == "run" and
                    function_json["func"].get("value", dict()).get("attr") == "notebook" and True
                )
                or (
                    function_json["func"]["attr"] == "getFullConnectionString" and 
                    function_json["func"].get("value", dict()).get("attr") == "credentials"
                )
            )
        ]
        dependencies = {
            dependency
            for function_json in abstract_syntax_tree_functions
            for dependency in get_dependencies(function_json)
        }

        return dependencies

    @classmethod
    def dependent_artifacts(cls, artifact: Dict[str, Any]) -> Set[str]:
        dependencies = super().dependent_artifacts(artifact)
        try:
            notebook_python = cls.convert_to_python(artifact)
        except NotAPythonNotebookException:
            # Only python notebooks can have dependencies
            return dependencies
        extra_dependencies = cls.get_dependencies_in_notebook_code(notebook_python)
        return dependencies | extra_dependencies

    @classmethod
    def can_be_archived(cls) -> bool:
        return True

    @classmethod
    def archive(cls, artifact: Dict[str, Any]) -> Dict[str, Any]:
        # This action breaks any mssparkutil or %run references. This is intentional, since it causes crashes which allows the team to catch any
        # mistakes during the archival process
        existing_folder = artifact["properties"].get("folder", dict())
        existing_folder_name = existing_folder.get("name", "")
        existing_folder.update(
            {
                "name": "/".join(["archive", existing_folder_name])
            }
        )
        artifact["properties"]["folder"] = existing_folder
        return artifact
