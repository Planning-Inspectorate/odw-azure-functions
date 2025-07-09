from pipelines.scripts.synapse_artifact.synapse_artifact_util import SynapseArtifactUtil
from pipelines.scripts.synapse_artifact.synapse_credential_util import SynapseCredentialUtil
from pipelines.scripts.synapse_artifact.synapse_dataset_util import SynapseDatasetUtil
from pipelines.scripts.synapse_artifact.synapse_linked_service_util import SynapseLinkedServiceUtil
from pipelines.scripts.synapse_artifact.synapse_managed_private_endpoint_util import SynapseManagedPrivateEndpointUtil
from pipelines.scripts.synapse_artifact.synapse_notebook_util import SynapseNotebookUtil
from pipelines.scripts.synapse_artifact.synapse_pipeline_util import SynapsePipelineUtil
from pipelines.scripts.synapse_artifact.synapse_spark_configuration_util import SynapseSparkConfigurationUtil
from pipelines.scripts.synapse_artifact.synapse_sql_script_util import SynapseSQLScriptUtil
from pipelines.scripts.synapse_artifact.synapse_trigger_util import SynapseTriggerUtil
from pipelines.scripts.synapse_artifact.synapse_integration_runtime_util import SynapseIntegrationRuntimeUtil
from typing import Type


class SynapseArtifactUtilFactory():
    artifact_util_map = {
        util_class.get_type_name(): util_class
        for util_class in [
            SynapseCredentialUtil,
            SynapseDatasetUtil,
            SynapseLinkedServiceUtil,
            SynapseManagedPrivateEndpointUtil,
            SynapseNotebookUtil,
            SynapsePipelineUtil,
            SynapseSparkConfigurationUtil,
            SynapseSQLScriptUtil,
            SynapseTriggerUtil,
            SynapseIntegrationRuntimeUtil
        ]
    }

    @classmethod
    def is_valid_type_name(cls, type_name: str) -> bool:
        return type_name in cls.artifact_util_map

    @classmethod
    def get(cls, type_name: str) -> Type[SynapseArtifactUtil]:
        if not cls.is_valid_type_name(type_name):
            raise ValueError(f"No artifact util class defined for type '{type_name}'")
        return cls.artifact_util_map[type_name]
