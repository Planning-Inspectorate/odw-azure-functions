# Introduction
This repo contains everything related to Azure Functions used by the PINS Operational Data Warehouse (ODW)

# Directory structure
```
├── odw-azure-functions
│    ├── .github/ # Where github workflows are defined
│    ├── functions/ # Where Azure Functions are defined
│    ├── images/ # Documentation
│    ├── pipelines/ # Where ADO deployment pipeline functionality is defined
│    │   └── stages/ # ADO Stages to run in the pipeline
│    │   └── jobs/ # ADO Jobs to run in the stages
│    │   └── steps/ # Ado Steps to run in the jobs
│    │   └── scripts/ # Pythons scripts used by ADO Steps
│    │   └── azure-pipelines.yaml # Main ADO deployment pipeline
│    ├── settings.json # Settings for the Azure Functions
```

# Getting Started
The following steps outline how to get up and running with this repo on your own system:
1.  Environment access
    1.  Github access - if you're reading this repo readme you probably already have this
    2.  Azure DevOps access to the [operational-data-warehouse](https://dev.azure.com/planninginspectorate/operational-data-warehouse) Azure DevOps project
    3.  Azure Portal access - additional access is required to the Azure Portal and the corresponding [Azure Resources in each environment](#environments)
2.  Application Installation - the following desktop applications are optional but provide advantages when working with some of the Azure resources - PINS Azure auth policy is to restrict access to PINS devices only so non-PINS devices will need to be whitelisted to use these
      1. Install [Visual Studio Code](https://code.visualstudio.com/) or equivalent IDE - for editing and commiting code artifacts
      2. Install [Azure Data Studio](https://docs.microsoft.com/en-us/sql/azure-data-studio) - for connecting to Azure SQL instances and managing/commiting data notebooks
      3. Install [Microsoft Azure Storage Explorer](https://azure.microsoft.com/en-us/features/storage-explorer/)
3.  Clone Repo
    1. Create a Personal Access Token in GitHub or use another authentication method e.g. SSH
    2. Clone the repo in VSCode/Azure Data Studio to a local folder

# Environments
The ODW environment is deployed to three Azure subscriptions as follows:

| Environment Name | Subscription Name | Subscription ID |
|------------------|-------------------|-----------------|
| Build | pins-odw-data-build-sub | 12806449-ae7c-4754-b104-65bcdc7b28c8 |
| Development | pins-odw-data-dev-sub | ff442a29-fc06-4a13-8e3e-65fd5da513b3 |
| Pre-Production | pins-odw-data-preprod-sub | 6b18ba9d-2399-48b5-a834-e0f267be122d |
| Production | pins-odw-data-prod-sub | a82fd28d-5989-4e06-a0bb-1a5d859f9e0c |

Within each subscription, the infrastructure is split into several resource groups, aligned to the [data landing zone architecture](https://learn.microsoft.com/en-us/azure/cloud-adoption-framework/scenarios/cloud-scale-analytics/architectures/data-landing-zone#data-landing-zone-architecture):

| Resource Group Name | Description |
|---------------------|---------|
| pins-rg-function-app-odw-_{env}_-_{region}_ | Contains the Function app |
| pins-rg-data-odw-_{env}_-_{region}_ | Contains the Data Lake and Synapse Workspace resources |
| pins-rg-data-odw-_{env}_-_{region}_-synapse-managed | Managed resource group for the Synapse Workspace |
| pins-rg-datamgmt-odw-_{env}_-_{region}_ | Contains data management resource such as Purview and Bastion VM(s) |
| pins-rg-datamgmt-odw-_{env}_-_{region}_-purview-managed | Managed resource group for the Purview Account |
| pins-rg-devops-odw-_{env}_-_{region}_ | Contains Azue DevOps agents for deployments into the private network |
| pins-rg-monitoring-odw-_{env}_-_{region}_ | Contains monitoring resources such as Log Analytics and App Insights |
| pins-rg-network-odw-_{env}_-global | Contains private DNS zones for private-link-enabled resources |
| pins-rg-network-odw-_{env}_-_{region}_ | Contains the virtual network, network security groups and private endpoints |
| pins-rg-shir-odw-_{env}_-_{region}_ | Contains self-hosted integration runtime VM(s) used by the Synapse Workspace |

Some of the key resources used in the deployment are:
| Resource Name | Description |
|---------------|-------------|
| Function App | Serverless compute service used for processing some of the data in the ODW |
| Synapse Workspace | Analytics product for loading, transforming and analysing data using SQL and/or Spark |
| ADLS Storage Account | Hierarchical namespace enabled Storage Account to act as a data lake |
| Key Vault | Secrets storage for connection strings, password, etc for connected services |
| Log Analytics | Activity and metric diagnostic log storage with querying capabilities using KQL |