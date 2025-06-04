# Databricks notebook source
# %restart_python

# COMMAND ----------

import mlflow
import os
# Set the registry URI to Unity Catalog
mlflow.set_registry_uri("databricks-uc")


from mlflow.models.resources import (
    DatabricksServingEndpoint,
    DatabricksVectorSearchIndex
)

## Constants
catalog = "pdm-pdm-dl-quality-docs-genai-dev"
db = "gvault_test"
MODEL_NAME = "veeva-qdms-kite-pilot"
MODEL_NAME_FQN = f"{catalog}.{db}.{MODEL_NAME}"

endpoint_name="pdm-dl-quality-docs-genai-veeva-qdms-kite-pilot"

input_example = {
    "messages": [
        {
            "role": "user",
            "content": "Define the responsibilities of process development?"
        }
    ],
    "custom_inputs": {"filters": {"doc_control_name": "Kite HQ","document_type": ["Specification","Governance and Procedures"], "document_subtype": ["Procedure","Material & Manufacturing"]}},
}

# COMMAND ----------

# MAGIC %md
# MAGIC #Register Model

# COMMAND ----------

resources_list = [DatabricksServingEndpoint(endpoint_name=f"pdm-dl-quality-docs-genai-amazon-titan-embed-text-v2-0"), DatabricksServingEndpoint(endpoint_name=f"pdm-dl-quality-docs-genai-anthropic-claude-3-5-sonnet-20241022"), DatabricksVectorSearchIndex(
        index_name=f"pdm-pdm-dl-quality-docs-genai-dev.gvault_test.vs_index_veeva_qdms_docs",
    )]



with mlflow.start_run(run_name="veeva_qdms_kite_pilot"):
    logged_chain_info = mlflow.langchain.log_model(
        lc_model=os.path.join(os.getcwd(), 'rag_chain.py'),  # Chain code file e.g., /path/to/the/chain.py 
        model_config='rag_chain_config.json',  # Chain configuration 
        artifact_path="rag_chain.py",  # Required by MLflow, the chain's code/config are saved in this directory
        input_example=input_example,
        pip_requirements=[
            "mlflow", "cloudpickle", "databricks-connect", "databricks-vectorsearch", "google-cloud-storage", "ipykernel", 
            "langchain-community", "langchain", "numpy", "pandas", "pyarrow", "pydantic", "pyspark", "databricks-langchain"
        ],  # Note: pip requirements added as a temporary workaround waiting for mlflow 2.20 to be released.
        example_no_conversion=True,
        resources = resources_list
    )

# Register to UC
uc_registered_model_info = mlflow.register_model(
    model_uri=logged_chain_info.model_uri, 
    name=MODEL_NAME_FQN
)

# COMMAND ----------

uc_registered_model_info

# COMMAND ----------

model_version=uc_registered_model_info.version


# COMMAND ----------

model_version

# COMMAND ----------

# MAGIC %md
# MAGIC #Deploy Model

# COMMAND ----------

from databricks.agents import deploy, set_review_instructions

# model_version=5

deployment_info = deploy(model_name = MODEL_NAME_FQN, model_version=model_version, scale_to_zero=True, wait_ready=True, endpoint_name=endpoint_name)

instructions_to_reviewer = f"""## Instructions for Testing the Q&A Assistant chatbot

Your inputs are invaluable for the developer team. By providing detailed feedback and corrections, you help us fix issues and improve the overall quality of the application. We rely on your expertise to identify any gaps or areas needing enhancement."""

# Add the user-facing instructions to the Review App
set_review_instructions(MODEL_NAME_FQN, instructions_to_reviewer)
