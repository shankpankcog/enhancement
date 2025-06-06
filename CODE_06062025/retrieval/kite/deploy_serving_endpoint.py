import mlflow
import os
from mlflow.models.resources import (
    DatabricksServingEndpoint,
    DatabricksVectorSearchIndex
)
from databricks.agents import deploy, set_review_instructions
import global_configs

# Set the registry URI to Unity Catalog
uc_label = global_configs.uc_label
mlflow.set_registry_uri(uc_label)    # changed

input_example = {
    "messages": [{
        "role": "user",
        "content": "Define the responsibilities of process development?"
    }],
    "custom_inputs": {"filters": {"doc_control_name": "Kite HQ","document_type": ["Specification","Governance and Procedures"], "document_subtype": ["Procedure","Material & Manufacturing"]}},
}

embedding_model_endpoint = global_configs.embedding_model_endpoint
llm_model_endpoint = global_configs.llm_model_endpoint
vs_index_fullname = global_configs.vs_index_fullname

resources_list = [
    DatabricksServingEndpoint(
        endpoint_name=embedding_model_endpoint   # changed
        # endpoint_name=f"pdm-dl-quality-docs-genai-amazon-titan-embed-text-v2-0"   # changed
    ), 
    DatabricksServingEndpoint(
        endpoint_name=llm_model_endpoint  # changed
        # endpoint_name=f"pdm-dl-quality-docs-genai-anthropic-claude-3-5-sonnet-20241022"  # changed
    ),
    DatabricksVectorSearchIndex(
        index_name=vs_index_fullname  # changed
        # index_name=f"pdm-pdm-dl-quality-docs-genai-dev.gvault_test.vs_index_veeva_qdms_docs",  # changed
    )
]

deployment_mlflow_run_name = global_configs.deployment_mlflow_run_name

with mlflow.start_run(run_name=deployment_mlflow_run_name):
# with mlflow.start_run(run_name="veeva_qdms_kite_pilot"):
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

# Constants
# catalog = "pdm-pdm-dl-quality-docs-genai-dev"
# db = "gvault_test"
# MODEL_NAME = "veeva-qdms-kite-pilot"
# MODEL_NAME_FQN = f"{catalog}.{db}.{MODEL_NAME}"    # FQN - Fully Qualified Name

model_name_fqn = global_configs.deployment_model_name_fqn
# Register to UC
uc_registered_model_info = mlflow.register_model(
    model_uri=logged_chain_info.model_uri, 
    name=MODEL_NAME_FQN
)
# uc_registered_model_info                                #Console

model_version=uc_registered_model_info.version
deployment_inter_endpoint_name = "pdm-dl-quality-docs-genai-veeva-qdms-kite-pilot"
deployment_info = deploy(
    model_name = MODEL_NAME_FQN, 
    model_version=model_version, 
    scale_to_zero=True, 
    wait_ready=True, 
    endpoint_name=deployment_inter_endpoint_name
)


# Optional...
instructions_to_reviewer = f"""## Instructions for Testing the Q&A Assistant chatbot

Your inputs are invaluable for the developer team. By providing detailed feedback and corrections, you help us fix issues and improve the overall quality of the application. We rely on your expertise to identify any gaps or areas needing enhancement."""

# Add the user-facing instructions to the Review App
set_review_instructions(MODEL_NAME_FQN, instructions_to_reviewer)
