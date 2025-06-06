# ----------------------------------- Ingestion Configs ----------------------------------- #
catalog_name = "pdm-pdm-dl-quality-docs-genai-dev"
schema_name = "gvault_test"
# Extract data
docs_metadata_table=f"`{catalog_name}`.{schema_name}.veeva_qdms_docs_metadata_sample_test"
doc_pages_table_name = f"`{catalog_name}`.`{schema_name}`.veeva_qdms_docs_pages_sample_test"
doc_pages_staging_table_name = f"`{catalog_name}`.`{schema_name}`.veeva_qdms_docs_pages_staging_sample_test"
doc_pages_staging_s3_path=f"s3://gilead-edp-pdm-dev-us-west-2-pdm-gvault-platform/gvault/IT SOP/{doc_pages_staging_table_name.split(".")[-1]}/"
# Chunk data
docs_metadata_table=f"`{catalog_name}`.{schema_name}.veeva_qdms_docs_metadata_sample_test"
doc_pages_table_name = f"`{catalog_name}`.`{schema_name}`.veeva_qdms_docs_pages_sample_test"
embeddings_table_name = f"`{catalog_name}`.`{schema_name}`.veeva_qdms_docs_embeddings_sample_test"
# Embed data
docs_metadata_table=f"`{catalog_name}`.{schema_name}.veeva_qdms_docs_metadata_sample_test"
doc_pages_table_name = f"`{catalog_name}`.`{schema_name}`.veeva_qdms_docs_pages_sample_test"
embeddings_table_name = f"`{catalog_name}`.`{schema_name}`.veeva_qdms_docs_embeddings_sample_test"
embedding_staging_table_name = f"`{catalog_name}`.`{schema_name}`.veeva_qdms_docs_embeddings_staging_sample_test"
embedding_staging_s3_path=f"s3://gilead-edp-pdm-dev-us-west-2-pdm-gvault-platform/gvault/IT SOP/{embedding_staging_table_name.split('.')[-1]}/"
## Resources...
embedding_endpoint = "pdm-dl-quality-docs-genai-amazon-titan-embed-text-v2-0"
# Index data
docs_metadata_table=f"`{catalog_name}`.{schema_name}.veeva_qdms_docs_metadata_sample_test"
embeddings_table_name = f"`{catalog_name}`.`{schema_name}`.veeva_qdms_docs_embedding_sample_tests"
embeddings_sync_table_name = f"`{catalog_name}`.`{schema_name}`.veeva_qdms_docs_embeddings_sync_sample_test"
vs_index_fullname = f"{catalog_name}.{schema_name}.vs_index_veeva_qdms_docs_sample_test"
## Resources...
embedding_endpoint = "pdm-dl-quality-docs-genai-amazon-titan-embed-text-v2-0"
vs_endpoint_name = "vs_endpoint_gvault_test_sahil"

# ----------------------------------- Retriver Configs ----------------------------------- #
uc_label = "databricks-uc"

catalog = "pdm-pdm-dl-quality-docs-genai-dev"
db      = "gvault_test"

vs_endpoint_name = "vs_endpoint_gvault_test_sahil"
embedding_model_endpoint = "pdm-dl-quality-docs-genai-amazon-titan-embed-text-v2-0"
llm_model_endpoint="pdm-dl-quality-docs-genai-anthropic-claude-3-5-sonnet-20241022"

deployment_inter_endpoint_name = "pdm-dl-quality-docs-genai-veeva-qdms-kite-pilot"    #is it restapi, sdk ?
deployment_model_name = "veeva-qdms-kite-pilot"  
deployment_mlflow_run_name="veeva_qdms_kite_pilot_sample_test"


# derived variables....
vs_index_fullname = f"{catalog}.{db}.vs_index_veeva_qdms_docs_sample_test"
deployment_model_name_fqn = f"{catalog}.{db}.{deployment_model_name}"

