# Databricks notebook source
# TODO remove the chunk ids in the embeddings_sync_table which are not present in the embeddings_table 
# TODO delete the older versions from embeddings_sync_table
# TODO ingest partial success documents and put index status as partial success for them - done
# TODO: TEST add conditional update: whenMatchedUpdate only if audit__updated_date is different - done

# COMMAND ----------

from io import BytesIO
import fitz 
import pandas as pd
from pyspark.sql.functions import col, explode, pandas_udf
from langchain.text_splitter import RecursiveCharacterTextSplitter
from pyspark.sql.functions import col, udf, length, pandas_udf, explode
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, LongType
from mlflow.deployments import get_deploy_client
import pyspark.sql.functions as F
from pyspark.sql.functions import input_file_name
from databricks.vector_search.client import VectorSearchClient
from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c
from langchain.vectorstores import DatabricksVectorSearch
from langchain.embeddings import DatabricksEmbeddings
from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
from langchain.chat_models import ChatDatabricks
from pyspark.sql.functions import regexp_extract
import time
import os
from io import StringIO
import pandas as pd
from pyspark.sql.functions import col
import datetime
from delta.tables import DeltaTable
from pyspark.storagelevel import StorageLevel

# COMMAND ----------

catalog_name = "pdm-pdm-dl-quality-docs-genai-dev"
schema_name = "gvault_test"

docs_metadata_table=f"`{catalog_name}`.{schema_name}.veeva_qdms_docs_metadata"
embeddings_table_name = f"`{catalog_name}`.`{schema_name}`.veeva_qdms_docs_embeddings"
embeddings_sync_table_name = f"`{catalog_name}`.`{schema_name}`.veeva_qdms_docs_embeddings_sync"
embeddings_dimension = 1024
embedding_endpoint = "pdm-dl-quality-docs-genai-amazon-titan-embed-text-v2-0"
# embedding_endpoint = "pdm-dl-quality-docs-genai-amazon-titan-embed-text-v1"
# embeddings_dimension = 1536

vs_endpoint_name = "vs_endpoint_gvault_test_sahil"
vs_index_fullname = f"{catalog_name}.{schema_name}.vs_index_veeva_qdms_docs"

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {embeddings_sync_table_name} (
  chunk_id BIGINT,
  document_number STRING,
  document_s3_path STRING,
  chunk_text STRING,
  embedding ARRAY <FLOAT>,
  document_total_pages INT,
  document_page_number INT,
  document_title STRING,
  document_type STRING,
  document_subtype STRING,
  document_name STRING,
  classification STRING,
  document_id BIGINT,
  document_status STRING,
  doc_control_name STRING,
  document_major_version BIGINT,
  document_minor_version BIGINT,
  owning_function_name STRING,
  owning_site_name STRING,
  is_latest_version BOOLEAN,
  file_modified_date TIMESTAMP,
  version_modified_date TIMESTAMP,
  audit__created_date TIMESTAMP DEFAULT current_timestamp(),
  audit__updated_date TIMESTAMP DEFAULT current_timestamp()
) LOCATION 's3://gilead-edp-pdm-dev-us-west-2-pdm-dl-quality-docs-genai/{embeddings_sync_table_name.split(".")[-1]}/'  
TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.feature.allowColumnDefaults = 'supported')""");
# filters colums explain

# COMMAND ----------

embeddings_table_df=spark.table(embeddings_table_name)
docs_metadata_df=spark.table(docs_metadata_table)
embeddings_sync_table_df=spark.table(embeddings_sync_table_name)

# COMMAND ----------

meta_ids_df=spark.sql(f"select metadata_id from {docs_metadata_table} where index_status='PENDING' and embedding_status in ('COMPLETED', 'PARTIAL_SUCCESS') ")

# meta_ids_df=spark.sql(f"select metadata_id, embedding_status from {docs_metadata_table} where embedding_status in ('PARTIAL_SUCCESS') ")

# COMMAND ----------

meta_rows=meta_ids_df.select("metadata_id", "embedding_status").collect()
meta_ids_list = [row.metadata_id for row in meta_rows]

# COMMAND ----------

chunk_text_df=embeddings_table_df.filter(F.col("metadata_id").isin(meta_ids_list))

# COMMAND ----------

docs_meta_trimmed = docs_metadata_df.drop("audit__updated_date")

embedding_sync_raw_df=chunk_text_df.join(docs_meta_trimmed, on='metadata_id', how='left').withColumnsRenamed({
    "status": "document_status"
}).withColumn("version_modified_date", F.to_timestamp(col("version_modified_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")).withColumn("file_modified_date", F.to_timestamp(col("file_modified_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
# embedding_sync_raw_df.display()

# COMMAND ----------

embeddings_sync_cols=embeddings_sync_table_df.columns
embeddings_sync_cols.remove("audit__created_date")
# embeddings_sync_cols.remove("audit__updated_date")
upsert_df=embedding_sync_raw_df.select(embeddings_sync_cols)

# COMMAND ----------

def upsert_rows(update_source_df, embeddings_sync_table_name, embeddings_sync_cols):
    print(f"updating embedding sync table {embeddings_sync_table_name}")

    if "audit__updated_date" in embeddings_sync_cols:
      embeddings_sync_cols.remove("audit__updated_date")

    # 1) Reference existing delta table
    embed_table = DeltaTable.forName(spark, embeddings_sync_table_name)

    # 2) Alias your new data
    sourceDf = update_source_df.alias("src")

    # 3) Perform the merge 
    (
    embed_table.alias("tgt")
      .merge(
          sourceDf,
          "src.chunk_id = tgt.chunk_id"
      ) 
      .whenMatchedUpdate(
        condition="tgt.audit__updated_date < src.audit__updated_date",
          set = {
            "embedding"      : "src.embedding",
            "chunk_text" : "src.chunk_text",
            "audit__updated_date": "current_timestamp()"
          }
      ).whenNotMatchedInsert(values={column:f"src.{column}"for column in embeddings_sync_cols})
      .execute()
    )
    print("update completed")

# COMMAND ----------

def update_metadata(update_meta_source_df, docs_metadata_table):
    print(f"updating metadata table {docs_metadata_table}")

    # 1) Reference existing delta table
    docs_meta_table = DeltaTable.forName(spark, docs_metadata_table)

    # 2) Alias your new data
    sourceDf = update_meta_source_df.alias("src")

    # 3) Perform the merge
    (
    docs_meta_table.alias("tgt")
      .merge(
          sourceDf,
          "tgt.metadata_id = src.metadata_id"
      )
      .whenMatchedUpdate(
          set = {
            "index_status"      : "src.index_status",
            "audit__updated_date": "current_timestamp()"
          }
      ).execute()
    )
    print("update completed")


# COMMAND ----------

upsert_rows(upsert_df, embeddings_sync_table_name, embeddings_sync_cols)

# COMMAND ----------

# spark.sql(f"select * from {docs_metadata_table} where doc_control_name != 'Information Technology' ").display()

# COMMAND ----------

def wait_for_index_no_update_status(index, interval: int = 5, timeout: int = 900):
    """
    Poll the index sync status every `interval` seconds until it reports 'READY' or until `timeout` seconds elapsed.
    Raises a TimeoutError if sync does not complete in time.
    """
    start_time = time.time()
    while True:
        status = index.describe()
        detailed_status=status["status"]["detailed_state"]
        print(f"Current sync status: {detailed_status}")

        if "FAIL" in detailed_status:
            print(f"Index sync failed. full status details: {status}")
            raise Exception("Error while indexing data")

        if detailed_status == "ONLINE_NO_PENDING_UPDATE":
            print(f"Index sync completed successfully.")
            return
        if time.time() - start_time > timeout:
            print(
                f"Sync for index did not complete within {timeout} seconds."
            )
            return
        time.sleep(interval)


# Initialize client
vsc = VectorSearchClient(disable_notice=True)

# 1) Fetch existing indexes on this endpoint
existing_indexes = vsc.list_indexes(vs_endpoint_name)

# 2) Check if our target index is already there
if any(idx["name"] == vs_index_fullname for idx in existing_indexes["vector_indexes"]):
    print(f"Index {vs_index_fullname} already exists on endpoint {vs_endpoint_name} – skipping creation.")
    print("invoking sync")
    index=vsc.get_index(vs_endpoint_name, vs_index_fullname)
    index.sync()
    wait_for_index_no_update_status(index)
    print("sync completed successfully")
else:
    # 3) Create it if missing
    source_sync_table=embeddings_sync_table_name.replace('`', '')
    print(f"Creating index {vs_index_fullname} on endpoint {vs_endpoint_name}... and enabling sync with the table {source_sync_table}")
    vsc.create_delta_sync_index_and_wait(
        endpoint_name=vs_endpoint_name,
        index_name=vs_index_fullname,
        source_table_name=source_sync_table,
        pipeline_type="TRIGGERED",
        embedding_vector_column='embedding',
        primary_key='chunk_id',
        embedding_model_endpoint_name=embedding_endpoint,
        embedding_dimension=embeddings_dimension
    )
    print(f"Index named {vs_index_fullname} created successfully.")

# COMMAND ----------

rows = [(row.metadata_id,row.embedding_status) for row in meta_rows]

schema = StructType([
    StructField("metadata_id", LongType(), nullable=False),
    StructField("index_status", StringType(), nullable=False)
])

update_meta_df = (
    spark.createDataFrame(rows, schema)
)

update_metadata(update_meta_df, docs_metadata_table)