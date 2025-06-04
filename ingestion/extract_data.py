# Databricks notebook source
from io import BytesIO
import fitz 
import pandas as pd
from pyspark.sql.functions import col, explode, pandas_udf
from pyspark.sql.functions import col, udf, length, pandas_udf, explode
from pyspark.sql.types import ArrayType, StringType
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
import pymupdf
import pymupdf4llm
from pyspark.sql.functions import col
import datetime
from delta.tables import DeltaTable
import boto3
from pyspark.storagelevel import StorageLevel
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# COMMAND ----------

# spark.sql.files.minPartitionNum
# spark.conf.set("spark.sql.files.minPartitionNum", 190)
# property did not work from notebook

# COMMAND ----------

catalog_name = "pdm-pdm-dl-quality-docs-genai-dev"
schema_name = "gvault_test"

docs_metadata_table=f"`{catalog_name}`.{schema_name}.veeva_qdms_docs_metadata"
doc_pages_table_name = f"`{catalog_name}`.`{schema_name}`.veeva_qdms_docs_pages"
doc_pages_staging_table_name = f"`{catalog_name}`.`{schema_name}`.veeva_qdms_docs_pages_staging"
doc_pages_staging_s3_path=f"s3://gilead-edp-pdm-dev-us-west-2-pdm-dl-quality-docs-genai/{doc_pages_staging_table_name.split(".")[-1]}/"

# COMMAND ----------

# spark.sql(f"DROP TABLE IF EXISTS {embedding_table_name}");
spark.sql(f"""CREATE TABLE IF NOT EXISTS {doc_pages_table_name} (
  metadata_id BIGINT,
  document_s3_path STRING,
  pdf_text array<struct<page_number:int,page_text:string,total_pages:int>>,
  extraction_status STRING,
  audit__created_date TIMESTAMP DEFAULT current_timestamp(),
  audit__updated_date TIMESTAMP DEFAULT current_timestamp()
) LOCATION 's3://gilead-edp-pdm-dev-us-west-2-pdm-dl-quality-docs-genai/{doc_pages_table_name.split(".")[-1]}/' 
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""");

# COMMAND ----------

from pyspark.sql.functions import pandas_udf


def get_pdf_bytes_from_s3(s3_full_path: str, aws_region: str = "us-west-2") -> bytes:
    """
    Download a PDF file from S3 and return its bytes.
    """
    s3 = boto3.client('s3', region_name=aws_region)
    bucket, key = s3_full_path.replace("s3://", "").replace("s3a://", "").replace("s3n://", "").split("/", 1)
    response = s3.get_object(Bucket=bucket, Key=key)
    return response['Body'].read()

def extract_text_from_pdf(s3_full_path: str):
    try:
        pdf_bytes = get_pdf_bytes_from_s3(s3_full_path)
        doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
        pages = pymupdf4llm.to_markdown(doc=doc, page_chunks=True)
        data=[{"page_number": page["metadata"]["page"], "page_text": page["text"], "total_pages": page["metadata"]["page_count"]} for page in pages]
        return ("COMPLETED", data)
    except Exception as e:
        return ("FAILED", [{"metadata": {"page": -1, "page_count": -1}, "text": f"error: {str(e)}"}])

# def process_with_udf(iterator):
#     results = []
#     for row in iterator:
#         # Apply custom transformations using row values
#         text = extract_text_from_pdf(row.document_s3_path)
#         results.append([text])
#     return iter(results)


schema = StructType([
    StructField("status", StringType(), True),
        StructField("pages", ArrayType(
        StructType([
            StructField("page_number", IntegerType(), True),
            StructField("page_text", StringType(), True),
            StructField("total_pages", IntegerType(), True)
        ])
    ), True)
])

# @pandas_udf(schema)
# def extract_pdf_text_udf(s3_path: pd.Series) -> pd.Series:
#     return s3_path.apply(extract_text_from_pdf)

@pandas_udf(schema)
def extract_pdf_text_udf(s3_path: pd.Series) -> pd.DataFrame:
    # apply your function, getting a Series of tuples (status, pages)
    results = s3_path.apply(extract_text_from_pdf)
    # separate into two Series
    statuses = results.map(lambda tpl: tpl[0])
    pages_arr = results.map(lambda tpl: tpl[1])
    # ***MUST*** return a DataFrame when the returnType is StructType
    return pd.DataFrame({
        "status": statuses,
        "pages": pages_arr
    })

# COMMAND ----------

def extract(meta_df, extraction_re_partition_count):
    df_repartitioned = meta_df.repartition(extraction_re_partition_count)
    data_df = (
    df_repartitioned
    .withColumn("pdf_tuple", extract_pdf_text_udf(col("document_s3_path")))
    .select(
        "metadata_id",
        "document_s3_path",
        col("pdf_tuple.pages").alias("pdf_text"),
        col("pdf_tuple.status").alias("extraction_status")
    )
    )
    return data_df

# COMMAND ----------

def upsert_extract_data(upsert_source_df, doc_pages_table_name):
    print(f"updating doc pages table {doc_pages_table_name}")

    # 1) Reference existing delta table
    doc_pages_table = DeltaTable.forName(spark, doc_pages_table_name)

    # 2) Alias your new data
    sourceDf = upsert_source_df.alias("src")

    # 3) Perform the merge
    (
    doc_pages_table.alias("tgt")
      .merge(
          sourceDf,
          "tgt.metadata_id = src.metadata_id"
      )
    #    When records already exist, update just the status columns
      .whenMatchedUpdate(
          set = {
            "pdf_text"      : "src.pdf_text",
            "extraction_status" : "src.extraction_status",
            "audit__updated_date": "current_timestamp()"
          }
      ).whenNotMatchedInsert(
            values={
                "metadata_id": "src.metadata_id",
                "document_s3_path": "src.document_s3_path",
                "pdf_text": "src.pdf_text",
                "extraction_status": "src.extraction_status",
                "audit__created_date": "current_timestamp()",
                "audit__updated_date": "current_timestamp()"
            }
        )
      .execute()
    )
    print("upsert completed")

# COMMAND ----------

def update_metadata(data_df, docs_metadata_table):
    print(f"updating metadata table {docs_metadata_table}")

    # 1) Reference existing delta table
    docs_meta_table = DeltaTable.forName(spark, docs_metadata_table)

    # 2) Alias your new data
    sourceDf = data_df.alias("src")

    # 3) Perform the merge
    (
    docs_meta_table.alias("tgt")
      .merge(
          sourceDf,
          "tgt.metadata_id = src.metadata_id"
      )
    #    When records already exist, update just the status columns
      .whenMatchedUpdate(
          set = {
            "extraction_status"      : "src.extraction_status",
            "audit__updated_date": "current_timestamp()"
          }
      ).execute()
    )
    print("update completed")

# COMMAND ----------

meta_df=spark.sql(f"select metadata_id, document_s3_path from {docs_metadata_table} where extraction_status='PENDING' and download_status='COMPLETED'")
# meta_df=spark.sql(f"select metadata_id, document_s3_path from {docs_metadata_table} where extraction_status='PENDING' and download_status='COMPLETED'")
# meta_df=spark.sql(f"select metadata_id, document_s3_path from {docs_metadata_table} where doc_control_name != 'Information Technology' ")
# and doc_control_name != 'Information Technology'

# COMMAND ----------

# spark.catalog.clearCache()

# COMMAND ----------

persisted_meta_df=meta_df.persist(StorageLevel.DISK_ONLY)

# COMMAND ----------

def get_re_partition_count():
    try:
        extraction_re_partition_count = int(dbutils.widgets.get("extraction_re_partition_count"))
        return extraction_re_partition_count
    except:
        print("Not able to fetch extraction_re_partition_count")
        return 50

# COMMAND ----------

extraction_re_partition_count=get_re_partition_count()
# data_df=extract(persisted_meta_df, extraction_re_partition_count)

staging_df = extract(persisted_meta_df, extraction_re_partition_count)
(staging_df
   .write
   .format("parquet")
   .mode("overwrite")
   .option("path", doc_pages_staging_s3_path)
   .saveAsTable(doc_pages_staging_table_name))

staging_table_df = spark.table(doc_pages_staging_table_name)
upsert_extract_data(staging_table_df, doc_pages_table_name)

# COMMAND ----------

persisted_meta_df.createOrReplaceTempView("persisted_meta_df")

# COMMAND ----------

updated_meta_df=spark.sql(f"select metadata_id, extraction_status from {doc_pages_table_name} where metadata_id in (select metadata_id from persisted_meta_df)")

# COMMAND ----------

update_metadata(updated_meta_df, docs_metadata_table)

# COMMAND ----------

# %sql
# select * from `pdm-pdm-dl-quality-docs-genai-dev`.gvault_test.veeva_qdms_docs_metadata where doc_control_name != 'Information Technology'

# COMMAND ----------

# %sql
# update `pdm-pdm-dl-quality-docs-genai-dev`.gvault_test.veeva_qdms_docs_metadata set extraction_status = 'PENDING', chunking_status = 'PENDING', embedding_status = 'PENDING', index_status = 'PENDING' where doc_control_name != 'Information Technology'