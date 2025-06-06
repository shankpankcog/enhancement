# Databricks notebook source
# TODO put embedding status in doc meta table for partial success and failed for docs where no pages are extracted - done
# TODO add reparitioning based number of chunks

# COMMAND ----------

from io import BytesIO
import fitz 
import pandas as pd
from pyspark.sql.functions import col, explode, pandas_udf
from langchain.text_splitter import RecursiveCharacterTextSplitter
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
from delta.tables import DeltaTable

# COMMAND ----------

catalog_name = "pdm-pdm-dl-quality-docs-genai-dev"
schema_name = "gvault_test"

docs_metadata_table=f"`{catalog_name}`.{schema_name}.veeva_qdms_docs_metadata_sample_test"
doc_pages_table_name = f"`{catalog_name}`.`{schema_name}`.veeva_qdms_docs_pages_sample_test"
embeddings_table_name = f"`{catalog_name}`.`{schema_name}`.veeva_qdms_docs_embeddings_sample_test"

embedding_endpoint = "pdm-dl-quality-docs-genai-amazon-titan-embed-text-v2-0" # Need to create new resource...
embedding_staging_table_name = f"`{catalog_name}`.`{schema_name}`.veeva_qdms_docs_embeddings_staging_sample_test"
embedding_staging_s3_path=f"s3://gilead-edp-pdm-dev-us-west-2-pdm-gvault-platform/gvault/IT SOP/{embedding_staging_table_name.split('.')[-1]}/"
# embedding_endpoint = "pdm-dl-quality-docs-genai-amazon-titan-embed-text-v1"
# pdm-dl-quality-docs-genai-amazon-titan-embed-text-v1

# COMMAND ----------

schema = StructType([
    StructField("status", StringType(), True),
    StructField("embedding", ArrayType(DoubleType()), True)
])

@pandas_udf(schema)
def get_embedding(contents: pd.Series) -> pd.Series:
    deploy_client = get_deploy_client("databricks")

    def get_single_embedding(text):
        max_retries = 4
        if (text==None) or (text==""):
            return ("FAILED", [])
        for attempt in range(max_retries):
            try:
                response = deploy_client.predict(
                    endpoint=embedding_endpoint, 
                    inputs={"input": [text]}
                )
                return ("COMPLETED", response["data"][0]["embedding"])
            except Exception as e:
                print(f"Attempt {attempt + 1}, Error: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    return ("FAILED", [])

    all_embeddings = contents.apply(get_single_embedding)
    status = all_embeddings.map(lambda tpl: tpl[0])
    embeddings = all_embeddings.map(lambda tpl: tpl[1])
    # ***MUST*** return a DataFrame when the returnType is StructType
    return pd.DataFrame({
        "status": status,
        "embedding": embeddings
    })
    
    # return all_embeddings

# COMMAND ----------

def embed_data(data_df, embedding_re_partition_count):
    # print("creating chunk embeddings")
    df_repartitioned = data_df.repartition(embedding_re_partition_count)
    data_df_repartitioned=df_repartitioned.withColumn("embedding_tuple", get_embedding("chunk_text")).select('chunk_id','metadata_id', F.col('embedding_tuple.embedding').alias("embedding"), F.col("embedding_tuple.status").alias("embedding_status"))
# .withColumn("embedding", col("embedding_tuple")[1]).withColumn("embedding_status", col("embedding_tuple")[0])
    return data_df_repartitioned.select("chunk_id", "metadata_id","embedding", "embedding_status")



# COMMAND ----------

def update_emdeddings(update_source_df, embedding_table_name):
    print(f"updating embedding table {embedding_table_name}")

    # 1) Reference existing delta table
    embed_table = DeltaTable.forName(spark, embedding_table_name)

    # 2) Alias your new data
    sourceDf = update_source_df.alias("src")

    # 3) Perform the merge
    (
    embed_table.alias("tgt")
      .merge(
          sourceDf,
          "tgt.chunk_id = src.chunk_id"
      )
    #    When records already exist, update just the status columns
      .whenMatchedUpdate(
          set = {
            "embedding"      : "src.embedding",
            "embedding_status" : "src.embedding_status",
            "audit__updated_date": "current_timestamp()"
          }
      ).execute()
    )
    print("update completed")

# COMMAND ----------

def update_metadata(update_source_df, docs_metadata_table):
    print(f"updating metadata table {docs_metadata_table}")
    # update_meta_source_df=update_source_df.select("metadata_id", "embedding_status").distinct()

#     update_meta_source_df = (
#     update_source_df
#     .groupBy("metadata_id")
#     .agg(
#         # count how many rows are completed
#         F.count(F.when(F.col("embedding_status") == "COMPLETED", True)).alias("n_completed"),
#         F.count("*").alias("n_total")
#     )
#     .withColumn(
#         "final_status",
#         F.when(F.col("n_completed") == F.col("n_total"), F.lit("COMPLETED"))
#          .otherwise(F.lit("FAILED"))
#     )
#     .select("metadata_id", "final_status")
# )

    update_meta_source_df = (
        update_source_df
        .groupBy("metadata_id")
        .agg(
            # how many COMPLETED vs. total
            F.count(F.when(F.col("embedding_status") == "COMPLETED", True)).alias("n_completed"),
            F.count("*").alias("n_total")
        )
        .withColumn(
            "final_status",
            F.when(F.col("n_completed") == F.col("n_total"), F.lit("COMPLETED"))        # all done
            .when(F.col("n_completed") > 0, F.lit("PARTIAL_SUCCESS"))                   # some done
            .otherwise(F.lit("FAILED"))                                                  # none done
        )
        .select("metadata_id", "final_status")
    )

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
            "embedding_status"      : "src.final_status",
            "audit__updated_date": "current_timestamp()"
          }
      ).execute()
    )
    print("update completed")


# COMMAND ----------

meta_ids_df=spark.sql(f"select metadata_id from {docs_metadata_table} where embedding_status='PENDING' and chunking_status = 'COMPLETED'")

# COMMAND ----------

meta_ids_list = [row.metadata_id for row in meta_ids_df.select("metadata_id").collect()]

# COMMAND ----------

chunk_text_df=spark.table(embeddings_table_name).select("metadata_id", "chunk_id", "chunk_text").filter(F.col("metadata_id").isin(meta_ids_list))

# COMMAND ----------

def get_re_partition_count():
    try:
        embedding_re_partition_count = int(dbutils.widgets.get("embedding_re_partition_count"))
        return embedding_re_partition_count
    except:
        print("Not able to fetch embedding_re_partition_count")
        return 100

# COMMAND ----------

embedding_re_partition_count=get_re_partition_count()
# update_source_df=embed_data(chunk_text_df, embedding_re_partition_count)

staging_df = embed_data(chunk_text_df, embedding_re_partition_count)
(staging_df
   .write
   .format("parquet")
   .mode("overwrite")
   .option("path", embedding_staging_s3_path)
   .saveAsTable(embedding_staging_table_name))

update_source_df = spark.table(embedding_staging_table_name)

# COMMAND ----------

update_emdeddings(update_source_df, embeddings_table_name)

# COMMAND ----------

updated_meta_df=spark.table(embeddings_table_name).select("metadata_id", "embedding_status").filter(F.col("metadata_id").isin(meta_ids_list))

# COMMAND ----------

# updated_meta_df=spark.table(embeddings_table_name).select("metadata_id", "embedding_status")

# COMMAND ----------

update_metadata(updated_meta_df, docs_metadata_table)

# COMMAND ----------

# spark.sql(f"select * from {docs_metadata_table} where doc_control_name != 'Information Technology' ").display()