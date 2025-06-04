# Databricks notebook source
import pyspark.sql.functions as f
import urllib3
import requests
from urllib.parse import urlparse
from DataEngineering.Databricks.ETL.Code.utils.constants import *
from delta.tables import DeltaTable
from pyspark.sql.functions import pandas_udf
import pandas as pd
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ../utils/utility

# COMMAND ----------

def update_download_status(df):
    metadata_df = spark.table(metadata_table_name)
    
    print(f"Size of updated dataframe is {df.count()}")

    targetDF = DeltaTable.forName(spark, metadata_table_name)
    targetDF.alias("target").merge(
        df.alias("updates"), "target.metadata_id = updates.metadata_id"
    ).whenMatchedUpdate(
        set={
            "download_status": "updates.status",
            "audit__updated_date": f.to_utc_timestamp(f.current_timestamp(), "UTC"),
        }
    ).execute()

# COMMAND ----------

def update_is_latest_status(df):
    metadata_df = spark.table(metadata_table_name)
    
    print(f"Size of updated dataframe for latest status is {df.count()}")

    targetDF = DeltaTable.forName(spark, metadata_table_name)
    targetDF.alias("target").merge(
        df.alias("updates"), "target.metadata_id = updates.metadata_id"
    ).whenMatchedUpdate(
        set={
            "is_latest_doc_in_dl": "false",
            "audit__updated_date": f.to_utc_timestamp(f.current_timestamp(), "UTC"),
        }
    ).execute()

# COMMAND ----------

def write_document_to_s3bucket(headers, file_download_url, s3_path):
    # Parse out bucket and key
    parsed = urlparse(s3_path)
    if parsed.scheme != 's3':
        raise ValueError(f"Invalid S3 path: {s3_path} (must start with s3://)")
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')

    # Create the S3 client
    s3 = boto3.client('s3')

    print(f"Uploading to S3 bucket={bucket}, key={key}")
    print(f"Downloading from {file_download_url}")

    try:
        with requests.get(file_download_url, headers=headers, stream=True) as r:
            r.raise_for_status()
            content_type = r.headers.get('content-type', '')

            if 'application/octet-stream' in content_type:
                r.raw.decode_content = True
                s3.upload_fileobj(r.raw, Bucket=bucket, Key=key)
                print("Upload complete.")
                return True
            else:
                print(f"Unexpected content-type: {content_type}; not uploading.")
                print(r.text)
                return False
    except requests.HTTPError as e:
        print(f"HTTPError occurred: {e}")
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

# COMMAND ----------

def process_row_udf(partition_data):
    for row in partition_data:
        print(f"Processing for {row.document_number}")
        files_staging_listing_path = "/services/file_staging/items"
        file_path = row.source_file_path
        doc_val = row.document_number
        doc_type = doc_val.split("-")[0]
        listing_url = (
            f"{base_url}{files_staging_listing_path}/content/{user_name}{file_path}"
        )
        s3_path = row.document_s3_path
        print(f"listing_url: {listing_url}")
        print(f"s3_path: {s3_path}")
        status = write_document_to_s3bucket(headers, listing_url, s3_path)
        if status:
            yield [row.metadata_id, STATUS_COMPLETED]
        else:
            yield [row.metadata_id, STATUS_FAILED]

# COMMAND ----------

def download_documents(metadata_df):
    metadata_df = metadata_df.repartition(number_of_partitions_for_download)
    df = metadata_df.rdd.mapPartitions(process_row_udf).toDF(["metadata_id", "status"])
    df.write.mode("overwrite").saveAsTable(download_status_table_name)

# COMMAND ----------

def check_for_is_latest():
    window_spec = Window.orderBy(f.desc("document_major_version"), f.desc("document_minor_version")).partitionBy("document_number")

    df = spark.table(metadata_table_name).withColumn("rank", f.rank().over(window_spec))

    old_docs_df = df.select("metadata_id").orderBy(f.col("rank").desc()).filter(f.col("rank") > 1)
    return old_docs_df

# COMMAND ----------

success_list = []
failed_list = []
metadata = spark.table(metadata_table_name).filter(f.col("download_status") == "PENDING")
print(f"Number of documents to download: {metadata.count()}")
download_documents(metadata)

# COMMAND ----------

df = spark.read.table(download_status_table_name)
update_download_status(df)

# COMMAND ----------

old_docs_df = check_for_is_latest()
update_is_latest_status(old_docs_df)