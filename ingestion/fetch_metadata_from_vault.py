# Databricks notebook source
import requests
import os
import json
import pandas as pd
import tempfile
import time
import logging
import boto3
from datetime import datetime, timezone, timedelta
import pyspark.sql.functions as f
import math
from io import StringIO
from DataEngineering.Databricks.ETL.Code.utils.constants import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run ../utils/utility

# COMMAND ----------

def get_job_status(request_url, headers, timeout_seconds):
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout_seconds:
            print('Timeout reached. Job status not obtained.')
            return None
        try:
            response = requests.get(request_url, headers=headers)
            job_status = response.json()
            status = job_status['data']['status']
            if status in ['SUCCESS', 'ERRORS_ENCOUNTERED']:
                print(job_status)
                print('Job Status: ' + status)
                return status
            else:
                print('job status:', status)
        except KeyError:
            print('KeyError occurred. Ignoring and retrying...')
        
        time.sleep(2)

# COMMAND ----------

def write_metadata_to_s3(result, batch_size):
    csv_file = StringIO(result)
    pandas_df = pd.read_csv(csv_file, low_memory=False)
    metadata_df = spark.createDataFrame(pandas_df)
    metadata_df = (
        metadata_df.withColumn(
            "dst_path", f.concat(f.col("id"), f.lit("/"), f.col("document_number__v"))
        )
    )
    new_column_names = [col.replace(".", "_").removesuffix("__v") for col in metadata_df.columns]

    metadata_df = metadata_df.toDF(*new_column_names)
    metadata_df = metadata_df.withColumn("index", f.monotonically_increasing_id())
    metadata_df = metadata_df.withColumn(
        "batch_id", (f.col("index") % f.lit(batch_size))
    )
    return metadata_df

# COMMAND ----------

def create_vql(query_config):
    query_parts = []

    def add_query_part(field, values, operator="CONTAINS"):
        values_str = ', '.join(f"'{value}'" for value in values)
        query_parts.append(f"{field} {operator} ({values_str})")

    if "document_number__v" in query_config:
        add_query_part("document_number__v", query_config["document_number__v"])

    if "doc_control_unit_name" in query_config:
        add_query_part("document_doc_control_unit__cr.name__v", query_config["doc_control_unit_name"])

    if "type_filters" in query_config:
        type_filter_parts = []
        for type_filter in query_config["type_filters"]:
            type_part = f"type__v = '{type_filter['type']}'"
            if "subtypes" in type_filter:
                subtype_part = f"subtype__v = '{type_filter['subtypes'][0]}'"
                type_filter_parts.append(f"(({type_part}) AND ({subtype_part}))")
            else:
                type_filter_parts.append(type_part)
        query_parts.append(f"({' OR '.join(type_filter_parts)})")

    if "status" in query_config:
        add_query_part("status__v", query_config["status"])

    return " AND ".join(query_parts)

# COMMAND ----------

def create_filter(query_config):
    vqls = {}
    kite_filter_json = {'object_type': 'document_versions__v',
        'extract_options': 'include_renditions__v',
        'fields': columns,
        }
    kite_filter_json['vql_criteria__v'] = create_vql(query_config)
    vqls = [kite_filter_json]
    return vqls

# COMMAND ----------

def fetch_metadata_extraction_response(http, headers, vql):
    r = http.request("POST", f"{base_url}/services/loader/extract", 
                    headers=headers, body= json.dumps(vql))
    metadata_extraction_response =  json.loads(r.data.decode('utf-8'))
    return metadata_extraction_response

# COMMAND ----------

def check_job_status(headers, metadata_extraction_response):
    job_id_value = str(metadata_extraction_response['job_id'])
    request_url = f"{base_url}/services/jobs/{job_id_value}"
    get_job_status(request_url, headers, 3000)

# COMMAND ----------

def fetch_metadata(http, headers, metadata_extraction_response, user_name, batch_size):
    s3_client = boto3.resource('s3')
    files_staging_listing_path = '/services/file_staging/items'
    base_s3_path = f"s3://{s3_bucket}/"
    dest_path = f"{s3_key_data}/"
    kite_s3_raw_file_path = f"{base_s3_path}{dest_path}"
    job_id_value = str(metadata_extraction_response['job_id'])
    modified_url = f"{base_url}/services/loader/{job_id_value}/tasks/1/results"
    r = http.request("GET", modified_url, headers=headers)
    result = r.data.decode('utf-8')
    lines = result.split('\n')
    if len(lines) > 2:
        metadata_df = write_metadata_to_s3(result, batch_size)
        return metadata_df
    else:
        print("The result variable is empty or None.")

# COMMAND ----------

def rename_metadata(metadata_df):
    kite_s3_raw_file_path = f"s3://{s3_bucket}/{s3_key_data}"
    renamed_metadata = (
        metadata_df
        .withColumnsRenamed(column_renames)
        .withColumn(
            "document_s3_path",
            f.concat(
                f.lit(f"{kite_s3_raw_file_path}/batch_id="), 
                f.col("batch_id"),
                f.lit("/"),
                f.col("document_number"),
                f.lit("."),
                f.element_at(f.split(f.col("source_file_path"), "\\."), -1)
            )
        )
        .drop("index")
    )

    existing_schema = spark.table(metadata_table_name).schema

    updated_schema_metadata = renamed_metadata.select(
        *[
            f.col(field.name).cast(field.dataType) 
            for field in existing_schema 
            if field.name in renamed_metadata.columns
        ]
    )

    return updated_schema_metadata

# COMMAND ----------

vql = create_filter(query_config)

# COMMAND ----------

metadata_extraction_response = fetch_metadata_extraction_response(http, headers, vql)
check_job_status(headers, metadata_extraction_response)
metadata_df = fetch_metadata(http, headers, metadata_extraction_response, user_name, number_of_batches)
renamed_metadata = rename_metadata(metadata_df)

targetDF = DeltaTable.forName(spark, metadata_table_name)
sourceDF = renamed_metadata

(targetDF.alias("target")
  .merge(sourceDF.alias("source"), merge_condition)
  .whenNotMatchedInsert(values=merge_values)
  .execute()
)