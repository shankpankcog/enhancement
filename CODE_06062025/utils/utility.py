# Databricks notebook source
from DataEngineering.Databricks.ETL.Code.utils.constants import *
import boto3
import json
from datetime import datetime, timezone, timedelta
import urllib3

# COMMAND ----------

def fetch_secrets_from_aws(secret_name):
      session = boto3.session.Session()
      client = session.client(
            service_name='secretsmanager',
            region_name='us-west-2'
      )
      try:
            get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
      except Exception as e:
            raise e
      secretString = get_secret_value_response['SecretString']
      return json.loads(secretString)

# COMMAND ----------

def get_headers_with_session_id(base_url):
    http = urllib3.PoolManager()
    auth_url = base_url + '/auth'
    auth_req = requests.post(auth_url, data={'username': secret_details['vault_username'], 
                                            'password': secret_details['vault_password']})
    auth_resp = auth_req.json()
    session_id = auth_resp['sessionId']
    headers = {'Authorization': session_id, 'Content-Type': 'application/json', 'Accept': 'application/json'}
    return http,headers

# COMMAND ----------

secret_details = fetch_secrets_from_aws(secret_name)
s3_bucket = secret_details['s3_bucket']
user_name = secret_details['user_name']
base_url = secret_details['base_url']
timestamp = datetime.utcnow().strftime("%Y_%m_%d_%H_%M_%S")
s3_key_data = f"landing/{timestamp}"
base_url = secret_details['base_url']
http, headers = get_headers_with_session_id(base_url)