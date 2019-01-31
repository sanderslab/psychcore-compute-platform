from .google_sdk import DataprocDeleteStatusPoller, GoogleCloudLambdaAuth
from google.cloud import storage
import googleapiclient.discovery
from pprint import pprint
import boto3
import json
import os
import time
import uuid
import yaml
import sys

s3_client = boto3.resource('s3', region_name='us-east-1')

def get_dataproc_client():
    """Builds an http client authenticated with the service account credentials."""
    dataproc = googleapiclient.discovery.build('dataproc', 'v1')
    return dataproc

def gcp_dataproc_cluster_delete(event):
    dataproc = googleapiclient.discovery.build('dataproc', 'v1')
    project_id = event['gcp-administrative']['project']
    zone = event['gcp-administrative']['zone']
    try:
        region_as_list = zone.split('-')[:-1]
        region = '-'.join(region_as_list)
    except (AttributeError, IndexError, ValueError):
        raise ValueError('Invalid zone provided, please check your input.')
    cluster = event['dataproc-administrative']['cluster_name']

    print('Tearing down cluster...')
    request = dataproc.projects().regions().clusters().delete(
        projectId=project_id,
        region=region,
        clusterName=cluster)

    result = request.execute()

    print(result)
    return result

# Grab all environ variables
project_id = os.environ['project_id']
zone = os.environ['zone']
region = "{}-{}".format(zone.split('-')[0], zone.split('-')[1])
cluster_name = os.environ['cluster_name']

GCP_creds_str = os.environ['GCP_creds']
cloud_template = os.environ['cloud_file']
assets_uri = os.environ['assets_uri']
print(assets_uri)

# Convert GCP_creds_str into dict
format_creds = GCP_creds_str.replace("'", "\"")
GCP_creds = json.loads(format_creds)

# Download cloud_template which has API calls for
# dataproc_create, dataproc_validate, and dataproc_qc
cloud_bucket = assets_uri.split('/')[2]
cloud_prefix = '/'.join(assets_uri.split('/')[3:-1])
cloud_key = '{}/{}'.format(cloud_prefix, cloud_template) if cloud_prefix != '' else cloud_template
cloud_template_local = '/tmp/{}'.format(cloud_template)
b = s3_client.Bucket(cloud_bucket)
b.download_file(cloud_key, cloud_template_local)

with open(cloud_template_local, 'r') as fh:
    cloud_yaml = yaml.safe_load(fh)

# Fill in run specific fields
dproc_delete_yaml = cloud_yaml['dataproc_delete']
dproc_delete_yaml['gcp-administrative']['project'] = project_id
dproc_delete_yaml['gcp-administrative']['zone'] = zone
dproc_delete_yaml['gcp-administrative']['credentials'] = GCP_creds

dproc_delete_yaml['dataproc-administrative']['cluster_name'] = cluster_name

# Activate creds
GoogleCloudLambdaAuth(GCP_creds).configure_google_creds()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/tmp/service_creds.json'

print("GCP Creds")
print(GCP_creds)

# Delete cluster 
deleted = False
delete_result = gcp_dataproc_cluster_delete(dproc_delete_yaml)
print("CLUSTER_DELETE RESULT: {}".format(delete_result))
print("Delete worked.")

sys.stdout.flush()
while deleted == False:
    poller = DataprocDeleteStatusPoller(GCP_creds, project_id, region, cluster_name)
    outcome = poller.polling_outcome()
    print(outcome)
    if outcome == "SUCCESS":
        deleted = True
    elif outcome == "FAIL":
        raise ValueError("Dataproc cluster failed to delete!")
    else:
        #In progress
        print("IN PROGRESS")
        time.sleep(2)
    sys.stdout.flush()
