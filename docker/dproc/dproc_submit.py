'''
Module for AWS Lambda handler function to test multi-cloud (GCP, AWS) orchestration using resp. python client libraries.
'''
from .google_sdk import DataprocSubmitStatusPoller, GoogleCloudLambdaAuth
from google.cloud import storage
import googleapiclient.discovery
from pprint import pprint
import boto3
import json
import os
import sys
import time
import uuid
import yaml

s3_client = boto3.resource('s3', region_name='us-east-1')


def get_dataproc_client():
    """Builds an http client authenticated with the service account credentials."""
    dataproc = googleapiclient.discovery.build('dataproc', 'v1')
    return dataproc

def gcp_dataproc_cluster_submit(event, args):
    """Submits hail script to dataproc cluster."""

    dataproc = get_dataproc_client()
    project = event['gcp-administrative']['project']
    zone = event['gcp-administrative']['zone']
    try:
        region_as_list = zone.split('-')[:-1]
        region = '-'.join(region_as_list)
    except (AttributeError, IndexError, ValueError):
        raise ValueError('Invalid zone provided, please check your input.')

    cluster_name = event['dataproc-administrative']['cluster_name']

    hail_script_bucket = event['hail-administrative']['script']['gs_bucket']
    hail_script_key = event['hail-administrative']['script']['gs_key']
    hail_hash = event['hail-administrative']['metadata']['HASH']
    spark_version = event['hail-administrative']['metadata']['SPARK']
    hail_version = event['hail-administrative']['metadata']['HAIL_VERSION']

    # Submits the Pyspark job to the cluster, assuming `hail_script` has 
    # already been uploaded to `bucket_name`

    job_details = {
        'projectId': project,
        'region': region,
        'job': {
            'placement': {
                'clusterName': cluster_name
            },
            'pysparkJob': {
                'mainPythonFileUri': 'gs://{}/{}'.format(hail_script_bucket, hail_script_key),
                'args': args,
                'pythonFileUris': 'gs://hail-common/builds/{}/python/hail-{}-{}.zip'.format(
                    hail_version, hail_version, hail_hash),
                'fileUris': 'gs://hail-common/builds/{}/jars/hail-{}-{}-Spark-{}.jar'.format(
                    hail_version, hail_version, hail_hash, spark_version),
                'properties': { 'spark.driver.extraClassPath' : './hail-{}-{}-Spark-{}.jar'.format(
                    hail_version, hail_hash, spark_version), 
                    'spark.executor.extraClassPath': 
                        './hail-{}-{}-Spark-{}.jar'.format(
                            hail_version, hail_hash, spark_version) }
            }
        }
    }
    print("Submitting work; getting dataproc result...")

    result = dataproc.projects().regions().jobs().submit(
        projectId=project,
        region=region,
        body=job_details).execute()

    job_id = result['reference']['jobId']
    print(result)
    sys.stdout.flush()
    return result

# Grab all environ variables
cohort_prefix = os.environ['cohort_prefix']
num_samples = os.environ['num_samples']
build = os.environ['build']
final_vcf_bucket = os.environ['final_vcf_bucket']
final_vcf = os.environ['final_vcf']

assets_uri = os.environ['assets_uri']
results_uri = os.environ['results_uri']
vcf_gs_uri = os.environ['vcf_gs_uri']
giab_bucket = os.environ['giab_bucket']
hail_script_bucket = os.environ['hail_script_bucket']
hail_script_key = os.environ['hail_script_key']

project_id = os.environ['project_id']
zone = os.environ['zone']
region = "{}-{}".format(zone.split('-')[0], zone.split('-')[1])
cluster_name = os.environ['cluster_name']

GCP_creds_str = os.environ['GCP_creds']
cloudspan_mode = os.environ['cloudspan_mode']
cloud_template = os.environ['cloud_file']
vcf_gs_bucket_prefix = "{}/".format("/".join(vcf_gs_uri.split("/")[:-1]))
final_vcf = vcf_gs_uri.split("/")[-1]

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
print(cloud_yaml)

if cloudspan_mode == 'validation':
    image_version = '1.2' 
    hail_hash = '833b374e20f2'
    hail_version = 'devel'
    spark_version = '2.2.0'
    miniconda_version = '4.4.10'
    miniconda_variant = ''
    args = [vcf_gs_bucket_prefix, final_vcf, cohort_prefix, build, giab_bucket]
elif cloudspan_mode == 'qc':
    image_version = '1.1' 
    hail_hash = '74bf1ebb3edc'
    hail_version = '0.1'
    spark_version = '2.0.2'
    miniconda_version = ''
    miniconda_variant = '2'
    args = [vcf_gs_bucket_prefix, final_vcf, cohort_prefix, num_samples]

# Fill in run specific fields
dproc_submit_yaml = cloud_yaml['dataproc_submit']
print("RAW DPROC_SUBMIT_YAML")
print(dproc_submit_yaml)
dproc_submit_yaml['cohort_prefix'] = cohort_prefix
dproc_submit_yaml['final_vcf'] = final_vcf
dproc_submit_yaml['results_uri'] = results_uri
dproc_submit_yaml['num_samples'] = str(num_samples)

# Fill in template fields
dproc_submit_yaml['gcp-administrative']['project'] = project_id
dproc_submit_yaml['gcp-administrative']['zone'] = zone
dproc_submit_yaml['gcp-administrative']['credentials'] = GCP_creds

dproc_submit_yaml['dataproc-administrative']['cluster_name'] = cluster_name
dproc_submit_yaml['dataproc-administrative']['image_version'] = image_version

dproc_submit_yaml['hail-administrative']['metadata']['HASH'] = hail_hash
dproc_submit_yaml['hail-administrative']['metadata']['SPARK'] = spark_version
dproc_submit_yaml['hail-administrative']['metadata']['HAIL_VERSION'] = hail_version
dproc_submit_yaml['hail-administrative']['metadata']['MINICONDA_VERSION'] = miniconda_version
dproc_submit_yaml['hail-administrative']['metadata']['MINICONDA_VARIANT'] = miniconda_variant
dproc_submit_yaml['hail-administrative']['metadata']['JAR'] = dproc_submit_yaml['hail-administrative']['metadata']['JAR'].format(hail_version=hail_version, hail_hash=hail_hash, spark_version=spark_version) 
dproc_submit_yaml['hail-administrative']['metadata']['ZIP'] = dproc_submit_yaml['hail-administrative']['metadata']['ZIP'].format(hail_version=hail_version, hail_hash=hail_hash)

dproc_submit_yaml['hail-administrative']['script']['gs_bucket'] = hail_script_bucket
dproc_submit_yaml['hail-administrative']['script']['gs_key'] = hail_script_key

print("DPROC_SUBMIT_YAML:")
print(dproc_submit_yaml)

sys.stdout.flush()

# Activate creds
GoogleCloudLambdaAuth(GCP_creds).configure_google_creds()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/tmp/service_creds.json'

# Submit script
complete = False
submit_result = gcp_dataproc_cluster_submit(dproc_submit_yaml, args)
print("SUBMIT RESULT: {}".format(submit_result))
print("Submitted work.")
submit_job_id = submit_result['reference']['jobId']

sys.stdout.flush()
while complete == False:
    poller = DataprocSubmitStatusPoller(credentials=str(GCP_creds), 
        project_id=project_id, region=region, job_id=submit_job_id)
    outcome = poller.polling_outcome()
    print(outcome)
    if outcome in ['DONE', 'SUCCESS']:
        # Success
        complete = True
    elif outcome in set(["FAIL", 
            "CANCELLED", 
            "ERROR", 
            "ATTEMPT_FAILURE", 
            "CANCEL_PENDING", 
            "CANCEL_STARTED"]):
        # Fail
        raise ValueError("Dataproc work failed!")
    else:
        # In progress
        print("IN PROGRESS")
        time.sleep(2)
    sys.stdout.flush()
