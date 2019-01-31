'''
Module for AWS Lambda handler function to test multi-cloud (GCP, AWS) orchestration using resp. python client libraries.
'''
from .google_sdk import DataprocCreateStatusPoller, GoogleCloudLambdaAuth
from google.cloud import storage
import googleapiclient.discovery
from pprint import pprint
import boto3
import json
import os
import time
import uuid
import yaml

s3_client = boto3.resource('s3', region_name='us-east-1')


def get_dataproc_client():
    """Builds an http client authenticated with the service account credentials."""
    dataproc = googleapiclient.discovery.build('dataproc', 'v1')
    return dataproc


def gcp_dataproc_cluster_create(event):
    pprint(event)

    project = event['gcp-administrative']['project']
    zone = event['gcp-administrative']['zone'] 
    cluster_name = event['dataproc-administrative']['cluster_name']

    bucket = event['hail-administrative']['script']['gs_bucket'].split('/')[0]

    master_type = event['dataproc-administrative']['masterConfig']['master_type']
    worker_type = event['dataproc-administrative']['workerConfig']['worker_type']
    preempt_type = event['dataproc-administrative']['preemptConfig']['preempt_type']

    num_masters = event['dataproc-administrative']['masterConfig']['num_masters']
    num_workers = event['dataproc-administrative']['workerConfig']['num_workers']
    num_preempt = event['dataproc-administrative']['preemptConfig']['num_preempt']

    master_boot_disk_size = event['dataproc-administrative']['masterConfig']['boot_disk_size']
    worker_boot_disk_size = event['dataproc-administrative']['workerConfig']['boot_disk_size']
    preempt_boot_disk_size = event['dataproc-administrative']['preemptConfig']['boot_disk_size']

    master_num_ssds = event['dataproc-administrative']['masterConfig']['num_local_ssds']
    worker_num_ssds = event['dataproc-administrative']['workerConfig']['num_local_ssds']
    preempt_num_ssds = event['dataproc-administrative']['preemptConfig']['num_local_ssds']

    image_version = event['dataproc-administrative']['image_version']
    metadata = event['hail-administrative']['metadata']

    try:
        region_as_list = zone.split('-')[:-1]
        region = '-'.join(region_as_list)
    except (AttributeError, IndexError, ValueError):
        raise ValueError('Invalid zone provided, please check your input.')

    dataproc = get_dataproc_client()

    zone_uri = 'https://www.googleapis.com/compute/v1/projects/{}/zones/{}'.format(project, zone)

    cluster_data = {
        'projectId': project,
        'clusterName': cluster_name,
        'config': {
            'configBucket': bucket,
            'gceClusterConfig': {
                'zoneUri': zone_uri,
                'metadata': metadata
            },
            'masterConfig': {
                'machineTypeUri': master_type,
                'numInstances': num_masters,
                'diskConfig': {
                    'bootDiskSizeGb': master_boot_disk_size,
                    'numLocalSsds': master_num_ssds
                }

            },
            'workerConfig': {
                'machineTypeUri': worker_type,
                'numInstances': num_workers,
                'diskConfig': {
                    'bootDiskSizeGb': worker_boot_disk_size,
                    'numLocalSsds': worker_num_ssds
                }
            },
            'secondaryWorkerConfig': {
                'isPreemptible': True, 
                'machineTypeUri': preempt_type,
                'numInstances': num_preempt,
                'diskConfig': {
                    'bootDiskSizeGb': preempt_boot_disk_size,
                    'numLocalSsds': preempt_num_ssds
                }
            },
            'softwareConfig': {
                'imageVersion': image_version
            },
            'initializationActions': [
              {
                'executableFile': 'gs://dataproc-initialization-actions/conda/bootstrap-conda.sh'
              },    
              {
                'executableFile': 'gs://hail-common/cloudtools/init_notebook1.py'
              }
            ]
        }
    }

    result = dataproc.projects().regions().clusters().create(
        projectId=project,
        region=region,
        body=cluster_data).execute()


    print(result)
    return result


# Grab all environ variables
cohort_prefix = os.environ['cohort_prefix']
num_samples = os.environ['num_samples']
build = os.environ['build']

assets_uri = os.environ['assets_uri']
results_uri = os.environ['results_uri']
sink_bucket = os.environ['sink_bucket']
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
cloudspan_mode = os.environ['cloudspan_mode']

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

if cloudspan_mode == 'validation':
    image_version = '1.2' 
    hail_hash = '833b374e20f2'
    hail_version = 'devel'
    spark_version = '2.2.0'
    miniconda_version = '4.4.10'
    miniconda_variant = ''
elif cloudspan_mode == 'qc':
    image_version = '1.1' 
    hail_hash = '74bf1ebb3edc'
    hail_version = '0.1'
    spark_version = '2.0.2'
    miniconda_version = ''
    miniconda_variant = '2'

# Fill in run specific fields
dproc_create_yaml = cloud_yaml['dataproc_create']
dproc_create_yaml['gcp-administrative']['project'] = project_id
dproc_create_yaml['gcp-administrative']['zone'] = zone
dproc_create_yaml['gcp-administrative']['credentials'] = GCP_creds

dproc_create_yaml['dataproc-administrative']['cluster_name'] = cluster_name
dproc_create_yaml['dataproc-administrative']['image_version'] = image_version

dproc_create_yaml['hail-administrative']['metadata']['HASH'] = hail_hash
dproc_create_yaml['hail-administrative']['metadata']['SPARK'] = spark_version
dproc_create_yaml['hail-administrative']['metadata']['HAIL_VERSION'] = hail_version
dproc_create_yaml['hail-administrative']['metadata']['MINICONDA_VERSION'] = miniconda_version
dproc_create_yaml['hail-administrative']['metadata']['MINICONDA_VARIANT'] = miniconda_variant
dproc_create_yaml['hail-administrative']['metadata']['JAR'] \
    = dproc_create_yaml['hail-administrative']\
    ['metadata']['JAR'].format(hail_version=hail_version,
     hail_hash=hail_hash, spark_version=spark_version) 
dproc_create_yaml['hail-administrative']['metadata']['ZIP'] \
    = dproc_create_yaml['hail-administrative']\
    ['metadata']['ZIP'].format(hail_version=hail_version,
     hail_hash=hail_hash)

dproc_create_yaml['hail-administrative']['script']['gs_bucket'] \
    = hail_script_bucket
dproc_create_yaml['hail-administrative']['script']['gs_key'] \
    = hail_script_key

# Activate creds
GoogleCloudLambdaAuth(GCP_creds).configure_google_creds()
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/tmp/service_creds.json'

# Create cluster
created = False
create_result = gcp_dataproc_cluster_create(dproc_create_yaml)

while created == False:
    poller = DataprocCreateStatusPoller(GCP_creds, project_id, region, cluster_name)
    outcome = poller.polling_outcome()
    print(outcome)
    if outcome == 'SUCCESS':
        created = True
    elif outcome in set(["CANCELLED", "ERROR", "ATTEMPT_FAILURE"]):
        raise ValueError("Dataproc cluster failed to create!")
    else:
        # In progress
        time.sleep(2)


