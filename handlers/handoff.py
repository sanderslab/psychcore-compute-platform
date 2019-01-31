from rkstr8.cloud.google import GoogleCloudLambdaAuth
from googleapiclient import discovery
from pprint import pprint
import boto3
import json
import datetime
import yaml


def handler(event, context):
    '''
    Handler to create a one-time transfer from Amazon S3 to Google Cloud Storage
    :param event: Dict
    :param context: Lambda Context Obj
    '''
    queue = event['queue']
    assets_uri = event['assets_uri']

    GCP_creds = event['gcp_info']['GCP_creds']
    project_id = event['gcp_info']['project_id']
    zone = event['gcp_info']['zone']
    cohort_prefix = event['cohort_prefix']
    results_uri = event['results_uri']
    cloud_transfer_outbucket = event['gcp_info']['sink_bucket']
    access_key_id = event['gcp_info']['aws_access_key']
    secret_access_key = event['gcp_info']['aws_secret_key']
    cloud_template = event['gcp_info']['cloud_template']
    print(GCP_creds)
    GoogleCloudLambdaAuth(GCP_creds).configure_google_creds()

    storagetransfer = discovery.build('storagetransfer', 'v1')
    description = '-'.join(('transfer-job', queue))

    # source_bucket = event['results_uri'].split('/')[2]
    source_bucket = 'pipeline-validation'
    print('HANDOFF SOURCE BUCKET', source_bucket)
    sink_bucket = cloud_transfer_outbucket.split('/')[2]
    print('HANDOFF SINK BUCKET', sink_bucket)
    include_prefix = '{}{}/{}{}'.format(
        '/'.join(results_uri.split('/')[3:]),
        'final-cohort-vcf', event['cohort_prefix'],
        '.gt.snp.indel.recal.vcf')

    print('INCLUDE_PREFIX', include_prefix)

    now = datetime.datetime.utcnow()

    day = now.day
    month = now.month
    year = now.year

    hours = now.hour
    minutes_obj = now + datetime.timedelta(minutes=2)
    minutes = minutes_obj.minute

    transfer_job = {
        'description': description,
        'status': 'ENABLED',
        'projectId': project_id,
        'schedule': {
            'scheduleStartDate': {
                'day': day,
                'month': month,
                'year': year
            },
            'scheduleEndDate': {
                'day': day,
                'month': month,
                'year': year
            },
            'startTimeOfDay': {
                'hours': hours,
                'minutes': minutes
            }
        },
        'transferSpec': {
            'objectConditions': {
                'includePrefixes': [
                    include_prefix
                ]
            },
            'awsS3DataSource': {
                'bucketName': source_bucket,
                'awsAccessKey': {
                    'accessKeyId': access_key_id,
                    'secretAccessKey': secret_access_key
                }
            },
            'gcsDataSink': {
                'bucketName': sink_bucket
            }
        }
    }


    result = storagetransfer.transferJobs().create(body=transfer_job).execute()
    print('Returned transferJob: {}'.format(
        json.dumps(result, indent=4)))

    print('TRANSFER RESULT')
    pprint(result)

    try:
        event['transferJobID'] = result['name']
    except KeyError as e:
        print('The transfer job ID does not exist.')
        raise e

    # Read in cloud template for dataproc

    s3_client = boto3.resource('s3', region_name='us-east-1')

    # Download and parse cloudspan list file

    cloud_bucket = assets_uri.split('/')[2]
    cloud_prefix = '/'.join(assets_uri.split('/')[3:-1])
    cloud_key = '{}/{}'.format(cloud_prefix, cloud_template) if cloud_prefix != '' else cloud_template
    cloud_template_local = '/tmp/{}'.format(cloud_template)
    b = s3_client.Bucket(cloud_bucket)
    b.download_file(cloud_key, cloud_template_local)

    with open(cloud_template_local, 'r') as fh:
        cloud_yaml = yaml.safe_load(fh)

    print('CLOUD YAML')
    pprint(cloud_yaml)

    # Join cloud_yaml with event
    cloud_event = dict(cloud_yaml, **event)

    print(cloud_event)

    gcp_info = event['gcp_info']
    gcp_info['region'] = '{}-{}'.format(zone.split('-')[0], zone.split('-')[1])
    gcp_info['transfer_job_id'] = result['name']
    gcp_info['results_uri'] = results_uri
    gcp_info['cohort_prefix'] = cohort_prefix
    gcp_info['vcf_gs_uri'] = 'gs://{}/{}'.format(sink_bucket, include_prefix)

    return gcp_info
