import datetime
import boto3
import json

s3_client = boto3.resource('s3', region_name='us-east-1')

def handler(event, context):
    '''
    Handler to submit jobs for sentieon_genotyper.
    :param event: Dict
    :param context: Lambda Context Obj
    '''

    # Download and parse sample list file
    sample_s3_prefix = event['sample_s3_prefix']
    sample_s3_bucket = event['sample_s3_bucket']
    sample_file = event['sample_file']
    sample_key = '{}/{}'.format(sample_s3_prefix, sample_file)

    print('Downloading {} from {} using key {}'.format(
        sample_file,
        sample_s3_bucket,
        sample_key))
    b = s3_client.Bucket(sample_s3_bucket)
    # In lambda, only /tmp/ is writeable
    sample_file_local = '/tmp/{}'.format(sample_file)
    b.download_file(sample_key, sample_file_local)

    samples = []
    with open('/tmp/{}'.format(sample_file), 'r') as f:
        for line in f:
            samples.append(line.strip())
    print(samples)

    job_defs = event['job_defs']
    job_queue = event['queue']
    cohort_prefix = event['cohort_prefix']

    ref_uri = event['ref_uri']
    assets_uri = event['assets_uri']
    results_uri = event['results_uri']
    sentieon_pkg = event['sentieon_pkg']
    sentieon_license = event['sentieon_license']
    param_file = event['param_file']
    build = event['build']
    ome = event['mode']['ome']

    if ome == 'wes' and 'target_file' in event:
        target_file = event['target_file']
    elif ome == 'wgs':
        target_file = 'None'
    else:
        raise ValueError('Ome set to wes, but no target_file was set!')

    job_ids = []

    now_unformat = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    now = now_unformat.replace(' ', '_').replace(':', '-')

    sentieon_genotyper_submit = boto3.client('batch').submit_job(
        jobName='sentieon_genotyper_{}_{}'.format(cohort_prefix, now),
        jobQueue=job_queue,
        jobDefinition=job_defs['sentieon_genotyper_job'],
        containerOverrides={
            'environment': [
                {
                    'name': 'build',
                    'value': build
                },

                {
                    'name': 'ome',
                    'value': ome
                },
                {
                    'name': 'target_file',
                    'value': target_file
                },
                {
                    'name': 'sample_file',
                    'value': sample_file
                },
                {
                    'name': 'prefix',
                    'value': cohort_prefix
                },
                {
                    'name': 'assets_uri',
                    'value': assets_uri
                },
                {
                    'name': 'param_file',
                    'value': param_file
                },
                {
                    'name': 'ref_uri',
                    'value': ref_uri
                },
                {
                    'name': 'in_uri',
                    'value': '{}bgz-gvcfs/'.format(results_uri)
                },
                {
                    'name': 'out_uri',
                    'value': '{}cohort-vcf-vqsr/'.format(results_uri)
                },
                {
                    'name': 'log_uri',
                    'value': '{}logs/'.format(results_uri)
                },
                {
                    'name': 'sentieon_pkg',
                    'value': sentieon_pkg
                },
                {
                    'name': 'sentieon_license',
                    'value': sentieon_license
                }
            ]
        },
    )

    job_ids.append(sentieon_genotyper_submit['jobId'])

    return job_ids
