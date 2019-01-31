import datetime
import boto3
import json

def handler(event, context):
    # '''
    # Handler to submit jobs for sentieon_haplotyper.
    # :param event: Dict
    # :param context: Lambda Context Obj
    # '''

    event_str = json.dumps(event)
    job_defs = event['job_defs']
    job_queue = event['queue']
    qc_queue = event['qc_queue']
    results_uri = event['results_uri']

    now_unformat = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    now = now_unformat.replace(' ', '_').replace(':', '-')

    # Submitter for haplotyper
    haplo_submit = boto3.client('batch').submit_job(
        jobName='haplotyper_submitter_{}'.format(now),
        jobQueue=job_queue,
        jobDefinition=job_defs['submitter_job'],
        containerOverrides={
            'environment': [
                {
                    'name': 'step',
                    'value': 'sentieon_haplotyper'
                },
                {
                    'name': 'event_str',
                    'value' : event_str
                },
                {
                    'name': 'log_uri',
                    'value' : '{}logs/'.format(results_uri)
                },
            ]
        },
    )

    # Submitter for bamQC
    if event['bam_qc']:
        bam_submit = boto3.client('batch').submit_job(
            jobName='bamQC_submitter_{}'.format(now),
            jobQueue=qc_queue,
            jobDefinition=job_defs['submitter_job'],
            containerOverrides={
                'environment': [
                    {
                        'name': 'step',
                        'value': 'bamQC'
                    },
                    {
                        'name': 'event_str',
                        'value' : event_str
                    },
                    {
                        'name': 'log_uri',
                        'value' : '{}logs/'.format(results_uri)
                    },
                ]
            },
        )

    # return job queue for poller
    return job_queue

