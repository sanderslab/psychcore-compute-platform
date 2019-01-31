import datetime
import boto3
import json

def handler(event, context):
    # '''
    # Handler to submit jobs for bwa-base_recal_table.
    # :param event: Dict
    # :param context: Lambda Context Obj
    # '''

    event_str = json.dumps(event)
    job_defs = event['job_defs']
    job_queue = event['queue']
    results_uri = event['results_uri']

    now_unformat = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    now = now_unformat.replace(' ', '_').replace(':', '-')

    triodenovo = boto3.client('batch').submit_job(
        jobName='triodenovo_submitter_{}'.format(now),
        jobQueue=job_queue,
        jobDefinition=job_defs['submitter_job'],
        containerOverrides={
            'environment': [
                {
                    'name': 'step',
                    'value': 'triodenovo'
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
