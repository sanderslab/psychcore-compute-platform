from rkstr8.cloud.batch import BatchJobListStatusPoller
from pprint import pprint
import boto3

client = boto3.client('batch')

def handler(event, context):
    """
    Handler for aws batch polling lambda function - polls for
    the completion of all jobs in event[jobs]
    :param event: Dict
    :param context: Lambda Context Obj
    """
    # event now contains the job queue
    print(event)
    job_ids = [event['dproc_create_job_id']]
    poller = BatchJobListStatusPoller(job_ids=job_ids)
    poller_outcome = poller.polling_outcome()
    return poller_outcome
