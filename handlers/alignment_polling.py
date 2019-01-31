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
    queue = event
    #job_ids = event
    job_ids = []
    if isinstance(event, list):
        job_ids = event
    else:
        for status in ['SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING', 'SUCCEEDED', 'FAILED']:
            # get all jobIds in queue of each status
            next_token = ''
            while next_token != 'null':
                sub_response = client.list_jobs(
                    jobQueue=queue,
                    jobStatus=status,
                    maxResults=100,
                    nextToken=next_token
                )
                pprint(sub_response)
                if len(sub_response['jobSummaryList']) > 100:
                    # next_token only present in resp
                    # if jobSummaryList has more than
                    # maxResults items in it
                    next_token = sub_response['nextToken']
                else:
                    # exit while loop -  no need to paginate
                    next_token = 'null'

                for job in sub_response['jobSummaryList']:
                    # Only add new job ids since a job could
                    # have switched statuses since last poll
                    if job['jobId'] not in job_ids:
                        job_ids.append(job['jobId'])
                print("NEXT_TOKEN: {}".format(next_token))
    print(job_ids)
    poller = BatchJobListStatusPoller(job_ids=job_ids)
    poller_outcome = poller.polling_outcome()
    return poller_outcome
    