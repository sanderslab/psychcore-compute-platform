from rkstr8.cloud.google import HandoffStatusPoller, GoogleCloudLambdaAuth
import os
import sys


def handler(event, context):
    print("THIS IS THE EVENT FROM HANDOFF_POLLER LAMBDA")
    print(event)
    credentials = event['GCP_creds']
    GoogleCloudLambdaAuth(credentials).configure_google_creds()
    project_id = event['project_id']
    transfer_job_id = event['transfer_job_id']
    poller = HandoffStatusPoller(credentials, project_id, transfer_job_id)
    sys.stdout.flush()
    poller_outcome = poller.polling_outcome()
    return poller_outcome
