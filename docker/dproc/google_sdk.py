from googleapiclient import discovery
import os
import json

# Where we temporarily store service creds in AWS Lambda container.
LAMBDA_GOOGLE_SERVICE_CREDS_FILE = '/tmp/service_creds.json'

#
#  POLLING OUTCOME TOKENS
#
POLLING_SUCCESS_TOKEN = 'SUCCESS'
POLLING_FAIL_TOKEN = 'FAIL'
POLLING_IN_PROGRESS_TOKEN = 'IN_PROGRESS'


class GoogleCloudLambdaAuth:
    """
    Class for basic Google Cloud Platform authentication from within AWS Lambda
    """

    def __init__(self, credentials):
        self.credentials = credentials

    # TODO: Replace w/ OAuth2 mechanism
    def configure_google_creds(self):
        with open(LAMBDA_GOOGLE_SERVICE_CREDS_FILE, 'w') as fh:
            json.dump(self.credentials, fh)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = LAMBDA_GOOGLE_SERVICE_CREDS_FILE


class HandoffStatusPoller(GoogleCloudLambdaAuth):
    """
    Polls the status of a GCP Storage Transfer Service job, returning a "Polling Outcome" token.
    """
    def __init__(self, credentials, project_id, transfer_job_id):
        GoogleCloudLambdaAuth.__init__(self, credentials)

        self.transfer_client = discovery.build('storagetransfer', 'v1')
        self.project_id = project_id
        self.transfer_job_id = transfer_job_id

    def polling_outcome(self):

        transfer_list_filter = {
            "project_id": self.project_id,
            "job_names": [self.transfer_job_id]
        }

        transfer_list_op = self.transfer_client.transferOperations().list(
            name="transferOperations",
            filter=json.dumps(transfer_list_filter)
        )
        response = transfer_list_op.execute()

        # TODO: Can we detect failures, other than via some heuristic, like a timeout?
        # Currently failure will be masked by an IN_PROGRESS token
        return POLLING_IN_PROGRESS_TOKEN if response == {} else POLLING_SUCCESS_TOKEN


class DataprocCreateStatusPoller(GoogleCloudLambdaAuth):
    """
    Polls the status of a list of Batch Jobs, returning a "Polling Outcome" token.
    """

    def __init__(self, credentials, project_id, region, cluster_name):
        GoogleCloudLambdaAuth.__init__(self, credentials)

        self.dataproc = discovery.build('dataproc', 'v1')
        self.project_id = project_id
        self.region = region
        self.cluster_name = cluster_name

    def polling_outcome(self):

        cluster_list_op = self.dataproc.projects().regions().clusters().list(
            projectId=self.project_id,
            region=self.region
        )

        cluster_list_response = cluster_list_op.execute()

        cluster_list = cluster_list_response['clusters']
        cluster = [c for c in cluster_list if c['clusterName'] == self.cluster_name][0]
        cluster_status = cluster['status']['state']

        if 'ERROR' in cluster_status:
            return POLLING_FAIL_TOKEN
        elif 'RUNNING' in cluster_status:
            return POLLING_SUCCESS_TOKEN
        else:
            return POLLING_IN_PROGRESS_TOKEN


class DataprocSubmitStatusPoller(GoogleCloudLambdaAuth):
    """
    Polls the status of a list of Batch Jobs, returning a "Polling Outcome" token.
    """

    def __init__(self, credentials, project_id, region, job_id):
        GoogleCloudLambdaAuth.__init__(self, credentials)

        self.dataproc= discovery.build('dataproc', 'v1')
        self.project_id = project_id
        self.region = region
        self.job_id = job_id

    def polling_outcome(self):

        job_list_op = self.dataproc.projects().regions().jobs().get(
            projectId=self.project_id,
            region=self.region,
            jobId=self.job_id
        )
        job_list_response = job_list_op.execute()
        job_status = job_list_response['status']['state']

        fail_responses = set(["CANCELLED", "ERROR", "ATTEMPT_FAILURE"])

        if job_status in fail_responses:
            return POLLING_FAIL_TOKEN
        elif job_status == "DONE":
            return POLLING_SUCCESS_TOKEN
        else:
            return POLLING_IN_PROGRESS_TOKEN

class DataprocDeleteStatusPoller(GoogleCloudLambdaAuth):
    """
    Polls the status of a GCP dataproc cluster, returning a "Polling Outcome" token.
    """

    def __init__(self, credentials, project_id, region, cluster_name):
        GoogleCloudLambdaAuth.__init__(self, credentials)

        self.dataproc = discovery.build('dataproc', 'v1')
        self.project_id = project_id
        self.region = region
        self.cluster_name = cluster_name

    def polling_outcome(self):
        try:
            cluster_list_op = self.dataproc.projects().regions().clusters().list(
                projectId=self.project_id,
                region=self.region
            )

            cluster_list_response = cluster_list_op.execute()

            print(cluster_list_response)

            cluster_list = cluster_list_response['clusters']
            cluster = [c for c in cluster_list if c['clusterName'] == self.cluster_name][0]
            cluster_status = cluster['status']['state']

            fail_responses = set(["UNKNOWN", "ERROR", "RUNNING"])

            if cluster_status in fail_responses:
                return POLLING_FAIL_TOKEN
            elif "DELETING" in cluster_status:
                return POLLING_IN_PROGRESS_TOKEN
        except KeyError:
                return POLLING_SUCCESS_TOKEN

