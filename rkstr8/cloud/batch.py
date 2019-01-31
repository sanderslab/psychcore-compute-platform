import boto3

batch_client = boto3.client('batch')

# TODO: Move to conf, or are these also used in Lambda?

#
# POLLING OUTCOME TOKENS
#
POLLING_SUCCESS_TOKEN = 'SUCCESS'
POLLING_FAIL_TOKEN = 'FAIL'
POLLING_IN_PROGRESS_TOKEN = 'IN_PROGRESS'

#
# BATCH JOB STATUS TOKENS
#
BATCH_FAIL_STATUS = 'FAILED'
BATCH_SUCCESS_STATUS = 'SUCCEEDED'


class BatchJobListStatusPoller:
    '''
    Polls the status of a list of Batch Jobs, returning a "Polling Outcome" token.
    '''

    def __init__(self, job_ids):
        self.job_ids = job_ids

    @staticmethod
    def split_list(l, n):
        '''Yield successive n-sized chunks from l.'''
        for i in range(0, len(l), n):
            yield l[i:i + n]

    @staticmethod
    def statuses_for_jobs(job_descriptions):
        '''
        For each job, one of:
            'SUBMITTED'|'PENDING'|'RUNNABLE'|'STARTING'|'RUNNING'|'SUCCEEDED'|'FAILED'
        '''
        return [job['status'] for job in job_descriptions['jobs']]

    def polling_outcome(self):
        all_job_stats = []
        for sublist_of_jobs in self.split_list(self.job_ids, 100):
            partial_job_descriptions = batch_client.describe_jobs(jobs=sublist_of_jobs)
            partial_job_stats = self.statuses_for_jobs(partial_job_descriptions)
            all_job_stats += (partial_job_stats)

        polling_outcome = self.polling_outcome_token(all_job_stats)
        print('outcome: {}'.format(polling_outcome))

        return polling_outcome

    def polling_outcome_token(self, job_stats):
        '''
        Takes in sequence over this set, call it JobStat:
            'SUBMITTED'|'PENDING'|'RUNNABLE'|'STARTING'|'RUNNING'|'SUCCEEDED'|'FAILED'

        And returns one term from this set, call it TaskStat:
            'SUCCESS'|'FAIL'|'IN_PROGRESS'

        I.e. This method implements a single valued function of many variables:
        F: JobStat^k -> TaskStat
        Where JobStat^k is the Cartesian product of Jobstat with itself, k times.
        k is the number of jobs we're polling for completion.
        '''
        print(job_stats)
        current_states = set(job_stats)

        # if all states are succeeded, outcome is SUCCESS_TOKEN
        if current_states == {BATCH_SUCCESS_STATUS}:
            return POLLING_SUCCESS_TOKEN

        # if there is one or more FAILED jobs, outcome is FAIL_TOKEN
        if BATCH_FAIL_STATUS in current_states:
            return POLLING_FAIL_TOKEN

        return POLLING_IN_PROGRESS_TOKEN
