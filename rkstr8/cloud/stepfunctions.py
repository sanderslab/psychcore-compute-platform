from rkstr8.cloud.stepfunctions_mx import *
from rkstr8.cloud import Service, BotoClientFactory, json_serial

import json
import uuid
from abc import ABC
import logging

logger = logging.getLogger(__name__)


#
# TODO: Move the execute method out. []
# TODO: Migrate CFN params here, also template(s) []
#
class PipelineSpecification(ABC):

    def __init__(self):
        self.mx = None
        self.mx_substitutions = None
        self.mx_input = None

    @abstractmethod
    def build_substitutions(self):
        '''
        Builds the array of substitutions for templating the template.
        These substitutions are for keys used in the machine definition,
        and get replaced by Cloudformation.
        :return:
        '''
        pass

    @abstractmethod
    def build_machine(self):
        '''
        Holds the machine definition, or otherwise gets that definition,
        and returns the built machine.
        Also setting self.mx to the built machine.

        :return:
        '''
        pass

    #
    # TODO: Make this @abstract once all the pipelines defined in pipelines.py have implementations
    #
    def build_template_params(self):
        '''
        Holds the CFN template parameters for the stack supporting this pipeline,
        or otherwise gets those parameters,
        and returns the list of rendered params?

        :return:
        '''
        return []

    @abstractmethod
    def build_input(self, stack):
        '''
        Must build an input object, and set self.mx_input,
        which is passed as input to the machine

        :param stack: boto3.CloudFormation.Stack
        :return: dictionary structure representing json object to
        be passed as input to machine execution
        '''
        pass


class StepFunctionsMachine:

    @staticmethod
    def start(machine_arn, input, exec_name=None, 
        sfn_client=BotoClientFactory.client_for(Service.STEPFUNCTIONS)):

        if not exec_name:
            exec_name = '-'.join(('aws-sfn-machine', str(uuid.uuid4())[0:8]))

        response = sfn_client.start_execution(
            stateMachineArn=machine_arn,
            name=exec_name,
            input=input
        )

        logger.debug(json.dumps(response, indent=4, sort_keys=False, default=json_serial))

        try:
            machine_exec_arn = response['executionArn']
            return machine_exec_arn
        except:
            logger.error('Failed to retreive executionArn from start_execution response')


class AsyncPoller:

    def __init__(self, async_task,
        pollr_task, faild_task, succd_task, stats_path, pollr_wait_time):
        '''
        Constructs an Asynchronous Polling component for a AWS StepFunctions State Machine.
        Expects client to provide
            1. The Task state that does work asynchronously (asynch_task_state)
            2. The Task state that polls for completion of asynch_task_state (polling_task_state)
            3. States for success and failure
        The methods of this class wire the provided components with the intrinsic components
        of the polling system.
        The polling_task_state must output a value in ('SUCCESS', 'FAIL', 'IN_PROGRESS')

        :param async_task: The asynch task. Must be a stepfunctions_mx.Task instance.
        :param pollr_task: The task which polls for asynch task completion.
        Must be a stepfunctions_mx.Task instance.
        :param faild_task: State which results on polling_task_state output of FAIL
        :param succd_task: State which results on polling_task_state output of SUCCESS
        :return:
        '''
        self.status_json_path = stats_path
        self.async_task_state = async_task
        self.polling_task_state = pollr_task
        self.poll_fail_state = faild_task
        self.poll_success_state = succd_task
        self.poll_wait_time = pollr_wait_time
        self.uid = str(uuid.uuid4())[:4]

    def states(self):
        '''
        Generates a poller for a given job, and returns it as a list of States.
        Intended to be used at StateMachine build-time, as a component of a larger machine.
        :return:
        '''

        SUCCESS_TOKEN = 'SUCCESS'
        FAIL_TOKEN = 'FAIL'
        IN_PROGRESS_TOKEN = 'IN_PROGRESS'

        # noinspection PyUnresolvedReferences,PyUnresolvedReferences,PyUnresolvedReferences,
        # PyUnresolvedReferences,PyUnresolvedReferences
        waiter = Wait(
            name='-'.join(('Wait', self.uid)),
            wait_time=self.poll_wait_time,
            next=self.polling_task_state.name
        )

        # noinspection PyUnresolvedReferences,PyUnresolvedReferences,PyUnresolvedReferences,
        # PyUnresolvedReferences,PyUnresolvedReferences
        choice = Choice(
            '-'.join(('Choice', self.uid)),
            self.poll_fail_state.name,
            Rule(
                StringEquals(
                    var=self.status_json_path,
                    val=FAIL_TOKEN
                ),
                next=self.poll_fail_state.name
            ),
            Rule(
                StringEquals(
                    var=self.status_json_path,
                    val=SUCCESS_TOKEN
                ),
                next=self.poll_success_state
            ),
            Rule(
                StringEquals(
                    var=self.status_json_path,
                    val=IN_PROGRESS_TOKEN
                ),
                next=waiter.name
            )
        )

        self.polling_task_state.next = choice.name

        return [self.async_task_state, self.polling_task_state, self.poll_fail_state, waiter, choice]
