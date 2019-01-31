'''Configures the state machines for each phase of the variant-calling pipeline. Each phase has two
designated state machines, both of which are of type 'task'. One runs the actual task (e.g. HaplotypeCaller),
while the second conducts 'polling'. This polling mechanism provides a way of monitoring the running task
and not moving onto the next phase until the previous one has completed or exited due to error.''' 

from rkstr8.cloud.stepfunctions import PipelineSpecification, AsyncPoller
from rkstr8.cloud.stepfunctions_mx import *
from rkstr8.cloud.cloudformation import CloudFormationTemplate, Stack
import json


class MultiSampleWGSRefactoredNested(PipelineSpecification):

    def __init__(self, conf):
        super().__init__()

        self.REMAP_SUBMIT_LAMBDA_ARN_TOKEN = 'remapSubmitLambdaArn'
        self.REMAP_POLLER_LAMBDA_ARN_TOKEN = 'remapPollerLambdaArn'
        self.ALIGNMT_SUBMIT_LAMBDA_ARN_TOKEN = 'alignmentSubmitLambdaArn'
        self.ALIGNMT_POLLER_LAMBDA_ARN_TOKEN = 'alignmentPollerLambdaArn'
        self.HAPLO_SUBMIT_LAMBDA_ARN_TOKEN = 'haploSubmitLambdaArn'
        self.HAPLO_POLLER_LAMBDA_ARN_TOKEN = 'haploPollerLambdaArn'
        self.GENO_SUBMIT_LAMBDA_ARN_TOKEN = 'genoSubmitLambdaArn'
        self.GENO_POLLER_LAMBDA_ARN_TOKEN = 'genoPollerLambdaArn'
        self.VQSR_SUBMIT_LAMBDA_ARN_TOKEN = 'vqsrSubmitLambdaArn'
        self.VQSR_POLLER_LAMBDA_ARN_TOKEN = 'vqsrPollerLambdaArn'
        self.TRIODENOVO_SUBMIT_LAMBDA_ARN_TOKEN = 'triodenovoSubmitLambdaArn'
        self.TRIODENOVO_POLLER_LAMBDA_ARN_TOKEN = 'triodenovoPollerLambdaArn'
        self.HDOF_SUBMIT_LAMBDA_ARN_TOKEN = 'handoffSubmitLambdaArn'
        self.HDOF_POLLER_LAMBDA_ARN_TOKEN = 'handoffPollerLambdaArn'
        self.CLOUDSPAN_SUBMIT_LAMBDA_ARN_TOKEN = 'cloudspanLambdaArn'
        self.CLOUDSPAN_POLLER_LAMBDA_ARN_TOKEN = 'cloudspanPollerArn'
        self.conf = conf

    def build_substitutions(self):

        self.mx_substitutions = {
            self.REMAP_SUBMIT_LAMBDA_ARN_TOKEN: {
                'Fn::ImportValue': {'Fn::Sub': '${StackUID}-RemapLambdaFunction'}},
            self.REMAP_POLLER_LAMBDA_ARN_TOKEN: {
                'Fn::ImportValue': {'Fn::Sub': '${StackUID}-BatchPollerLambdaFunction'}},
            self.ALIGNMT_SUBMIT_LAMBDA_ARN_TOKEN: {
                'Fn::ImportValue': {'Fn::Sub': '${StackUID}-AlignmentLambdaFunction'}},
            self.ALIGNMT_POLLER_LAMBDA_ARN_TOKEN: {
                'Fn::ImportValue': {'Fn::Sub': '${StackUID}-BatchPollerLambdaFunction'}},
            self.HAPLO_SUBMIT_LAMBDA_ARN_TOKEN: {
                'Fn::ImportValue': {'Fn::Sub': '${StackUID}-HaploLambdaFunction'}},
            self.HAPLO_POLLER_LAMBDA_ARN_TOKEN: {
                'Fn::ImportValue': {'Fn::Sub': '${StackUID}-BatchPollerLambdaFunction'}},
            self.GENO_SUBMIT_LAMBDA_ARN_TOKEN: {
                'Fn::ImportValue': {'Fn::Sub': '${StackUID}-GenoLambdaFunction'}},
            self.GENO_POLLER_LAMBDA_ARN_TOKEN: {
                'Fn::ImportValue': {'Fn::Sub': '${StackUID}-BatchPollerLambdaFunction'}},
            self.VQSR_SUBMIT_LAMBDA_ARN_TOKEN: {
                'Fn::ImportValue': {'Fn::Sub': '${StackUID}-VQSRLambdaFunction'}},
            self.VQSR_POLLER_LAMBDA_ARN_TOKEN: {
                'Fn::ImportValue': {'Fn::Sub': '${StackUID}-BatchPollerLambdaFunction'}},
            self.TRIODENOVO_SUBMIT_LAMBDA_ARN_TOKEN: {
                'Fn::ImportValue': {'Fn::Sub': '${StackUID}-TriodenovoLambdaFunction'}},
            self.TRIODENOVO_POLLER_LAMBDA_ARN_TOKEN: {
                'Fn::ImportValue': {'Fn::Sub': '${StackUID}-BatchPollerLambdaFunction'}},
            self.HDOF_SUBMIT_LAMBDA_ARN_TOKEN: {
                'Fn::ImportValue': {'Fn::Sub': '${StackUID}-HandoffLambdaFunction'}},
            self.HDOF_POLLER_LAMBDA_ARN_TOKEN: {
                'Fn::ImportValue': {'Fn::Sub': '${StackUID}-HandoffPollerLambdaFunction'}},
            self.CLOUDSPAN_SUBMIT_LAMBDA_ARN_TOKEN: {
                'Fn::ImportValue': {'Fn::Sub': '${StackUID}-CloudspanLambdaFunction'}},
            self.CLOUDSPAN_POLLER_LAMBDA_ARN_TOKEN: {
                'Fn::ImportValue': {'Fn::Sub': '${StackUID}-CloudspanPollerLambdaFunction'}}
        }

        return self.mx_substitutions

    def _cfn_sub_variable(self, name):
        return '${' + name + '}'

    @staticmethod
    def flatten(*state_composites):
        '''
        Take in variable number of either stepfunctions_mx.States
        or lists of them, return flattened list
        :param state_composites:
        :return:
        '''
        flattened = []
        for scomp in state_composites:
            if isinstance(scomp, State):
                flattened.append(scomp)
            elif isinstance(scomp, list):
                are_states = [isinstance(item, State) for item in scomp]
                if not all(are_states):
                    raise ValueError('Not every component is a stepfunctions_mx.State')
                flattened.extend(scomp)
        return flattened

    def build_template_params(self):
        parameters = [
            {
                'key': self.conf['RKSTR8_PKG_CFN_PARAM_BUCKET_NAME'],
                'val': self.conf['RKSTR8_PKG_CFN_ARGMT_BUCKET_NAME']
            },
            {
                'key': self.conf['RKSTR8_PKG_CFN_PARAM_KEY_NAME'],
                'val': self.conf['RKSTR8_PKG_CFN_ARGMT_KEY_NAME']
            },
            {

                'key': self.conf["REMAP_CFN_PARAM_LAMBDA_BUCKET_NAME"],
                'val': self.conf["REMAP_CFN_ARGMT_LAMBDA_BUCKET_NAME"]
            },
            {
                'key': self.conf["REMAP_CFN_PARAM_LAMBDA_KEY_NAME"],
                'val': self.conf["REMAP_CFN_ARGMT_LAMBDA_KEY_NAME"]
            },
            {
                'key': self.conf["REMAP_CFN_PARAM_LAMBDA_MODULE_NAME"],
                'val': self.conf["REMAP_CFN_ARGMT_LAMBDA_MODULE_NAME"]
            },
            {
                'key': self.conf["ALIGN_CFN_PARAM_LAMBDA_BUCKET_NAME"],
                'val': self.conf["ALIGN_CFN_ARGMT_LAMBDA_BUCKET_NAME"]
            },
            {
                'key': self.conf['ALIGN_CFN_PARAM_LAMBDA_KEY_NAME'],
                'val': self.conf['ALIGN_CFN_ARGMT_LAMBDA_KEY_NAME']
            },
            {
                'key': self.conf['ALIGN_CFN_PARAM_LAMBDA_MODULE_NAME'],
                'val': self.conf['ALIGN_CFN_ARGMT_LAMBDA_MODULE_NAME']
            },
            {
                'key': self.conf['HAPLO_CFN_PARAM_LAMBDA_BUCKET_NAME'],
                'val': self.conf['HAPLO_CFN_ARGMT_LAMBDA_BUCKET_NAME']
            },
            {
                'key': self.conf['HAPLO_CFN_PARAM_LAMBDA_KEY_NAME'],
                'val': self.conf['HAPLO_CFN_ARGMT_LAMBDA_KEY_NAME']
            },
            {
                'key': self.conf['HAPLO_CFN_PARAM_LAMBDA_MODULE_NAME'],
                'val': self.conf['HAPLO_CFN_ARGMT_LAMBDA_MODULE_NAME']
            },
            {
                'key': self.conf['GENO_CFN_PARAM_LAMBDA_BUCKET_NAME'],
                'val': self.conf['GENO_CFN_ARGMT_LAMBDA_BUCKET_NAME']
            },
            {
                'key': self.conf['GENO_CFN_PARAM_LAMBDA_KEY_NAME'],
                'val': self.conf['GENO_CFN_ARGMT_LAMBDA_KEY_NAME']
            },
            {
                'key': self.conf['GENO_CFN_PARAM_LAMBDA_MODULE_NAME'],
                'val': self.conf['GENO_CFN_ARGMT_LAMBDA_MODULE_NAME']
            },
            {
                'key': self.conf['VQSR_CFN_PARAM_LAMBDA_BUCKET_NAME'],
                'val': self.conf['VQSR_CFN_ARGMT_LAMBDA_BUCKET_NAME']
            },
            {
                'key': self.conf['VQSR_CFN_PARAM_LAMBDA_KEY_NAME'],
                'val': self.conf['VQSR_CFN_ARGMT_LAMBDA_KEY_NAME']
            },
            {
                'key': self.conf['VQSR_CFN_PARAM_LAMBDA_MODULE_NAME'],
                'val': self.conf['VQSR_CFN_ARGMT_LAMBDA_MODULE_NAME']
            },
            {
                'key': self.conf['TRIODENOVO_CFN_PARAM_LAMBDA_BUCKET_NAME'],
                'val': self.conf['TRIODENOVO_CFN_ARGMT_LAMBDA_BUCKET_NAME']
            },
            {
                'key': self.conf['TRIODENOVO_CFN_PARAM_LAMBDA_KEY_NAME'],
                'val': self.conf['TRIODENOVO_CFN_ARGMT_LAMBDA_KEY_NAME']
            },
            {
                'key': self.conf['TRIODENOVO_CFN_PARAM_LAMBDA_MODULE_NAME'],
                'val': self.conf['TRIODENOVO_CFN_ARGMT_LAMBDA_MODULE_NAME']
            },
            {

                'key': self.conf['HDOF_CFN_PARAM_LAMBDA_BUCKET_NAME'],
                'val': self.conf['HDOF_CFN_ARGMT_LAMBDA_BUCKET_NAME']
            },
            {
                'key': self.conf['HDOF_CFN_PARAM_LAMBDA_KEY_NAME'],
                'val': self.conf['HDOF_CFN_ARGMT_LAMBDA_KEY_NAME']
            },
            {
                'key': self.conf['HDOF_CFN_PARAM_LAMBDA_MODULE_NAME'],
                'val': self.conf['HDOF_CFN_ARGMT_LAMBDA_MODULE_NAME']
            },
            {
                'key': self.conf['HDOF_POLLER_CFN_PARAM_LAMBDA_BUCKET_NAME'],
                'val': self.conf['HDOF_POLLER_CFN_ARGMT_LAMBDA_BUCKET_NAME']
            },
            {
                'key': self.conf['HDOF_POLLER_CFN_PARAM_LAMBDA_KEY_NAME'],
                'val': self.conf['HDOF_POLLER_CFN_ARGMT_LAMBDA_KEY_NAME']
            },
            {
                'key': self.conf['HDOF_POLLER_CFN_PARAM_LAMBDA_MODULE_NAME'],
                'val': self.conf['HDOF_POLLER_CFN_ARGMT_LAMBDA_MODULE_NAME']
            },
            {
                'key': self.conf['CLOUDSPAN_CFN_PARAM_LAMBDA_BUCKET_NAME'],
                'val': self.conf['CLOUDSPAN_CFN_ARGMT_LAMBDA_BUCKET_NAME']
            },
            {
                'key': self.conf['CLOUDSPAN_CFN_PARAM_LAMBDA_KEY_NAME'],
                'val': self.conf['CLOUDSPAN_CFN_ARGMT_LAMBDA_KEY_NAME']
            },
            {
                'key': self.conf['CLOUDSPAN_CFN_PARAM_LAMBDA_MODULE_NAME'],
                'val': self.conf['CLOUDSPAN_CFN_ARGMT_LAMBDA_MODULE_NAME']
            },
            {
                'key': self.conf['CLOUDSPAN_POLLER_CFN_PARAM_LAMBDA_BUCKET_NAME'],
                'val': self.conf['CLOUDSPAN_POLLER_CFN_ARGMT_LAMBDA_BUCKET_NAME']
            },
            {
                'key': self.conf['CLOUDSPAN_POLLER_CFN_PARAM_LAMBDA_KEY_NAME'],
                'val': self.conf['CLOUDSPAN_POLLER_CFN_ARGMT_LAMBDA_KEY_NAME']
            },
            {
                'key': self.conf['CLOUDSPAN_POLLER_CFN_PARAM_LAMBDA_MODULE_NAME'],
                'val': self.conf['CLOUDSPAN_POLLER_CFN_ARGMT_LAMBDA_MODULE_NAME']
            },
            {
                'key': self.conf['BATCH_POLLER_CFN_PARAM_LAMBDA_BUCKET_NAME'],
                'val': self.conf['BATCH_POLLER_CFN_ARGMT_LAMBDA_BUCKET_NAME']
            },
            {
                'key': self.conf['BATCH_POLLER_CFN_PARAM_LAMBDA_KEY_NAME'],
                'val': self.conf['BATCH_POLLER_CFN_ARGMT_LAMBDA_KEY_NAME']
            },
            {
                'key': self.conf['BATCH_POLLER_CFN_PARAM_LAMBDA_MODULE_NAME'],
                'val': self.conf['BATCH_POLLER_CFN_ARGMT_LAMBDA_MODULE_NAME']
            },
            {
                'key': 'StackUID',
                'val': self.conf['STACK_UID']
            },
            {
                'key': self.conf['CFN_PARAM_STACK_NAME'],
                'val': self.conf['CFN_ARGMT_STACK_NAME']
            },
            {
                'key': self.conf['CFN_PARAM_GPCE_VPC_ID'],
                'val': self.conf['CFN_ARGMT_GPCE_VPC_ID']
            },
            {
                'key': self.conf['CFN_PARAM_GPCE_INSTANCE_TYPES'],
                'val': self.conf['CFN_ARGMT_GPCE_INSTANCE_TYPES']
            },
            {
                'key': self.conf['CFN_PARAM_GPCE_MAX_CPUS'],
                'val': self.conf['CFN_ARGMT_GPCE_MAX_CPUS']
            },
            {
                'key': self.conf['CFN_PARAM_GPCE_SSH_KEY_PAIR'],
                'val': self.conf['CFN_ARGMT_GPCE_SSH_KEY_PAIR']
            },
            {
                'key': self.conf['LAMBDA_CFN_PARAM_TEMPLATE_URL'],
                'val': self.conf['LAMBDA_CFN_ARGMT_TEMPLATE_URL']
            },
            {
                'key': self.conf['NETWORK_CFN_PARAM_TEMPLATE_URL'],
                'val': self.conf['NETWORK_CFN_ARGMT_TEMPLATE_URL']
            },
            {
                'key': self.conf['BATCH_CFN_PARAM_TEMPLATE_URL'],
                'val': self.conf['BATCH_CFN_ARGMT_TEMPLATE_URL']
            },
            {
                'key': self.conf['STEP_FUNCTIONS_PARAM_TEMPLATE_URL'],
                'val': self.conf['STEP_FUNCTIONS_ARGMT_TEMPLATE_URL']
            }
        ]

        # Cast all parameters to string, boto can only create params from `str`
        str_parameters = [
            {
                'key': p['key'],
                'val': str(p['val'])
            } for p in parameters]

        # Render the parameters above into format expected by boto.cloudformation.create_stack
        return [CloudFormationTemplate.Parameter(**p).to_cfn() for p in str_parameters]

    def build_machine(self):
        '''
        Holds the machine definition, or otherwise gets that definition,
        and returns the built machine.
        Also setting self.mx to the built machine.

        :return:
        '''
        MACHINE_NAME = 'MultiSampleWGS'

        REMAP_SUBMIT = 'RemapSubmitTask'
        REMAP_POLLER = 'RemapPollerTask'

        ALIGNMT_SUBMIT = 'AlignmentSubmitTask'
        ALIGNMT_POLLER = 'AlignmentPollerTask'

        HAPLO_SUBMIT = 'HaploSubmitTask'
        HAPLO_POLLER = 'HaploPollerTask'

        GENO_SUBMIT = 'GenoSubmitTask'
        GENO_POLLER = 'GenoPollerTask'

        VQSR_SUBMIT = 'VQSRSubmitTask'
        VQSR_POLLER = 'VQSRPollerTask'

        TRIODENOVO_SUBMIT = 'TriodenovoSubmitTask'
        TRIODENOVO_POLLER = 'TriodenovoPollerTask'

        HDOF_SUBMIT = 'HandoffSubmitTask'
        HDOF_POLLER = 'HandoffPollerTask'

        DPROC_CREATE = 'ClusterCreateTask'
        DPROC_CREATE_POLLER = 'ClusterCreatePollerTask'

        DPROC_SUBMIT = 'QCSubmitTask'
        DPROC_SUBMIT_POLLER = 'QCPollerTask'

        DPROC_DELETE = 'ClusterDeleteTask'
        DPROC_DELETE_POLLER = 'ClusterDeletePollerTask'

        REMAP_SUBMIT_ARN_VAR = self._cfn_sub_variable(self.REMAP_SUBMIT_LAMBDA_ARN_TOKEN)
        REMAP_POLLER_ARN_VAR = self._cfn_sub_variable(self.REMAP_POLLER_LAMBDA_ARN_TOKEN)

        ALIGNMT_SUBMIT_ARN_VAR = self._cfn_sub_variable(self.ALIGNMT_SUBMIT_LAMBDA_ARN_TOKEN)
        ALIGNMT_POLLER_ARN_VAR = self._cfn_sub_variable(self.ALIGNMT_POLLER_LAMBDA_ARN_TOKEN)

        HAPLO_SUBMIT_ARN_VAR = self._cfn_sub_variable(self.HAPLO_SUBMIT_LAMBDA_ARN_TOKEN)
        HAPLO_POLLER_ARN_VAR = self._cfn_sub_variable(self.HAPLO_POLLER_LAMBDA_ARN_TOKEN)

        GENO_SUBMIT_ARN_VAR = self._cfn_sub_variable(self.GENO_SUBMIT_LAMBDA_ARN_TOKEN)
        GENO_POLLER_ARN_VAR = self._cfn_sub_variable(self.GENO_POLLER_LAMBDA_ARN_TOKEN)

        VQSR_SUBMIT_ARN_VAR = self._cfn_sub_variable(self.VQSR_SUBMIT_LAMBDA_ARN_TOKEN)
        VQSR_POLLER_ARN_VAR = self._cfn_sub_variable(self.VQSR_POLLER_LAMBDA_ARN_TOKEN)

        TRIODENOVO_SUBMIT_ARN_VAR = self._cfn_sub_variable(self.TRIODENOVO_SUBMIT_LAMBDA_ARN_TOKEN)
        TRIODENOVO_POLLER_ARN_VAR = self._cfn_sub_variable(self.TRIODENOVO_POLLER_LAMBDA_ARN_TOKEN)

        HDOF_SUBMIT_ARN_VAR = self._cfn_sub_variable(self.HDOF_SUBMIT_LAMBDA_ARN_TOKEN)
        HDOF_POLLER_ARN_VAR = self._cfn_sub_variable(self.HDOF_POLLER_LAMBDA_ARN_TOKEN)

        DPROC_CREATE_ARN_VAR = self._cfn_sub_variable(self.CLOUDSPAN_SUBMIT_LAMBDA_ARN_TOKEN)
        DPROC_CREATE_POLLER_ARN_VAR = self._cfn_sub_variable(self.CLOUDSPAN_POLLER_LAMBDA_ARN_TOKEN)

        DPROC_SUBMIT_ARN_VAR = self._cfn_sub_variable(self.CLOUDSPAN_SUBMIT_LAMBDA_ARN_TOKEN)
        DPROC_SUBMIT_POLLER_ARN_VAR = self._cfn_sub_variable(self.CLOUDSPAN_POLLER_LAMBDA_ARN_TOKEN)

        DPROC_DELETE_ARN_VAR = self._cfn_sub_variable(self.CLOUDSPAN_SUBMIT_LAMBDA_ARN_TOKEN)
        DPROC_DELETE_POLLER_ARN_VAR = self._cfn_sub_variable(self.CLOUDSPAN_POLLER_LAMBDA_ARN_TOKEN)

        REMAP_POLLER_STATUS_PATH = '$.remap_poll_status'
        ALIGNMT_POLLER_STATUS_PATH = '$.alignment_poll_status'
        HAPLO_POLLER_STATUS_PATH = '$.haplo_poll_status'
        GENO_POLLER_STATUS_PATH = '$.geno_poll_status'
        VQSR_POLLER_STATUS_PATH = '$.vqsr_poll_status'
        TRIODENOVO_POLLER_STATUS_PATH = '$.triodeovo_poll_status'
        HDOF_POLLER_STATUS_PATH = '$.handoff_poll_status'

        DPROC_CREATE_POLLER_STATUS_PATH = '$.dproc_create_poll_status'
        DPROC_CREATE_RESULT_PATH = '$.gcp_info'

        DPROC_SUBMIT_POLLER_STATUS_PATH = '$.dproc_submit_poll_status'
        DPROC_SUBMIT_RESULT_PATH = '$.gcp_info'

        DPROC_DELETE_POLLER_STATUS_PATH = '$.dproc_delete_poll_status'
        DPROC_DELETE_INPUT_PATH = '$.dataproc_submit.response'
        DPROC_DELETE_RESULT_PATH = '$.dataproc_delete.response'

        REMAP_JOB_IDS_PATH = '$.remap_job_ids'
        ALIGNMT_JOB_IDS_PATH = '$.alignment_job_ids'
        HAPLO_JOB_IDS_PATH = '$.haplo_job_ids'
        GENO_JOB_IDS_PATH = '$.geno_job_ids'
        VQSR_JOB_IDS_PATH = '$.vqsr_job_ids'
        TRIODENOVO_JOB_IDS_PATH = '$.triodenovo_job_ids'
        HDOF_RESULT_PATH = '$.gcp_info'

        START_POINT = self.conf['START_POINT']
        REMAP_REQ = self.conf['REMAP_REQ']
        call_denovos = self.conf['CALL_DENOVOS']

        # VQSR's success state depends on whether
        # VCF_QC should be run
        vcf_qc = True if self.conf['QC'] != None and 'VCF' in self.conf['QC'] else False
        if call_denovos:
            vqsr_succ = TRIODENOVO_SUBMIT
        elif vcf_qc:
            vqsr_succ = HDOF_SUBMIT
        else:
            vqsr_succ = 'PipelineSucceeded'

        #vqsr_succ = HDOF_SUBMIT if vcf_qc else 'PipelineSucceeded'
        #align_succ = 'PipelineSucceeded' if START_POINT == "remap" else HAPLO_SUBMIT
        #haplo_succ = 'PipelineSucceeded' if START_POINT == "fastq_to_haplo" else GENO_SUBMIT

        remap_state = AsyncPoller(
            async_task=Task(
                name=REMAP_SUBMIT,
                resource=REMAP_SUBMIT_ARN_VAR,
                result_path=REMAP_JOB_IDS_PATH,
                next=REMAP_POLLER
            ),
            pollr_task=Task(
                name=REMAP_POLLER,
                resource=REMAP_POLLER_ARN_VAR,
                input_path=REMAP_JOB_IDS_PATH,
                result_path=REMAP_POLLER_STATUS_PATH
            ),
            faild_task=Fail(
                name='RemapFailed'
            ),
            succd_task=ALIGNMT_SUBMIT,
            stats_path=REMAP_POLLER_STATUS_PATH,
            pollr_wait_time=self.conf["POLLER_WAIT_TIME"]
        ).states()
        alignment_state = AsyncPoller(
            async_task=Task(
                name=ALIGNMT_SUBMIT,
                resource=ALIGNMT_SUBMIT_ARN_VAR,
                result_path=ALIGNMT_JOB_IDS_PATH,
                next=ALIGNMT_POLLER
            ),
            pollr_task=Task(
                name=ALIGNMT_POLLER,
                resource=ALIGNMT_POLLER_ARN_VAR,
                input_path=ALIGNMT_JOB_IDS_PATH,
                result_path=ALIGNMT_POLLER_STATUS_PATH
            ),
            faild_task=Fail(
                name='AlignmentProcessingFailed'
            ),
            succd_task=HAPLO_SUBMIT,
            stats_path=ALIGNMT_POLLER_STATUS_PATH,
            pollr_wait_time=self.conf['POLLER_WAIT_TIME']
        ).states()
        haplo_state = AsyncPoller(
            stats_path=HAPLO_POLLER_STATUS_PATH,
            async_task=Task(
                name=HAPLO_SUBMIT,
                resource=HAPLO_SUBMIT_ARN_VAR,
                result_path=HAPLO_JOB_IDS_PATH,
                next=HAPLO_POLLER
            ),
            pollr_task=Task(
                name=HAPLO_POLLER,
                resource=HAPLO_POLLER_ARN_VAR,
                input_path=HAPLO_JOB_IDS_PATH,
                result_path=HAPLO_POLLER_STATUS_PATH
            ),
            faild_task=Fail(
                name='HaploProcessingFailed'
            ),
            succd_task=GENO_SUBMIT,
            pollr_wait_time=self.conf['POLLER_WAIT_TIME']
        ).states()
        geno_state = AsyncPoller(
            stats_path=GENO_POLLER_STATUS_PATH,
            async_task=Task(
                name=GENO_SUBMIT,
                resource=GENO_SUBMIT_ARN_VAR,
                result_path=GENO_JOB_IDS_PATH,
                next=GENO_POLLER
            ),
            pollr_task=Task(
                name=GENO_POLLER,
                resource=GENO_POLLER_ARN_VAR,
                input_path=GENO_JOB_IDS_PATH,
                result_path=GENO_POLLER_STATUS_PATH
            ),
            faild_task=Fail(
                name='GenoProcessingFailed'
            ),
            succd_task=VQSR_SUBMIT,
            pollr_wait_time=self.conf['POLLER_WAIT_TIME']
        ).states()
        vqsr_state = AsyncPoller(
            stats_path=VQSR_POLLER_STATUS_PATH,
            async_task=Task(
                name=VQSR_SUBMIT,
                resource=VQSR_SUBMIT_ARN_VAR,
                result_path=VQSR_JOB_IDS_PATH,
                next=VQSR_POLLER
            ),
            pollr_task=Task(
                name=VQSR_POLLER,
                resource=VQSR_POLLER_ARN_VAR,
                input_path=VQSR_JOB_IDS_PATH,
                result_path=VQSR_POLLER_STATUS_PATH
            ),
            faild_task=Fail(
                name='VQSRFailed'
            ),
            succd_task=vqsr_succ,
            pollr_wait_time=self.conf['POLLER_WAIT_TIME']
        ).states()
        triodenovo_state = AsyncPoller(
            stats_path=TRIODENOVO_POLLER_STATUS_PATH,
            async_task=Task(
                name=TRIODENOVO_SUBMIT,
                resource=TRIODENOVO_SUBMIT_ARN_VAR,
                result_path=TRIODENOVO_JOB_IDS_PATH,
                next=TRIODENOVO_POLLER
            ),
            pollr_task=Task(
                name=TRIODENOVO_POLLER,
                resource=TRIODENOVO_POLLER_ARN_VAR,
                input_path=TRIODENOVO_JOB_IDS_PATH,
                result_path=TRIODENOVO_POLLER_STATUS_PATH
            ),
            faild_task=Fail(
                name='TriodenovoFailed'
            ),
            succd_task='PipelineSucceeded',
            pollr_wait_time=self.conf['POLLER_WAIT_TIME']
        ).states()
        hdof_state = AsyncPoller(
            stats_path=HDOF_POLLER_STATUS_PATH,
            async_task=Task(
                name=HDOF_SUBMIT,
                resource=HDOF_SUBMIT_ARN_VAR,
                result_path=HDOF_RESULT_PATH,
                next=HDOF_POLLER
            ),
            pollr_task=Task(
                name=HDOF_POLLER,
                resource=HDOF_POLLER_ARN_VAR,
                input_path=HDOF_RESULT_PATH,
                result_path=HDOF_POLLER_STATUS_PATH
            ),
            faild_task=Fail(
                name='HandoffFailed'
            ),
            succd_task=DPROC_CREATE,
            pollr_wait_time=self.conf['POLLER_WAIT_TIME']
        ).states()
        dproc_create_state = AsyncPoller(
            stats_path=DPROC_CREATE_POLLER_STATUS_PATH,
            async_task=Task(
                name=DPROC_CREATE,
                resource=DPROC_CREATE_ARN_VAR,
                #input_path=DPROC_CREATE_INPUT_PATH,
                result_path=DPROC_CREATE_RESULT_PATH,
                next=DPROC_CREATE_POLLER
            ),
            pollr_task=Task(
                name=DPROC_CREATE_POLLER,
                resource=DPROC_CREATE_POLLER_ARN_VAR,
                input_path=DPROC_CREATE_RESULT_PATH,
                result_path=DPROC_CREATE_POLLER_STATUS_PATH
            ),
            faild_task=Fail(
                name='ClusterCreationFailed'
            ),
            succd_task=DPROC_SUBMIT,
            pollr_wait_time=self.conf['POLLER_WAIT_TIME']
        ).states()
        dproc_submit_state = AsyncPoller(
            stats_path=DPROC_SUBMIT_POLLER_STATUS_PATH,
            async_task=Task(
                name=DPROC_SUBMIT,
                resource=DPROC_SUBMIT_ARN_VAR,
                #input_path=DPROC_SUBMIT_INPUT_PATH,
                result_path=DPROC_SUBMIT_RESULT_PATH,
                next=DPROC_SUBMIT_POLLER
            ),
            pollr_task=Task(
                name=DPROC_SUBMIT_POLLER,
                resource=DPROC_SUBMIT_POLLER_ARN_VAR,
                input_path=DPROC_SUBMIT_RESULT_PATH,
                result_path=DPROC_SUBMIT_POLLER_STATUS_PATH
            ),
            faild_task=Fail(
                name='QCFailed'
            ),
            succd_task=DPROC_DELETE,
            pollr_wait_time=self.conf['POLLER_WAIT_TIME']
        ).states()
        dproc_delete_state = AsyncPoller(
            stats_path=DPROC_DELETE_POLLER_STATUS_PATH,
            async_task=Task(
                name=DPROC_DELETE,
                resource=DPROC_DELETE_ARN_VAR,
                input_path=DPROC_DELETE_INPUT_PATH,
                result_path=DPROC_DELETE_RESULT_PATH,
                next=DPROC_DELETE_POLLER
            ),
            pollr_task=Task(
                name=DPROC_DELETE_POLLER,
                resource=DPROC_DELETE_POLLER_ARN_VAR,
                input_path=DPROC_DELETE_RESULT_PATH,
                result_path=DPROC_DELETE_POLLER_STATUS_PATH
            ),
            faild_task=Fail(
                name='ClusterDeleteFailed'
            ),
            succd_task='PipelineSucceeded',
            pollr_wait_time=self.conf['POLLER_WAIT_TIME']
        ).states()

        if not vcf_qc:
            if call_denovos:
                if START_POINT == 'fastq':
                    MACHINE_START = ALIGNMT_SUBMIT
                    MACHINE_STATES = States(*self.flatten(
                        alignment_state,
                        haplo_state,
                        geno_state,
                        vqsr_state,
                        triodenovo_state,
                        Succeed(name='PipelineSucceeded')))
                elif START_POINT == 'bam':
                    if REMAP_REQ:
                        MACHINE_START = REMAP_SUBMIT
                        MACHINE_STATES = States(*self.flatten(
                            remap_state,
                            alignment_state,
                            haplo_state,
                            geno_state,
                            vqsr_state,
                            triodenovo_state,
                            Succeed(name='PipelineSucceeded')))
                    else:
                        MACHINE_START = HAPLO_SUBMIT
                        MACHINE_STATES = States(*self.flatten(
                            haplo_state,
                            geno_state,
                            vqsr_state,
                            triodenovo_state,
                            Succeed(name='PipelineSucceeded')))
                elif START_POINT == 'gvcf':
                    MACHINE_START = GENO_SUBMIT
                    MACHINE_STATES = States(*self.flatten(
                        geno_state,
                        vqsr_state,
                        triodenovo_state,
                        Succeed(name='PipelineSucceeded')))
                elif START_POINT == 'vcf':
                    MACHINE_START = VQSR_SUBMIT
                    MACHINE_STATES = States(*self.flatten(
                        vqsr_state,
                        triodenovo_state,
                        Succeed(name='PipelineSucceeded')))
                else:
                    raise ValueError('Invalid START_POINT; must be fastq|bam|gvcf|vcf!')
            else:
                if START_POINT == 'fastq':
                    MACHINE_START = ALIGNMT_SUBMIT
                    MACHINE_STATES = States(*self.flatten(
                        alignment_state,
                        haplo_state,
                        geno_state,
                        vqsr_state,
                        Succeed(name='PipelineSucceeded')))
                elif START_POINT == 'bam':
                    if REMAP_REQ:
                        MACHINE_START = REMAP_SUBMIT
                        MACHINE_STATES = States(*self.flatten(
                            remap_state,
                            alignment_state,
                            haplo_state,
                            geno_state,
                            vqsr_state,
                            Succeed(name='PipelineSucceeded')))
                    else:
                        MACHINE_START = HAPLO_SUBMIT
                        MACHINE_STATES = States(*self.flatten(
                            haplo_state,
                            geno_state,
                            vqsr_state,
                            Succeed(name='PipelineSucceeded')))
                elif START_POINT == 'gvcf':
                    MACHINE_START = GENO_SUBMIT
                    MACHINE_STATES = States(*self.flatten(
                        geno_state,
                        vqsr_state,
                        Succeed(name='PipelineSucceeded')))
                elif START_POINT == 'vcf':
                    MACHINE_START = VQSR_SUBMIT
                    MACHINE_STATES = States(*self.flatten(
                        vqsr_state,
                        Succeed(name='PipelineSucceeded')))
                else:
                    raise ValueError('Invalid START_POINT; must be fastq|bam|gvcf|vcf!')
        else:
            # Run hdof -> hail
            print('Running VCF QC on GCP.')
            if START_POINT == 'fastq':
                MACHINE_START = ALIGNMT_SUBMIT
                MACHINE_STATES = States(*self.flatten(
                    alignment_state,
                    haplo_state,
                    geno_state,
                    vqsr_state,
                    hdof_state,
                    dproc_create_state,
                    dproc_submit_state,
                    dproc_delete_state,
                    Succeed(name='PipelineSucceeded')))
            elif START_POINT == 'bam':
                if REMAP_REQ:
                    MACHINE_START = REMAP_SUBMIT
                    MACHINE_STATES = States(*self.flatten(
                        remap_state,
                        alignment_state,
                        haplo_state,
                        geno_state,
                        vqsr_state,
                        hdof_state,
                        dproc_create_state,
                        dproc_submit_state,
                        dproc_delete_state,
                        Succeed(name='PipelineSucceeded')))
                else:
                    MACHINE_START = HAPLO_SUBMIT
                    MACHINE_STATES = States(*self.flatten(
                        haplo_state,
                        geno_state,
                        vqsr_state,
                        hdof_state,
                        dproc_create_state,
                        dproc_submit_state,
                        dproc_delete_state,
                        Succeed(name='PipelineSucceeded')))
            elif START_POINT == 'gvcf':
                MACHINE_START = GENO_SUBMIT
                MACHINE_STATES = States(*self.flatten(
                    geno_state,
                    vqsr_state,
                    hdof_state,
                    dproc_create_state,
                    dproc_submit_state,
                    dproc_delete_state,
                    Succeed(name='PipelineSucceeded')))
            elif START_POINT == 'vcf':
                MACHINE_START = VQSR_SUBMIT
                MACHINE_STATES = States(*self.flatten(
                    vqsr_state,
                    hdof_state,
                    dproc_create_state,
                    dproc_submit_state,
                    dproc_delete_state,
                    Succeed(name='PipelineSucceeded')))
            elif START_POINT == 'hdof':
                MACHINE_START = HDOF_SUBMIT
                MACHINE_STATES = States(*self.flatten(
                    hdof_state,
                    dproc_create_state,
                    dproc_submit_state,
                    dproc_delete_state,
                    Succeed(name='PipelineSucceeded')))
            else:
                raise ValueError('Invalid START_POINT; must be fastq|bam|gvcf|vcf|hdof!')

        #State machines are defined below for each pipeline phase
        self.mx = StateMachine(
            name=MACHINE_NAME,
            start=MACHINE_START,
            states=MACHINE_STATES
        )

        return self.mx

    def get_nested_batch_stack_hack(self, parent_stack):
        batch_stack_name = parent_stack.Resource('BatchResourcesStack').physical_resource_id.split('/')[1]
        print('Batch stack name: {}'.format(batch_stack_name))
        batch_stack = Stack.from_stack_name(stack_name=batch_stack_name).stack
        print('Batch stack type: {}'.format(type(batch_stack)))
        return batch_stack

    def build_input(self, stack):
        '''
        The following are input objects are provided to the state machines above.

        :param stack: boto3.CloudFormation.Stack
        :return: dictionary structure representing json object to be
        passed as input to machine execution
        '''

        stack = self.get_nested_batch_stack_hack(parent_stack=stack)

        def get_phys_resource_id(resource):
            return stack.Resource(resource).physical_resource_id.split('/')[-1]

        queue_physical_resource_id = get_phys_resource_id('GeneralPurposeQueue')
        qc_q_physical_resource_id = get_phys_resource_id('QCQueue')


        # Remapping bams job def
        sam_to_fq_job_def = get_phys_resource_id("samtofqJobDef")

        # Alignment and alignment processing job defs
        submitter_job_def = get_phys_resource_id('submitterJobDef')
        bwa_job_def = get_phys_resource_id('bwamemJobDef')
        sort_sam_job_def = get_phys_resource_id('sortsamJobDef')
        mark_dups_job_def = get_phys_resource_id('markdupsJobDef')
        index_bam_job_def = get_phys_resource_id('indexbamJobDef')
        base_recal_table_job_def = get_phys_resource_id('baserecaltableJobDef')
        base_recal_job_def = get_phys_resource_id('baserecalJobDef')

        # Bam QC job defs
        pipeline_bam_qc_job_def = get_phys_resource_id('pipelinebamqcJobDef')

        # Variant calling and genotyping job defs
        sentieon_haplotyper_job_def = get_phys_resource_id('sentieonhaplotyperJobDef')
        sentieon_genotyper_job_def = get_phys_resource_id('sentieongenotyperJobDef')

        # VQSR job defs
        vqsr_snp_model_job_def = get_phys_resource_id('vqsrsnpmodelJobDef')
        vqsr_snp_apply_job_def = get_phys_resource_id('vqsrsnpapplyJobDef')
        vqsr_indel_model_job_def = get_phys_resource_id('vqsrindelmodelJobDef')
        vqsr_indel_apply_job_def = get_phys_resource_id('vqsrindelapplyJobDef')

        # Denovo calling job defs
        fam_vcf_from_cohort_job_def = get_phys_resource_id('famvcffromcohortJobDef')
        scrub_vcf_job_def = get_phys_resource_id('scrubvcfJobDef')
        ped_from_vcf_job_def = get_phys_resource_id('pedfromvcfJobDef')
        triodenovo_job_def = get_phys_resource_id('triodenovoJobDef')

        # GCP job defs
        dproc_create_job_def = get_phys_resource_id('dproccreateJobDef')
        dproc_submit_job_def = get_phys_resource_id('dprocsubmitJobDef')
        dproc_delete_job_def = get_phys_resource_id('dprocdeleteJobDef')

        # Interpret the ref bucket to use depending on user input

        start_point = self.conf['START_POINT']
        ref_uri = self.conf['REF_URI']
        in_uri = self.conf['INPUT_PREFIX_URI']
        results_uri = self.conf['OUTPUT_PREFIX_URI']
        sample_s3_prefix = self.conf['USER_ASSETS_PREFIX']
        sample_s3_bucket = self.conf['USER_ASSETS_BUCKET']
        sample_file = self.conf['SAMPLE_FILE']
        fam_file = self.conf['FAM_FILE']
        num_samples = self.conf['NUM_SAMPLES']
        cohort_prefix = self.conf['COHORT_LABEL']
        test_cohort = self.conf['VQSR_TEST_COHORT_KEY']
        test_in_uri = self.conf['VQSR_TEST_DATA_URI_PREFIX']
        suffix = self.conf['FASTQ_SUFFIX']
        mode = self.conf['MODE']
        build = self.conf['BUILD']
        ome = self.conf['OME']
        target_file = self.conf['TARGET_FILE_NAME']
        sentieon_pkg = self.conf['SENTIEON_PACKAGE_NAME']
        sentieon_license = self.conf['SENTIEON_LICENSE_FILE_NAME']
        param_file = self.conf['PARAM_FILE']
        cloud_transfer_outbucket = self.conf['CLOUD_TRANSFER_OUTBUCKET']
        access_key_id = self.conf['ACCESS_KEY_ID']
        secret_access_key = self.conf['SECRET_ACCESS_KEY']
        zone = self.conf['ZONE']
        project_id = self.conf['PROJECT_ID']
        cloud_file = self.conf['CLOUD_FILE']
        cloudspan_mode = self.conf['CLOUDSPAN_MODE']
        cluster_name = self.conf['CLUSTER_NAME']
        giab_bucket = self.conf['GIAB_BUCKET']
        hail_script_bucket = self.conf['HAIL_SCRIPT_BUCKET']
        validation_script_key = self.conf['VALIDATION_SCRIPT_KEY']
        qc_script_key = self.conf['QC_SCRIPT_KEY']
        bam_qc = True if self.conf['QC'] != None and 'BAM' in self.conf['QC'] else False
        remap = True if self.conf['REMAP_REQ'] is True else False
        #Load json file as a python dict
        with open(self.conf['GCP_CREDS']) as fh:
            gcp_credentials = json.loads(fh.read())

        # Check if lassets_uri has prefix, join appropriately
        if self.conf['USER_ASSETS_PREFIX'] != '':
            assets_uri = 's3://{}/{}/'.format(
                self.conf['USER_ASSETS_BUCKET'],
                self.conf['USER_ASSETS_PREFIX'])
        else:
            assets_uri = 's3://{}/'.format(self.conf['USER_ASSETS_BUCKET'])

        #
        # Mode argument is one of ['test', 'prod'], this is only checked in vqsr.py
        #

        self.mx_input = {
            'start_point' : start_point,
            'sample_s3_prefix' : sample_s3_prefix,
            'sample_s3_bucket' : sample_s3_bucket,
            'sample_file' : sample_file,
            'fam_file' : fam_file,
            'remap' : remap,
            'suffix' : suffix,
            'cohort_prefix': cohort_prefix,
            'build': build,
            #
            # For testing modes
            #
            'mode' : {
                'label':mode,
                'ome':ome,
                'test': {
                    'threads': {
                        'bwa' : 8,
                        'brt' : 4,
                        'hap' : 8
                    }

                },
                'prod': {
                    'threads': {
                        'bwa' : 36,
                        'brt' : 16,
                        'hap' : 36
                    }

                }
            },
            'target_file': target_file,
            'param_file': param_file,
            'test_in_uri': test_in_uri,
            'test_cohort': test_cohort,
            'in_uri': in_uri,
            'results_uri': results_uri,
            'ref_uri': ref_uri,
            'assets_uri' : assets_uri,
            'sentieon_pkg' : sentieon_pkg,
            'sentieon_license': sentieon_license,
            'bam_qc' : bam_qc,
            'queue': queue_physical_resource_id,
            'qc_queue' : qc_q_physical_resource_id,
            'job_defs': {
                'submitter_job': submitter_job_def,
                'sam_to_fq_job': sam_to_fq_job_def,
                'bwa_mem_job': bwa_job_def,
                'sort_sam_job': sort_sam_job_def,
                'mark_dups_job': mark_dups_job_def,
                'index_bam_job': index_bam_job_def,
                'base_recal_table_job': base_recal_table_job_def,
                'base_recal_job': base_recal_job_def,
                'sentieon_haplotyper_job': sentieon_haplotyper_job_def,
                'pipeline_bam_qc_job' : pipeline_bam_qc_job_def,
                'sentieon_genotyper_job': sentieon_genotyper_job_def,
                'vqsr_snp_model_job' : vqsr_snp_model_job_def,
                'vqsr_snp_apply_job' : vqsr_snp_apply_job_def,
                'vqsr_indel_model_job' : vqsr_indel_model_job_def,
                'vqsr_indel_apply_job' : vqsr_indel_apply_job_def,
                'fam_vcf_from_cohort_job' : fam_vcf_from_cohort_job_def,
                'scrub_vcf_job' : scrub_vcf_job_def,
                'ped_from_vcf_job' : ped_from_vcf_job_def,
                'triodenovo_job' : triodenovo_job_def,
                'dproc_create_job' : dproc_create_job_def,
                'dproc_submit_job' : dproc_submit_job_def,
                'dproc_delete_job' : dproc_delete_job_def
            },
            'gcp_info': {
                #
                # Handoff
                #
                'project_id' : project_id,
                'sink_bucket': cloud_transfer_outbucket,
                'aws_access_key': access_key_id,
                'aws_secret_key': secret_access_key,
                'GCP_creds': gcp_credentials,
                #
                # Dataproc
                #
                'dproc_created' : False,
                'zone' : zone,
                'cloud_template' : cloud_file,
                'cloudspan_mode' : cloudspan_mode,
                'cluster_name' : cluster_name,
                'giab_bucket' : giab_bucket,
                'hail_script_bucket' : hail_script_bucket,
                'validation_script_key' : validation_script_key,
                'qc_script_key' : qc_script_key,
                'num_samples' : num_samples
            }
        }

        return self.mx_input
