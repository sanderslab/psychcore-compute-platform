import uuid


def calculate_max_cpus(num_samples, mode):
    '''
    Calculate the max number of CPUs for the compute environment
    based on the number of samples.
    :param num_samples: Int
    :return: max_cpus: Int
    '''
    if mode == 'prod':
        instance_num_vcpus = [
            ('c5.9xlarge', 36, 'multi'),
            ('c5.18xlarge', 72, 'single'),
            ('r4.2xlarge', 8, 'multi'),
            ('r4.4xlarge', 16, 'single')]
    elif mode == 'test':
        instance_num_vcpus = [
            ('c5.2xlarge', 8, 'multi'),
            ('r4.xlarge', 4, 'multi'),
            ('r4.4xlarge', 16, 'single')]
    else:
        raise ValueError('Invalid mode!')
    max_cpus = 0
    for inst in instance_num_vcpus:
        if inst[2] == 'multi':
            max_cpus += num_samples * inst[1]
        elif inst[2] == 'single':
            max_cpus += 1
    return max_cpus


def generate_uid():
    return str(uuid.uuid4())[:4]


def merge_configs(user_settings, args):
    '''
    Generate system configuration (conf) object and merge
    it with user configuration
    :param user_settings: Dict
    :return conf: Dict
    '''
    conf = dict()
    conf['STACK_UID'] = generate_uid()
    default_params = [
        'DOCKER_ACCOUNT',
        'STACK_NAME',
        'GPCE_VPC_ID',
        'GPCE_INSTANCE_TYPES',
        'CONTAINER_NAMES',
        'PARAM_FILE',
        'PARAM_PATH',
        'PARAM_BUCKET',
        'PARAM_PREFIX',
        'FASTQ_SUFFIX',
        'MODE',
        'POLL_TIME',
        'RESOURCE_CFN_TMPL_DEPLOY_BUCKET'
    ]

    #
    # Credentials for Handoff
    #
    conf['ACCESS_KEY_ID'] = args.access_key_id
    conf['SECRET_ACCESS_KEY'] = args.secret_access_key

    #
    # Runtime/launch settings from user, no defaults
    #
    conf['START_POINT'] = user_settings['START_POINT']
    conf['REMAP_REQ'] = True if 'REMAP' in user_settings else False
    conf['REF_URI'] = user_settings['REF_URI']
    conf['INPUT_PREFIX_URI'] = user_settings['INPUT_URI']
    conf['OUTPUT_PREFIX_URI'] = user_settings['OUTPUT_URI']
    conf['USER_ASSETS_URI'] = user_settings['USER_ASSETS_URI']
    conf['LOCAL_ASSETS_DIR'] = user_settings['LOCAL_ASSETS_DIR']
    conf['COHORT_LABEL'] = user_settings['COHORT_PREFIX']
    conf['COHORT_PREFIX'] = user_settings['COHORT_PREFIX']
    conf['QC'] = user_settings['QC']
    # Get assets bucket and prefix from uri
    conf['USER_ASSETS_BUCKET'] = conf['USER_ASSETS_URI'].split('/')[2]
    conf['USER_ASSETS_PREFIX'] = '/'.join(conf['USER_ASSETS_URI'].split('/')[3:-1])

    conf['NUM_SAMPLES'] = user_settings['NUM_SAMPLES']
    conf['SAMPLE_FILE'] = user_settings['SAMPLE_FILE']
    conf['FAM_FILE'] = user_settings['FAM_FILE'] if 'FAM_FILE' in user_settings else None
    conf['CALL_DENOVOS'] = True if conf['FAM_FILE'] != None else False

    conf['MODE'] = user_settings['MODE']
    conf['BUILD'] = user_settings['BUILD']
    conf['OME'] = user_settings['OME']
    if conf['OME'] == 'wes':
        conf['TARGET_FILE_NAME'] = user_settings['TARGET']
    else:
        conf['TARGET_FILE_NAME'] = 'None'

        conf['TARGET_FILE_S3_KEY_PREFIX'] = 'None'
        conf['TARGET_FILE_S3_BUCKET'] = 'None'

    conf['POLLER_WAIT_TIME'] = 30
    conf['CFN_PARAM_GPCE_MAX_CPUS'] = 'GPCEMaxVcpus'
    conf['RESOURCE_CFN_TMPL_DEPLOY_BUCKET'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['RESOURCE_CFN_TMPL_DEPLOY_KEY'] = conf['STACK_UID']


    #
    # Default params which can be altered by user
    #

    conf['DOCKER_ACCOUNT'] = 'ucsfpsychcore'
    conf['STACK_NAME'] = 'psychcore-ngs-pipeline'

    conf['GPCE_VPC_ID'] = 1234
    conf['GPCE_INSTANCE_TYPES'] = 'm4.large, c5.2xlarge, c5.9xlarge, c5.18xlarge, r4.xlarge, r4.2xlarge, r4.4xlarge'
    conf['GPCE_INSTANCE_TYPE_MEMS'] = {
        'c5.2xlarge': 16,
        'c5.9xlarge': 72,
        'c5.18xlarge': 144,
        'r4.xlarge': 30.5,
        'r4.2xlarge': 61,
        'r4.4xlarge': 122
    }
    conf['CONTAINER_NAMES'] = {
        'submitter' : 'submitter',
        'bwa_mem' : 'bwa_mem',
        'sam_to_fq' : 'picard',
        'sort_sam' : 'picard',
        'mark_dups' : 'picard',
        'index_bam' : 'picard',
        'base_recal_table' : 'gatk',
        'base_recal' : 'gatk',
        'sentieon_haplotyper' : 'sentieon',
        'sentieon_genotyper' : 'sentieon',
        'vqsr_snp_model' : 'gatk',
        'vqsr_snp_apply' : 'gatk',
        'vqsr_indel_model' : 'gatk',
        'vqsr_indel_apply' : 'gatk',
        'pipeline_bam_qc' : 'picard',
        'fam_vcf_from_cohort' : 'gatk',
        'ped_from_vcf' : 'triodenovo',
        'scrub_vcf' : 'triodenovo',
        'triodenovo' : 'triodenovo',
        'dproc_create' : 'dproc',
        'dproc_submit' : 'dproc',
        'dproc_delete' : 'dproc'
    }
        
    conf['PARAM_FILE'] = 'tool_parameter_{}_{}.yaml'.format(conf['OME'], conf['BUILD'])
    conf['FASTQ_SUFFIX'] = '_001'
    conf['MODE'] = 'prod'
    conf['POLL_TIME'] = 300

    conf['VQSR_TEST_DATA_URI_PREFIX'] = 's3://pipeline-validation/sfn-test/'
    conf['VQSR_TEST_COHORT_KEY'] = 'SSC_chr17_02_28_sentieon'

    conf['CFN_ARGMT_GPCE_MAX_CPUS'] = calculate_max_cpus(int(conf['NUM_SAMPLES']), conf['MODE'])

    #
    # Change user changeable configuration if the param is in run.yaml
    #

    for param in default_params:
        print(param)
        if param in user_settings:
            conf[param] = user_settings[param]

    conf['RKSTR8_PKG_LOCAL_PATH'] = 'rkstr8'
    conf['RKSTR8_PKG_ARCHIVE_LOCAL_PATH_PREFIX'] = 'rkstr8_{}.pkg'.format(conf['STACK_UID'])
    conf['RKSTR8_PKG_ARCHIVE_LOCAL_PATH'] = 'rkstr8_{}.pkg.zip'.format(conf['STACK_UID'])
    conf['RKSTR8_PKG_REMOTE_BUCKET'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['RKSTR8_PKG_REMOTE_KEY'] = conf['RKSTR8_PKG_ARCHIVE_LOCAL_PATH']

    #
    # Lambda
    #

    conf['LAMBDA_MODULE_NAME'] = 'cloudspan'

    conf['LAMBDA_SCRIPT_FILE'] = 'handlers/{module_name}.py'.format(
        module_name=conf['LAMBDA_MODULE_NAME'])
    conf['LAMBDA_REQUIREMENTS_FILE'] = 'handlers/cloudspan_requirements.txt'
    conf['LAMBDA_BUILD_DIR'] = '.lambda_build'
    conf['LAMBDA_DEPLOYMENT_ZIP'] = 'sfn.deployable'
    conf['LAMBDA_DEPLOY_BUCKET'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['LAMBDA_DEPLOY_KEY'] = 'sfn.deployable.zip'


    #
    # Lambda (and StateMachine) building config
    #


    conf['REMAP_LAMBDA_MODULE_NAME'] = 'sam_to_fq'
    conf['REMAP_LAMBDA_SCRIPT_FILE'] = 'handlers/{module_name}.py'.format(
        module_name=conf['REMAP_LAMBDA_MODULE_NAME'])
    conf['REMAP_LAMBDA_REQUIREMENTS_FILE'] = 'handlers/sam_to_fq_requirements.txt'
    conf['REMAP_LAMBDA_BUILD_DIR'] = '.sam_to_fq_lambda_build'
    conf['REMAP_LAMBDA_DEPLOYMENT_ZIP'] = 'sam_to_fq.deployable'
    conf['REMAP_LAMBDA_DEPLOY_BUCKET'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['REMAP_LAMBDA_DEPLOY_KEY'] = 'sam_to_fq.deployable.zip'

    conf['ALIGN_POLLER_LAMBDA_MODULE_NAME'] = 'alignment_polling'
    conf['ALIGN_POLLER_LAMBDA_SCRIPT_FILE'] = 'handlers/{module_name}.py'.format(
        module_name=conf['ALIGN_POLLER_LAMBDA_MODULE_NAME'])
    conf['ALIGN_POLLER_LAMBDA_REQUIREMENTS_FILE'] = 'handlers/alignment_polling_requirements.txt'
    conf['ALIGN_POLLER_LAMBDA_BUILD_DIR'] = '.alignment_polling_lambda_build'
    conf['ALIGN_POLLER_LAMBDA_DEPLOYMENT_ZIP'] = 'alignment_polling.deployable'
    conf['ALIGN_POLLER_LAMBDA_DEPLOY_BUCKET'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['ALIGN_POLLER_LAMBDA_DEPLOY_KEY'] = 'alignment_polling.deployable.zip'

    conf['VQSR_LAMBDA_MODULE_NAME'] = 'vqsr'
    conf['VQSR_LAMBDA_SCRIPT_FILE'] = 'handlers/{module_name}.py'.format(
        module_name=conf['VQSR_LAMBDA_MODULE_NAME'])
    conf['VQSR_LAMBDA_REQUIREMENTS_FILE'] = 'handlers/vqsr_requirements.txt'
    conf['VQSR_LAMBDA_BUILD_DIR'] = '.vqsr_lambda_build'
    conf['VQSR_LAMBDA_DEPLOYMENT_ZIP'] = 'vqsr.deployable'
    conf['VQSR_LAMBDA_DEPLOY_BUCKET'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['VQSR_LAMBDA_DEPLOY_KEY'] = 'vqsr.deployable.zip'

    conf['TRIODENOVO_LAMBDA_MODULE_NAME'] = 'triodenovo'
    conf['TRIODENOVO_LAMBDA_SCRIPT_FILE'] = 'handlers/{module_name}.py'.format(
        module_name=conf['TRIODENOVO_LAMBDA_MODULE_NAME'])
    conf['TRIODENOVO_LAMBDA_REQUIREMENTS_FILE'] = 'handlers/triodenovo_requirements.txt'
    conf['TRIODENOVO_LAMBDA_BUILD_DIR'] = '.triodenovo_lambda_build'
    conf['TRIODENOVO_LAMBDA_DEPLOYMENT_ZIP'] = 'triodenovo.deployable'
    conf['TRIODENOVO_LAMBDA_DEPLOY_BUCKET'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['TRIODENOVO_LAMBDA_DEPLOY_KEY'] = 'triodenovo.deployable.zip'

    conf['ALIGN_LAMBDA_MODULE_NAME'] = 'alignment_processing'
    conf['ALIGN_LAMBDA_SCRIPT_FILE'] = 'handlers/{module_name}.py'.format(
        module_name=conf['ALIGN_LAMBDA_MODULE_NAME'])
    conf['ALIGN_LAMBDA_REQUIREMENTS_FILE'] = 'handlers/alignment_processing_requirements.txt'
    conf['ALIGN_LAMBDA_BUILD_DIR'] = '.alignment_processing_lambda_build'
    conf['ALIGN_LAMBDA_DEPLOYMENT_ZIP'] = 'alignment_processing.deployable'
    conf['ALIGN_LAMBDA_DEPLOY_BUCKET'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['ALIGN_LAMBDA_DEPLOY_KEY'] = 'alignment_processing.2.deployable.zip'

    conf['HAPLO_LAMBDA_MODULE_NAME'] = 'sentieon_haplotyper'
    conf['HAPLO_LAMBDA_SCRIPT_FILE'] = 'handlers/{module_name}.py'.format(
        module_name=conf['HAPLO_LAMBDA_MODULE_NAME'])
    conf['HAPLO_LAMBDA_REQUIREMENTS_FILE'] = 'handlers/sentieon_haplotyper_requirements.txt'
    conf['HAPLO_LAMBDA_BUILD_DIR'] = '.sentieon_haplotyper_lambda_build'
    conf['HAPLO_LAMBDA_DEPLOYMENT_ZIP'] = 'sentieon_haplotyper.deployable'
    conf['HAPLO_LAMBDA_DEPLOY_BUCKET'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['HAPLO_LAMBDA_DEPLOY_KEY'] = 'sentieon_haplotyper.deployable.zip'

    conf['GENO_LAMBDA_MODULE_NAME'] = 'sentieon_genotyper'
    conf['GENO_LAMBDA_SCRIPT_FILE'] = 'handlers/{module_name}.py'.format(
        module_name=conf['GENO_LAMBDA_MODULE_NAME'])
    conf['GENO_LAMBDA_REQUIREMENTS_FILE'] = 'handlers/sentieon_genotyper_requirements.txt'
    conf['GENO_LAMBDA_BUILD_DIR'] = '.sentieon_genotyper_lambda_build'
    conf['GENO_LAMBDA_DEPLOYMENT_ZIP'] = 'sentieon_genotyper.deployable'
    conf['GENO_LAMBDA_DEPLOY_BUCKET'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['GENO_LAMBDA_DEPLOY_KEY'] = 'sentieon_genotyper.deployable.zip'

    conf['CLOUDSPAN_POLLER_LAMBDA_MODULE_NAME'] = 'cloudspan_polling'
    conf['CLOUDSPAN_POLLER_LAMBDA_SCRIPT_FILE'] = 'handlers/{module_name}.py'.format(
        module_name=conf['CLOUDSPAN_POLLER_LAMBDA_MODULE_NAME'])
    conf['CLOUDSPAN_POLLER_LAMBDA_REQUIREMENTS_FILE'] = 'handlers/cloudspan_polling_requirements.txt'
    conf['CLOUDSPAN_POLLER_LAMBDA_BUILD_DIR'] = '.cloudspan_polling_lambda_build'
    conf['CLOUDSPAN_POLLER_LAMBDA_DEPLOYMENT_ZIP'] = 'cloudspan_polling.deployable'
    conf['CLOUDSPAN_POLLER_LAMBDA_DEPLOY_BUCKET'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['CLOUDSPAN_POLLER_LAMBDA_DEPLOY_KEY'] = 'cloudspan_polling.deployable.zip'

    conf['HDOF_LAMBDA_MODULE_NAME'] = 'handoff'
    conf['HDOF_LAMBDA_DEPLOY_KEY'] = 'handoff.deployable.zip'

    conf['VAL_LAMBDA_MODULE_NAME'] = 'validation'

    #
    # Lambda Cloudformation (Parameter, Argument) pairs
    #

    conf['CLOUDSPAN_CFN_PARAM_LAMBDA_BUCKET_NAME'] = 'CloudspanLambdaFuncS3BucketName'
    conf['CLOUDSPAN_CFN_PARAM_LAMBDA_KEY_NAME'] = 'CloudspanLambdaFuncS3KeyName'
    conf['CLOUDSPAN_CFN_PARAM_LAMBDA_MODULE_NAME'] = 'CloudspanLambdaFuncModuleName'

    conf['CLOUDSPAN_CFN_ARGMT_LAMBDA_BUCKET_NAME'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['CLOUDSPAN_CFN_ARGMT_LAMBDA_KEY_NAME'] = conf['LAMBDA_DEPLOY_KEY']
    conf['CLOUDSPAN_CFN_ARGMT_LAMBDA_MODULE_NAME'] = conf['LAMBDA_MODULE_NAME']

    conf['REMAP_CFN_PARAM_LAMBDA_BUCKET_NAME'] = 'RemapLambdaFuncS3BucketName'
    conf['REMAP_CFN_PARAM_LAMBDA_KEY_NAME'] = 'RemapLambdaFuncS3KeyName'
    conf['REMAP_CFN_PARAM_LAMBDA_MODULE_NAME'] = 'RemapLambdaFuncModuleName'

    conf['REMAP_CFN_ARGMT_LAMBDA_BUCKET_NAME'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['REMAP_CFN_ARGMT_LAMBDA_KEY_NAME'] = conf['REMAP_LAMBDA_DEPLOY_KEY']
    conf['REMAP_CFN_ARGMT_LAMBDA_MODULE_NAME'] = conf['REMAP_LAMBDA_MODULE_NAME']

    conf['ALIGN_CFN_PARAM_LAMBDA_BUCKET_NAME'] = 'AlignmentLambdaFuncS3BucketName'
    conf['ALIGN_CFN_PARAM_LAMBDA_KEY_NAME'] = 'AlignmentLambdaFuncS3KeyName'
    conf['ALIGN_CFN_PARAM_LAMBDA_MODULE_NAME'] = 'AlignmentLambdaFuncModuleName'

    conf['ALIGN_CFN_ARGMT_LAMBDA_BUCKET_NAME'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['ALIGN_CFN_ARGMT_LAMBDA_KEY_NAME'] = conf['ALIGN_LAMBDA_DEPLOY_KEY']
    conf['ALIGN_CFN_ARGMT_LAMBDA_MODULE_NAME'] = conf['ALIGN_LAMBDA_MODULE_NAME']

    conf['HAPLO_CFN_PARAM_LAMBDA_BUCKET_NAME'] = 'HaploLambdaFuncS3BucketName'
    conf['HAPLO_CFN_PARAM_LAMBDA_KEY_NAME'] = 'HaploLambdaFuncS3KeyName'
    conf['HAPLO_CFN_PARAM_LAMBDA_MODULE_NAME'] = 'HaploLambdaFuncModuleName'

    conf['HAPLO_CFN_ARGMT_LAMBDA_BUCKET_NAME'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['HAPLO_CFN_ARGMT_LAMBDA_KEY_NAME'] = conf['HAPLO_LAMBDA_DEPLOY_KEY']
    conf['HAPLO_CFN_ARGMT_LAMBDA_MODULE_NAME'] = conf['HAPLO_LAMBDA_MODULE_NAME']

    conf['GENO_CFN_PARAM_LAMBDA_BUCKET_NAME'] = 'GenoLambdaFuncS3BucketName'
    conf['GENO_CFN_PARAM_LAMBDA_KEY_NAME'] = 'GenoLambdaFuncS3KeyName'
    conf['GENO_CFN_PARAM_LAMBDA_MODULE_NAME'] = 'GenoLambdaFuncModuleName'

    conf['GENO_CFN_ARGMT_LAMBDA_BUCKET_NAME'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['GENO_CFN_ARGMT_LAMBDA_KEY_NAME'] = conf['GENO_LAMBDA_DEPLOY_KEY']
    conf['GENO_CFN_ARGMT_LAMBDA_MODULE_NAME'] = conf['GENO_LAMBDA_MODULE_NAME']

    conf['VQSR_CFN_PARAM_LAMBDA_BUCKET_NAME'] = 'VQSRLambdaFuncS3BucketName'
    conf['VQSR_CFN_PARAM_LAMBDA_KEY_NAME'] = 'VQSRLambdaFuncS3KeyName'
    conf['VQSR_CFN_PARAM_LAMBDA_MODULE_NAME'] = 'VQSRLambdaFuncModuleName'

    conf['VQSR_CFN_ARGMT_LAMBDA_BUCKET_NAME'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['VQSR_CFN_ARGMT_LAMBDA_KEY_NAME'] = conf['VQSR_LAMBDA_DEPLOY_KEY']
    conf['VQSR_CFN_ARGMT_LAMBDA_MODULE_NAME'] = conf['VQSR_LAMBDA_MODULE_NAME']

    conf['TRIODENOVO_CFN_PARAM_LAMBDA_BUCKET_NAME'] = 'TriodenovoLambdaFuncS3BucketName'
    conf['TRIODENOVO_CFN_PARAM_LAMBDA_KEY_NAME'] = 'TriodenovoLambdaFuncS3KeyName'
    conf['TRIODENOVO_CFN_PARAM_LAMBDA_MODULE_NAME'] = 'TriodenovoLambdaFuncModuleName'

    conf['TRIODENOVO_CFN_ARGMT_LAMBDA_BUCKET_NAME'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['TRIODENOVO_CFN_ARGMT_LAMBDA_KEY_NAME'] = conf['TRIODENOVO_LAMBDA_DEPLOY_KEY']
    conf['TRIODENOVO_CFN_ARGMT_LAMBDA_MODULE_NAME'] = conf['TRIODENOVO_LAMBDA_MODULE_NAME']

    conf['HDOF_CFN_PARAM_LAMBDA_BUCKET_NAME'] = 'HandoffLambdaFuncS3BucketName'
    conf['HDOF_CFN_PARAM_LAMBDA_KEY_NAME'] = 'HandoffLambdaFuncS3KeyName'
    conf['HDOF_CFN_PARAM_LAMBDA_MODULE_NAME'] = 'HandoffLambdaFuncModuleName'

    conf['HDOF_CFN_ARGMT_LAMBDA_BUCKET_NAME'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['HDOF_CFN_ARGMT_LAMBDA_KEY_NAME'] = conf['HDOF_LAMBDA_DEPLOY_KEY']
    conf['HDOF_CFN_ARGMT_LAMBDA_MODULE_NAME'] = conf['HDOF_LAMBDA_MODULE_NAME']

    conf['BATCH_POLLER_CFN_PARAM_LAMBDA_BUCKET_NAME'] = 'BatchPollerLambdaFuncS3BucketName'
    conf['BATCH_POLLER_CFN_ARGMT_LAMBDA_BUCKET_NAME'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['BATCH_POLLER_CFN_PARAM_LAMBDA_KEY_NAME'] = 'BatchPollerLambdaFuncS3KeyName'
    conf['BATCH_POLLER_CFN_ARGMT_LAMBDA_KEY_NAME'] = conf['ALIGN_POLLER_LAMBDA_DEPLOY_KEY']
    conf['BATCH_POLLER_CFN_PARAM_LAMBDA_MODULE_NAME'] = 'BatchPollerLambdaFuncModuleName'
    conf['BATCH_POLLER_CFN_ARGMT_LAMBDA_MODULE_NAME'] = conf['ALIGN_POLLER_LAMBDA_MODULE_NAME']

    conf['CLOUDSPAN_POLLER_CFN_PARAM_LAMBDA_BUCKET_NAME'] = 'CloudspanPollerLambdaFuncS3BucketName'
    conf['CLOUDSPAN_POLLER_CFN_ARGMT_LAMBDA_BUCKET_NAME'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['CLOUDSPAN_POLLER_CFN_PARAM_LAMBDA_KEY_NAME'] = 'CloudspanPollerLambdaFuncS3KeyName'
    conf['CLOUDSPAN_POLLER_CFN_ARGMT_LAMBDA_KEY_NAME'] = conf['CLOUDSPAN_POLLER_LAMBDA_DEPLOY_KEY']
    conf['CLOUDSPAN_POLLER_CFN_PARAM_LAMBDA_MODULE_NAME'] = 'CloudspanPollerLambdaFuncModuleName'
    conf['CLOUDSPAN_POLLER_CFN_ARGMT_LAMBDA_MODULE_NAME'] = conf['CLOUDSPAN_POLLER_LAMBDA_MODULE_NAME']
 
    #
    # Other Cloudformation (Parameter, Argument) config pairs
    #

    conf['STACK_NAME'] = '{}-{}'.format(user_settings['STACK_NAME'], conf['STACK_UID'])
    conf['CFN_PARAM_STACK_NAME'] = 'StackName'
    conf['CFN_ARGMT_STACK_NAME'] = {'Name': conf['STACK_NAME']}
    conf['CFN_ARGMT_GPCE_VPC_ID'] = conf['GPCE_VPC_ID']
    conf['CFN_PARAM_GPCE_VPC_ID'] = 'GPCEVpcId'
    conf['CFN_PARAM_GPCE_INSTANCE_TYPES'] = 'GPCEInstanceTypes'
    conf['CFN_PARAM_GPCE_SSH_KEY_PAIR'] = 'GPCESSHKeyPair'
    conf['CFN_ARGMT_GPCE_INSTANCE_TYPES'] = conf['GPCE_INSTANCE_TYPES']
    conf['CFN_ARGMT_GPCE_SSH_KEY_PAIR'] = user_settings['GPCE_SSH_KEY_PAIR']

    conf['CFN_FSA_LOGICAL_RESOURCE_ID'] = 'PipelineStateMachine'

    #
    # Cloudformation template deployment config
    #

    #
    # CFN Templates and related
    #

    conf['PARENT_TEMPLATE_PATH'] = 'templates/platform_parent.stack.yaml'
    conf['LAMBDA_TEMPLATE_PATH'] = 'templates/lambda_resources.stack.yaml'
    conf['NETWORK_TEMPLATE_PATH'] = 'templates/network_resources.stack.yaml'
    conf['BATCH_TEMPLATE_PATH'] = 'templates/batch_resources.stack.yaml'
    conf['STEPFUNCTIONS_TEMPLATE_PATH'] = 'templates/step_functions_resources.stack.yaml'
    conf['STATE_MACHINE_RESOURCE_FRAGMENT'] = 'templates/fragments/statemachine.json'
    conf['FRAGMENTS_DIR_PATH'] = 'templates/fragments'

    conf['TEMPLATES'] = [
        conf['PARENT_TEMPLATE_PATH'],
        conf['LAMBDA_TEMPLATE_PATH'],
        conf['NETWORK_TEMPLATE_PATH'],
        conf['BATCH_TEMPLATE_PATH'],
        conf['STEPFUNCTIONS_TEMPLATE_PATH']]

    conf['TEMPLATE_LABEL_PATH_MAP'] = {
        'launch': conf['PARENT_TEMPLATE_PATH'],
        'lambda': conf['LAMBDA_TEMPLATE_PATH'],
        'network': conf['NETWORK_TEMPLATE_PATH'],
        'batch': conf['BATCH_TEMPLATE_PATH'],
        'sfn': conf['STEPFUNCTIONS_TEMPLATE_PATH']
    }

    conf['LAMBDA_CFN_PARAM_TEMPLATE_URL'] = 'LambdaTemplateURL'
    conf['NETWORK_CFN_PARAM_TEMPLATE_URL'] = 'NetworkTemplateURL'
    conf['BATCH_CFN_PARAM_TEMPLATE_URL'] = 'BatchTemplateURL'
    conf['STEP_FUNCTIONS_PARAM_TEMPLATE_URL'] = 'StepFunctionsTemplateURL'

    conf['LAMBDA_CFN_ARGMT_TEMPLATE_URL'] = 'https://s3.amazonaws.com/{}/lambda_resources.stack.yaml'.format(conf['RESOURCE_CFN_TMPL_DEPLOY_BUCKET'])
    conf['NETWORK_CFN_ARGMT_TEMPLATE_URL'] = 'https://s3.amazonaws.com/{}/network_resources.stack.yaml'.format(conf['RESOURCE_CFN_TMPL_DEPLOY_BUCKET'])
    conf['BATCH_CFN_ARGMT_TEMPLATE_URL'] = 'https://s3.amazonaws.com/{}/batch_resources.stack.yaml'.format(conf['RESOURCE_CFN_TMPL_DEPLOY_BUCKET'])
    conf['STEP_FUNCTIONS_ARGMT_TEMPLATE_URL'] = 'https://s3.amazonaws.com/{}/step_functions_resources.stack.yaml'.format(conf['RESOURCE_CFN_TMPL_DEPLOY_BUCKET'])

    #
    # Batch config
    #
    
    conf['PIPELINE_CMD_TOOL_PARAM_FILE'] = 'tool_parameter_{}_{}.yaml'.format(conf['OME'], conf['BUILD'])
    conf['PIPELINE_CMD_TOOL_PARAM_LOCAL_PATH'] = './docker'

    conf['SENTIEON_PACKAGE_NAME'] = user_settings['SENTIEON_PACKAGE_NAME']
    conf['SENTIEON_LICENSE_FILE_NAME'] = user_settings['SENTIEON_LICENSE_NAME']


    #
    # Job Def dimension
    #
    conf['MEMS'] = {
        'test': {
            'submitter': '6000',
            'sam_to_fq': '30000',
            'bwa_mem' : '15000',
            'sort_sam' : '30000',
            'mark_dups' : '30000',
            'index_bam' : '30000',
            'base_recal_table' : '30000',
            'base_recal' : '30000',
            'sentieon_haplotyper' : '15000',
            'sentieon_genotyper' : '30000',
            'vqsr_snp_model' : '120000',
            'vqsr_snp_apply' : '120000',
            'vqsr_indel_model' : '120000',
            'vqsr_indel_apply' : '120000',
            'pipeline_bam_qc' : '30000',
            'dproc_create' : '6000',
            'dproc_submit' : '6000',
            'dproc_delete' : '6000',
            'fam_vcf_from_cohort' : '30000',
            'scrub_vcf' : '15000',
            'ped_from_vcf' : '15000',
            'triodenovo' : '15000'
        },
        'prod': {
            'submitter': '6000',
            'sam_to_fq': '56000',
            'bwa_mem' : '66500',
            'sort_sam' : '56000',
            'mark_dups' : '56000',
            'index_bam' : '56000',
            'base_recal_table' : '117000',
            'base_recal' : '117000',
            'sentieon_haplotyper' : '66000',
            'sentieon_genotyper' : '132000',
            'vqsr_snp_model' : '132000',
            'vqsr_snp_apply' : '132000',
            'vqsr_indel_model' : '132000',
            'vqsr_indel_apply' : '132000',
            'pipeline_bam_qc' : '56000',
            'dproc_create' : '6000',
            'dproc_submit' : '6000',
            'dproc_delete' : '6000',
            'fam_vcf_from_cohort' : '56000',
            'scrub_vcf' : '15000',
            'ped_from_vcf' : '15000',
            'triodenovo' : '15000'
        }
    }
    conf['VCPUS'] = {
        'test': {
            'submitter': '2',
            'sam_to_fq': '4',
            'bwa_mem' : '8',
            'sort_sam' : '4',
            'mark_dups' : '4',
            'index_bam' : '4',
            'base_recal_table' : '4',
            'base_recal' : '4',
            'sentieon_haplotyper' : '8',
            'sentieon_genotyper' : '4',
            'vqsr_snp_model' : '16',
            'vqsr_snp_apply' : '16',
            'vqsr_indel_model' : '16',
            'vqsr_indel_apply' : '16',
            'pipeline_bam_qc' : '4',
            'dproc_create' : '2',
            'dproc_submit' : '2',
            'dproc_delete' : '2',
            'fam_vcf_from_cohort' : '4',
            'scrub_vcf' : '8',
            'ped_from_vcf' : '8',
            'triodenovo' : '8'
        },
        'prod': {
            'submitter': '2',
            'sam_to_fq': '8',
            'bwa_mem' : '36',
            'sort_sam' : '8',
            'mark_dups' : '8',
            'index_bam' : '8',
            'base_recal_table' : '16',
            'base_recal' : '16',
            'sentieon_haplotyper' : '36',
            'sentieon_genotyper' : '72',
            'vqsr_snp_model' : '72',
            'vqsr_snp_apply' : '72',
            'vqsr_indel_model' : '72',
            'vqsr_indel_apply' : '72',
            'pipeline_bam_qc' : '8',
            'dproc_create' : '2',
            'dproc_submit' : '2',
            'dproc_delete' : '2',
            'fam_vcf_from_cohort' : '8',
            'scrub_vcf' : '8',
            'ped_from_vcf' : '8',
            'triodenovo' : '8'
        }
    }

    #
    # Google Cloud params
    #
    conf['GCP_CREDS'] = user_settings['GCP_CREDS_FILE']
    conf['CLUSTER_NAME'] = (conf['STACK_NAME']).lower()
    conf['PROJECT_ID'] = user_settings['PROJECT_ID']
    conf['ZONE'] = user_settings['ZONE']

    conf['HDOF_LAMBDA_MODULE_NAME'] = 'handoff'
    conf['HDOF_LAMBDA_SCRIPT_FILE'] = 'handlers/{module_name}.py'.format(
        module_name=conf['HDOF_LAMBDA_MODULE_NAME'])
    conf['HDOF_LAMBDA_REQUIREMENTS_FILE'] = 'handlers/handoff_requirements.txt'
    conf['HDOF_LAMBDA_BUILD_DIR'] = '.handoff_lambda_build'
    conf['HDOF_LAMBDA_DEPLOYMENT_ZIP'] = 'handoff.deployable'
    conf['HDOF_LAMBDA_DEPLOY_BUCKET'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['HDOF_LAMBDA_DEPLOY_KEY'] = 'handoff.deployable.zip'

    conf['HDOF_POLLER_LAMBDA_MODULE_NAME'] = 'handoff_poller'
    conf['HDOF_POLLER_LAMBDA_SCRIPT_FILE'] = 'handlers/{module_name}.py'.format(
        module_name=conf['HDOF_POLLER_LAMBDA_MODULE_NAME'])
    conf['HDOF_POLLER_LAMBDA_REQUIREMENTS_FILE'] = 'handlers/handoff_poller_requirements.txt'
    conf['HDOF_POLLER_LAMBDA_BUILD_DIR'] = '.handoff_poller_lambda_build'
    conf['HDOF_POLLER_LAMBDA_DEPLOYMENT_ZIP'] = 'handoff_poller.deployable'
    conf['HDOF_POLLER_LAMBDA_DEPLOY_BUCKET'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['HDOF_POLLER_LAMBDA_DEPLOY_KEY'] = 'handoff_poller.deployable.zip'

    #
    # Lambda Cloudformation (Parameter, Argument) pairs
    #

    conf['CLOUDSPAN_CFN_PARAM_LAMBDA_BUCKET_NAME'] = 'CloudspanLambdaFuncS3BucketName'
    conf['CLOUDSPAN_CFN_PARAM_LAMBDA_KEY_NAME'] = 'CloudspanLambdaFuncS3KeyName'
    conf['CLOUDSPAN_CFN_PARAM_LAMBDA_MODULE_NAME'] = 'CloudspanLambdaFuncModuleName'

    conf['CLOUDSPAN_CFN_ARGMT_LAMBDA_BUCKET_NAME'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['CLOUDSPAN_CFN_ARGMT_LAMBDA_KEY_NAME'] = conf['LAMBDA_DEPLOY_KEY']
    conf['CLOUDSPAN_CFN_ARGMT_LAMBDA_MODULE_NAME'] = conf['LAMBDA_MODULE_NAME']

    conf['HDOF_POLLER_CFN_PARAM_LAMBDA_BUCKET_NAME'] = 'HandoffPollerLambdaFuncS3BucketName'
    conf['HDOF_POLLER_CFN_ARGMT_LAMBDA_BUCKET_NAME'] = user_settings['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']
    conf['HDOF_POLLER_CFN_PARAM_LAMBDA_KEY_NAME'] = 'HandoffPollerLambdaFuncS3KeyName'
    conf['HDOF_POLLER_CFN_ARGMT_LAMBDA_KEY_NAME'] = conf['HDOF_POLLER_LAMBDA_DEPLOY_KEY']
    conf['HDOF_POLLER_CFN_PARAM_LAMBDA_MODULE_NAME'] = 'HandoffPollerLambdaFuncModuleName'
    conf['HDOF_POLLER_CFN_ARGMT_LAMBDA_MODULE_NAME'] = conf['HDOF_POLLER_LAMBDA_MODULE_NAME']

    conf['RKSTR8_PKG_CFN_PARAM_BUCKET_NAME'] = 'Rkstr8PkgBucketName'
    conf['RKSTR8_PKG_CFN_ARGMT_BUCKET_NAME'] = conf['RKSTR8_PKG_REMOTE_BUCKET']
    conf['RKSTR8_PKG_CFN_PARAM_KEY_NAME'] = 'Rkstr8PkgKeyName'
    conf['RKSTR8_PKG_CFN_ARGMT_KEY_NAME'] = conf['RKSTR8_PKG_REMOTE_KEY']


    #
    # Google Cloud params
    #

    conf['CLOUDSPAN_MODE'] = user_settings['CLOUDSPAN_MODE']
    conf['CLOUD_FILE'] = user_settings['CLOUD_FILE']
    conf['CLOUD_TRANSFER_OUTBUCKET'] = user_settings['CLOUD_TRANSFER_OUTBUCKET']
    conf['GCP_CREDS'] = user_settings['GCP_CREDS_FILE']
    conf['GIAB_BUCKET'] = 'gs://pipeline-assets'
    conf['HAIL_SCRIPT_BUCKET'] = 'pipeline-assets'
    conf['VALIDATION_SCRIPT_KEY'] = 'hail-validation.py'
    conf['QC_SCRIPT_KEY'] = 'run_general_QC.py'
    conf['STACK_LAUNCH_TIMEOUT_MINUTES'] = 10
    return conf


class Config(object):

    def __init__(self, config, run_config, args):
        self._config = config
        self._run_config = run_config
        self._args = args

    def get_config_item(self, property_name):
        if property_name not in self._config.keys(): # we don't want KeyError
            return None  # just return None if not found
        return self._config[property_name]

    def get_run_config_item(self, property_name):
        if property_name not in self._run_config.keys(): # we don't want KeyError
            return None  # just return None if not found
        return self._run_config[property_name]

    def get_args_item(self, argument_name):
        try:
            return self._args.__getattribute__(argument_name)
        except AttributeError:
            return None


class LambdaConfig(Config):

    @property
    def handlers(self):
        return self.get_run_config_item('lambda')['handlers']

    @property
    def rkstr8_path(self):
        return self.get_config_item('RKSTR8_PKG_LOCAL_PATH')

    @property
    def asset_bucket(self):
        return self.get_run_config_item('RESOURCE_CFN_TMPL_DEPLOY_BUCKET')
