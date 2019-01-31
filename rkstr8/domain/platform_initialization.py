from rkstr8.cloud.lambda_ import deploy, LambdaDeployment, LambdaDeploymentInitialization
from rkstr8.cloud.s3 import S3Upload
from rkstr8.cloud.stepfunctions import StepFunctionsMachine
from rkstr8.cloud.cloudformation import StackLauncher, Stack, TemplateProcessor, CloudFormationTemplate
from rkstr8.domain.pipelines import MultiSampleWGSRefactoredNested
from rkstr8.cloud import json_serial, TemplateValidationException, TimeoutException
from rkstr8.conf import LambdaConfig

import logging
import shutil
import subprocess
import json
from pprint import pprint
from string import Template
from pathlib import Path
import tempfile
import os
import yaml

#
# Configure the logger for this module
#
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)s [%(levelname)s]: %(message)s')

#
# Mapping of end-user pipeline names to PipelineSpecification implementation classes
#
pipelines = {
    'germline_wgs': MultiSampleWGSRefactoredNested
}


def pipeline_for(pipeline_name, conf):
    '''
    Use the pipelines map to return a pipeline object given a name.
    :param pipeline_name: Str
    :return: pipeline: PipelineSpecification subclass
    '''
    try:
        pipeline_cls = pipelines[pipeline_name]
    except KeyError:
        raise Exception('No pipeline named: {}. Try {}.'.format(
            pipeline_name, list(pipelines.keys())
        ))
    else:
        return pipeline_cls(conf)


def initialize(conf, run_conf, args):

    # Commands
    initialization_actions = []

    lambda_init = LambdaDeploymentInitialization(
        lambda_deployment=LambdaDeployment(
            lambda_config=LambdaConfig(conf, run_conf, args)
        )
    )

    initialization_actions.append(lambda_init)

    for action in initialization_actions:
        action.execute()


def render_templates(pipeline, conf):
    '''
    Render the partial CloudFormation templates into valid templates
    '''
    logging.debug('Building StateMachine from Pipeline spec...')

    #
    # Invoke SFN StateMachine definition to get rkstr8.cloud.stepfunctions_mx.StateMachine
    # Also get associated StateMachine resource substitution dictionary
    #
    state_machine = pipeline.build_machine()
    state_machine_dict = state_machine.build()
    state_machine_json = json.dumps(state_machine_dict, indent=2, sort_keys=False)
    substitutions_dict = pipeline.build_substitutions()

    logging.debug('Rendering templates...')

    #
    # Read in Template fragments for CFN StateMachine and JobDefinitions
    #

    # StateMachine CFN Resource from json fragment. Add StateMachine definition and substitutions.

    with open(conf['STATE_MACHINE_RESOURCE_FRAGMENT']) as frag_fh:
        state_machine_resource_tmpl = frag_fh.read().replace('\n', '')
        state_machine_resource = json.loads(state_machine_resource_tmpl)
        # Set the DefinitionString to the machine json and its substitutions
        state_machine_resource['Properties']['DefinitionString']['Fn::Sub'] = [state_machine_json, substitutions_dict]

    sfn_template_base = [conf['STEPFUNCTIONS_TEMPLATE_PATH']]

    # Define the template rendering pipeline
    sfn_template_rendered = TemplateProcessor(sfn_template_base) \
        .from_yaml(as_path=True) \
        .add_resource(conf['CFN_FSA_LOGICAL_RESOURCE_ID'], state_machine_resource)\
        .to_yaml()

    # Run the template rendering pipeline, leaving results in list
    sfn_template = list(sfn_template_rendered)[0]

    #
    # Build job defs from fragments and render onto Batch base template 
    #

    job_def_templates_dir = Path(conf['FRAGMENTS_DIR_PATH'])
    job_def_templates = dict()
    for job_def_templ in job_def_templates_dir.glob('job_def_template.json'):
        with open(str(job_def_templ)) as frag_fh:
            key = job_def_templ.name.rstrip('.json')
            job_def_templates[key] = Template(frag_fh.read().replace('\n', ''))

    job_def_names = []
    job_defs = []
    mode = conf['MODE']
    docker_account = conf['DOCKER_ACCOUNT']

    for job in conf['CONTAINER_NAMES']:
        job_def_template_key = 'job_def_template'
        job_def_resource_tmpl = job_def_templates[job_def_template_key]
        job_def_names.append('{}JobDef'.format(job.replace('_', '')))
        job_uid = job + conf['STACK_UID']
        container = conf['CONTAINER_NAMES'][job]
        job_def_resource_str = job_def_resource_tmpl.substitute(
            account=docker_account,
            container=container,
            job=job,
            job_uid=job_uid)
        job_def = json.loads(job_def_resource_str)
        job_def['Properties']['ContainerProperties']['Memory'] = conf['MEMS'][mode][job]
        job_def['Properties']['ContainerProperties']['Vcpus'] = conf['VCPUS'][mode][job]
        pprint(job_def)
        job_defs.append(job_def)

    #
    # Render Resource definitions onto Base template to create effective (final) CFN Template
    #

    # TODO: Add a mod_resource function toe cloud/cloudformation.py that 
    # does this.
    batch_template_base = [conf['BATCH_TEMPLATE_PATH']]
    batch_template_base_path = batch_template_base[0]
    modified_batch_template_path = '{}.mod'.format(batch_template_base_path)
    modified_batch_template = [modified_batch_template_path]

    tags = {'Name' : conf['STACK_NAME'], 'UserKey' : conf['CFN_ARGMT_GPCE_SSH_KEY_PAIR']}
    # Modify batch template base with stack name for CE tagging
    with open(batch_template_base_path, 'r') as raw_template:
        yaml_template = yaml.safe_load(raw_template)
        yaml_template['Resources']\
        ['GeneralPurposeComputeEnvironment']\
        ['Properties']\
        ['ComputeResources']['Tags'] = tags

    with open(modified_batch_template_path, 'w') as mod_template:
        yaml.dump(yaml_template, mod_template)

    # Define the template rendering pipeline
    batch_template_rendered = TemplateProcessor(modified_batch_template)\
        .from_yaml(as_path=True)\
        .add_resources(job_def_names, job_defs)\
        .to_yaml()

    # Run the template rendering pipeline, leaving results in list
    batch_template = list(batch_template_rendered)[0]

    static_templates = {
        'launch': conf['PARENT_TEMPLATE_PATH'],
        'lambda': conf['LAMBDA_TEMPLATE_PATH'],
        'network': conf['NETWORK_TEMPLATE_PATH']
    }

    final_template_strings = {
        'batch': batch_template,
        'sfn': sfn_template
    }

    for name, static_template in static_templates.items():
        with open(static_template, 'r') as fh:
            final_template_strings[name] = fh.read()
    return final_template_strings





def stage_assets(final_template_strings, conf):

    for label, final_template in final_template_strings.items():

        # Validate the rendered/final template
        if not CloudFormationTemplate(final_template).validate():
            raise Exception('{} failed to validate.'.format(label))

        if label != 'launch':
            # TODO: Make _check_upload support strings, so we don't needlessly have to write out to files
            final_template_tmp_file = tempfile.NamedTemporaryFile(delete=False)
            try:
                final_template_tmp_file.write(final_template.encode(encoding='utf-8'))
                final_template_tmp_file_path = final_template_tmp_file.name
            finally:
                final_template_tmp_file.close()

            _check_upload(
                local_path=final_template_tmp_file_path,
                bucket_name=conf['RESOURCE_CFN_TMPL_DEPLOY_BUCKET'],
                key_name=Path(conf['TEMPLATE_LABEL_PATH_MAP'][label]).name
            )

            os.unlink(final_template_tmp_file_path)
            if os.path.exists(final_template_tmp_file_path):
                raise Exception('probably failed to delete {}'.format(final_template_tmp_file_path))

    logging.debug('Uploading tool param jsons...')
    user_assets_bucket = conf['USER_ASSETS_BUCKET']
    user_assets_prefix = conf['USER_ASSETS_PREFIX']
    local_assets_path = conf['LOCAL_ASSETS_DIR']

    def stage_user_assets(filename, local_path):
        if user_assets_prefix != '':
            key_name = '/'.join((user_assets_prefix, filename))
        else:
            key_name = filename
        file_path = Path(local_path).joinpath(filename) 
        _check_upload(
            local_path=str(file_path),
            bucket_name=user_assets_bucket,
            key_name=key_name
        )

    def stage_gcp_user_assets(filename, local_path):
        file_path = ''.join((local_path, filename))
        sink_bucket = 'gs://' + conf['HAIL_SCRIPT_BUCKET']
        subprocess.call('gsutil cp {} {}'.format(str(file_path), sink_bucket), shell=True) # nosec
        print('FILE PATH= ', file_path)
        print('SINK_BUCKET= ', sink_bucket)

    #
    # Upload AWS assets
    #

    user_assets = [conf['SAMPLE_FILE'], conf['SENTIEON_LICENSE_FILE_NAME'],
        conf['SENTIEON_PACKAGE_NAME'], conf['PIPELINE_CMD_TOOL_PARAM_FILE'],
        conf['CLOUD_FILE']]

    if conf['CALL_DENOVOS']:
        user_assets.append(conf['FAM_FILE'])
    for asset in user_assets:
        stage_user_assets(filename=asset, local_path=local_assets_path)

    #
    # Upload GCP assets
    #

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = conf['GCP_CREDS']

    stage_gcp_user_assets(conf['VALIDATION_SCRIPT_KEY'], './assets/validation/')
    stage_gcp_user_assets(conf['QC_SCRIPT_KEY'], './assets/qc/')

    #
    # Uploading target file
    #
    mode = conf['OME']
    if mode == 'wes':
        target_file_name = conf['TARGET_FILE_NAME']
        stage_user_assets(filename=target_file_name, local_path=local_assets_path)

    #
    # Lambda asset building and staging
    #

    logging.debug('Building and Uploading Lambdas...')

    scripts = [
      conf['LAMBDA_SCRIPT_FILE'],
      conf["REMAP_LAMBDA_SCRIPT_FILE"],
      conf["ALIGN_LAMBDA_SCRIPT_FILE"],
      conf['HAPLO_LAMBDA_SCRIPT_FILE'],
      conf['GENO_LAMBDA_SCRIPT_FILE'],
      conf['VQSR_LAMBDA_SCRIPT_FILE'],
      conf['TRIODENOVO_LAMBDA_SCRIPT_FILE'],
      conf['ALIGN_POLLER_LAMBDA_SCRIPT_FILE'],
      conf['CLOUDSPAN_POLLER_LAMBDA_SCRIPT_FILE'],
      conf['HDOF_LAMBDA_SCRIPT_FILE'],
      conf['HDOF_POLLER_LAMBDA_SCRIPT_FILE']
    ]
    rkstr8_path = conf["RKSTR8_PKG_LOCAL_PATH"]
    s3_dest_bucket = conf['RESOURCE_CFN_TMPL_DEPLOY_BUCKET']

    for script in scripts:
        with deploy(script,rkstr8_path,s3_dest_bucket) as deployer:
            deployer.create_deployment_package()
            deployer.upload_deployment_package()
    #
    # Archive rkstr8 python package and upload to S3
    #

    logging.debug('Zipping and uploading rkstr8 pkg to s3...')

    try:
        shutil.make_archive(conf['RKSTR8_PKG_ARCHIVE_LOCAL_PATH_PREFIX'], 'zip', conf['RKSTR8_PKG_LOCAL_PATH'])

        _check_upload(
            local_path=conf['RKSTR8_PKG_ARCHIVE_LOCAL_PATH'],
            bucket_name=conf['RKSTR8_PKG_REMOTE_BUCKET'],
            key_name=conf['RKSTR8_PKG_REMOTE_KEY']
        )
    finally:
        os.unlink(conf['RKSTR8_PKG_ARCHIVE_LOCAL_PATH'])
        if os.path.exists(conf['RKSTR8_PKG_ARCHIVE_LOCAL_PATH']):
            raise Exception('Probably failed to delete {}'.format(conf['RKSTR8_PKG_ARCHIVE_LOCAL_PATH']))


    #
    # Build and push Docker images if the user utilizes their own account
    #

    docker_account = conf['DOCKER_ACCOUNT']
    if docker_account != 'ucsfpsychcore':
        # User has indicated that they want to use their own dockerhub account
        # User must have provided their dockerhub password in run.yaml
        docker_password = conf['DOCKER_PASSWORD']
        # TODO: handle possible exits from .call in sane way
        # TODO: this insults `bandit` in the following way:
        # Issue: [B607:start_process_with_partial_path] Starting a process with a partial executable path
        # Not sure what is solution or good workaround
        subprocess.call([ # nosec
            'docker', 'login',
            '--username={}'.format(docker_account),
            '--password={}'.format(docker_password)])
        # subprocess.call(['bash', './docker/updateContainers.sh', docker_account])


def create_resources(pipeline, rendered_templates, conf):

    launchable_template = rendered_templates['launch']

    launcher = StackLauncher(
        template_string=launchable_template,
        conf=conf
    )

    if launcher.validate_template():
        launcher.upload_template()
    else:
        raise TemplateValidationException()

    template_parameters = pipeline.build_template_params()
    launcher.check_params(template_parameters)

    response = launcher.create(
        template_url='/'.join((
            'https://s3.amazonaws.com',
            conf['RESOURCE_CFN_TMPL_DEPLOY_BUCKET'],
            conf['RESOURCE_CFN_TMPL_DEPLOY_KEY']
        )),
        parameters=template_parameters,
        timeout=conf['STACK_LAUNCH_TIMEOUT_MINUTES'],
        capabilities=['CAPABILITY_IAM'],
        tags=[
            {
                'Key': 'Name',
                'Value': launcher.stack_name
            }
        ]
    )

    logging.debug('create_stack response: ')
    logging.debug(json.dumps(response, indent=4, sort_keys=False, default=json_serial))

    if not launcher.wait_for_stack():
        raise TimeoutException('Failed waiting for stack to create.')

    return Stack.from_stack_name(stack_name=launcher.stack_name).stack


def run(pipeline, stack, conf):
    '''
    Builds the pipeline input and executes the pipeline as a StepFunctions State Machine.
    :return:
    '''
    # To execute StateMachine, need
    #
    # 1. the machine arn (from stack)
    # 2. an execution name
    # 3. the PipelineSpec input as json

    # TODO: Find way to get the stack name without resorting to string surgery
    stack_with_machine = Stack.from_stack_name(
            stack_name=stack.Resource('StepFunctionResourcesStack').physical_resource_id.split('/')[1]
    ).stack

    machine_arn = stack_with_machine.Resource(conf['CFN_FSA_LOGICAL_RESOURCE_ID']).physical_resource_id
    execution_name = '-'.join(('PipelineExecution', conf['STACK_UID']))
    machine_input_json = json.dumps(pipeline.build_input(stack))

    print(machine_input_json)

    execution_arn = StepFunctionsMachine.start(
        machine_arn=machine_arn,
        input=machine_input_json,
        exec_name=execution_name
    )

    logging.info('execution_arn: {}'.format(execution_arn))


def _check_upload(local_path, bucket_name, key_name):
    # TODO: This should also use the string/BytesIO upload, not just file
    S3Upload.upload_file(
        local_path=local_path,
        bucket_name=bucket_name,
        key_name=key_name
    )

    if not S3Upload.object_exists(
            bucket_name=bucket_name,
            key_name=key_name
    ):
        raise ValueError('Unable to find {} in S3 after upload.'.format(key_name))
