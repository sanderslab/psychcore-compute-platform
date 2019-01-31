from rkstr8.cloud import BotoClientFactory, BotoResourceFactory, Service, StackLaunchException
from rkstr8.cloud.s3 import S3Upload

from botocore.client import ClientError
import yaml
import logging
import io
import time

logger = logging.getLogger(__name__)


class TemplateProcessor:
    '''
    Builder pattern implementation of CloudFormation YAML Template rendering pipeline.
    Reads/Writes YAML. Functions to add resource definitions to the Resources block.

    Usage:
        1. Build a pipeline using the builder pattern. E.g.

        templates = ['templates/rkstr8.stack.yaml']

        rendered_templates = TemplateProcessor(templates)\
            .from_yaml()\
            .add_resource('MyStateMachine', state_machine_resource)\
            .add_resources(job_def_names, job_defs)\
            .to_yaml()

        2. Run pipeline by casting the instance to a list, which will invoke the __iter__ function, thus applying the
           pipeline stages to the collection elements. Results of the pipeline are left in the list.

        final_template = list(rendered_templates)[0]
    '''

    def __init__(self, collection):
        self.collection = collection
        self.pipeline = []

    def __iter__(self):
        for item in self.collection:
            for stage in self.pipeline:
                item = stage(item)
            yield item

    def from_yaml(self, as_path=True):
        def _from_yaml(path_or_string):
            stream = open(path_or_string, 'r') if as_path else io.StringIO(path_or_string)
            return yaml.safe_load(stream)
        self.pipeline.append(_from_yaml)
        return self

    def add_resource(self, name, definition):
        def _add_element(yaml_data):
            yaml_data['Resources'][name] = definition
            return yaml_data
        self.pipeline.append(_add_element)
        return self

    def add_resources(self, names, definitions):
        if len(names) != len(definitions):
            raise Exception('Resource names and definitions not of same length')
        def _add_elements(yaml_data):
            for name, definition in zip(names, definitions):
                yaml_data['Resources'][name] = definition
            return yaml_data
        self.pipeline.append(_add_elements)
        return self

    def get_params(self):
        def _get_params(yaml_data):
            try:
                return yaml_data['Parameters']
            except KeyError:
                return []
        self.pipeline.append(_get_params)
        return self

    def to_yaml(self):
        def _to_yaml(yaml_data):
            return yaml.dump(yaml_data, default_flow_style=True)
        self.pipeline.append(_to_yaml)
        return self


class CloudFormationTemplate:
    '''
    Provides validation of CloudFormation templates and holds nested CFN parameter formatting class
    '''
    class Parameter(object):

        def __init__(self, key, val, use_prev_val=True):
            self.key = key
            self.value = val
            self.use_prev_val = use_prev_val

        def to_cfn(self):
            return {
                'ParameterKey': self.key,
                'ParameterValue': self.value,
                'UsePreviousValue': self.use_prev_val
            }

    def __init__(self, template_string):
        self.template = template_string

    def validate(self, cfn_client=BotoClientFactory.client_for(Service.CLOUDFORMATION)):
        '''
        Validates the template using boto3, returning True/False.
        Difficult to tell if the validation is 'deeper' than simple YAML validation, i.e. if Cloudformation semantics
          are validated as well. See:
          http://boto3.readthedocs.io/en/latest/reference/services/cloudformation.html#CloudFormation.Client.validate_template

        Can raise botocore.client.ClientError in exceptional circumstances.

        :param cfn_client:
        :return:
        '''
        logger.debug('Validating...')
        try:
            cfn_client.validate_template(TemplateBody=self.template)
        except ClientError as ce:
            # Validation error is signaled via exception with these response elements/values
            if ce.response['Error']['Code'] == 'ValidationError':
                logger.error('Received ValidationError')
                logger.error(ce)
                return False
            else:
                # Some other type of error occured, so raise as exception
                print('Received unexpected botocore.client.ClientError code')
                raise ce
        else:
            return True


class StackLauncher:
    '''
    Currently supports operations involved in launching a stack from a template.
    Possibly refactor this into Stack(Lifecycle)Manager if we wish to also handle deletion
    '''

    def __init__(self, template_string, conf):
        self.conf = conf
        self.stack_name = self.conf['STACK_NAME']
        self.template_string = template_string

    def validate_template(self):
        template = CloudFormationTemplate(self.template_string)
        return template.validate()

    def upload_template(self, s3_cli=BotoClientFactory.client_for(Service.S3)):
        s3_cli.upload_fileobj(
            Fileobj=io.BytesIO(self.template_string.encode('utf-8')),
            Bucket=self.conf['RESOURCE_CFN_TMPL_DEPLOY_BUCKET'],
            Key=self.conf['RESOURCE_CFN_TMPL_DEPLOY_KEY']
        )

        template_upload_exists = S3Upload.object_exists(
            bucket_name=self.conf['RESOURCE_CFN_TMPL_DEPLOY_BUCKET'],
            key_name=self.conf['RESOURCE_CFN_TMPL_DEPLOY_KEY']
        )

        if not template_upload_exists:
            raise StackLaunchException('Template does not exist in S3. Upload failed?')

    def create(self, template_url, parameters, timeout, capabilities, tags,
               cfn_client=BotoClientFactory.client_for(Service.CLOUDFORMATION)):

        logger.debug('Creating stack..')

        return cfn_client.create_stack(
            StackName=self.stack_name,
            TemplateURL=template_url,
            Parameters=parameters,
            DisableRollback=False,
            TimeoutInMinutes=timeout,
            Capabilities=capabilities,
            Tags=tags
        )

    def wait_for_stack(self, tries=15, wait=60):
        logger.debug('Waiting for stack to create...')

        for try_ in range(tries):
            logger.debug('Attempt {num} of {max}'.format(num=try_ + 1, max=tries))

            if self.stack_create_complete():
                return True
            else:
                time.sleep(wait)

        return False

    def stack_create_complete(self, cfn_client=BotoClientFactory.client_for(Service.CLOUDFORMATION)):
        '''
        Returns True if CFN stack for stack_name exists and is in CREATE_COMPLETE state, False otherwise.
        '''
        try:
            logger.debug('Attempting to describe stack {}: '.format(self.stack_name))

            response = cfn_client.describe_stacks(
                StackName=self.stack_name
            )

            logger.debug('\t{name}: {status}'.format(
                name=response['Stacks'][0]['StackName'],
                status=response['Stacks'][0]['StackStatus']
            ))
        except ClientError as ce:
            if ce.response['Error']['Code'] == 'ValidationError':
                return False
            else:
                # Unexpected error code
                logger.error('Received unexpected botocore.client.ClientError code')
                raise ce

        try:
            response_stack_name = response['Stacks'][0]['StackName']
            response_stack_stat = response['Stacks'][0]['StackStatus']
        except KeyError as be:
            logger.error('Failed parsing describe_stacks response object')
            raise be

        if response_stack_name == self.stack_name:
            return response_stack_stat == 'CREATE_COMPLETE'
        else:
            raise Exception('Returned stack has unexpected name: {}'.format(response_stack_name))

    #
    # TODO: TemplateProcessor should be moved to this class
    # Note: Not sure what I meant by that TODO...
    #
    def check_params(self, supplied_cfn_parameters):
        template_params_list = TemplateProcessor([self.template_string]) \
            .from_yaml(as_path=False) \
            .get_params()

        template_parameters = list(template_params_list)[0]

        requirements_met, missing_required_keys, defaults_used_keys, supplied_extra_keys = self.compare_params(
            template_params=template_parameters,
            supplied_params=supplied_cfn_parameters
        )

        if not requirements_met:
            raise Exception(
                'Supplied Stack parameters do not match required parameters. Missing {}'.format(missing_required_keys)
            )

        if len(defaults_used_keys) > 0:
            logging.info('Using default values for parameters: {}'.format(defaults_used_keys))

        if len(supplied_extra_keys) > 0:
            logging.info('The follow extraneous parameters were given: {}'.format(supplied_extra_keys))

    @staticmethod
    def compare_params(template_params, supplied_params):
        # separate template params into required and 'defaultable'/optional
        templ_param_keys_required = [key for key, value_data in template_params.items() if 'Default' not in value_data]
        templ_param_keys_optional = [key for key, value_data in template_params.items() if 'Default' in value_data]

        # construct list of user-supplied parameter keys
        suppl_param_keys = [cfn_p['ParameterKey'] for cfn_p in supplied_params]

        # construct base key sets used in comparisons and other set constructions below
        required_set = set(templ_param_keys_required)
        supplied_set = set(suppl_param_keys)
        optional_set = set(templ_param_keys_optional)

        # 1. Did they fulfill the required params?

        requirements_met = required_set.issubset(supplied_set)

        missing_required_set = required_set - supplied_set

        # 2. Are any defaults used?
        #   2.i. if so, which?

        defaults_used_set = optional_set - supplied_set

        # 3. Did they supply any extra params
        #   3.i. if so, which?

        supplied_extras_set = supplied_set - (required_set | optional_set)

        return requirements_met, missing_required_set, defaults_used_set, supplied_extras_set


# TODO: Refactor to module-level method, or move into other class

class Stack:
    '''
    Represents a live stack. Wraps boto3 Resource API
    '''

    def __init__(self, stack):
        self.stack = stack

    @classmethod
    def from_stack_name(cls, stack_name, cloudformation=BotoResourceFactory.resource_for(Service.CLOUDFORMATION)):
        # creates the Stack resource, but does not fail if Stack does not exist
        stack_from_name = cloudformation.Stack(stack_name)
        try:
            status = stack_from_name.stack_status
        except ClientError:
            logger.error('Stack with that name doesnt exist')
            raise ValueError('Stack with name, {}, doesnt exist'.format(stack_name))

        if status != 'CREATE_COMPLETE':
            raise ValueError('Stack exists but is not in CREATE_COMPLETE state')

        return cls(stack=stack_from_name)
