import rkstr8.domain.platform_initialization as platform
import yaml
from rkstr8.conf import merge_configs
from argparse import ArgumentParser
import sys
from bitmath import GiB


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('-p', '--pipeline-name', type=str, required=True)
    parser.add_argument('-a', '--access-key-id', type=str, required=False)
    parser.add_argument('-s', '--secret-access-key', type=str, required=False)

    return parser.parse_args()


def configure_platform(args):

    # TODO: this check should be in validate_config, which itself should be moved. See TODO below.
    if args.pipeline_name == 'validation' or args.pipeline_name == 'qc':
        args_in = vars(args)

        if any((
            args_in['access_key_id'] is None,
            args_in['secret_access_key'] is None
        )):
            raise ValueError('User must specify aws user, access_key_id, and secret_access_key for validation.')

    try:
        with open("run.yaml", "r") as f:
            user_settings = yaml.safe_load(f)
    except FileNotFoundError as fnfe:
        print(fnfe)
        print('Please see the example run.yaml in the ./examples folder.')
        print('Use that as a guide to configure your environment, and place in root directory.')
        return None

    conf = merge_configs(user_settings, args)

    return conf


def validate_config(conf):

    # TODO: move this into conf, or platform_initialization. Too low-level for driver module.

    # Check that ome and build match the tool parameter file being used
    ome, build = conf['OME'], conf['BUILD']
    if conf['PARAM_FILE'] != 'tool_parameter_{}_{}.yaml'.format(ome, build):
        raise ValueError('Param file name does not match requested ome and build!')

    # Check that the job definition memory requirements fit into instance types
    mems = conf['GPCE_INSTANCE_TYPE_MEMS']
    MiB_list = []
    for instance in mems:
        MiB_list.append(GiB(mems[instance]).to_MiB())

    mode = conf['MODE']
    for job_def_mem_req in conf['MEMS'][mode]:
        if int(conf['MEMS'][mode][job_def_mem_req])/1000 >= 0.90 * max(MiB_list):
            raise ValueError('Job def {} requires too much mem!'.format(job_def_mem_req))


def main(args):

    # Build configuration object from all config sources
    conf = configure_platform(args)

    if not conf:
        # TODO: exit codes for the various error modes
        sys.exit(7)

    # Validate conf before creating resources
    validate_config(conf)

    # Get the pipeline object by name
    pipeline = platform.pipeline_for(args.pipeline_name, conf)

    # Render the partial CloudFormation templates into valid templates
    templates = platform.render_templates(pipeline, conf)

    # Stage system assets in S3 consumed by CloudFormation and StepFunctions
    platform.stage_assets(templates, conf)

    # Create AWS resources stack using CloudFormation on rendered template
    stack = platform.create_resources(pipeline, templates, conf)

    # Execute the SFN State Machine that implements the pipeline
    platform.run(pipeline, stack, conf)


if __name__ == '__main__':
    main(
        parse_args()
    )

