from rkstr8.cloud.s3 import S3Upload
import logging
import os
import shutil
import subprocess
import errno
from contextlib import contextmanager
from pathlib import PurePath
from rkstr8.cloud import Command

PIP_INSTALL_REQUIREMENTS_TMPL = ' '.join((
    'pip install -r {requirements}',
    '-t {build_dir}'
))


class LambdaDeployment(object):

    def __init__(self, lambda_config):
        self.lambda_config = lambda_config

    def stage_deployments(self):
        handlers = self.lambda_config.handlers
        rkstr8_path = self.lambda_config.rkstr8_path
        s3_dest_bucket = self.lambda_config.asset_bucket

        for module_name in [PurePath(h).stem for h in handlers]:
            with deploy(module_name, rkstr8_path, s3_dest_bucket) as deployer:
                deployer.create_deployment_package()
                # work with Stageable
                deployer.upload_deployment_package()


class LambdaDeploymentInitialization(Command):

    def __init__(self, lambda_deployment):
        self.lambda_deployment = lambda_deployment

    def execute(self):
        self.lambda_deployment.stage_deployments()


@contextmanager
def deploy(script_path, rkstr8_path, s3_bucket):

    script = PurePath(script_path)

    build_dir = '.%s' % (script.stem)
    requirements_file = script.parent / '{}_requirements.txt'.format(script.stem)
    deployment_zip = '%s.deployable' % (script.stem)
    deployment_bucket = s3_bucket
    deployment_key = '.'.join((deployment_zip, 'zip'))

    builder = DeploymentBuilder(
        build_dir=build_dir,
        script_file=script_path,
        requirements_file=requirements_file,
        deployment_zip=deployment_zip,
        deployment_bucket=deployment_bucket,
        deployment_key=deployment_key,
        rkstr8_path=rkstr8_path
    )

    yield builder

    builder.tear_down()


class DeploymentBuilder(object):

    def __init__(self, build_dir, script_file, requirements_file, deployment_zip, deployment_bucket, deployment_key, rkstr8_path):

        self.build_dir = build_dir
        self.script_file = script_file
        self.requirements_file = requirements_file
        self.deployment_zip = deployment_zip
        self.deployment_bucket = deployment_bucket
        self.deployment_key = deployment_key
        self.rkstr8_path = rkstr8_path

    def validate(self):
        # TODO: import the lambda function to catch syntax errors
        # CAVEAT: how to deal with lambda-specific imports
        pass

    def upload_deployment_package(self):

        S3Upload.upload_file(
            local_path='.'.join((self.deployment_zip, 'zip')),
            bucket_name=self.deployment_bucket,
            key_name=self.deployment_key
        )

        deployment_upload_exists = S3Upload.object_exists(
            bucket_name=self.deployment_bucket,
            key_name=self.deployment_key
        )

        if not deployment_upload_exists:
            raise RuntimeError('Lambda deployment does not exist in S3. Upload failed?')

    def create_deployment_package(self):
        '''
        1. Create build directory
        2. Save .py files to root of that dir
        3. Install libraries to the build dir with pip, like: pip install module-name -t /path/to/project-dir
        4. Zip the content of the project-dir directory, which is your deployment package.
        :return:
        '''
        try:
            self.ensure_build_dir()
        except BaseException as be:
            logging.error('Failed to ensure an empty build dir')
            raise be

        # Build dir exists (or existed as of the last check)
        #
        # Copy script to build dir.
        #   - On fail, tear down build dir
        try:
            self.copy_script()
        except BaseException as be:
            logging.error('Failed to copy script, {script}, to build dir'.format(script=self.script_file))
            logging.error('Attempting to remove build dir...')
            try:
                self.tear_down_build_dir()
            except:
                print('Deleting build dir failed.')
            raise be

        # Copy rkstr8 project to build dir.
        #   - On fail, tear down build dir
        try:
            self.copy_rkstr8()
        except BaseException as be:
            logging.error('Failed to copy script, {script}, to build dir'.format(script=self.script_file))
            logging.error('Attempting to remove build dir...')
            try:
                self.tear_down_build_dir()
            except:
                print('Deleting build dir failed.')
            raise be

        try:
            self.install_requirements()
        except BaseException as be:
            logging.error('Failed to copy script, {script}, to build dir'.format(script=self.script_file))
            logging.error('Attempting to remove build dir...')
            try:
                self.tear_down_build_dir()
            except:
                print('Deleting build dir failed.')
            raise be

        try:
            self.zip_build_dir()
        except BaseException as be:
            logging.error(
                'Failed to create zip file, {zip}, to from build dir, {dir}'.format(zip=self.deployment_zip,
                                                                                    dir=self.build_dir))
            logging.error('Attempting to remove build dir...')
            try:
                self.tear_down_build_dir()
            except:
                logging.error('Deleting build dir failed.')
            raise be

    def ensure_build_dir(self):
        '''
        1. Test existence of build dir
        2a. If exists, remove contents
        2b. If not exsists, create empty
        :return:
        '''
        logging.debug('Ensuring build dir, {build_dir}, exists and is empty'.format(build_dir=self.build_dir))
        try:
            # raises OSError if path exists..
            os.makedirs(self.build_dir, exist_ok=False)
        except OSError:
            if not os.path.isdir(self.build_dir):
                raise

        # self.build_dir exists

        # recursively delete contents of build_dir, if exist
        # - let exceptions bubble up
        for file_ in os.listdir(self.build_dir):
            file_path = os.path.join(self.build_dir, file_)
            if os.path.isfile(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)

    def copy_script(self):
        '''
        Copy self.script_file to that dir
        :return:
        '''
        copy_source = self.script_file
        copy_dest = os.path.join(self.build_dir, os.path.basename(self.script_file))

        logging.debug('Copying {src} to {dst}..'.format(src=copy_source, dst=copy_dest))

        shutil.copyfile(
            src=copy_source,
            dst=copy_dest
        )

    def copy_rkstr8(self):
        '''
        Copy self.script_file to that dir
        :return:
        '''
        copy_source = self.rkstr8_path
        copy_dest = os.path.join(self.build_dir, os.path.basename(self.rkstr8_path))

        logging.debug('Copying {src} to {dst}..'.format(src=copy_source, dst=copy_dest))

        try:
            shutil.copytree(copy_source, copy_dest)
        except OSError as exc: # python >2.5
            if exc.errno == errno.ENOTDIR:
                shutil.copy(copy_source, copy_dest)
            else:
                raise Exception('Unable to copy {} to {}'.format(copy_source, copy_dest))


    def install_requirements(self):
        '''
        Install lambda requirements to that dir, from
        :return:
        '''
        logging.debug('Attempting to pip install requirements to build dir...')
        try:
            pip_install_cmd = PIP_INSTALL_REQUIREMENTS_TMPL.format(requirements=self.requirements_file,
                                                                          build_dir=self.build_dir)
            completed_process = subprocess.run(pip_install_cmd, shell=True, check=True, # nosec
                                               stdout=subprocess.PIPE)

            logging.debug('Successful pip install.')
            logging.debug('stdout: {}'.format(completed_process.stdout))
            logging.debug('stderr: {}'.format(completed_process.stderr))
        except subprocess.CalledProcessError as e:
            logging.error('Failed to install pip requirements to build dir..')
            raise e

    def zip_build_dir(self):
        '''
        Zip the content of the project-dir directory, which is your deployment package.
        :return:
        '''
        logging.debug('Attempting to zip build dir...')
        shutil.make_archive(self.deployment_zip, 'zip', self.build_dir)
        logging.debug('Successfully zipd build dir...')

    def tear_down(self):
        self.tear_down_build_dir()
        self.tear_down_deployment_zip()

    def tear_down_build_dir(self):
        logging.debug('Tearing down build dir..')
        shutil.rmtree(self.build_dir)

    def tear_down_deployment_zip(self):
        print('Tearing down lambda deployable..')
        delete_target = '.'.join((self.deployment_zip, 'zip'))
        try:
            os.remove(delete_target)
        except OSError:
            logging.error('Error trying to remove {}'.format(delete_target))

