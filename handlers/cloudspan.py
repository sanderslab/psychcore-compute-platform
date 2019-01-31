'''
Module for AWS Lambda handler function to test multi-cloud (GCP, AWS) orchestration using resp. python client libraries.
'''
from datetime import datetime
from pprint import pprint 
import boto3
import json
import os
import uuid
import yaml


def handler(event, context):
    '''
    Lambda handler function to test multi-cloud orchestration using resp. python client libraries.

    :param event: the event dictionary passed in on invocation, handled by container, defined by the caller
    :param context: the context dictionary passed in on invocation, handled by the container, defined by the caller
    :return: test results as json-like dict
    '''

    # '
    # Handler to create dataproc clusters.
    # :param event: Dict
    # :param context: Lambda Context Obj
    # '
    pprint(event)

    job_defs = event['job_defs']
    job_queue = event['queue']
    build = event['build']
    assets_uri = event['assets_uri']
    results_uri = event['results_uri']
    cohort_prefix = event['cohort_prefix']
    final_vcf_bucket = '{}/{}'.format(
        event['gcp_info']['sink_bucket'], '/'.join(results_uri.split('/')[3:]))
    print('FINAL_VCF_BUCKET', final_vcf_bucket)
    final_vcf = cohort_prefix + '.gt.snp.indel.recal.vcf'

    GCP_creds = str(event['gcp_info']['GCP_creds'])
    num_samples = str(event['gcp_info']['num_samples'])

    sink_bucket = event['gcp_info']['sink_bucket']
    giab_bucket = event['gcp_info']['giab_bucket']
    vcf_gs_uri = event['gcp_info']['vcf_gs_uri']

    project_id = event['gcp_info']['project_id']
    zone = event['gcp_info']['zone']
    cluster_name = event['gcp_info']['cluster_name']
    cloudspan_mode = event['gcp_info']['cloudspan_mode']
    cloud_template = event['gcp_info']['cloud_template']

    now_unformat = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    now = now_unformat.replace(' ', '_').replace(':', '-')

    gcp_info = event['gcp_info']
    gcp_info['queue'] = job_queue

    hail_script_bucket = event['gcp_info']['hail_script_bucket']
    # Different scripts exist for validation and qc; check which mode
    # and set hail_script_key accordingly
    if cloudspan_mode == 'validation':
        hail_script_key = event['gcp_info']['validation_script_key']
    elif cloudspan_mode == 'qc':
        hail_script_key = event['gcp_info']['qc_script_key']
    else:
        raise ValueError('Invalid cloudspan_mode!')

    # Dproc create and submit poll gcp from within the container
    # only jobs complete when work is done so only need
    # to use job dependencies.

    dproc_create = boto3.client('batch').submit_job(
        jobName='dproc_create_{}'.format(now),
        jobQueue=job_queue,
        jobDefinition=job_defs['dproc_create_job'],
        containerOverrides={
            'environment': [
                {
                    'name' : 'cohort_prefix',
                    'value' : cohort_prefix
                },
                {
                    'name' : 'num_samples',
                    'value' : num_samples
                },
                {
                    'name' : 'build',
                    'value' : build
                },
                {
                    'name' : 'assets_uri',
                    'value' : assets_uri
                },
                {
                    'name' : 'results_uri',
                    'value' : results_uri
                },
                {
                    'name' : 'sink_bucket',
                    'value' : sink_bucket
                },
                {
                    'name' : 'giab_bucket',
                    'value' : giab_bucket
                },
                {
                    'name' : 'hail_script_bucket',
                    'value' : hail_script_bucket
                },
                {
                    'name' : 'hail_script_key',
                    'value' : hail_script_key
                },
                {
                    'name' : 'project_id',
                    'value' : project_id
                },
                {
                    'name' : 'zone',
                    'value' : zone
                },
                {
                    'name' : 'cluster_name',
                    'value' : cluster_name
                },
                {
                    'name' : 'GCP_creds',
                    'value' : GCP_creds
                },
                {
                    'name' : 'cloudspan_mode',
                    'value' : cloudspan_mode
                },
                {
                    'name' : 'cloud_file',
                    'value' : cloud_template
                },
                {
                    'name' : 'cloudspan_mode',
                    'value' : cloudspan_mode
                }

            ]
        },
    )
    gcp_info['dproc_create_job_id'] = dproc_create['jobId']
    dproc_submit = boto3.client('batch').submit_job(
        jobName='dproc_submit_{}'.format(now),
        jobQueue=job_queue,
        dependsOn=[{'jobId':dproc_create['jobId']}],
        jobDefinition=job_defs['dproc_submit_job'],
        containerOverrides={
            'environment': [
                {
                    'name' : 'cohort_prefix',
                    'value' : cohort_prefix
                },
                {
                    'name' : 'num_samples',
                    'value' : num_samples
                },
                {
                    'name' : 'build',
                    'value' : build
                },
                {
                    'name' : 'assets_uri',
                    'value' : assets_uri
                },
                {
                    'name' : 'results_uri',
                    'value' : results_uri
                },
                {
                    'name' : 'vcf_gs_uri',
                    'value' : vcf_gs_uri
                },
                {
                    'name' : 'giab_bucket',
                    'value' : giab_bucket
                },
                {
                    'name' : 'hail_script_bucket',
                    'value' : hail_script_bucket
                },
                {
                    'name' : 'hail_script_key',
                    'value' : hail_script_key
                },
                {
                    'name' : 'project_id',
                    'value' : project_id
                },
                {
                    'name' : 'zone',
                    'value' : zone
                },
                {
                    'name' : 'cluster_name',
                    'value' : cluster_name
                },
                {
                    'name' : 'GCP_creds',
                    'value' : GCP_creds
                },
                {
                    'name' : 'cloudspan_mode',
                    'value' : cloudspan_mode
                },
                {
                    'name' : 'cloud_file',
                    'value' : cloud_template
                },
                {
                    'name' : 'cloudspan_mode',
                    'value' : cloudspan_mode
                },
                {
                    'name' : 'final_vcf_bucket',
                    'value' : final_vcf_bucket
                },
                {
                    'name' : 'final_vcf',
                    'value' : final_vcf
                }

            ]
        },
    )
    gcp_info['dproc_submit_job_id'] = dproc_submit['jobId']
    dproc_delete = boto3.client('batch').submit_job(
        jobName='dproc_delete_{}'.format(now),
        jobQueue=job_queue,
        jobDefinition=job_defs['dproc_delete_job'],
        dependsOn=[{'jobId':dproc_submit['jobId']}],
        containerOverrides={
            'environment': [
                {
                    'name' : 'project_id',
                    'value' : project_id
                },
                {
                    'name' : 'zone',
                    'value' : zone
                },
                {
                    'name' : 'cluster_name',
                    'value' : cluster_name
                },
                {
                    'name' : 'GCP_creds',
                    'value' : GCP_creds
                },
                {
                    'name' : 'cloud_file',
                    'value' : cloud_template
                },
                {
                    'name' : 'assets_uri',
                    'value' : assets_uri
                }

            ]
        },
    )
    gcp_info['dproc_delete_job_id'] = dproc_delete['jobId']

    return gcp_info
