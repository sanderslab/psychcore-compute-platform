import datetime
import boto3
import json

def handler(event, context):

    mode = event['mode']['label']
    if mode == 'prod':
        cohort_prefix = event['cohort_prefix']
        results_uri = event['results_uri']
    elif mode == 'test':
        results_uri = event['test_in_uri']
        cohort_prefix = event['test_cohort']
    else:
        raise Exception('UNRECOGNIZED MODE: {}'.format(mode))

    job_defs = event['job_defs']
    job_queue = event['queue']

    ref_uri = event['ref_uri']
    assets_uri = event['assets_uri']
    build = event['build']
    param_file = event['param_file']

    job_ids = []

    now_unformat = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    now = now_unformat.replace(' ', '_').replace(':', '-')

    vqsr_snp_model_submit = boto3.client('batch').submit_job(
        jobName='vqsr_snp_model_{}_{}'.format(cohort_prefix, now),
        jobQueue=job_queue,
        jobDefinition=job_defs['vqsr_snp_model_job'],
        containerOverrides={
            'environment': [
                {
                    'name': 'build',
                    'value': build
                },
                {
                    'name': 'prefix',
                    'value': cohort_prefix
                },
                {
                    'name': 'assets_uri',
                    'value': assets_uri
                },
                {
                    'name': 'param_file',
                    'value': param_file
                },
                {
                    'name': 'ref_uri',
                    'value': ref_uri
                },
                {
                    'name': 'in_uri',
                    'value': '{}cohort-vcf-vqsr/'.format(results_uri)
                },
                {
                    'name': 'out_uri',
                    'value': '{}cohort-vcf-vqsr/'.format(results_uri)
                },
                {
                    'name': 'log_uri',
                    'value': '{}logs/'.format(results_uri)
                }
            ]
        },
    )
    job_ids.append(vqsr_snp_model_submit['jobId'])

    vqsr_snp_apply_submit = boto3.client('batch').submit_job(
        jobName='vqsr_snp_apply_{}_{}'.format(cohort_prefix, now),
        jobQueue=job_queue,
        jobDefinition=job_defs['vqsr_snp_apply_job'],
        dependsOn=[{'jobId':vqsr_snp_model_submit['jobId']}],
        containerOverrides={
            'environment': [
                {
                    'name': 'build',
                    'value': build
                },
                {
                    'name': 'prefix',
                    'value': cohort_prefix
                },
                {
                    'name': 'assets_uri',
                    'value': assets_uri
                },
                {
                    'name': 'param_file',
                    'value': param_file
                },
                {
                    'name': 'ref_uri',
                    'value': ref_uri
                },
                {
                    'name': 'in_uri',
                    'value': '{}cohort-vcf-vqsr/'.format(results_uri)
                },
                {
                    'name': 'out_uri',
                    'value': '{}cohort-vcf-vqsr/'.format(results_uri)
                },
                {
                    'name': 'log_uri',
                    'value': '{}logs/'.format(results_uri)
                }
            ]
        },
    )
    job_ids.append(vqsr_snp_apply_submit['jobId'])

    vqsr_indel_model_submit = boto3.client('batch').submit_job(
        jobName='vqsr_indel_model_{}_{}'.format(cohort_prefix, now),
        jobQueue=job_queue,
        jobDefinition=job_defs['vqsr_indel_model_job'],
        dependsOn=[{'jobId':vqsr_snp_apply_submit['jobId']}],
        containerOverrides={
            'environment': [
                {
                    'name': 'build',
                    'value': build
                },
                {
                    'name': 'prefix',
                    'value': cohort_prefix
                },
                {
                    'name': 'assets_uri',
                    'value': assets_uri
                },
                {
                    'name': 'ref_uri',
                    'value': ref_uri
                },
                {
                    'name': 'param_file',
                    'value': param_file
                },
                {
                    'name': 'in_uri',
                    'value': '{}cohort-vcf-vqsr/'.format(results_uri)
                },
                {
                    'name': 'out_uri',
                    'value': '{}cohort-vcf-vqsr/'.format(results_uri)
                },
                {
                    'name': 'log_uri',
                    'value': '{}logs/'.format(results_uri)
                }
            ]
        },
    )
    job_ids.append(vqsr_indel_model_submit['jobId'])

    vqsr_indel_apply_submit = boto3.client('batch').submit_job(
        jobName='vqsr_indel_apply_{}_{}'.format(cohort_prefix, now),
        jobQueue=job_queue,
        jobDefinition=job_defs['vqsr_indel_apply_job'],
        dependsOn=[{'jobId':vqsr_indel_model_submit['jobId']}],
        containerOverrides={
            'environment': [
                {
                    'name': 'build',
                    'value': build
                },
                {
                    'name': 'prefix',
                    'value': cohort_prefix
                },
                {
                    'name': 'assets_uri',
                    'value': assets_uri
                },
                {
                    'name': 'ref_uri',
                    'value': ref_uri
                },
                {
                    'name': 'param_file',
                    'value': param_file
                },
                {
                    'name': 'in_uri',
                    'value': '{}cohort-vcf-vqsr/'.format(results_uri)
                },
                {
                    'name': 'out_uri',
                    'value': '{}final-cohort-vcf/'.format(results_uri)
                },
                {
                    'name': 'log_uri',
                    'value': '{}logs/'.format(results_uri)
                }
            ]
        },
    )
    job_ids.append(vqsr_indel_apply_submit['jobId'])

    return job_ids
