from pprint import pprint
import boto3
import datetime
import json
import os
import sys
import yaml

BATCH_CLIENT = boto3.client('batch', region_name='us-east-1')
S3_CLIENT = boto3.resource('s3', region_name='us-east-1')

def get_info_dict(bucket, key, file, file_type, start_point=None):
    b = S3_CLIENT.Bucket(bucket)
    b.download_file(key, file)

    return_dict = {}
    print(start_point)
    with open(file, 'r') as f:
        for line in f:
            fields = line.split('\t')
            if file_type == 'sample_info':
                sample = fields[0].strip()
                if start_point == 'fastq':
                    R1 = fields[1].strip()
                    R2 = fields[2].strip()
                    return_dict[sample] = (R1, R2)
                else:
                    # start point is bam, gvcf, vcf
                    # sample file has sample_name uri
                    uri = fields[1].strip()
                    return_dict[sample] = uri
            elif file_type == 'fam_info':
                fam_id = fields[0]
                pat = fields[1]
                fil = fields[2]
                mat = fields[3]
                return_dict[fam_id] = (fil, pat, mat)
            else:
                raise ValueError('Unrecognised input file type.')
    pprint(return_dict)
    return return_dict


def main():

    # Turn event_str into dict
    event = yaml.safe_load(os.environ['event_str'])

    # Get required env variables
    step = os.environ['step']
    start_point = event['start_point']
    remap = event['remap']

    # Download and parse sample list file
    sample_s3_prefix = event['sample_s3_prefix']
    sample_s3_bucket = event['sample_s3_bucket']
    sample_file = event['sample_file']
    fam_file = event['fam_file']
    cohort_prefix = event['cohort_prefix']
    sample_key = '{}/{}'.format(sample_s3_prefix, sample_file)
    file_type = 'sample_info' if step != 'triodenovo' else 'fam_info'

    info_dict = get_info_dict(
        bucket=sample_s3_bucket,
        key=sample_key,
        file=sample_file if step != 'triodenovo' else fam_file,
        file_type=file_type,
        start_point=start_point)

    # Get other container params from event
    job_defs = event['job_defs']
    job_queue = event['queue']
    qc_queue = event['qc_queue']

    ref_uri = event['ref_uri']
    in_uri = event['in_uri']
    results_uri = event['results_uri']
    assets_uri = event['assets_uri']

    param_file = event['param_file']
    build = event['build']
    mode = event['mode']['label']
    ome = event['mode']['ome']
    job_ids = []

    # Target file only required for wes
    if ome == 'wes' and 'target_file' in event:
        target_file = event['target_file']
    elif ome == 'wgs':
        target_file = 'None'
    else:
        raise ValueError('Ome set to wes, but no target_file was set!')

    # Get step specific env variables
    # Only need submitters for parallel steps, eg align_proc and haplo
    if step == 'alignment_processing':
        bwa_threads = str(event['mode'][mode]['threads']['bwa'])
        brt_threads = str(event['mode'][mode]['threads']['brt'])
        for sample in info_dict:
            if not remap:
                R1 = info_dict[sample][0]
                R2 = info_dict[sample][1]
            else:
                # Run started with bams that were remapped;
                # fastqs are in
                in_uri = '{}remap_fqs/'.format(results_uri)
                R1 = '{}{}_R1.fastq.gz'.format(in_uri, sample)
                R2 = '{}{}_R2.fastq.gz'.format(in_uri, sample)
            print('Submitting bwa_mem -> base_recal for {}'.format(sample))
            now_unformat = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            now = now_unformat.replace(' ', '_').replace(':', '-')
            bwa_mem_submit = BATCH_CLIENT.submit_job(
                jobName='bwa_mem_{}_{}'.format(sample, now),
                jobQueue=job_queue,
                jobDefinition=job_defs['bwa_mem_job'],
                containerOverrides={
                    'environment': [
                        {
                            'name': 'build',
                            'value': build
                        },
                        {
                            'name': 'prefix',
                            'value': sample
                        },
                        {
                            'name': 'R1',
                            'value': R1
                        },
                        {
                            'name': 'R2',
                            'value': R2
                        },
                        {
                            'name': 'threads',
                            'value': bwa_threads
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
                            'name': 'in_uri',
                            'value': in_uri,
                        },
                        {
                            'name': 'param_file',
                            'value': param_file
                        },
                        {
                            'name': 'out_uri',
                            'value': '{}bam-processing/'.format(results_uri)
                        },
                        {
                            'name': 'log_uri',
                            'value': '{}logs/'.format(results_uri)
                        }
                    ]
                },
            )
            job_ids.append(bwa_mem_submit['jobId'])

            sort_sam_submit = BATCH_CLIENT.submit_job(
                jobName='sort_sam_{}_{}'.format(sample, now),
                jobQueue=job_queue,
                jobDefinition=job_defs['sort_sam_job'],
                dependsOn=[{'jobId':bwa_mem_submit['jobId']}],
                containerOverrides={
                    'environment': [
                        {
                            'name': 'build',
                            'value': build
                        },
                        {
                            'name': 'prefix',
                            'value': sample
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
                            'value': '{}bam-processing/'.format(results_uri)
                        },
                        {
                            'name': 'out_uri',
                            'value': '{}bam-processing/'.format(results_uri)
                        },
                        {
                            'name': 'log_uri',
                            'value': '{}logs/'.format(results_uri)
                        }
                    ]
                },
            )
            job_ids.append(sort_sam_submit['jobId'])

            mark_dups_submit = BATCH_CLIENT.submit_job(
                jobName='mark_dups_{}_{}'.format(sample, now),
                jobQueue=job_queue,
                jobDefinition=job_defs['mark_dups_job'],
                dependsOn=[{'jobId':sort_sam_submit['jobId']}],
                containerOverrides={
                    'environment': [
                        {
                            'name': 'build',
                            'value': build
                        },
                        {
                            'name': 'prefix',
                            'value': sample
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
                            'value': '{}bam-processing/'.format(results_uri)
                        },
                        {
                            'name': 'out_uri',
                            'value': '{}bam-processing/'.format(results_uri)
                        },
                        {
                            'name': 'log_uri',
                            'value': '{}logs/'.format(results_uri)
                        }
                    ]
                },
            )
            job_ids.append(mark_dups_submit['jobId'])

            index_bam_submit = BATCH_CLIENT.submit_job(
                jobName='index_bam_{}_{}'.format(sample, now),
                jobQueue=job_queue,
                jobDefinition=job_defs['index_bam_job'],
                dependsOn=[{'jobId':mark_dups_submit['jobId']}],
                containerOverrides={
                    'environment': [
                        {
                            'name': 'build',
                            'value': build
                        },
                        {
                            'name': 'prefix',
                            'value': sample
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
                            'value': '{}bam-processing/'.format(results_uri)
                        },
                        {
                            'name': 'out_uri',
                            'value': '{}bam-processing/'.format(results_uri)
                        },
                        {
                            'name': 'log_uri',
                            'value': '{}logs/'.format(results_uri)
                        }
                    ]
                },
            )
            job_ids.append(index_bam_submit['jobId'])

            base_recal_table_submit = BATCH_CLIENT.submit_job(
                jobName='base_recal_table_{}_{}'.format(sample, now),
                jobQueue=job_queue,
                jobDefinition=job_defs['base_recal_table_job'],
                dependsOn=[{'jobId':index_bam_submit['jobId']}],
                containerOverrides={
                    'environment': [
                        {
                            'name': 'build',
                            'value': build
                        },
                        {
                            'name': 'ome',
                            'value': ome
                        },
                        {
                            'name': 'target_file',
                            'value': target_file
                        },
                        {
                            'name': 'prefix',
                            'value': sample
                        },
                        {
                            'name': 'threads',
                            'value': brt_threads
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
                            'value': '{}bam-processing/'.format(results_uri)
                        },
                        {
                            'name': 'out_uri',
                            'value': '{}bam-processing/'.format(results_uri)
                        },
                        {
                            'name': 'log_uri',
                            'value': '{}logs/'.format(results_uri)
                        }
                    ]
                },
            )
            job_ids.append(base_recal_table_submit['jobId'])

            base_recal_submit = BATCH_CLIENT.submit_job(
                jobName='base_recal_{}_{}'.format(sample, now),
                jobQueue=job_queue,
                jobDefinition=job_defs['base_recal_job'],
                dependsOn=[{'jobId':base_recal_table_submit['jobId']}],
                containerOverrides={
                    'environment': [
                        {
                            'name': 'build',
                            'value': build
                        },
                        {
                            'name': 'prefix',
                            'value': sample
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
                            'value': '{}bam-processing/'.format(results_uri)
                        },
                        {
                            'name': 'out_uri',
                            'value': '{}processed-bams/'.format(results_uri)
                        },
                        {
                            'name': 'log_uri',
                            'value': '{}logs/'.format(results_uri)
                        }
                    ]
                },
            )
            job_ids.append(base_recal_submit['jobId'])

    elif step == 'sam_to_fq':
        for sample in info_dict:
            print('sam_to_fq for {}'.format(sample))
            now_unformat = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            now = now_unformat.replace(' ', '_').replace(':', '-')
            sam_to_fq_submit = BATCH_CLIENT.submit_job(
                jobName='sam_to_fq_{}_{}'.format(sample, now),
                jobQueue=job_queue,
                jobDefinition=job_defs['sam_to_fq_job'],
                containerOverrides={
                    'environment': [
                        {
                            'name': 'build',
                            'value': build
                        },
                        {
                            'name': 'ome',
                            'value': ome
                        },
                        {
                            'name': 'param_file',
                            'value': param_file
                        },
                        {
                            'name': 'in_file',
                            'value': info_dict[sample]
                        },
                        {
                            'name': 'in_uri',
                            'value': in_uri
                        },
                        {
                            'name': 'out_uri',
                            'value': '{}remap_fqs/'.format(results_uri)
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
                            'name': 'prefix',
                            'value': sample
                        },
                        {
                            'name': 'log_uri',
                            'value': '{}logs/'.format(results_uri)
                        }
                    ]
                },
            )
            job_ids.append(sam_to_fq_submit['jobId'])
            print(sam_to_fq_submit)

    elif step == 'sentieon_haplotyper':
        sentieon_license = event['sentieon_license']
        sentieon_pkg = event['sentieon_pkg']
        hap_threads = str(event['mode'][mode]['threads']['hap'])
        for sample in info_dict:
            print('Submitting sentieon_haplotyper for {}'.format(sample))
            now_unformat = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            now = now_unformat.replace(' ', '_').replace(':', '-')

            if start_point == 'bam':
                in_uri = '{}/'.format('/'.join(info_dict[sample].split('/')[:-1]))
                in_file = info_dict[sample].split('/')[-1]
            else:
                in_uri = '{}processed-bams/'.format(results_uri)
                in_file = 'None'

            haplotyper_submit = BATCH_CLIENT.submit_job(
                jobName='haplotyper_{}_{}'.format(sample, now),
                jobQueue=job_queue,
                jobDefinition=job_defs['sentieon_haplotyper_job'],
                containerOverrides={
                    'environment': [
                        {
                            'name': 'start_point',
                            'value': start_point
                        },
                        {
                            'name': 'build',
                            'value': build
                        },
                        {
                            'name': 'ome',
                            'value': ome
                        },
                        {
                            'name': 'target_file',
                            'value': target_file
                        },
                        {
                            'name': 'prefix',
                            'value': sample
                        },
                        {
                            'name': 'threads',
                            'value': hap_threads
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
                            'name': 'sentieon_pkg',
                            'value': sentieon_pkg
                        },
                        {
                            'name': 'sentieon_license',
                            'value': sentieon_license
                        },
                        {
                            'name': 'in_uri',
                            'value': in_uri
                        },
                        {
                            'name': 'out_uri',
                            'value': '{}bgz-gvcfs/'.format(results_uri)
                        },
                        {
                            'name': 'log_uri',
                            'value': '{}logs/'.format(results_uri)
                        },
                        {
                            'name': 'in_file',
                            'value': in_file
                        }
                    ]
                },
            )
            job_ids.append(haplotyper_submit['jobId'])

    elif step == 'triodenovo':
        for fam_id in info_dict:
            print('fam_vcf_from_cohort for {}'.format(fam_id))
            now_unformat = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            now = now_unformat.replace(' ', '_').replace(':', '-')
            fil = info_dict[fam_id][0]
            mat = info_dict[fam_id][1]
            pat = info_dict[fam_id][2]
            in_uri = '{}final-cohort-vcf/'.format(results_uri)
            fam_vcf_from_cohort_submit = BATCH_CLIENT.submit_job(
                jobName='fam_vcf_from_cohort_{}_{}'.format(fam_id, now),
                jobQueue=job_queue,
                jobDefinition=job_defs['fam_vcf_from_cohort'],
                containerOverrides={
                    'environment': [
                        {
                            'name': 'build',
                            'value': build
                        },
                        {
                            'name': 'ome',
                            'value': ome
                        },
                        {
                            'name': 'param_file',
                            'value': param_file
                        },
                        {
                            'name': 'in_uri',
                            'value': in_uri
                        },
                        {
                            'name': 'out_uri',
                            'value': '{}fam-vcfs/'.format(results_uri)
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
                            'name': 'prefix',
                            'value': cohort_prefix
                        },
                        {
                            'name': 'fam_id',
                            'value': fam_id
                        },
                        {
                            'name': 'fil',
                            'value': fil
                        },
                        {
                            'name': 'mat',
                            'value': mat
                        },
                        {
                            'name': 'pat',
                            'value': pat
                        },
                        {
                            'name': 'log_uri',
                            'value': '{}logs/'.format(results_uri)
                        }
                    ]
                },
            )
            job_ids.append(fam_vcf_from_cohort_submit['jobId'])
            print(fam_vcf_from_cohort_submit)

            scrub_vcf_submit = BATCH_CLIENT.submit_job(
                jobName='scrub_vcf_{}_{}'.format(fam_id, now),
                jobQueue=job_queue,
                jobDefinition=job_defs['scrub_vcf'],
                dependsOn=[{'jobId':fam_vcf_from_cohort_submit['jobId']}],
                containerOverrides={
                    'environment': [
                        {
                            'name': 'build',
                            'value': build
                        },
                        {
                            'name': 'ome',
                            'value': ome
                        },
                        {
                            'name': 'param_file',
                            'value': param_file
                        },
                        {
                            'name': 'in_uri',
                            'value': '{}fam-vcfs/'.format(results_uri)
                        },
                        {
                            'name': 'out_uri',
                            'value': '{}denovo-processing/'.format(results_uri)
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
                            'name': 'prefix',
                            'value': cohort_prefix
                        },
                        {
                            'name': 'fam_id',
                            'value': fam_id
                        },
                        {
                            'name': 'log_uri',
                            'value': '{}logs/'.format(results_uri)
                        }
                    ]
                },
            )
            job_ids.append(scrub_vcf_submit['jobId'])
            print(scrub_vcf_submit)

            ped_from_vcf_submit = BATCH_CLIENT.submit_job(
                jobName='ped_from_vcf_{}_{}'.format(fam_id, now),
                jobQueue=job_queue,
                jobDefinition=job_defs['ped_from_vcf'],
                dependsOn=[{'jobId':fam_vcf_from_cohort_submit['jobId']}],
                containerOverrides={
                    'environment': [
                        {
                            'name': 'build',
                            'value': build
                        },
                        {
                            'name': 'ome',
                            'value': ome
                        },
                        {
                            'name': 'param_file',
                            'value': param_file
                        },
                        {
                            'name': 'in_uri',
                            'value': '{}fam-vcfs/'.format(results_uri)
                        },
                        {
                            'name': 'out_uri',
                            'value': '{}denovo-processing/'.format(results_uri)
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
                            'name': 'prefix',
                            'value': cohort_prefix
                        },
                        {
                            'name': 'fam_id',
                            'value': fam_id
                        },
                        {
                            'name': 'log_uri',
                            'value': '{}logs/'.format(results_uri)
                        }
                    ]
                },
            )
            job_ids.append(ped_from_vcf_submit['jobId'])
            print(ped_from_vcf_submit)

            triodenovo_submit = BATCH_CLIENT.submit_job(
                jobName='triodenovo_{}_{}'.format(fam_id, now),
                jobQueue=job_queue,
                jobDefinition=job_defs['triodenovo'],
                dependsOn=[{'jobId':fam_vcf_from_cohort_submit['jobId']}],
                containerOverrides={
                    'environment': [
                        {
                            'name': 'build',
                            'value': build
                        },
                        {
                            'name': 'ome',
                            'value': ome
                        },
                        {
                            'name': 'param_file',
                            'value': param_file
                        },
                        {
                            'name': 'in_uri',
                            'value': '{}denovo-processing/'.format(results_uri)
                        },
                        {
                            'name': 'out_uri',
                            'value': '{}triodenovo-results/'.format(results_uri)
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
                            'name': 'prefix',
                            'value': cohort_prefix
                        },
                        {
                            'name': 'fam_id',
                            'value': fam_id
                        },
                        {
                            'name': 'log_uri',
                            'value': '{}logs/'.format(results_uri)
                        }
                    ]
                },
            )
            job_ids.append(triodenovo_submit['jobId'])
            print(triodenovo_submit)

    elif step == 'bamQC':
        for sample in info_dict:
            print('Submitting bamQC for {}'.format(sample))
            now_unformat = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            now = now_unformat.replace(' ', '_').replace(':', '-')
            bam_qc_in = '{}.sorted.deduped.recalibrated.bam'.format(sample)
            bam_qc_out = '{}.collect_wgs_metrics.txt'.format(sample)
            ref_fasta = 'Homo_sapiens_assembly38.fasta' if build == 'GRCh38' else 'human_g1k_v37.fasta'
            bamQC_submit = BATCH_CLIENT.submit_job(
                jobName='picard_bamQC_{}_{}'.format(sample, now),
                jobQueue=qc_queue,
                jobDefinition=job_defs['pipeline_bam_qc_job'],
                containerOverrides={
                    'environment': [
                        {
                            'name': 'bam',
                            'value': bam_qc_in
                        },
                        {
                            'name': 'out',
                            'value': bam_qc_out
                        },
                        {
                            'name': 'ref',
                            'value': ref_fasta
                        },
                        {
                            'name': 'ref_uri',
                            'value': ref_uri
                        },
                        {
                            'name': 'bam_uri',
                            'value': '{}processed-bams/'.format(results_uri)
                        },
                        {
                            'name': 'out_uri',
                            'value': '{}bam-qc/'.format(results_uri)
                        },
                        {
                            'name': 'log_uri',
                            'value': '{}logs/'.format(results_uri)
                        }
                    ]
                },
            )
            job_ids.append(bamQC_submit['jobId'])

    else:
        raise ValueError('Submitter only supports steps alignment_processing, sentieon_haplotyper, and bamQC.')

    print(job_ids)

if __name__ == '__main__':
    main()