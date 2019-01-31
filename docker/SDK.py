'''
CCDG-compliant SDK for common use functions in pipeline
'''
from time import sleep
import boto3
import os
import shutil
import subprocess
import sys
import uuid
import yaml


s3 = boto3.resource('s3')

class Task:
    '''
    A step in the pipeline to run.
    '''
    def __init__(self, step, prefix, in_files, param_file,
            ref_uri, in_uri, out_uri, assets_uri,
            sample_file=None, target_file=None, sentieon_pkg=None,
            license_file=None, threads=None, fam_dict=None):

        self.step = step
        self.prefix = prefix
        self.in_files = in_files
        self.param_file = param_file
        self.sample_file = sample_file

        self.ref_uri = ref_uri
        self.in_uri = in_uri
        self.out_uri = out_uri
        self.assets_uri = assets_uri

        # thread values for bwa/brt/hap
        self.threads = threads

        # license for sentieon
        self.license_file = license_file
        self.sentieon_pkg = sentieon_pkg

        # target file for exome
        self.target_file = target_file

        self.tmp_dir = '/home/localDir/tmp_{}/'.format(str(uuid.uuid4())[:3])

        # params will be populated after get_prog_params is called
        self.params = {}
        # result_files will be populated depending on which step is run
        self.result_files = []
        # intermediate_files will be populated depending on which step
        # and is used only for cleanup
        self.intermediate_files = []

        self.ref_files = []

        # fam_dict for denovo calling, keys are fam_id, fil, mat, pat
        if fam_dict != None:
            self.fam_id = fam_dict['fam_id']
            self.fil = fam_dict['fil']
            self.pat = fam_dict['pat']
            self.mat = fam_dict['mat']

    def get_reference_files(self, build):
        '''
        Sets instance variable to describe what reference files
        are needed for a given build and step
        '''
        if build == 'GRCh38':
            if self.step == 'bwa_mem':
                self.ref_files = [
                    'Homo_sapiens_assembly38.fasta',
                    'Homo_sapiens_assembly38.fasta.64.amb',
                    'Homo_sapiens_assembly38.fasta.64.ann',
                    'Homo_sapiens_assembly38.fasta.64.bwt',
                    'Homo_sapiens_assembly38.fasta.64.pac',
                    'Homo_sapiens_assembly38.fasta.64.sa',
                    'Homo_sapiens_assembly38.fasta.64.alt']
            elif self.step == 'base_recal_table':
                self.ref_files = [
                    'Homo_sapiens_assembly38.fasta',
                    'Homo_sapiens_assembly38.fasta.fai',
                    'Homo_sapiens_assembly38.dict',
                    'Mills_and_1000G_gold_standard.indels.hg38.vcf.gz',
                    'Mills_and_1000G_gold_standard.indels.hg38.vcf.gz.tbi',
                    'Homo_sapiens_assembly38.dbsnp138.vcf',
                    'Homo_sapiens_assembly38.dbsnp138.vcf.idx',
                    'Homo_sapiens_assembly38.known_indels.vcf.gz',
                    'Homo_sapiens_assembly38.known_indels.vcf.gz.tbi']
            elif self.step in [
                'base_recal', 'vqsr_snp_apply', 'vqsr_indel_apply', 'sam_to_fq', 'fam_vcf_from_cohort']:
                self.ref_files = [
                    'Homo_sapiens_assembly38.fasta',
                    'Homo_sapiens_assembly38.fasta.fai',
                    'Homo_sapiens_assembly38.dict']
            elif self.step == 'haplotyper' or self.step == 'genotyper':
                self.ref_files = [
                    'Homo_sapiens_assembly38.fasta',
                    'Homo_sapiens_assembly38.fasta.fai',
                    'Homo_sapiens_assembly38.dbsnp138.vcf',
                    'Homo_sapiens_assembly38.dbsnp138.vcf.idx']
            elif self.step == 'vqsr_snp_model':
                self.ref_files = [
                    'Homo_sapiens_assembly38.fasta',
                    'Homo_sapiens_assembly38.fasta.fai',
                    'Homo_sapiens_assembly38.dict',
                    'hapmap_3.3.hg38.vcf.gz',
                    'hapmap_3.3.hg38.vcf.gz.tbi',
                    '1000G_omni2.5.hg38.vcf.gz',
                    '1000G_omni2.5.hg38.vcf.gz.tbi',
                    '1000G_phase1.snps.high_confidence.hg38.vcf.gz',
                    '1000G_phase1.snps.high_confidence.hg38.vcf.gz.tbi',
                    'Homo_sapiens_assembly38.dbsnp138.vcf',
                    'Homo_sapiens_assembly38.dbsnp138.vcf.idx']
            elif self.step == 'vqsr_indel_model':
                self.ref_files = [
                    'Homo_sapiens_assembly38.fasta',
                    'Homo_sapiens_assembly38.fasta.fai',
                    'Homo_sapiens_assembly38.dict',
                    'Homo_sapiens_assembly38.known_indels.vcf.gz',
                    'Homo_sapiens_assembly38.known_indels.vcf.gz.tbi',
                    'Mills_and_1000G_gold_standard.indels.hg38.vcf.gz',
                    'Mills_and_1000G_gold_standard.indels.hg38.vcf.gz.tbi',
                    'Homo_sapiens_assembly38.dbsnp138.vcf',
                    'Homo_sapiens_assembly38.dbsnp138.vcf.idx']
        elif build == 'GRCh37':
            if self.step == 'bwa_mem':
                self.ref_files = [
                    'human_g1k_v37.fasta',
                    'human_g1k_v37.fasta.amb',
                    'human_g1k_v37.fasta.ann',
                    'human_g1k_v37.fasta.bwt',
                    'human_g1k_v37.fasta.fai',
                    'human_g1k_v37.fasta.pac',
                    'human_g1k_v37.fasta.sa']
            elif self.step == 'base_recal_table':
                self.ref_files = [
                    'human_g1k_v37.fasta',
                    'human_g1k_v37.fasta.fai',
                    'human_g1k_v37.dict',
                    'Mills_and_1000G_gold_standard.indels.b37.vcf',
                    'dbsnp_138.b37.vcf',
                    '1000G_phase1.snps.high_confidence.b37.vcf',
                    'xgen-exome-research-panel-targets-cols1-2-noChr-merged.bed']
            elif self.step in [
                'base_recal', 'vqsr_snp_apply', 'vqsr_indel_apply', 'sam_to_fq', 'fam_vcf_from_cohort']:
                self.ref_files = [
                    'human_g1k_v37.fasta',
                    'human_g1k_v37.fasta.fai',
                    'human_g1k_v37.dict']
            elif self.step == 'haplotyper' or self.step == 'genotyper':
                self.ref_files = [
                    'human_g1k_v37.fasta',
                    'human_g1k_v37.fasta.fai',
                    'dbsnp_138.b37.vcf',
                    'dbsnp_138.b37.vcf.idx']
            elif self.step == 'vqsr_snp_model':
                self.ref_files = [
                    'human_g1k_v37.fasta',
                    'human_g1k_v37.fasta.fai',
                    'human_g1k_v37.dict',
                    'hapmap_3.3.b37.vcf',
                    '1000G_omni2.5.b37.vcf',
                    '1000G_omni2.5.b37.vcf.idx',
                    '1000G_phase1.snps.high_confidence.b37.vcf',
                    'dbsnp_138.b37.vcf']
            elif self.step == 'vqsr_indel_model':
                self.ref_files = [
                    'human_g1k_v37.fasta',
                    'human_g1k_v37.fasta.fai',
                    'human_g1k_v37.dict',
                    'Mills_and_1000G_gold_standard.indels.b37.vcf',
                    'dbsnp_138.b37.vcf']

    def get_genotyping_samples(self):
        '''
        Helper step just for genotyper - builds input_files
        from the sample_file.
        '''
        bucket_name = self.assets_uri.split('/')[2]
        prefix = '/'.join(self.assets_uri.split('/')[3:-1])
        key = '{}/{}'.format(prefix, self.sample_file) if prefix != '' else self.sample_file
        sys.stdout.flush()
        b = s3.Bucket(bucket_name)
        b.download_file(key, self.sample_file)
        gvcfs = []
        with open(self.sample_file, 'r') as s:
            for line in s:
                fields = line.split('\t')
                sample = fields[0].strip()
                gvcfs.append('{}.gvcf.gz'.format(sample))
        # Add .idx or .tbi to list of in_infiles
        [self.in_files.extend(['{}.tbi'.format(g), g]) for g in gvcfs]
        print(self.in_files)

    def download_files(self, source):
        '''
        Downloads every file from the list of files from the source.
        Source must be either 'in' or 'ref'
        :param source: Str
        :return: None
        '''
        if source == 'INPUT':
            bucket_name = self.in_uri.split('/')[2]
            prefix = '/'.join(self.in_uri.split('/')[3:-1])
            files = self.in_files
        elif source == 'REF':
            bucket_name = self.ref_uri.split('/')[2]
            prefix = '/'.join(self.ref_uri.split('/')[3:-1])
            files = self.ref_files
        elif source == 'PARAMS':
            if self.assets_uri is None or self.param_file is None:
                raise ValueError('assets_uri not set!')
            bucket_name = self.assets_uri.split('/')[2]
            prefix = '/'.join(self.assets_uri.split('/')[3:-1])
            files = [self.param_file]
        elif source == 'SENTIEON':
            # license_uri is the full path of the file, including the
            # file name, eg s3://path/to/license/license.lic
            if self.assets_uri is None or self.license_file is None:
                raise ValueError('assets_uri not set!')
            bucket_name = self.assets_uri.split('/')[2]
            prefix = '/'.join(self.assets_uri.split('/')[3:-1])
            files = [self.license_file, self.sentieon_pkg]
        elif source == 'TARGET':
            bucket_name = self.assets_uri.split('/')[2]
            prefix = '/'.join(self.assets_uri.split('/')[3:-1])
            files = [self.target_file]
        else:
            raise ValueError('Improper source for download')
        for file_name in files:
            if self.step in ('bwa_mem', 'sam_to_fq') and source == 'INPUT':
                # In bwa and sam_to_fq in_files also include URIs
                key = '/'.join(file_name.split('/')[3:])
                bucket_name = file_name.split('/')[2]
                file_name = file_name.split('/')[-1]
            else:
                key = '{}/{}'.format(prefix, file_name) if prefix != '' else file_name
            dir_contents = os.listdir('.')
            if not set(file_name).issubset(set(dir_contents)):
                print('Downloading {} from bucket {}, key {}.'.format(file_name, bucket_name, key))
                sys.stdout.flush()
                b = s3.Bucket(bucket_name)
                b.download_file(key, file_name)

    def import_cmd_template(self):
        '''
        Imports the tool_parameter yaml file with unformatted
        bioinformatic tool commands
        :return raw_cmd_list: List
        '''
        if self.param_file is None:
            raise ValueError('No param file was set.')
        with open(self.param_file, 'r') as f:
            all_cmds = yaml.safe_load(f)
            raw_cmd_list = all_cmds[self.step].rstrip()
            if self.step in ['haplotyper', 'genotyper']:
                # These steps use vcfconvert
                zip_cmd_list = all_cmds['sentieon_zipper'].rstrip()
                return [raw_cmd_list, zip_cmd_list]
            return [raw_cmd_list]

    def build_cmd(self):
        '''
        Formats the unformatted bioinformatic too command
        and returns the command as a list of strings for
        subprocess to run
        :return cmd_list: List
        '''

        unformat_cmd_str = self.import_cmd_template()
        if self.step not in ['haplotyper', 'genotyper']:
            unformat_cmd_str = unformat_cmd_str[0]

        if self.step == 'sam_to_fq':
            bam = [f for f in self.in_files if f.endswith('.bam')][0].split('/')[-1]
            R1 = '{}_R1.fastq'.format(self.prefix)
            R2 = '{}_R2.fastq'.format(self.prefix)
            picard_cmd = unformat_cmd_str.format(
                bam=bam,
                R1=R1,
                R2=R2,
                tmp_dir=self.tmp_dir)
            gzip_r1_cmd = 'gzip {}'.format(R1)
            gzip_r2_cmd = 'gzip {}'.format(R2)
            cmd_strs = [picard_cmd, gzip_r1_cmd, gzip_r2_cmd]
            self.result_files = ['{}.gz'.format(R1), '{}.gz'.format(R2)]
        elif self.step == 'bwa_mem':
            # TODO: change this back to R1 and R2
            R1 = [f for f in self.in_files if '_R1' in f][0].split('/')[-1]
            R2 = [f for f in self.in_files if '_R2' in f][0].split('/')[-1]
            header = '@RG\\tID:' + self.prefix + '\\tPL:ILLUMINA\\tSM:' + self.prefix
            cmd_strs = [unformat_cmd_str.format(
                header=header,
                R1=R1,
                R2=R2,
                threads=self.threads)]
        elif self.step == 'sort_sam':
            sam = [f for f in self.in_files if f.endswith('.sam')][0]
            output = '{}.sorted.bam'.format(self.prefix)
            cmd_strs = [unformat_cmd_str.format(
                tmp_dir=self.tmp_dir,
                sam=sam,
                output=output)]
            self.result_files = [output]
        elif self.step == 'mark_dups':
            bam = [f for f in self.in_files if f.endswith('.sorted.bam')][0]
            output = '{}.sorted.deduped.bam'.format(self.prefix)
            cmd_strs = [unformat_cmd_str.format(
                tmp_dir=self.tmp_dir,
                prefix=self.prefix,
                bam=bam,
                output=output)]
            self.result_files = [output]
        elif self.step == 'index_bam':
            bam = [f for f in self.in_files if f.endswith('.sorted.deduped.bam')][0]
            output = '{}.sorted.deduped.bam.bai'.format(self.prefix)
            cmd_strs = [unformat_cmd_str.format(
                tmp_dir=self.tmp_dir,
                bam=bam,
                output=output)]
            self.result_files = [output]
        elif self.step == 'base_recal_table':
            bam = [f for f in self.in_files if f.endswith('.sorted.deduped.bam')][0]
            output = '{}.base_recal_table.txt'.format(self.prefix)
            cmd_strs = [unformat_cmd_str.format(
                tmp_dir=self.tmp_dir,
                prefix=self.prefix,
                threads=self.threads,
                bam=bam,
                output=output,
                target=self.target_file)]
            self.result_files = [output]
        elif self.step == 'base_recal':
            bam = [f for f in self.in_files if f.endswith('.sorted.deduped.bam')][0]
            bqsr = '{}.base_recal_table.txt'.format(self.prefix)
            output = '{}.sorted.deduped.recalibrated.bam'.format(self.prefix)
            output_bai = '{}.sorted.deduped.recalibrated.bai'.format(self.prefix)
            cmd_strs = [unformat_cmd_str.format(
                tmp_dir=self.tmp_dir,
                bam=bam,
                bqsr=bqsr,
                output=output)]
            self.result_files = [output, output_bai]
        elif self.step == 'haplotyper':
            # The haplotyper step will contain three parts:
            # 1) Unzipping the sentieon package,
            # 2) Running sentieon haplotyper
            # 3) Running sentieon bgzip
            untar_cmd_str = 'tar xzf {}'.format(self.sentieon_pkg)
            bam = [f for f in self.in_files if f.endswith('.sorted.deduped.recalibrated.bam')][0]
            #bam = [f for f in self.in_files if f.endswith('.cram')][0]
            fasta = [f for f in self.ref_files if f.endswith('.fasta')][0]
            dbsnp = [f for f in self.ref_files if 'dbsnp' in f][0]
            output = '{}.gvcf'.format(self.prefix)
            self.intermediate_files = [output, '{}.idx'.format(output)]
            output_gz = '{}.gvcf.gz'.format(self.prefix)
            haplo_cmd_str = unformat_cmd_str[0].format(
                threads=self.threads,
                bam=bam,
                output=output,
                target=self.target_file)
            zip_cmd_str = unformat_cmd_str[1].format(
                output=output,
                output_gz=output_gz)
            self.result_files = [output_gz, '{}.tbi'.format(output_gz)]
            cmd_strs = [untar_cmd_str, haplo_cmd_str, zip_cmd_str]
        elif self.step == 'genotyper':
            untar_cmd_str = 'tar xzf {}'.format(self.sentieon_pkg)
            fasta = [f for f in self.ref_files if f.endswith('.fasta')][0]
            dbsnp = [f for f in self.ref_files if 'dbsnp' in f][0]
            gvcfs = [f for f in self.in_files if f.endswith('.gvcf.gz') or f.endswith('.gvcf')]
            #Create -V arg list for all gvcfs
            gvcf_args = []
            [gvcf_args.extend(['-v', g]) for g in gvcfs]
            output = '{}.gt.vcf'.format(self.prefix)
            output_gz = '{}.gt.vcf.gz'.format(self.prefix)
            geno_cmd_str = unformat_cmd_str[0].format(output=output, target=self.target_file)
            geno_cmd_str = '{} {}'.format(geno_cmd_str, ' '.join(gvcf_args))
            self.intermediate_files = [output, '{}.idx'.format(output)]
            zip_cmd_str = unformat_cmd_str[1].format(output=output, output_gz=output_gz)
            self.result_files = [output_gz, '{}.tbi'.format(output_gz)]
            cmd_strs = [untar_cmd_str, geno_cmd_str, zip_cmd_str]
        elif self.step == 'vqsr_snp_model':
            vcf = [f for f in self.in_files if f.endswith('.gt.vcf.gz')][0]
            recal_file = '{}.gt.snp.recal.model'.format(self.prefix)
            tranches_file = '{}.gt.snp.recal.tranches'.format(self.prefix)
            rscript_file = '{}.gt.snp.recal.plots.R'.format(self.prefix)
            cmd_strs = [unformat_cmd_str.format(
                tmp_dir=self.tmp_dir,
                vcf=vcf,
                recal_file=recal_file,
                tranches_file=tranches_file,
                rscript_file=rscript_file)]
            self.result_files = [recal_file, tranches_file, rscript_file]
        elif self.step == 'vqsr_snp_apply':
            vcf = [f for f in self.in_files if f.endswith('.gt.vcf.gz')][0]
            recal_file = '{}.gt.snp.recal.model'.format(self.prefix)
            tranches_file = '{}.gt.snp.recal.tranches'.format(self.prefix)
            output = '{}.gt.snp.recal.vcf'.format(self.prefix)
            cmd_strs = [unformat_cmd_str.format(
                tmp_dir=self.tmp_dir,
                vcf=vcf,
                recal_file=recal_file,
                tranches_file=tranches_file,
                output=output)]
            self.result_files = [output, '{}.idx'.format(output)]
        elif self.step == 'vqsr_indel_model':
            vcf = [f for f in self.in_files if f.endswith('.gt.snp.recal.vcf')][0]
            recal_file = '{}.gt.snp.indel.recal.model'.format(self.prefix)
            tranches_file = '{}.gt.snp.indel.tranches'.format(self.prefix)
            rscript_file = '{}.gt.snp.indel.plots.R'.format(self.prefix)
            cmd_strs = [unformat_cmd_str.format(
                tmp_dir=self.tmp_dir,
                vcf=vcf,
                recal_file=recal_file,
                tranches_file=tranches_file,
                rscript_file=rscript_file)]
            self.result_files = [recal_file, tranches_file, rscript_file]
        elif self.step == 'vqsr_indel_apply':
            vcf = [f for f in self.in_files if f.endswith('.gt.snp.recal.vcf')][0]
            recal_file = '{}.gt.snp.indel.recal.model'.format(self.prefix)
            tranches_file = '{}.gt.snp.indel.tranches'.format(self.prefix)
            output = '{}.gt.snp.indel.recal.vcf'.format(self.prefix)
            cmd_strs = [unformat_cmd_str.format(
                tmp_dir=self.tmp_dir,
                vcf=vcf,
                recal_file=recal_file,
                tranches_file=tranches_file,
                output=output)]
            self.result_files = [output, '{}.idx'.format(output)]
        elif self.step == 'fam_vcf_from_cohort':
            vcf = [f for f in self.in_files if f.endswith('.gt.snp.indel.recal.vcf')][0]
            cmd_strs = [unformat_cmd_str.format(
                tmp_dir=self.tmp_dir,
                vcf=vcf,
                fam_id=self.fam_id,
                fil=self.fil,
                pat=self.pat,
                mat=self.mat)]
            self.result_files = ['{}.vcf'.format(self.fam_id), '{}.vcf.idx'.format(self.fam_id)]
        elif self.step == 'scrub_vcf':
            vcf = vcf = [f for f in self.in_files if f.endswith('.vcf')][0]
            cmd_strs = [unformat_cmd_str.format(
                vcf=vcf,
                fam_id=self.fam_id)]
            self.result_files = ['{}.scrubbed.vcf'.format(self.fam_id)]
        elif self.step == 'ped_from_vcf':
            vcf = vcf = [f for f in self.in_files if f.endswith('.vcf')][0]
            cmd_strs = [unformat_cmd_str.format(
                vcf=vcf,
                fam_id=self.fam_id)]
            self.result_files = ['{}.ped'.format(self.fam_id)]
        elif self.step == 'triodenovo':
            cmd_strs = [unformat_cmd_str.format(
                fam_id=self.fam_id)]
            self.result_files = ['{}.triodenovo.vcf'.format(self.fam_id)]
        else:
            print('Unrecognised pipeline step {}!'.format(self.step))
            exit(1)
        cmd_list = [cmd.split(' ') for cmd in cmd_strs]
        sys.stdout.flush()
        return cmd_list

    def run_cmd(self):
        '''
        Runs the bioinformatics tool for the current step of the pipeline.
        '''
        cmd = self.build_cmd()
        if self.step == 'haplotyper' or self.step == 'genotyper':
            print('UNZIPPING SENTIEON PACKAGE: {}'.format(cmd[0]))
            sys.stdout.flush()
            return_code_0 = subprocess.call(cmd[0])
            main_str = ' '.join(cmd[1])
            print('STEP TO RUN: {}'.format(self.step))
            print('COMMAND TO RUN: {}'.format(main_str))
            sys.stdout.flush()
            return_code_1 = subprocess.call(cmd[1])

            gz_str = ' '.join(cmd[2])
            print('COMMAND TO RUN: {}'.format(gz_str))
            sys.stdout.flush()
            return_code_2 = subprocess.call(cmd[2])
            return_code = return_code_0 + return_code_1 + return_code_2
            if return_code > 0:
                print('NON ZERO EXIT: {}'.format(str(return_code)))
                sys.stdout.flush()
                exit(return_code)
        elif self.step == 'sam_to_fq':
            print('RUNNING PICARD SAMTOFASTQ')
            sys.stdout.flush()
            return_code_0 = subprocess.call(cmd[0])
            print('ZIPPING')
            sys.stdout.flush()
            return_code_1 = subprocess.call(cmd[1])
            return_code_2 = subprocess.call(cmd[2])
            return_code = return_code_0 + return_code_1 + return_code_2
            if return_code > 0:
                print('NON ZERO EXIT: {}'.format(str(return_code)))
                sys.stdout.flush()
                exit(return_code)
        else:
            cmd_str = ' '.join(cmd[0])
            print('STEP TO RUN: {}'.format(self.step))
            print('COMMAND TO RUN: {}'.format(cmd_str))
            sys.stdout.flush()
            if self.step == 'bwa_mem':
                # BWA needs extra pipeing...
                output = '{}.sam'.format(self.prefix)
                self.result_files = [output]
                with open(output, 'w') as f:
                    return_code = subprocess.call(cmd[0], stdout=f)
                if return_code > 0:
                    print('NON ZERO EXIT: {}'.format(str(return_code)))
                    sys.stdout.flush()
                    exit(return_code)
            else:
                return_code = subprocess.call(cmd[0])
                if return_code > 0:
                    print('NON ZERO EXIT: {}'.format(str(return_code)))
                    sys.stdout.flush()
                    exit(return_code)

    def upload_results(self):
        '''
        Uploads any output files to out_uri.
        '''
        print('Step {} produced these files: {}.'.format(self.step, ' ,'.join(self.result_files)))
        bucket_name = self.out_uri.split('/')[2]
        prefix = '/'.join(self.out_uri.split('/')[3:])
        for file_name in self.result_files:
            key = '{}{}'.format(prefix, file_name)
            print('Uploading {} to bucket {}, key {}.'.format(file_name, bucket_name, key))
            b = s3.Bucket(bucket_name)
            b.upload_file(file_name, key)
        sys.stdout.flush()

    def cleanup(self):
        '''
        Removes input, output, intermediate, and license files from cwd.
        Removes the tmp directory if a java program was used.
        '''
        print('Cleaning up instance...')
        for file in self.result_files:
            print('Removing result file {}.'.format(file))
            os.remove(file)
        if self.intermediate_files != []:
            for file in self.intermediate_files:
                print('Removing intermediate file {}.'.format(file))
                os.remove(file)
        if self.step in ['haplotyper', 'genotyper']:
            #Remove sentieon_pkg and license_file
            os.remove(self.sentieon_pkg)
            shutil.rmtree(self.sentieon_pkg.split('.tar')[0])
            os.remove(self.license_file)
        if self.step not in ['bwa_mem', 'haplotyper', 'genotyper']:
            #bwa, haplotyper, genotyper don't use tmp dirs
            print('Removing tmp dir.')
            shutil.rmtree(self.tmp_dir)
        for file in self.in_files:
            if self.step in ('bwa_mem', 'sam_to_fq'):
                to_del = file.split('/')[-1]
                print('Removing input file {}'.format(file))
                os.remove(to_del)
            else:
                print('Removing input file {}'.format(file))
                os.remove(file)
        sys.stdout.flush()
