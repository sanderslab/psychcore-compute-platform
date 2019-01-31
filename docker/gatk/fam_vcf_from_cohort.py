import SDK
import os
from datetime import datetime

def main():
    prefix = os.environ['prefix']
    param_file = os.environ['param_file']
    ref_uri = os.environ['ref_uri']
    in_uri = os.environ['in_uri']
    out_uri = os.environ['out_uri']
    assets_uri = os.environ['assets_uri']
    build = os.environ['build']
    vcf = '{}.gt.snp.indel.recal.vcf'.format(prefix)
    idx = '{}.gt.snp.indel.recal.vcf.idx'.format(prefix)
    in_files = [vcf, idx]
    fam_id = os.environ['fam_id']
    fil = os.environ['fil']
    mat = os.environ['mat']
    pat = os.environ['pat']
    fam_dict = {
        'fam_id' : fam_id,
        'fil' : fil,
        'mat' : mat,
        'pat' : pat
    }

    start_time = datetime.now()
    print('FAM VCF FROM COHORT for {} was started at {}.'.format(fam_id, str(start_time)))

    task = SDK.Task(
        step='fam_vcf_from_cohort',
        prefix=prefix,
        in_files=in_files,
        param_file=param_file,
        ref_uri=ref_uri,
        in_uri=in_uri,
        out_uri=out_uri,
        assets_uri=assets_uri,
        fam_dict=fam_dict)
    task.get_reference_files(build)
    task.download_files('INPUT')
    task.download_files('REF')
    task.download_files('PARAMS')
    task.build_cmd()
    task.run_cmd()
    task.upload_results()
    task.cleanup()

    end_time = datetime.now()
    print('FAM VCF FROM COHORT for {} ended at {}.'.format(fam_id, str(end_time)))
    total_time = end_time - start_time
    print('Total time for FAM VCF FROM COHORT was {}.'.format(str(total_time)))

if __name__ == '__main__':
    main()
