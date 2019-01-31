import SDK
import os
from datetime import datetime

def main():
    start_point = os.environ['start_point']
    prefix = os.environ['prefix']
    param_file = os.environ['param_file']
    ref_uri = os.environ['ref_uri']
    in_uri = os.environ['in_uri']
    out_uri = os.environ['out_uri']
    assets_uri = os.environ['assets_uri']
    sentieon_pkg = os.environ['sentieon_pkg']
    license_file = os.environ['sentieon_license']
    build = os.environ['build']
    ome = os.environ['ome']
    if ome == 'wes':
        target_file = os.environ['target_file']
    else:
        target_file = None
    if start_point == 'fastq':
        bam = '{}.sorted.deduped.recalibrated.bam'.format(prefix)
        bai = '{}.sorted.deduped.recalibrated.bai'.format(prefix)
    else:
        bam = os.environ['in_file']
        bai = '{}.bai'.format('.'.join(bam.split('.')[:-1]))
        #bai = '{}.bai'.format(bam)
        #bai = '{}.crai'.format(bam)

    in_files = [bam, bai]
    threads = os.environ['threads']

    start_time = datetime.now()
    print('Sentieons HAPLOTYPER for {} was started at {}.'.format(prefix, str(start_time)))

    task = SDK.Task(
        step='haplotyper',
        prefix=prefix,
        threads=threads,
        in_files=in_files,
        param_file=param_file,
        sentieon_pkg=sentieon_pkg,
        license_file=license_file,
        ref_uri=ref_uri,
        in_uri=in_uri,
        out_uri=out_uri,
        assets_uri=assets_uri,
        target_file=target_file)

    if ome == 'wes' and target_file:
        task.download_files('TARGET')
    task.get_reference_files(build)
    task.download_files('INPUT')
    task.download_files('REF')
    task.download_files('SENTIEON')
    task.download_files('PARAMS')
    task.build_cmd()
    task.run_cmd()
    task.upload_results()
    task.cleanup()

    end_time = datetime.now()
    print('Sentieons HAPLOTYPER for {} ended at {}.'.format(prefix, str(end_time)))
    total_time = end_time - start_time
    print('Total time for Sentieons HAPLOTYPER was {}.'.format(str(total_time)))

if __name__ == '__main__':
    main()
