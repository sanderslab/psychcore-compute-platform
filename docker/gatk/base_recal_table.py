import SDK
import os
from datetime import datetime

def main():
    prefix = os.environ["prefix"]
    param_file = os.environ["param_file"]
    ref_uri = os.environ["ref_uri"]
    in_uri = os.environ["in_uri"]
    out_uri = os.environ["out_uri"]
    assets_uri = os.environ["assets_uri"]
    ome = os.environ["ome"]
    build = os.environ["build"]
    if ome == "wes":
        target_file = os.environ["target_file"]
    else:
        target_file = None
    bam = "{}.sorted.deduped.bam".format(prefix)
    bai = "{}.sorted.deduped.bam.bai".format(prefix)
    in_files = [bam, bai]
    threads = os.environ['threads']

    start_time = datetime.now()
    print("BASE RECAL TABLE for {} was started at {}.".format(prefix, str(start_time)))

    task = SDK.Task(
        step="base_recal_table",
        prefix=prefix,
        threads=threads,
        in_files=in_files,
        param_file=param_file,
        ref_uri=ref_uri,
        in_uri=in_uri,
        out_uri=out_uri,
        assets_uri=assets_uri,
        target_file=target_file)

    if ome == "wes" and target_file:
        task.download_files("TARGET")
    task.get_reference_files(build)
    task.download_files("INPUT")
    task.download_files("REF")
    task.download_files("PARAMS")
    task.build_cmd()
    task.run_cmd()
    task.upload_results()
    task.cleanup()

    end_time = datetime.now()
    print("BASE RECAL TABLE for {} was ended at {}.".format(prefix, str(end_time)))
    total_time = end_time - start_time
    print("Total time for BASE RECAL TABLE was {}.".format(str(total_time)))

if __name__ == '__main__':
    main()
