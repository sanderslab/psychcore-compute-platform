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
    build = os.environ["build"]
    R1 = os.environ["R1"]
    R2 = os.environ["R2"]
    in_files = [R1, R2]
    threads = os.environ['threads']

    print(in_files)

    start_time = datetime.now()
    print("BWA MEM for {} was started at {}.".format(prefix, str(start_time)))

    task = SDK.Task(
        step="bwa_mem",
        prefix=prefix,
        threads=threads,
        in_files=in_files,
        param_file=param_file,
        ref_uri=ref_uri,
        in_uri=in_uri,
        out_uri=out_uri,
        assets_uri=assets_uri)
    dir_contents = os.listdir(".")

    print("Current dir contents: {}".format(str(dir_contents)))
    task.get_reference_files(build)
    task.download_files("INPUT")
    task.download_files("REF")
    task.download_files("PARAMS")
    task.build_cmd()
    task.run_cmd()
    task.upload_results()
    task.cleanup()

    end_time = datetime.now()
    print("BWA MEM for {} ended at {}.".format(prefix, str(end_time)))
    total_time = end_time - start_time
    print("Total time for BWA MEM was {}.".format(str(total_time)))

if __name__ == '__main__':
    main()
