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
    sample_file = os.environ["sample_file"]
    sentieon_pkg = os.environ["sentieon_pkg"]
    license_file = os.environ["sentieon_license"]
    ome = os.environ["ome"]
    build = os.environ["build"]
    if ome == "wes":
        target_file = os.environ["target_file"]
    else:
        target_file = None
    start_time = datetime.now()
    print("Sentieon's GENOTYPER for {} was started at {}.".format(prefix, str(start_time)))
    in_files = []
    task = SDK.Task(
        step="genotyper",
        prefix=prefix,
        in_files=in_files,
        param_file=param_file,
        sentieon_pkg=sentieon_pkg,
        license_file=license_file,
        ref_uri=ref_uri,
        in_uri=in_uri,
        out_uri=out_uri,
        assets_uri=assets_uri,
        target_file=target_file,
        sample_file=sample_file)

    if ome == "wes" and target_file:
        task.download_files("TARGET")
    task.get_reference_files(build)
    task.get_genotyping_samples()
    task.download_files("INPUT")
    task.download_files("REF")
    task.download_files("SENTIEON")
    task.download_files("PARAMS")
    # Note: genotyper is the only step where in_files is set as an env variable
    # Build in_files from sample_file
    task.build_cmd()
    task.run_cmd()
    task.upload_results()
    task.cleanup()

    end_time = datetime.now()
    print("Sentieon's GENOTYPER for {} ended at {}.".format(prefix, str(end_time)))
    total_time = end_time - start_time
    print("Total time for Sentieon's GENOTYPER was {}.".format(str(total_time)))

if __name__ == '__main__':
    main()
