# import sys
# import os
# import json
# import yaml
# import SDK
# import InputAdapters

# # For scrub_vcf, we can either have one sample or multiple but inputFiles 
# # must be a list
# if "YAML" in os.environ:
# 	y = os.environ["YAML"]
# 	yml = yaml.load(y)
# 	prefix = yml["prefix"]
# 	refBucket = SDK.Bucket(yml["refBucket"], "read")
# 	inBucket = SDK.Bucket(yml["inBucket"], "read")
# 	outBucket = SDK.Bucket(yml["outBucket"], "write")
# elif "inBucket" in os.environ and \
# 	"outBucket" in os.environ and \
# 	"refBucket" in os.environ:
# 	refBucket = SDK.Bucket(os.environ["refBucket"], "read")
# 	inBucket = SDK.Bucket(os.environ["inBucket"], "read")
# 	outBucket = SDK.Bucket(os.environ["outBucket"], "write")
# 	prefix = os.environ["prefix"]
# else:
# 	raise ValueError("Environment not configured properly. Either provide a YAML file or set environment variables inputFiles, refBucket, inBucket, and outbucket.")


# # Initialize working environment (which has an output dir in it)
# # in the mounted Volume called localDir
# wd = SDK.WorkDir(os.getcwd() + "/localDir/")

# # Get input files from inBucket
# if not wd.contains(prefix + ".vcf"):
# 	wd.putS3Object(inBucket.getKey(prefix + ".vcf"))


# os.system("python /scrub_vcf/InputAdapters.py -i " +  wd.getFile(prefix + ".vcf") + " -o " + wd.outputDir + prefix + ".scrubbed.vcf")
# outBucket.putFile(wd.outputDir + prefix + ".scrubbed.vcf")
# # Output files should be in wd/Output, copy them to s3
# #for file in os.listdir(wd.outputDir):
# 	#outBucket.putFile(wd.outputDir + file)

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
    fam_id = os.environ['fam_id']
    vcf = '{}.vcf'.format(fam_id)
    idx = '{}.idx'.format(vcf)

    in_files = [vcf, idx]

    print(in_files)

    start_time = datetime.now()
    print('SCRUB VCF for {} was started at {}.'.format(prefix, str(start_time)))

    task = SDK.Task(
        step='scrub_vcf',
        prefix=prefix,
        in_files=in_files,
        param_file=param_file,
        ref_uri=ref_uri,
        in_uri=in_uri,
        out_uri=out_uri,
        assets_uri=assets_uri)
    dir_contents = os.listdir('.')

    print('Current dir contents: {}'.format(str(dir_contents)))
    task.get_reference_files(build)
    task.download_files('INPUT')
    task.download_files('REF')
    task.download_files('PARAMS')
    task.build_cmd()
    task.run_cmd()
    task.upload_results()
    task.cleanup()

    end_time = datetime.now()
    print('SCRUB VCF for {} ended at {}.'.format(prefix, str(end_time)))
    total_time = end_time - start_time
    print('Total time for SCRUB VCF was {}.'.format(str(total_time)))

if __name__ == '__main__':
    main()
