# '''
# The pedigree_from_vcf step in the ClinE pipeline; takes a vcf for a trio 
# and creates a ped file.

# '''
# import sys
# import os
# import yaml
# import SDK
# import ped_from_vcf_main

# # Get Bucket s3:// locations and input files from JSON from Dockerfile Env

# #j = os.environ['JSON']
# #js = json.loads(j)

# '''
#  Get buckets and input vcf from YAML from Dockerfile Env
# # Example yaml file: 

# ---
# refBucket: 's3://test-references/'
# inBucket: 's3://test-in-bucket/'
# outBucket: 's3://test-out-bucket/'
# vcf: 
#  - '457.vcf'
# '''
# print os.environ
# if "YAML" in os.environ:
# 	y = os.environ["YAML"]
# 	yml = yaml.load(y)
# 	inputFiles = yml["inputFiles"]
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
# # in the mounted Volume called localDir so that there's enough space
# wd = SDK.WorkDir(os.getcwd() + "/localDir/")

# # Run ped_from_vcf_main
# if not wd.contains(prefix + ".vcf"):
# 	wd.putS3Object(inBucket.getKey(prefix + ".vcf"))

# os.system("python /pedigree_from_vcf/ped_from_vcf_main.py " + wd.getFile(prefix + ".vcf") + " > " + wd.outputDir + prefix + ".ped")


# # Output files should be in wd/Output, copy them to s3
# for file in os.listdir(wd.outputDir):
# 	outBucket.putFile(wd.outputDir + file)
# 	#os.remove(wd.outputDir + file)


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
    print('PED FROM VCF for {} was started at {}.'.format(prefix, str(start_time)))

    task = SDK.Task(
        step='ped_from_vcf',
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
    print('PED FROM VCF for {} ended at {}.'.format(prefix, str(end_time)))
    total_time = end_time - start_time
    print('Total time for PED FROM VCF was {}.'.format(str(total_time)))

if __name__ == '__main__':
    main()
