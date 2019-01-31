# '''
# The trio step in the ClinE pipeline; takes a vcf for a trio 
# and creates a ped file.

# '''
# import sys
# import os
# import yaml
# import SDK

# # Get Bucket s3:// locations and input files from JSON from Dockerfile Env

# '''
#  Get buckets and input vcf from YAML from Dockerfile Env
# # Example yaml file: 

# ---
# refBucket: 's3://test-references/'
# inBucket: 's3://test-in-bucket/'
# outBucket: 's3://test-out-bucket/'
# inputFiles:
#  - "457" 
#  - "617" 
#  - "658" 
#  - "680" 
#  - "689" 
#  - "696" 
#  - "773" 
#  - "780" 
#  - "783" 
#  - "795" 
#  - "796" 
#  - "801" 
#  - "803" 
#  - "806" 
#  - "817" 

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
# # Get input ped and vcf for a sample
# vcf = prefix + ".scrubbed.vcf"
# ped = prefix + ".ped"

# if not wd.contains(ped):
# 	wd.putS3Object(inBucket.getKey(ped))
# if not wd.contains(vcf):
# 	wd.putS3Object(inBucket.getKey(vcf))

# os.system("triodenovo --ped " + wd.getFile(ped) + " --in_vcf " + wd.getFile(vcf) \
# 		+ " --out_vcf " + wd.outputDir + prefix + ".triodenovo.vcf" \
# 		+ " --minDepth 10 " " --chrX X --mixed_vcf_records")


# outBucket.putFile(wd.outputDir + prefix + ".triodenovo.vcf")
# # Output files should be in wd/Output, copy them to s3
# for file in os.listdir(wd.outputDir):
# 	#outBucket.putFile(wd.outputDir + file)
# 	os.remove(wd.outputDir + file)


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
    fix_tar = 'triodenovo-fix.tar.gz'
    vcf = '{}.scrubbed.vcf'.format(fam_id)
    #idx = '{}.idx'.format(vcf)
    ped = '{}.ped'.format(fam_id)

    in_files = [vcf, ped, fix_tar]

    print(in_files)

    start_time = datetime.now()
    print('TRIODENOVO for {} was started at {}.'.format(prefix, str(start_time)))

    task = SDK.Task(
        step='triodenovo',
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
    print('TRIODENOVO for {} ended at {}.'.format(prefix, str(end_time)))
    total_time = end_time - start_time
    print('Total time for TRIODENOVO was {}.'.format(str(total_time)))

if __name__ == '__main__':
    main()
