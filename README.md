# PsychCore Compute Platform #

![Miner](https://vignette.wikia.nocookie.net/fallout/images/b/bf/Earth_Mover.png/revision/latest?cb=20181106144309)

**PsychCore Compute Platform** is a cloud-based computing platform that supports diverse NGS data analyses, large and small.  Through the implementation of *pipelines*, users create or customize a bioinformatic analysis that runs on the cloud *platform*.  A core design philosophy of the project is enabling the community to create arbitrary data analysis pipelines that address central or ad-hoc research inquiries.  To seed this effort, we provide a whole-genome/whole-exome variant calling pipeline that is high-throughput, low-cost, and calls variants with a genotype concordance of 99.96% (on the [PrecessionFDA Truth Challenge](https://precision.fda.gov/challenges/truth) dataset).  The team is actively working on additional *-omics* pipelines, such as RNASeq, ChIPSeq, CWAS, and more.  As well as constantly improving the platform and its user-facing API.

![Documentation Status](https://readthedocs.org/projects/psychcore-ngs-pipeline/badge/?version=latest)
![Build Status](https://codebuild.us-east-1.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiSGJVdjVzckFkVnlsWkZGdFVCTElvN08vMmMrRVU1TStsVmdsdk1Hc0NvYXh1Z1EvL3crVkUvbm9GZzJ3ZHhkclNOYXhBUnFBM0I3VjM4Qk95Wlk3ZDBVPSIsIml2UGFyYW1ldGVyU3BlYyI6ImtWWmNBTVU1Sk5pUnIvN2QiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=master)

## Features

- **Runs user- and community-defined data analysis pipelines on the cloud.** Users define an analysis pipeline as a [state machine](https://en.wikipedia.org/wiki/Finite-state_machine). Each state can execute code in a variety of environments, including AWS Lambda, Docker containers on AWS Batch, Spark jobs on GCP Dataproc, and others. As states complete and transition to new states in the machine, the analysis pipeline makes progress.  When writing an analysis pipeline, you can focus on solving the data problem at hand, and allow the platform to handle resource management.  We've included a CCDG-compliant, whole-genome/whole-exome pipeline that scales in a cost-effective way to thousands of samples.
- **No on-site cluster needed.** Through the use of a cloud platform, you do not need access to a departmental or institutional compute cluster. This overcomes central limitations of on-premises technology, such as: scaling up workloads to thousands of machines, utilizing specialized, non-commodity hardware (GPU and FPGA systems, for instance), and the installation and management of software packages by a third party administrator.
- **Push button, automated end-to-end execution.** With pipelines defined and configured, the execution of the entire pipeline can be kicked off with a single key press. It will run to completion without any further intervention by the user and can recover from interittent service failures.
- **Multi-cloud *Big Data* processing.** This system uses big data technologies on both the AWS and Google Cloud Platform.  For tasks that can be parallelized by data division, AWS Batch is used. Batch is effectively a set of job queues that are consumed by docker container workers.  For tasks that cannot be solved by data division, Google Cloud Platform's Dataproc is used.  Dataproc is an easy-to-use service to run hadoop ecosystem clusters, and we specifically use Spark for WGS applications where the large datasets and operations are better modeled as dataframes.
- **Stateless and Serverless.** Every component of a pipeline is stateless and runs in a managed service environment by the cloud platform, including the pipeline orchestration system.  This means there is no need to configure or manage any servers, databases, or container orchestrators. Being stateless and serverless gives rise to the security, resiliency and scalability properties.
- **Scalable.** Pipeline scaling behavior is dictated by a combination of the computation being performed and the underlying technology used to implement the pipeline stage. Generally speaking that means your pipeline can scale with an AWS Batch compute environment for parallelizable stages. Batch can scale to thousands of VM's that each run many docker containers.  For solutions using Hadoop or Spark, Google Dataproc can, again, scale to a cluster of thousands of VM's.  The way a stage scales on the platform will also largely depend on how it is implemented.
- **Containerized.** Implement the stages of your pipeline with Docker containers. Develop locally and push your changes to DockerHub to speed up the development process.  Containers guarantee that local execution will be identical to remote execution.  In addition, when a parallel stage of a pipeline executes, AWS Batch optimizes the placement of containers onto running VM instances, while optimizing the number and size of the VMs running the analysis. This ensures that resource utilization is optimized and cost is kept down.
- **Tuned for cost and runtime.** Time is money. Both the platform and the example mutisample WGS pipeline are time and cost-optimized. The platform was designed around free services in the cloud platforms, where one is only billed for long-running compute tasks. Where costly resources are used, resource orchestration technology is used to optimize the placement of jobs onto the compute resource, as well as the number and type of the compute resources needed to consume a queue of jobs. The example WGS pipeline has configuration options tuned based on performance data from many trial runs.
- **Infrastructure management for free.** All cloud resources and their interconnections are described and managed through an *Infrastructure as Code* system. The code is handed to AWS, and thereafter the resource lifecycle is totally managed by AWS.  There is no need to worry about orphaned resources when tearing down the system, which incur additional costs and are security risks. Also, with the infrastructre described in code, the compute system is gauranteed to be the same between runs. Which leads to its repeatability and reporducibility properties.
- **Repeatability and Reproducibility.** The *Infrastructure as Code* and git commit guarantee the same compute environment between runs.  So long as the user provides the same input, all deterministic components of the system will behave identically between runs.  The driver allows a user to easily repeat an experiment, and also reproduce the results of prior executions.  These properties make it easy to share pipelines with colleagues, collaborators, and peer reviewers that wish to regenerate results or hack on a pipeline.
- **Realtime monitoring** Containers and Lambda functions log to CloudWatch Logs. GCP Dataproc jobs can be monitored in the Spark UI running on the driver (Master node).
- **Secure.** Serverless orchestration through lambda functions, compute farm is in private subnet. So no servers that are addressable from the public internet.

## Quick Start

First check the pre-requisites<sup>&#8224;</sup> below for preparing your AWS Account and local machine to run a pipeline.

### Running the example pipeline

Clone the repo:

```
$ cd /path/to/parent/folder
$ git clone https://github.com/sanderslab-admin/psychcore-compute-platform.git
```

Install Python dependencies:

```
$ cd psychcore-compute-platform
$ pip install -r requirements.txt
```

(It's recommended to create a [virtual Python environment](https://realpython.com/python-virtual-environments-a-primer/) for the pipeline driver and its dependencies [from `requirements.txt`]. Consider using [Miniconda](https://conda.io/en/latest/miniconda.html) or [virtualenv](https://virtualenv.pypa.io/en/latest/) to manage your Python environments.)

Copy `run.yaml` from the `examples` folder to the project root and fill in the values.  See the full documentation for which keys in `run.yaml` are typically necessary to change. This typically requires pointing to the appropriate S3 bucket, sample file, and Sentieon license file.

```
$ cp examples/run.yaml .
$ vim run.yaml
```

Run driver on provided WES/WGS germline pipeline

```
$ python rkstr8_driver.py -p germline_wgs
```

### <sup>&#8224;</sup> Pre-requisites

There is a small amount of preparation necessary in order to run a pipeline. First, an [Amazon Web Services](https://aws.amazon.com/) account is required to execute the pipeline. In addition to that, you'll need to get the code onto your machine from [GitHub](https://github.com/), using [git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git). Finally, [Python](https://www.python.org/) is required to run the driver that kicks off the pipeline.

#### Create and configure AWS Account

- **Create an Amazon Web Services account**
  - Full instructions can be found [here](https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/).
  - Accounts start out as 12 month, $0 cost for 40 services
- **Create an AWS user with admin credentials**
  - Follow the instructions [here](https://docs.aws.amazon.com/IAM/latest/UserGuide/getting-started_create-admin-group.html) to create an Administrative user
  - This step can be skipped for existing accounts that have an IAM user with `AdministratorAccess` permissions
- **S3**
  - `S3` is one of the storage services offered by AWS. It is used extensively by the pipeline. 
  - If there are no `S3` buckets created in your account, you can create one following [these instructions](https://docs.aws.amazon.com/quickstarts/latest/s3backup/step-1-create-bucket.html)
- **Limits**
  - AWS accounts start off with certain limits imposed. For example, the number and type of machines that can be running simultaneously. 
  - For large datasets, some limits will need to be increased. Please follow [these](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-resource-limits.html) instructions.
- **SSH KeyPair**
  - You will need at least one SSH Key Pair in your AWS account.
  - Please follow [these](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html) instructions to create the Key Pair.
- **AWS Service Credentials**
  - In order to run the Pipeline driver, you will need an AWS administrator user in your account (see above for instruction on creating one)
  - In addition, you will need the Access Keys for this user.
  - These Access Keys would have been created at the time of creating the Administrator user.  If no access keys are present, please follow [these](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html#Using_CreateAccessKey) instructions to create them.

#### Prepare local machine to run pipeline

To run a pipeline, you'll need to clone the repository from GitHub and run the Python driver script. Optionally you can install the AWS Command Line Interface (CLI) for easier access to services like S3, where your data lives. 

- **Python 3.6** 
  - There are comprehensive instructions on installing Python 3.6 [here](https://realpython.com/installing-python/), on a variety of platforms including Linux, Windows, and macOS/Mac OS X.
- **Git** 
  - To clone the repository from GitHub, you can use the `git` program, which has detailed installation instructions [here](https://www.atlassian.com/git/tutorials/install-git).
  - Optionally you can bypass the use of `git` and download the repository as an `zip` archive, using your brower or a command-line HTTP client like `wget` or `cURL`.
- **AWS CLI (optional)** 
  - The AWS CLI is a program that allows you to read, write, create, and delete resources on AWS, such as files in S3. It runs on your operating system's command-line and provides a text-based interface to managing AWS resources.  Instructions on installing the CLI are [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html).
    - To work with your account, the CLI must be configured with access keys. If you followed the steps above you should have generated access keys when creating an AWS user under whom to run the pipeline. Details on various ways to configure the AWS CLI can be found [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html).
  - The AWS CLI is optional since you can perform most operations using the [AWS Web Console](https://aws.amazon.com/console/).  It's recommended, however, to use the AWS CLI to upload data files to S3 if the files are large, like FASTQs.  Using the Web Console to upload large files through your browser is not recommended.