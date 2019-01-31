.. _sec-conf:

========================
Configuring the Pipeline
========================

.. _runyaml:

Your Pipeline Run
-----------------
The pipeline requires some information from the user to run.
In the current build of the pipeline, you, the user, will have to modify the 
``run.yaml`` file located in the root of the project.  It has 23 fields and looks
like the following:

.. code-block:: yaml

	---
	# Infrastructure configuration
	STACK_NAME : 
	RESOURCE_CFN_TMPL_DEPLOY_BUCKET : 
	GPCE_SSH_KEY_PAIR : 
	START_POINT : 
	QC : 

	# Input/output file locations
	INPUT : 
	OUTPUT : 
	REF_URI : 
	USER_ASSETS_URI : 

	# User assets
	SAMPLE_FILE : 
	TARGET : 
	SENTIEON_PACKAGE_NAME : 
	SENTIEON_LICENSE_NAME : 

	# Cohort information
	NUM_SAMPLES : 
	COHORT_PREFIX : 
	BUILD : 
	OME : 

	# Google Cloud Platform related configuration
	CLOUDSPAN_MODE: 
	GCP_CREDS_FILE : 
	CLOUD_TRANSFER_OUTBUCKET : 
	PROJECT_ID : 
	ZONE : 
	CLOUD_FILE: 

	# Docker
	DOCKER_ACCOUNT: 

Listed below are descriptions for each of the fields.  Please note that the GCP-related parameters only need to be filled in if you are running downstream processes on the pipeline's resulting VCF. For more information about the yaml
file format, see the `official yaml website`_.

*Infrastructure configuration*

* ``STACK_NAME``: The name of the Cloudformation (CFN) stack
* ``RESOURCE_CFN_TMPL_DEPLOY_BUCKET``: Bucket name (e.g. pipeline-run)
* ``GPCE_SSH_KEY_PAIR``: Accou pecific key pair for using AWS EC2 (e.g. John_Key)
* ``START_POINT``: The format of the input files (fastq|bam|gvcf|vcf)
* ``QC (optional)``: List of what QC to run (BAM|VCF)

*Input/output file locations*

* ``INPUT``: S3 location of your fastq files (e.g. s3://pipeline-run/fastqs/)
* ``OUTPUT``: S3 location of all resulting files (e.g. s3://pipeline-run/results/)
* ``REF_URI``: S3 location of reference genome files (e.g. s3://GRCh38-references/)
* ``USER_ASSETS_URI``: S3 location for users' assets upload


*User assets*

* ``SAMPLE_FILE``: Name of the file which has the list of sample names (prefix to .fastq.gz)
* ``TARGET (optional)``: Interval BED file if ome is "wes" (e.g. Exome-NGv3.bed)
* ``SENTIEON_PACKAGE_NAME``: The Sentieon software file (e.g. sentieon-genomics-201808.03.tar.gz)
* ``SENTIEON_LICENSE_NAME``: Name of the Sentieon license file (e.g mylicense.lic)

*Cohort information*

* ``NUM_SAMPLES``: Number of samples to run, e.g. the length of SAMPLE_FILE
* ``COHORT_PREFIX``: Prefix for your resulting VCF (e.g. cohort1)
* ``BUILD``: Build of the reference genome (GRCh38 | GRCh37)
* ``OME``: Choice between whole exome or whole genome (wgs | wes)

*Google Cloud Platform related configuration*

* ``CLOUDSPAN_MODE``: Which hail method to run (validation|qc)
* ``GCP_CREDS_FILE``: Absolute path to your Google Cloud service account json (e.g. /Users/Keys/service_creds.json)
* ``CLOUD_TRANSFER_OUTBUCKET``: The Google Cloud bucket to which VCF will be transferred (e.g. gs://pipeline-run)
* ``PROJECT_ID``: ID for your Google Cloud Project (e.g. GCP assigns names like "summer-water-78325")
* ``ZONE``: The zone in which you want your data to be stored on Google Cloud (e.g. us-east4-b)
* ``CLOUD_FILE``: The name of the file in USER_ASSETS which has the gcp-related template file

*Docker Information*

* ``DOCKER_ACCOUNT``: The name of your docker account (if not using the default: ucsfpsychcore)

Next Step 
Once run.yaml has been properly filled out, the pipeline is ready to be run. Please see :ref:`run` to continue.

.. _official yaml website: http://yaml.org
