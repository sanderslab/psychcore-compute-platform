.. _sec_gettings-started:

===============
Getting Started
===============

You will need:

* An AWS account
* A Sentieon License
* Conda

Creating and Setting up your Amazon Web Services (AWS) Account
--------------------------------------------------------------

If you do not have one already, please create an Amazon Web Services (AWS_) 
account; the pipeline's infrastructure is made up of several AWS Services
(see :ref:`infrastructure`).

Depending on the current status of your AWS account and the number of samples
on which you plan to call variants, you may need to increase the number of 
instance types to support the respective sample scale. You can do so by visiting
the Limits_ page under the `EC2 dashboard`_ in the AWS console. 
Note that this may take some time to process, so it should be done early.

By default, the pipeline makes use of the following instance types:

* c5.9xlarge, c5.18xlarge, r4.2xlarge, r4.4xlarge.

The pricing specification for each of the AWS EC2 instance types can be found 
on the `AWS Instance Pricing page`_.

.. _refs:


Download and Upload Reference Files to S3
-----------------------------------------

The pipeline performs many operations which require several reference files.
(Eg. the human reference genome fasta and its indices). These must be uploaded
to AWS S3 before the pipeline can be run.  The standard reference files are
provided by the Broad Institute's `GATK Resource Bundle`_.  
Currently, the pipeline supports two builds of the human reference 
genome - GRCh37_ (hg19) and GRCh38_ (hg38).  GRCh37 files are located on
the Broad Institute's ftp site, while GRCH38 is hosted on Google Cloud Storage.

In order to upload the reference files to AWS S3, you will need to install
the AWS Command Line Interface - please see `AWS CLI Installation`_.
For uploading files onto S3, please see the `AWS S3 documentation`_.

Obtain a Sentieon License File
---------------------------------

Currently, the pipeline utilizes only Sentieon_ in its haplotyping and joint
genotyping steps.  Thus, in order to use the pipeline, you must first contact
Sentieon and obtain a license.  They also offer a free trial_.


Install Conda and your Dev Environment
--------------------------------------

In order to run the pipeline, you will need to install Conda_.

* If you have python 2.7 installed currently, pick that installer.
* If you have python 3.6 installed currently, pick that.
* Run the installer. The defaults should be fine.

Then, create a python 3.6 environment:
::

	$ conda create -n psy-ngs python=3.6

Activate the newly created environment:
(you may need to start a new terminal session)
::

	$ source activate psy-ngs

You can verify that the environment has activated by checking the python version
(if it is different than your base):
::

	$ python --version

You should also see the environment name prepended to your shell prompt, e.g.:
::

	(psy-ngs) $ echo "See the environment name?"


After activating the environment install the pipeline's python dependencies:
::

	(psy-ngs) $ cd path/to/this/repo/
	(psy-ngs) $ pip install -r requirements.txt

Next Steps
----------

You are almost ready to run the pipeline - next you will need to configure it
with your run specifications.  Please see :ref:`runyaml` to continue.


.. _AWS: https://aws.amazon.com
.. _Limits: https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#Limits:
.. _EC2 dashboard: https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#Home:
.. _AWS Instance Pricing page: https://aws.amazon.com/ec2/pricing/on-demand/
.. _Sentieon: https://www.sentieon.com
.. _Trial: https://www.sentieon.com/home/free-trial/
.. _Conda: https://conda.io/miniconda.html
.. _GATK Resource Bundle: https://software.broadinstitute.org/gatk/download/bundle
.. _GRCh37: ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/
.. _GRCh38: https://console.cloud.google.com/storage/browser/genomics-public-data/resources/broad/hg38/v0
.. _AWS CLI Installation: https://docs.aws.amazon.com/cli/latest/userguide/installing.html
.. _AWS S3 documentation: https://docs.aws.amazon.com/cli/latest/reference/s3/cp.html