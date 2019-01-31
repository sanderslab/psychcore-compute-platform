#!/bin/bash
# Add AWS CLI to path
export PATH=$PATH:/bwa_mem/bwa-0.7.15/
source ./SDK.sh 
program_setup_run bwa_mem bwa_mem
