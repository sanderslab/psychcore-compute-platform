#!/bin/bash

#function program_setup_run {
# Add AWS CLI to path
export PATH=$PATH:~/.local/bin/
cd /home/localDir/

t=$(date "+%F-%T")
log_file="/home/localDir/${1}_${prefix}_${t}.log"
touch $log_file

echo "This run's time was set from within the container at: ${t}." 2>&1 | tee -a $log_file

python /home/$1.py  2>&1 | tee -a $log_file
PIPESTAT=$PIPESTATUS

# Check pipe status after upload so error-y log is still uploaded
if [ $PIPESTAT -gt 0 ] ; then
    echo "$1 failed!  The log can be found at ${log_uri}." 2>&1 | tee -a $log_file
    aws s3 cp $log_file $log_uri
    exit 1
fi

# Add git commit id to the end of the log
if [ -z $COMMIT ] ; then
    echo "This run was done using commit ID ${commit}." 2>&1 | tee -a $log_file
else
    echo "No commit ID was associated with this run." 2>&1 | tee -a $log_file
fi

aws s3 cp $log_file $log_uri
UPLOADSTAT=$?
if [  $UPLOADSTAT -gt 0 ] ; then
    echo "LOG UPLOAD FAILED"
    exit 1
fi
#}