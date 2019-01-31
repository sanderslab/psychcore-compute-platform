#!/bin/bash

export PATH=$PATH:~/.local/bin/
now=`date +%Y-%m-%d-%H:%M:%S`

echo "INPUT BAM: $bam"
echo "BAM BUCKET: $bam_uri"
echo "OUTPUT FILE: $out"
echo "OUTPUT BUCKET: $out_uri"
echo "REF FILE: $ref"
echo "REF BUCKET: $ref_uri"
echo "LOG BUCKET: $log_uri"
echo "Current time: $now"

ref_path=/pipeline_bam_qc/localDir/$ref
bam_path=/pipeline_bam_qc/localDir/$bam
out_path=/pipeline_bam_qc/localDir/$out
log_path=/pipeline_bam_qc/localDir/${out}_${now}.log

if [ ! -f $bam_path ] ; then
    echo "Downloading $bam from $bam_uri to $bam_path"
    aws s3 cp $bam_uri$bam $bam_path
fi

if [ ! -f $ref_path ] ; then
    echo "Downloading $ref from $ref_uri to $ref_path"
    aws s3 cp $ref_uri$ref $ref_path
    aws s3 cp ${ref_uri}${ref}.fai ${ref_path}.fai
fi

java -Xmx256g -Djava.io.tmpdir=/pipeline_bam_qc/tmp/ \
    -jar /pipeline_bam_qc/picard/build/libs/picard.jar CollectWgsMetrics \
    R=$ref_path \
    I=$bam_path \
    USE_FAST_ALGORITHM=true \
    MAX_RECORDS_IN_RAM=40000000 \
    O=$out_path 2>&1 | tee $log_path

PIPESTAT=$PIPESTATUS

if [ $PIPESTAT -gt 0 ] ; then
    echo "CollectWGSMetrics failed for $bam"
    aws s3 cp $log_path $log_uri
    exit 1
fi

aws s3 cp $out_path $out_uri

if [  $? -gt 0 ] ; then
    echo "$out failed to upload to $out_uri"
    aws s3 cp $log_path $log_uri
    exit 1
fi

aws s3 cp $log_path $log_uri
