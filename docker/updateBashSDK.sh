#!/bin/bash


WORKING_DIR=${1?Please give the directory containing the container directories as the first argument}

cd $WORKING_DIR
for DIR in `ls` ; do
    if [ -d $DIR ] ; then
        echo $DIR
        rm  $DIR/SDK.sh
        cp "${WORKING_DIR}/SDK.sh" $DIR
    fi
done