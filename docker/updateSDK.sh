#!/bin/bash

WORKING_DIR=${1?Please give the directory containing the container directories as the first argument}

cd $WORKING_DIR
for DIR in `ls` ; do
    if [ -d $DIR ] ; then
        echo $DIR
        rm  -f $DIR/SDK.py
        cp "${WORKING_DIR}/SDK.py" $DIR
    fi
done