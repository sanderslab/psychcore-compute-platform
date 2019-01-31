#!/usr/bin/env bash

SECRETS_FILE=${1?Please provide the secrets filename as first argument}

function check_secrets_file() {
    if [ ! -e $SECRETS_FILE ]; then
        echo "Could not find secrets file named: $SECRETS_FILE."
        echo "Exiting."
        exit 1
    fi
}

function add_secrets() {
    while read SECRET; do
        git secrets --add --literal "$SECRET"
    done < $SECRETS_FILE
}

check_secrets_file
add_secrets
