#!/usr/bin/env bash

#
# Pull secrets from the pipeline Name Space in AWS Secrets Manager, write cleartext to file for git secrets --scan<-history>
#

SECRETS_FILE=${1?Please provide the secrets filename as first argument}
NAMESPACE=${2?Please provide the namespace from which to retrieve secrets}

function verify_dependencies() {

    if ! command -v jq; then
        echo "Looks like jq is not installed."
        echo "Please install: https://stedolan.github.io/jq/download/"
        exit 1
    fi

    if ! command -v aws; then
        echo "Looks like the AWS CLI is not installed."
        echo "Please install: https://docs.aws.amazon.com/cli/latest/userguide/install-windows.html"
        exit 1
    fi

}

function ensure_empty_secrets_file() {

    # (Over)Write $SECRETS_FILE with no content

    >| $SECRETS_FILE

    if [ -f $SECRETS_FILE ] && [ ! -s $SECRETS_FILE ]; then
        # Exists and is empty
        :
    else
        echo "Secrets file exists and is non-empty."
        echo "Must exit!"
        exit 2
    fi
}

function get_secrets_and_write_to_file_jmespath() {

    aws secretsmanager list-secrets \
        --query 'SecretList[?starts_with(Name, `'"${NAMESPACE}"'`) == `true`].Name' \
        --output text \
        | awk 'BEGIN { OFS = "\n" } { $1=$1; print }' \
        | while read SECRET_NAME; do
            SECRET_STRING=$(aws secretsmanager get-secret-value --secret-id "$SECRET_NAME" | jq -r '.SecretString')
            # check no error
            echo "$SECRET_STRING" >> $SECRETS_FILE
        done

}

function get_secrets_and_write_to_file_jq() {

    # This (test) failed on jq 1.3. Docs indicated it should work. TODO: Get link from history

    #
    # Loop over secrets in the account, filter for "pipeline" namespace, and collect secret names.
    # For each secret name, fetch the secret value, and append that value to the secrets file.
    #

    aws secretsmanager list-secrets \
        | jq -r '.SecretList[] | select(.Name | test("'"${NAMESPACE}"'")) | .Name' \
        | while read SECRET_NAME; do
            SECRET_STRING=$(aws secretsmanager get-secret-value --secret-id "$SECRET_NAME" | jq -r '.SecretString')
            echo "$SECRET_STRING" >> $SECRETS_FILE
        done

}

verify_dependencies
ensure_empty_secrets_file
get_secrets_and_write_to_file_jmespath
