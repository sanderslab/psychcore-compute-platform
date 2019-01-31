#!/usr/bin/env bash

# Errors and warnings to ignore

BANDIT_IGNORES=(
    B404
    B603
    B108
)

BANDIT_EXCLUDE_PATHS=(
    docker/triodenovo
)

function join_by() {
    local IFS="$1"
    shift
    echo "$*"
}

function join_array_commas() {
    ARRAY_TO_JOIN=("$@")
    JOINED_BY_SPACES=`printf '%s ' "${ARRAY_TO_JOIN[@]}"`
    ARRAY_JOINED=$(join_by , $JOINED_BY_SPACES)
    echo "$ARRAY_JOINED"
}

function check_dependencies() {

    if ! command -v bandit; then
        echo "Could not find bandit. Please install."
        exit 1
    fi

}

function quality_scan() {

    BANDIT_IGNORE_ARG=$(join_array_commas "${BANDIT_IGNORES[@]}")
    BANDIT_EXCLUDE_ARG=$(join_array_commas "${BANDIT_EXCLUDE_PATHS[@]}")

    if ! bandit --recursive --skip "$BANDIT_IGNORE_ARG" --exclude "$BANDIT_EXCLUDE_ARG" .; then
        echo "Bandit failed. Aborting commit."
        exit 2
    fi

}

check_dependencies
quality_scan
