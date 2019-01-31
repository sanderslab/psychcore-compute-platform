#!/usr/bin/env bash

# Errors and warnings to ignore

FLAKE8_IGNORES=(
    E501
    E203
    F405
    E265
    E702
    E122
    E123
    E125
    E126
    E128
    E211
    E226
    E231
    E261
    E303
    E305
    E306
    E701
    E704
    E711
    E713
    E722
    F403
    W291
    W292
    W293
    W391
    W504
)

# Filename patterns/directories to exclude from scan

FLAKE8_EXCLUDES=(
    docker
)

function check_dependencies() {

    if ! command -v flake8; then
        echo "Could not find flake8. Please install."
        exit 1
    fi

}

function quality_scan() {

    FLAKE8_IGNORE_ARG=`printf '%s,' "${FLAKE8_IGNORES[@]}"`
    FLAKE8_EXCLUDE_ARG=`printf '%s,' "${FLAKE8_EXCLUDES[@]}"`

    if ! flake8 --ignore "$FLAKE8_IGNORE_ARG" --exclude "$FLAKE8_EXCLUDE_ARG"; then
        echo "Flake8 failed. Aborting commit."
        exit 4
    fi

}

check_dependencies
quality_scan
