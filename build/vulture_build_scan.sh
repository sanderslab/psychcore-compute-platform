#!/usr/bin/env bash


# Names to ignore

VULTURE_IGNORES=(
    Or
    And
    Not
)

# Filename patterns/directories to exclude from scan

VULTURE_EXCLUDES=(
    docs/
    docker/
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

    if ! command -v vulture; then
        echo "Could not find vulture. Please install."
        exit 1
    fi

}

function quality_scan() {

    VULTURE_IGNORE_ARG=$(join_array_commas "${VULTURE_IGNORES[@]}")
    VULTURE_EXCLUDE_ARG=$(join_array_commas "${VULTURE_EXCLUDES[@]}")

    if ! vulture . --ignore-names "$VULTURE_IGNORE_ARG" --exclude "$VULTURE_EXCLUDE_ARG"; then
        echo "Vulture failed. Aborting commit."
        exit 4
    fi

}

check_dependencies
quality_scan
