#!/bin/bash

DOCKER_HUB_USER=${1?Please give DockerHub username as first argument}

docker build -t "${DOCKER_HUB_USER}"/bwa_mem:dev docker/bwa_mem
docker build -t "${DOCKER_HUB_USER}"/picard:dev docker/picard
docker build -t "${DOCKER_HUB_USER}"/sentieon:dev docker/sentieon
docker build -t "${DOCKER_HUB_USER}"/gatk:dev docker/gatk
docker build -t "${DOCKER_HUB_USER}"/dproc:dev docker/dproc
docker build -t "${DOCKER_HUB_USER}"/submitter:dev docker/submitter
docker build -t "${DOCKER_HUB_USER}"/triodenovo:dev docker/triodenovo

docker push "${DOCKER_HUB_USER}"/bwa_mem:dev
docker push "${DOCKER_HUB_USER}"/picard:dev
docker push "${DOCKER_HUB_USER}"/sentieon:dev
docker push "${DOCKER_HUB_USER}"/gatk:dev
docker push "${DOCKER_HUB_USER}"/dproc:dev
docker push "${DOCKER_HUB_USER}"/submitter:dev
docker push "${DOCKER_HUB_USER}"/triodenovo:dev
