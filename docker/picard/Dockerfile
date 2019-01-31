# Use ubuntu:latest as parent image
FROM ubuntu:16.04

# Set the working directory in container
WORKDIR /home

# Install python, pip, and aws cli
# Need to install sudo first to remove prompts of [Y/n]
RUN apt-get update && apt-get install -y sudo && rm -rf /var/lib/apt/lists/* \
    && sudo apt-get -y update \
    && sudo apt-get -y install python-pip python-dev build-essential \
    && sudo apt-get install -y groff \
    && sudo pip install --upgrade pip \
    && sudo pip install --upgrade virtualenv \
    && sudo pip install awscli --upgrade --user \
    && sudo apt-get install git -y \
    && pip install boto3 \
    && pip install fasteners

RUN sudo apt-get install default-jre -y \
    && sudo apt-get install default-jdk -y \ 
    && git clone https://github.com/broadinstitute/picard.git \ 
    && cd picard/ \
    && ./gradlew shadowJar

ADD . /home

RUN chmod a+x /home/SDK.sh


