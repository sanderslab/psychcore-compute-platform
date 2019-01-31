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
    && pip install boto3 \
    && pip install fasteners

RUN sudo apt-get install -y wget \
    && sudo apt-get install zlib1g-dev \
    && sudo apt-get -y install libbz2-dev \
    && wget https://github.com/samtools/htslib/releases/download/1.5/htslib-1.5.tar.bz2 \
    && tar -xvjf htslib-1.5.tar.bz2 \
    && rm htslib-1.5.tar.bz2 \
    && cd /home/htslib-1.5 \
    && ./configure --prefix /usr/local/ --disable-lzma \
    && make \
    && make install \
    && cd /home \
    && wget https://github.com/lh3/bwa/releases/download/v0.7.15/bwa-0.7.15.tar.bz2 \
    && tar -xvjf bwa-0.7.15.tar.bz2 \
    && rm bwa-0.7.15.tar.bz2 \
    && cd bwa-0.7.15/ \
    && make

ENV PATH="${PATH}:/home/bwa-0.7.15/:~/.local/bin/"

ADD . /home

RUN chmod a+x /home/SDK.sh
