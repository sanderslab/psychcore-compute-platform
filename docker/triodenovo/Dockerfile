# Use ubuntu:latest as parent image
FROM ubuntu:16.04

# Set the working directory in container
WORKDIR /home

# Install python, pip, and aws cli
# Need to install sudo first to remove prompts of [Y/n]
RUN apt-get update && apt-get install -y sudo && rm -rf /var/lib/apt/lists/* \
    && sudo apt-get -y update \
    && sudo apt-get -y install \
    	build-essential \
        git \
    	groff \
    	libbz2-dev \
		zlib1g-dev \
    	python-dev \
    	python-pip \
    	wget \
    && sudo pip install --upgrade pip \
    && sudo pip install --upgrade virtualenv \
    && sudo pip install awscli --upgrade --user \
    && pip install boto3 

RUN wget https://github.com/samtools/htslib/releases/download/1.5/htslib-1.5.tar.bz2 \
	&& tar -xvjf htslib-1.5.tar.bz2 \
	&& rm htslib-1.5.tar.bz2 \
	&& cd htslib-1.5 \
	&& ./configure --prefix /usr/local/ --disable-lzma \
	&& make \
	&& make install

ADD . /home

RUN chmod a+x /home/SDK.sh

ENV PATH=$PATH:/home/triodenovo-fix/bin/:/home/