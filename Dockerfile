FROM debian:10-slim
LABEL maintainer="jfloff@inesc-id.pt"

RUN set -ex ;\
    apt-get update ;\
    apt-get install -y --no-install-recommends \
            git \
            # python
            python3 \
            python3-dev \
            python3-pip \
            # aws
            awscli \
            # some command line utils
            vim \
            curl \
            ; \
    rm -rf /var/lib/apt/lists/*

RUN set -ex ;\
    ln -s /usr/bin/python3 /usr/bin/python ;\
    ln -s /usr/bin/pip3 /usr/bin/pip ;\
    pip install --upgrade --no-cache-dir \
            setuptools \
            ;\
    pip install --upgrade --no-cache-dir \
            pyyaml \
            aws-sam-cli \
            setuptools \
            pip \
            pandas \
            pymysql \
            pprint \
            boto3 \
            psutil \
            ;\
    # make sure nothing is on pip cache folder
    rm -rf ~/.cache/pip/

COPY credentials /root/.aws/credentials