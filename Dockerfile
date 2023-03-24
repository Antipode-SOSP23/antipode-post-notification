FROM debian:10-slim
LABEL maintainer="jfloff@inesc-id.pt"

RUN set -ex ;\
    apt-get update ;\
    apt-get install -y --no-install-recommends \
        build-essential \
        git \
        groff \
        activemq \
        # python
        python3 \
        python3-dev \
        python3-pip \
        # some command line utils
        tree \
        vim \
        curl \
        wget \
        unzip \
        ; \
    rm -rf /var/lib/apt/lists/*

#--------------
# AWS & SAM Cli
#--------------
ENV SAM_CLI_TELEMETRY=0
RUN set -ex ;\
    cd /tmp ;\
    wget https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip ;\
    unzip awscli-exe-linux-x86_64.zip ;\
    ./aws/install ;\
    aws --version ;\
    wget https://github.com/aws/aws-sam-cli/releases/latest/download/aws-sam-cli-linux-x86_64.zip ;\
    unzip aws-sam-cli-linux-x86_64.zip -d aws-sam ;\
    ./aws-sam/install ;\
    sam --version ;\
    rm -rf /tmp/*

#--------------
# amazonmq-cli
#--------------
ENV AMAZONMQ_CLI_VERSION='0.2.2'
RUN set -ex ;\
    mkdir -p /tools ;\
    cd /tools ;\
    wget https://github.com/antonwierenga/amazonmq-cli/releases/download/v${AMAZONMQ_CLI_VERSION}/amazonmq-cli-${AMAZONMQ_CLI_VERSION}.zip ;\
    unzip amazonmq-cli-${AMAZONMQ_CLI_VERSION}.zip ;\
    rm -rf amazonmq-cli-${AMAZONMQ_CLI_VERSION}.zip ;\
    mv amazonmq-cli-${AMAZONMQ_CLI_VERSION} amazonmq-cli ;\
    ln -s /tools/amazonmq-cli/bin/amazonmq-cli /usr/bin/amazonmq-cli

#--------------
# Python
#--------------
ENV PATH="/root/.local/bin:${PATH}"
RUN set -ex ;\
    ln -s /usr/bin/python3 /usr/bin/python ;\
    ln -s /usr/bin/pip3 /usr/bin/pip ;\
    pip install --user --upgrade --no-cache-dir \
        setuptools \
        pip \
        ;\
    pip install --user --upgrade --no-cache-dir \
        pyyaml \
        pandas \
        pymysql \
        boto3 \
        plumbum \
        jinja2 \
        tqdm \
        click \
        matplotlib \
        seaborn \
        grpcio \
        grpcio-tools \
        ;\
    # make sure nothing is on pip cache folder
    rm -rf ~/.cache/pip/

COPY credentials /root/.aws/credentials
COPY config /root/.aws/config
WORKDIR /app