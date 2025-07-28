FROM us-central1-docker.pkg.dev/dnastack-asap-parkinsons/workflow-images/util:1.2.0 as scripts

LABEL MAINTAINER="Karen Fang <karen@dnastack.com>"

ARG IMAGE_NAME
ENV IMAGE_NAME "${IMAGE_NAME}"
ARG IMAGE_TAG
ENV IMAGE_TAG "${IMAGE_TAG}"

ARG GCLOUD_CLI_VERSION
ENV GCLOUD_CLI_VERSION "${GCLOUD_CLI_VERSION}"

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get -qq update \
	&& apt-get -qq install \
		build-essential \
		time \
		curl \
		zlib1g-dev \
		libncursesw5-dev \
		libssl-dev \
		libsqlite3-dev \
		tk-dev \
		libgdbm-dev \
		libc6-dev \
		libbz2-dev \
		libffi-dev \
		liblzma-dev \
	&& rm -rf /var/lib/apt/lists/*

ARG MULTIQC_VERSION
ENV MULTIQC_VERSION "${MULTIQC_VERSION}"

ARG PYTHON3_VERSION
ENV PYTHON3_VERSION "${PYTHON3_VERSION}"
RUN curl -O https://www.python.org/ftp/python/${PYTHON3_VERSION}/Python-${PYTHON3_VERSION}.tar.xz && \
	tar -xvf Python-${PYTHON3_VERSION}.tar.xz --directory /opt/ && \
	rm Python-${PYTHON3_VERSION}.tar.xz
RUN cd /opt/Python-${PYTHON3_VERSION} && \
	./configure && \
	make && \
	make altinstall

RUN update-alternatives --install /usr/bin/python3 python3 /opt/Python-${PYTHON3_VERSION}/python 1 \
	&& update-alternatives --set python3 /opt/Python-${PYTHON3_VERSION}/python

COPY ./requirements.txt /opt/requirements.txt
RUN python3 -m pip install -r /opt/requirements.txt

COPY ./multiqc_config.yaml /root/.multiqc_config.yaml
