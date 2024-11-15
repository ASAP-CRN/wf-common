FROM us-central1-docker.pkg.dev/dnastack-asap-parkinsons/workflow-images/util:1.1.1 as scripts

LABEL MAINTAINER="Karen Fang <karen@dnastack.com>"

ARG IMAGE_NAME
ENV IMAGE_NAME "${IMAGE_NAME}"
ARG IMAGE_TAG
ENV IMAGE_TAG "${IMAGE_TAG}"

ARG GCLOUD_CLI_VERSION
ENV GCLOUD_CLI_VERSION "${GCLOUD_CLI_VERSION}"

RUN apt-get -qq update \
	&& apt-get -qq install \
		wget \
		unzip \
		time \
		perl \
		default-jre \
	&& rm -rf /var/lib/apt/lists/*

# FastQC
ARG FASTQC_VERSION
ENV FASTQC_VERSION "${FASTQC_VERSION}"
RUN wget https://www.bioinformatics.babraham.ac.uk/projects/fastqc/fastqc_v${FASTQC_VERSION}.zip \
	&& unzip fastqc_v${FASTQC_VERSION}.zip -d /opt \
	&& rm fastqc_v${FASTQC_VERSION}.zip
RUN chmod +x /opt/FastQC/fastqc \
	&& ln -s /opt/FastQC/fastqc /usr/local/bin/
