FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:524.0.0-slim

RUN apt-get -qq update \
	&& apt-get -qq install \
		xxd \
		python3-dev

COPY scripts /opt/scripts

ENV PATH "${PATH}:/opt/scripts"
