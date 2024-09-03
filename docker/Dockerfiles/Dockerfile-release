FROM docker.elastic.co/wolfi/python:3.11.7-dev

ARG RALLY_VERSION
ARG RALLY_LICENSE

ENV RALLY_RUNNING_IN_DOCKER=True

USER root

RUN apk update 
RUN apk add curl git gcc pigz bash

# pbzip2 doesn't have a package for wolfi, so we build it from source
RUN apk add bzip2-dev make wget glibc-dev zlib-dev openssl-dev libffi-dev \
    bzip2-dev gdbm-dev ncurses-dev nss \
    readline-dev sqlite-dev openssl-dev llvm16


RUN cd /tmp && \
  wget -q https://launchpad.net/pbzip2/1.1/1.1.13/+download/pbzip2-1.1.13.tar.gz && \
  tar -xzf pbzip2-1.1.13.tar.gz && \
  cd pbzip2-1.1.13/ && \
  make install && \
  rm -r /tmp/pbzip2-1.1.13/


RUN addgroup --gid 1001 rally && \
    adduser --system --home /rally --ingroup rally --no-create-home --uid 1001 --shell /bin/bash rally

RUN mkdir /rally/

WORKDIR /rally
COPY --chown=1001:0 docker/bin/entrypoint.sh /entrypoint.sh

# Openshift overrides USER and uses random ones with uid>1024 and gid=0
# Allow ENTRYPOINT (and Rally) to run even with a different user
RUN chgrp 0 /entrypoint.sh && \
    chmod g=u /etc/passwd && \
    chmod 0775 /entrypoint.sh

RUN mkdir -p /rally/.rally && \
    chown -R 1001:0 /rally/

USER 1001

RUN python3 -m venv /rally/venv
ENV PATH=/rally/venv/bin:$PATH

RUN python3 -m pip install --upgrade hatch hatchling pip wheel
RUN python3 -m pip install esrally==$RALLY_VERSION


LABEL org.label-schema.schema-version="1.0" \
  org.label-schema.vendor="Elastic" \
  org.label-schema.name="rally" \
  org.label-schema.version="${RALLY_VERSION}" \
  org.label-schema.url="https://esrally.readthedocs.io/en/stable/" \
  org.label-schema.vcs-url="https://github.com/elastic/rally" \
  license="${RALLY_LICENSE}"

VOLUME ["/rally/.rally"]

ENTRYPOINT ["/entrypoint.sh"]
