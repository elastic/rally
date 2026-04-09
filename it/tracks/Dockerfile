# Fork of docker/Dockerfiles/dev/Dockerfile for it/tracks: bundles elastic/rally-tracks under /tracks (read-only for rally user).
# Publicly pullable base (CI and forks); docker.elastic.co wolfi mirrors require registry auth on anonymous runners.
FROM cgr.dev/chainguard/wolfi-base:latest AS builder
ARG RALLY_VERSION
ARG RALLY_LICENSE

ENV RALLY_RUNNING_IN_DOCKER=True

USER root

RUN apk update
RUN for i in 1 2 3; do \
      apk add --no-cache bash bzip2 bzip2-dev curl git gcc gzip jq make openssh pigz procps tini wget zstd && break || sleep 5; \
    done

# pbzip2 doesn't have a package for wolfi, so we build it from source
RUN for i in 1 2 3; do \
      cd /tmp && \
      rm -f pbzip2-1.1.13.tar.gz && \
      wget -q https://launchpad.net/pbzip2/1.1/1.1.13/+download/pbzip2-1.1.13.tar.gz && \
      tar -xzf pbzip2-1.1.13.tar.gz && \
      cd pbzip2-1.1.13/ && \
      make install && \
      rm -r /tmp/pbzip2-1.1.13/ && \
      break || sleep 5; \
    done

RUN addgroup --gid 1000 rally && \
    adduser --system --home /rally --ingroup rally --no-create-home --uid 1000 --shell /bin/bash rally

WORKDIR /rally

# Openshift overrides USER and uses random ones with uid>1024 and gid=0
RUN mkdir -p /rally/.rally && \
    chown -R 1000:0 /rally/ && \
    chmod g=u /etc/passwd

COPY --from=ghcr.io/astral-sh/uv:0.8.22 /uv /uvx /bin/

USER 1000

COPY Makefile /rally/
COPY README.md /rally/
COPY .ci/variables.json /rally/.ci/
COPY esrally/ /rally/esrally/
COPY pyproject.toml /rally/
COPY uv.lock /rally/

ENV VIRTUAL_ENV=/rally/venv
ENV PATH=/rally/venv/bin:$PATH
ENV UV_PROJECT_ENVIRONMENT=/rally/venv

# Wipe away any lingering caches, copied over from the local machine
RUN find /rally -name "__pycache__" -exec rm -rf -- \{\} \; 2>/dev/null || true
RUN find /rally -name ".pyc" -exec rm -rf -- \{\} \; 2>/dev/null || true
RUN make install

LABEL org.label-schema.schema-version="1.0" \
  org.label-schema.vendor="Elastic" \
  org.label-schema.name="rally" \
  org.label-schema.version="${RALLY_VERSION}" \
  org.label-schema.url="https://esrally.readthedocs.io/en/stable/" \
  org.label-schema.vcs-url="https://github.com/elastic/rally" \
  license="${RALLY_LICENSE}"

USER root
ARG RALLY_TRACKS_REF=master
RUN git clone --depth 1 --branch "${RALLY_TRACKS_REF}" https://github.com/elastic/rally-tracks.git /tracks
RUN chmod -R a-w /tracks

USER 1000

VOLUME ["/rally/.rally"]

# IT image always invokes esrally; compose passes e.g. race, list, … (no shell passthrough like docker/bin/entrypoint.sh).
ENTRYPOINT ["/sbin/tini", "--", "/rally/venv/bin/esrally"]
