FROM ubuntu:18.04

ENV ELASTIC_CONTAINER true
ENV JAVA_HOME /opt/jdk-${jdkVersion}

COPY --from=builder /opt/jdk-${jdkVersion} /opt/jdk-${jdkVersion}

RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev
    python3-pip \
  && rm -rf /var/lib/apt/lists/*

RUN pip3 install -y esrally

RUN groupadd -g 1000 rally && \
    adduser -u 1000 -g 1000 -d /rally rally

WORKDIR /rally

COPY --chown=1000:0 bin/docker-entrypoint.sh /docker-entrypoint.sh

# Openshift overrides USER and uses random ones with uid>1024 and gid=0
# Allow ENTRYPOINT (and Rally) to run even with a different user
RUN chgrp 0 /docker-entrypoint.sh && \
    chmod g=u /etc/passwd && \
    chmod 0775 /docker-entrypoint.sh

EXPOSE 9200 9300

LABEL org.label-schema.schema-version="1.0" \
  org.label-schema.vendor="Elastic" \
  org.label-schema.name="rally" \
  org.label-schema.version="${version}" \
  org.label-schema.url="https://esrally.readthedocs.io/en/stable/" \
  org.label-schema.vcs-url="https://github.com/elastic/rally" \
  license="${license}"

ENTRYPOINT ["/docker-entrypoint.sh"]
# Dummy overridable parameter parsed by entrypoint
CMD ["eswrapper"]
