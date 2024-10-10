#!/usr/bin/env bash

set -eo pipefail

source .buildkite/retry.sh

set +x

RELEASE_VERSION=$(buildkite-agent meta-data get RELEASE_VERSION)
PUSH_LATEST=$(buildkite-agent meta-data get PUSH_LATEST)

# login to docker registry
DOCKER_PASSWORD=$(vault read -field token /secret/ci/elastic-rally/release/docker-hub-rally)
retry 5 docker login -u elasticmachine -p $DOCKER_PASSWORD
unset DOCKER_PASSWORD

set -x
export TERM=dumb
export LC_ALL=en_US.UTF-8
./release-docker-manifest.sh "$RELEASE_VERSION" "$PUSH_LATEST"
