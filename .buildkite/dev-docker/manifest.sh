#!/usr/bin/env bash

set -eo pipefail

source .buildkite/retry.sh

set +x

BUILD_FROM_BRANCH=$(buildkite-agent meta-data get BUILD_FROM_BRANCH)

# login to docker registry
DOCKER_PASSWORD=$(vault read -field token /secret/ci/elastic-rally/release/docker-hub-rally)
retry 5 docker login -u elasticmachine -p $DOCKER_PASSWORD
unset DOCKER_PASSWORD

set -x
export TERM=dumb
export LC_ALL=en_US.UTF-8
./build-dev-docker-manifest.sh "$BUILD_FROM_BRANCH"
