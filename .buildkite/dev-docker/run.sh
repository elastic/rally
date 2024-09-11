#!/usr/bin/env bash

set -eo pipefail

source .buildkite/retry.sh

set +x

buildkite-agent meta-data keys

BUILD_FROM_BRANCH=$(buildkite-agent meta-data get BUILD_FROM_BRANCH)

# login to docker registry
DOCKER_PASSWORD=$(vault read -field token /secret/ci/elastic-rally/release/docker-hub-rally)
retry 5 docker login -u elasticmachine -p $DOCKER_PASSWORD
unset DOCKER_PASSWORD

tmp_dir=$(mktemp --directory)
pushd "$tmp_dir"
git clone https://github.com/elastic/rally
pushd rally
git checkout "${BUILD_FROM_BRANCH}"
git --no-pager show

set -x
export TERM=dumb
export LC_ALL=en_US.UTF-8
./build-dev-docker.sh "$BUILD_FROM_BRANCH"

popd
popd
rm -rf "$tmp_dir"

