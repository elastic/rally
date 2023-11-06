#!/usr/bin/env bash

set -eo pipefail

source .buildkite/retry.sh

set +x
RELEASE_VERSION=$(buildkite-agent meta-data get RELEASE_VERSION)
# login to docker registry
DOCKER_PASSWORD=$(vault read -field token /secret/ci/elastic-rally/release/docker-hub-rally)
retry 5 docker login -u elasticmachine -p $DOCKER_PASSWORD
unset DOCKER_PASSWORD

tmp_dir=$(mktemp --directory)
pushd "$tmp_dir"
git clone https://github.com/elastic/rally
pushd rally
git checkout "${RELEASE_VERSION}"
git --no-pager show

set -x
export TERM=dumb
export LC_ALL=en_US.UTF-8
./release-docker.sh "$RELEASE_VERSION"

popd
popd
rm -rf "$tmp_dir"
