#!/usr/bin/env bash

set -eo pipefail

source .buildkite/retry.sh

set +x

BUILD_FROM_BRANCH=$(buildkite-agent meta-data get BUILD_FROM_BRANCH)
PUSH_LATEST=$(buildkite-agent meta-data get PUSH_LATEST)

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <arch>"
    exit 1
fi

ARCH="$1"

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
./build-dev-docker.sh "$BUILD_FROM_BRANCH" "$ARCH" "$PUSH_LATEST"

popd
popd
rm -rf "$tmp_dir"

