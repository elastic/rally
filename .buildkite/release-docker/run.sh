#!/usr/bin/env bash

set -eo pipefail

source .buildkite/retry.sh

set +x
RELEASE_VERSION=$(buildkite-agent meta-data get RELEASE_VERSION)
PUSH_LATEST=$(buildkite-agent meta-data get PUSH_LATEST)

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 (amd64|arm64)"
    exit 1
fi

ARCH="$1"

# login to docker registry
DOCKER_PASSWORD=$(vault read -field token /secret/ci/elastic-rally/release/docker-hub-rally)
retry 5 docker login -u elasticmachine -p $DOCKER_PASSWORD
unset DOCKER_PASSWORD

set -x
tmp_dir=$(mktemp --directory)
pushd "$tmp_dir"
git clone https://github.com/elastic/rally
pushd rally

printenv

# checkout the latest version, to make sure we get the latest docker security fixes
if [[ ! -z "${BUILDKITE_BRANCH}" ]]; then
    git checkout "${BUILDKITE_BRANCH}"
else
    git checkout "${RELEASE_VERSION}"
fi

git --no-pager show

export TERM=dumb
export LC_ALL=en_US.UTF-8
./release-docker.sh "$RELEASE_VERSION" "$ARCH" "$PUSH_LATEST"

popd
popd
rm -rf "$tmp_dir"
