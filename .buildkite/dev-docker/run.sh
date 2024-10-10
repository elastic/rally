#!/usr/bin/env bash

set -eo pipefail

source .buildkite/retry.sh

set +x

BUILD_FROM_BRANCH=$(buildkite-agent meta-data get BUILD_FROM_BRANCH)
if [[ -z "${BUILD_FROM_BRANCH}" ]]; then
    BUILD_FROM_BRANCH=${BUILDKITE_BRANCH}
fi
PUSH_LATEST=$(buildkite-agent meta-data get PUSH_LATEST)
PUBLIC_DOCKER_REPO=$(buildkite-agent meta-data get PUBLIC_DOCKER_REPO)

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <arch>"
    exit 1
fi

ARCH="$1"

# login to docker registry
if [[ $PUBLIC_DOCKER_REPO == "true" ]]; then
    VAULT_PATH="secret/ci/elastic-rally/release/docker-hub-rally"
    DOCKER_REGISTRY="docker.io"
    DOCKER_PASSWORD=$(vault read -field token "${VAULT_PATH}")
else
    VAULT_PATH="/secret/ci/elastic-elasticsearch-benchmarks/employees/cloud/docker-registry-api-credentials"
    DOCKER_REGISTRY="docker.elastic.co"
    DOCKER_PASSWORD=$(vault read -field password "${VAULT_PATH}")
fi

DOCKER_USERNAME=$(vault read -field username "${VAULT_PATH}")
retry 5 docker login -u ${DOCKER_USERNAME} -p ${DOCKER_PASSWORD} ${DOCKER_USERNAME}
unset DOCKER_PASSWORD
unset DOCKER_USERNAME

tmp_dir=$(mktemp --directory)
pushd "$tmp_dir"
git clone https://github.com/elastic/rally
pushd rally
# checkout the version from the buildkite branch, but build it from the branch we specified
if [[ ! -z "${BUILDKITE_BRANCH}" ]]; then
    git checkout "${BUILDKITE_BRANCH}"
else
    git checkout "${BUILD_FROM_BRANCH}"
fi
git --no-pager show

set -x
export TERM=dumb
export LC_ALL=en_US.UTF-8
./build-dev-docker.sh "$BUILD_FROM_BRANCH" "$ARCH" "$PUSH_LATEST" "$PUBLIC_DOCKER_REPO"

popd
popd
rm -rf "$tmp_dir"

