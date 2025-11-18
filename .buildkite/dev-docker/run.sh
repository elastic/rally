#!/usr/bin/env bash

set -eo pipefail

# shellcheck source=.buildkite/retry.sh
source .buildkite/retry.sh

set +x

BUILD_FROM_BRANCH="$(buildkite-agent meta-data get BUILD_FROM_BRANCH --default "${BUILDKITE_BRANCH}")"
PUSH_LATEST="$(buildkite-agent meta-data get PUSH_LATEST --default true)"
# SCHEDULED_PUBLIC_REPO is set by the "Daily Public" schedule in catalog-info.yaml
PUBLIC_DOCKER_REPO="${SCHEDULED_PUBLIC_REPO:-$(buildkite-agent meta-data get PUBLIC_DOCKER_REPO --default false)}"

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 (build|manifest) ..."
    exit 1
fi

ACTION="$1"

# login to docker registry
if [[ $PUBLIC_DOCKER_REPO == "true" ]]; then
    VAULT_PATH="secret/ci/elastic-rally/release/docker-hub-rally"
    DOCKER_REGISTRY="docker.io"
    PASSWORD_FIELD="token"
else
    VAULT_PATH="kv/ci-shared/elasticsearch-benchmarks/cloud/docker-registry-api-credentials"
    DOCKER_REGISTRY="docker.elastic.co"
    PASSWORD_FIELD="password"
fi

DOCKER_USERNAME=$(retry 5 vault kv get -field username "${VAULT_PATH}")
DOCKER_PASSWORD=$(retry 5 vault kv get -field "${PASSWORD_FIELD}" "${VAULT_PATH}")
retry 5 docker login -u "${DOCKER_USERNAME}" -p "${DOCKER_PASSWORD}" "${DOCKER_REGISTRY}"
unset DOCKER_PASSWORD
unset DOCKER_USERNAME

build_docker_image() {
    tmp_dir=$(mktemp --directory)
    pushd "$tmp_dir"
    git clone https://github.com/elastic/rally
    pushd rally
    # checkout the version from the buildkite branch, but build it from the branch we specified
    local checkout_branch
    if [[ -n "${BUILDKITE_BRANCH}" ]]; then
      checkout_branch="${BUILDKITE_BRANCH}"
    else
      checkout_branch="${BUILD_FROM_BRANCH}"
    fi

    # If the branch is a pull request, it will be in the format "refs/pull/123/head".
    # Strip off the "refs/" prefix to get the branch name, and check it out to a temporary branch.
    if [[ "${checkout_branch}" =~ refs\/.* ]]; then
      local branch_name
      branch_name=$(echo "${checkout_branch}" | sed 's/refs\///')
      git fetch origin "${branch_name}:_temp"
      git checkout "_temp"
    else
      git checkout "${checkout_branch}"
    fi
    echo "Docker commit: $(git --no-pager log --oneline -n1)"

    set -x
    export TERM=dumb
    export LC_ALL=en_US.UTF-8
    ./build-dev-docker.sh "$BUILD_FROM_BRANCH" "$ARCH" "$PUSH_LATEST" "$PUBLIC_DOCKER_REPO"

    popd
    popd
    rm -rf "$tmp_dir"
}

build_docker_manifest() {
    set -x
    export TERM=dumb
    export LC_ALL=en_US.UTF-8
    ./build-dev-docker-manifest.sh "$BUILD_FROM_BRANCH" "$PUSH_LATEST" "$PUBLIC_DOCKER_REPO"
}

case "$ACTION" in
    "build")
        if [[ $# -lt 2 ]]; then
            echo "Usage: $0 build [amd64|arm64]"
            exit 1
        fi
        ARCH="$2"
        build_docker_image
        ;;
    "manifest")
        build_docker_manifest
        ;;
    *)
        echo "Unknown action: $ACTION"
        exit 1
        ;;
esac
