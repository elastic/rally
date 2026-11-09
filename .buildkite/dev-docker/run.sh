#!/usr/bin/env bash

set -eo pipefail

# shellcheck source=.buildkite/retry.sh
source .buildkite/retry.sh

set +x

BUILD_FROM_BRANCH="$(buildkite-agent meta-data get BUILD_FROM_BRANCH --default "${BUILDKITE_BRANCH}")"
BUILD_FROM_BRANCH="${BUILD_FROM_BRANCH:-${BUILDKITE_BRANCH}}"
PUSH_LATEST="$(buildkite-agent meta-data get PUSH_LATEST --default true)"
DRY_RUN="$(buildkite-agent meta-data get DRY_RUN --default "${DRY_RUN:-false}")"
case "$DRY_RUN" in
    true|false) ;;
    *) echo "DRY_RUN must be true or false"; exit 1 ;;
esac
export DRY_RUN
# SCHEDULED_PUBLIC_REPO is set by the "Daily Public" schedule in catalog-info.yaml
PUBLIC_DOCKER_REPO="${SCHEDULED_PUBLIC_REPO:-$(buildkite-agent meta-data get PUBLIC_DOCKER_REPO --default false)}"

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 (prepare|build|manifest) ..."
    exit 1
fi

ACTION="$1"

# Resolve the source and tag once, before the architecture jobs start. The
# default branch uses Buildkite's pinned checkout, not the current remote HEAD.
if [[ "$ACTION" == "prepare" ]]; then
    if [[ "$BUILD_FROM_BRANCH" == "$BUILDKITE_BRANCH" ]]; then
        RALLY_BUILD_COMMIT=$(git rev-parse HEAD)
    else
        git fetch origin "$BUILD_FROM_BRANCH"
        RALLY_BUILD_COMMIT=$(git rev-parse FETCH_HEAD)
    fi
    branch_name="${BUILD_FROM_BRANCH//\//_}"
    RALLY_BUILD_VERSION="${branch_name}-${RALLY_BUILD_COMMIT:0:8}-$(date -u +%Y%m%d)"
    buildkite-agent meta-data set RALLY_BUILD_COMMIT "$RALLY_BUILD_COMMIT"
    buildkite-agent meta-data set RALLY_BUILD_VERSION "$RALLY_BUILD_VERSION"
    echo "Resolved Rally commit: $RALLY_BUILD_COMMIT"
    echo "Resolved Rally version: $RALLY_BUILD_VERSION"
    exit 0
fi

RALLY_BUILD_COMMIT=$(buildkite-agent meta-data get RALLY_BUILD_COMMIT)
RALLY_BUILD_VERSION=$(buildkite-agent meta-data get RALLY_BUILD_VERSION)
export RALLY_BUILD_COMMIT RALLY_BUILD_VERSION

if [[ "$DRY_RUN" == "true" ]]; then
    echo "DRY RUN: Docker commands and image tests will only be printed; registry login is skipped."
else
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
fi

build_docker_image() {
    # build-dev-docker.sh needs uv (for `uvx hatch version`), which is not installed on the agents
    if ! command -v uv >/dev/null 2>&1; then
        curl -LsSf https://astral.sh/uv/0.11.19/install.sh | env UV_UNMANAGED_INSTALL="${HOME}/.local/bin" sh
        export PATH="${HOME}/.local/bin:${PATH}"
    fi

    set -x
    export TERM=dumb
    export LC_ALL=en_US.UTF-8
    ./build-dev-docker.sh "$BUILD_FROM_BRANCH" "$ARCH" "$PUSH_LATEST" "$PUBLIC_DOCKER_REPO"
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
