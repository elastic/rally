#!/usr/bin/env bash

set -eo pipefail

source .buildkite/retry.sh

set +x
RELEASE_VERSION=$(buildkite-agent meta-data get RELEASE_VERSION)
PUSH_LATEST=$(buildkite-agent meta-data get PUSH_LATEST)

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 (build|manifest)"
    exit 1
fi

ACTION="$1"

# login to docker registry
DOCKER_PASSWORD=$(retry 5 vault kv get -field token /secret/ci/elastic-rally/release/docker-hub-rally)
retry 5 docker login -u elasticmachine -p "${DOCKER_PASSWORD}"
unset DOCKER_PASSWORD

build_docker_image() {
    tmp_dir=$(mktemp --directory)
    pushd "$tmp_dir"
    git clone https://github.com/elastic/rally
    pushd rally

    # checkout the latest version, to make sure we get the latest docker security fixes
    if [[ -n "${BUILDKITE_BRANCH}" ]]; then
        # If the branch is a pull request, it will be in the format "refs/pull/123/head".
        # Strip off the "refs/" prefix to get the branch name, and check it out to a temporary branch.
        # This is not necessary for a release version, so we only need it in this branch.
        if [[ "${BUILDKITE_BRANCH}" =~ refs\/.* ]]; then
          local branch_name
          branch_name=$(echo "${BUILDKITE_BRANCH}" | sed 's/refs\///')
          git fetch origin "${branch_name}:_temp"
          git checkout "_temp"
        else
          git checkout "${BUILDKITE_BRANCH}"
        fi
    else
        git checkout "${RELEASE_VERSION}"
    fi

    echo "Docker commit: $(git --no-pager log --oneline -n1)"

    set -x
    export TERM=dumb
    export LC_ALL=en_US.UTF-8
    ./release-docker.sh "$RELEASE_VERSION" "$ARCH" "$PUSH_LATEST"

    popd
    popd
    rm -rf "$tmp_dir"
}

build_docker_manifest() {
    set -x
    export TERM=dumb
    export LC_ALL=en_US.UTF-8
    ./release-docker-manifest.sh "$RELEASE_VERSION" "$PUSH_LATEST"
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
