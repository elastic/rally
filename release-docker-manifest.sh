#!/usr/bin/env bash

# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Prerequisites for releasing:

# Logged in on Docker Hub (docker login)

# fail this script immediately if any command fails with a non-zero exit code
set -eu

function push_failed {
    echo "Error while pushing Docker image. Did you \`docker login\`?"
}

if [[ $# -eq 0 ]] ; then
    echo "ERROR: $0 requires the Rally version to push as a command line argument and you didn't supply it."
    echo "For example: $0 master true"
    exit 1
fi
export RALLY_VERSION=$1
export PUSH_LATEST=$2
export RALLY_LICENSE=$(awk 'FNR>=2 && FNR<=2' LICENSE | sed 's/^[ \t]*//')

export GIT_SHA=$(git rev-parse --short HEAD)
export DATE=$(date +%Y%m%d)

export RALLY_VERSION_TAG="${RALLY_VERSION}-${DATE}"
export DOCKER_TAG_VERSION="${RALLY_VERSION}"
export DOCKER_TAG_LATEST="latest"

echo "======================================================="
echo "Creating Docker manifest list for Rally $RALLY_VERSION_TAG"
echo "======================================================="

docker manifest create elastic/rally:${RALLY_VERSION_TAG} \
    --amend elastic/rally:${RALLY_VERSION_TAG}-amd64 \
    --amend elastic/rally:${RALLY_VERSION_TAG}-arm64

trap push_failed ERR
echo "======================================================="
echo "Publishing Docker manifest list elastic/rally:$RALLY_VERSION_TAG"
echo "======================================================="
docker manifest push elastic/rally:${RALLY_VERSION_TAG}
trap - ERR

if [[ $PUSH_LATEST == "true" ]]; then
    echo "======================================================="
    echo "Creating Docker manifest list for Rally $DOCKER_TAG_VERSION"
    echo "======================================================="

    docker manifest create elastic/rally:${DOCKER_TAG_VERSION} \
        --amend elastic/rally:${RALLY_VERSION_TAG}-amd64 \
        --amend elastic/rally:${RALLY_VERSION_TAG}-arm64

    echo "======================================================="
    echo "Creating Docker manifest list for Rally $DOCKER_TAG_LATEST"
    echo "======================================================="

    docker manifest create elastic/rally:${DOCKER_TAG_LATEST} \
        --amend elastic/rally:${RALLY_VERSION_TAG}-amd64 \
        --amend elastic/rally:${RALLY_VERSION_TAG}-arm64

    echo "======================================================="
    echo "Publishing Docker manifest list elastic/rally:${DOCKER_TAG_LATEST}"
    echo "======================================================="

    trap push_failed ERR
    docker manifest push elastic/rally:${DOCKER_TAG_VERSION}
    docker manifest push elastic/rally:${DOCKER_TAG_LATEST}
fi

trap - ERR

