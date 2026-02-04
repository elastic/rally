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
    echo "ERROR: $0 requires the Rally branch to build from as a command line argument and you didn't supply it."
    echo "For example: $0 master true"
    exit 1
fi
export RALLY_BRANCH=$1
export PUSH_LATEST=$2
export PUBLIC_DOCKER_REPO=$3
if [[ $PUBLIC_DOCKER_REPO == "true" ]]; then
    export RALLY_DOCKER_IMAGE="elastic/rally"
else
    export RALLY_DOCKER_IMAGE="docker.elastic.co/es-perf/rally"
fi

RALLY_LICENSE=$(awk 'FNR>=2 && FNR<=2' LICENSE | sed 's/^[ \t]*//')
export RALLY_LICENSE

GIT_SHA=$(git rev-parse --short HEAD)
DATE=$(date +%Y%m%d)

if [[ "${RALLY_BRANCH}" =~ .*\/.* ]]; then
  branch_name=$(echo "${RALLY_BRANCH}" | sed 's/\//_/g')
else
  branch_name="${RALLY_BRANCH}"
fi
export RALLY_VERSION="${branch_name}-${GIT_SHA}-${DATE}"
MAIN_BRANCH=$(git remote show origin | sed -n '/HEAD branch/s/.*: //p')

if [[ "$RALLY_BRANCH" == "$MAIN_BRANCH" ]]; then
    export DOCKER_TAG_LATEST="dev-latest"
else
    export DOCKER_TAG_LATEST="${branch_name}-latest"
fi

echo "========================================================"
echo "Pulling Docker images for Rally $RALLY_VERSION          "
echo "========================================================"

docker pull --platform=linux/amd64 "${RALLY_DOCKER_IMAGE}:${RALLY_VERSION}-amd64"
docker pull --platform=linux/arm64 "${RALLY_DOCKER_IMAGE}:${RALLY_VERSION}-arm64"

echo "======================================================="
echo "Creating Docker manifest image for Rally $RALLY_VERSION"
echo "======================================================="

docker manifest create ${RALLY_DOCKER_IMAGE}:${RALLY_VERSION} \
    --amend ${RALLY_DOCKER_IMAGE}:${RALLY_VERSION}-amd64 \
    --amend ${RALLY_DOCKER_IMAGE}:${RALLY_VERSION}-arm64

trap push_failed ERR
echo "======================================================="
echo "Publishing Docker image ${RALLY_DOCKER_IMAGE}:$RALLY_VERSION   "
echo "======================================================="
docker manifest push ${RALLY_DOCKER_IMAGE}:${RALLY_VERSION}

trap - ERR

if [[ $PUSH_LATEST == "true" ]]; then
    echo "======================================================="
    echo "Creating Docker manifest image for Rally $DOCKER_TAG_LATEST"
    echo "======================================================="

    docker manifest create ${RALLY_DOCKER_IMAGE}:${DOCKER_TAG_LATEST} \
        --amend ${RALLY_DOCKER_IMAGE}:${DOCKER_TAG_LATEST}-amd64 \
        --amend ${RALLY_DOCKER_IMAGE}:${DOCKER_TAG_LATEST}-arm64

    trap push_failed ERR
    echo "======================================================="
    echo "Publishing Docker image ${RALLY_DOCKER_IMAGE}:${DOCKER_TAG_LATEST}"
    echo "======================================================="
    docker manifest push ${RALLY_DOCKER_IMAGE}:${DOCKER_TAG_LATEST}
fi

trap - ERR
