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
set -eu -eE -o functrace

function push_failed {
    echo "Error while pushing Docker image. Did you \`docker login\`?"
}

if [[ $# -eq 0 ]] ; then
    echo "ERROR: $0 requires the Rally version to push as a command line argument and you didn't supply it."
    echo "For example: $0 master"
    exit 1
fi
export RALLY_BRANCH=$1
export RALLY_LICENSE=$(awk 'FNR>=2 && FNR<=2' LICENSE | sed 's/^[ \t]*//')

export GIT_SHA=$(git rev-parse --short HEAD)
export DATE=$(date +%Y%m%d)

export RALLY_VERSION="${RALLY_BRANCH}-${GIT_SHA}-${DATE}"
export MAIN_BRANCH=$(git remote show origin | sed -n '/HEAD branch/s/.*: //p')

if [[ $RALLY_BRANCH == $MAIN_BRANCH ]]; then
    export DOCKER_TAG_LATEST="dev-latest"
else
    export DOCKER_TAG_LATEST="${RALLY_BRANCH}-latest"
fi

echo "========================================================"
echo "Pulling Docker images for Rally $RALLY_VERSION          "
echo "========================================================"

docker pull elastic/rally:${RALLY_VERSION}-amd64
docker pull elastic/rally:${RALLY_VERSION}-arm64

echo "======================================================="
echo "Creating Docker manifest image for Rally $RALLY_VERSION"
echo "======================================================="

docker manifest create elastic/rally:${RALLY_VERSION} \
    --amend elastic/rally:${RALLY_VERSION}-amd64 \
    --amend elastic/rally:${RALLY_VERSION}-arm64

echo "======================================================="
echo "Creating Docker manifest image for Rally $DOCKER_TAG_LATEST"
echo "======================================================="

docker manifest create elastic/rally:${DOCKER_TAG_LATEST} \
    --amend elastic/rally:${DOCKER_TAG_LATEST}-amd64 \
    --amend elastic/rally:${DOCKER_TAG_LATEST}-arm64

echo "======================================================="
echo "Publishing Docker image elastic/rally:$RALLY_VERSION   "
echo "======================================================="

trap push_failed ERR
docker manifest push elastic/rally:${RALLY_VERSION}
docker manifest push elastic/rally:${DOCKER_TAG_LATEST}

trap - ERR