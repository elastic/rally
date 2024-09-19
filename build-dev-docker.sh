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

if [[ $# -ne 3 ]] ; then
    echo "ERROR: $0 requires the Rally branch to build, the architecture, and whether to push the latest \
        as command line arguments and they weren't supplied."
    echo "For example: $0 master amd64 true"
    exit 1
fi
export RALLY_BRANCH=$1
export ARCH=$2
export PUSH_LATEST=$3

export RALLY_LICENSE=$(awk 'FNR>=2 && FNR<=2' LICENSE | sed 's/^[ \t]*//')

export GIT_SHA=$(git rev-parse --short HEAD)
export DATE=$(date +%Y%m%d)

export RALLY_VERSION="${RALLY_BRANCH}-${GIT_SHA}-${DATE}-${ARCH}"
export RALLY_VERSION_TAG="${RALLY_VERSION}"
export MAIN_BRANCH=$(git remote show origin | sed -n '/HEAD branch/s/.*: //p')

if [[ $RALLY_BRANCH == $MAIN_BRANCH ]]; then
    export DOCKER_TAG_LATEST="dev-latest-${ARCH}"
else
    export DOCKER_TAG_LATEST="${RALLY_BRANCH}-latest-${ARCH}"
fi

echo "========================================================"
echo "Building Docker image for Rally $RALLY_VERSION          "
echo "========================================================"

docker build -t elastic/rally:${RALLY_VERSION} --build-arg RALLY_VERSION --build-arg RALLY_LICENSE -f docker/Dockerfiles/dev/Dockerfile $PWD

echo "======================================================="
echo "Testing Docker image for Rally release $RALLY_VERSION  "
echo "======================================================="

./release-docker-test.sh dev

echo "======================================================="
echo "Publishing Docker image elastic/rally:$RALLY_VERSION   "
echo "======================================================="

trap push_failed ERR
docker push elastic/rally:${RALLY_VERSION}

if [[ $PUSH_LATEST == "true" ]]; then
    echo "============================================"
    echo "Publishing Docker image elastic/rally:${DOCKER_TAG_LATEST}"
    echo "============================================"

    docker tag elastic/rally:${RALLY_VERSION} elastic/rally:${DOCKER_TAG_LATEST}
    docker push elastic/rally:${DOCKER_TAG_LATEST}
fi

trap - ERR
