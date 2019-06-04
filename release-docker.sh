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

# Logged in on Docker hub (docker login)

# fail this script immediately if any command fails with a non-zero exit code
set -eu

function docker_compose {
    if [[ "$1" == "up" ]]; then
        docker-compose -f docker/docker-compose-tests.yml up --abort-on-container-exit
    elif [[ "$1" == "down" ]]; then
        docker-compose -f docker/docker-compose-tests.yml down -v
    fi
}

function test_docker_image {
    # First ensure any left overs have been cleaned up
    docker_compose down

    printf "* Testing Rally docker image uses the right version\n"
    actual_version=$(docker run --rm elastic/rally:${RALLY_VERSION} esrally --version | cut -d ' ' -f 2,2)
    if [[ ${actual_version} != ${RALLY_VERSION} ]]; then
        printf "Rally version in Docker image: [${actual_version}] doesn't match the expected version [${RALLY_VERSION}]"
        exit 1
    fi

    export TEST_COMMAND="--pipeline=benchmark-only --test-mode --track=geonames --challenge=append-no-conflicts-index-only --target-hosts=es01:9200"
    printf "* Testing Rally docker image using parameters: ${TEST_COMMAND}\n"
    docker_compose up
    docker_compose down

    # --list should work
    export TEST_COMMAND="list tracks"
    printf "* Testing Rally docker image using parameters: ${TEST_COMMAND}\n"
    docker_compose up
    docker_compose down

    # --help should work
    export TEST_COMMAND="--help"
    printf "* Testing Rally docker image using parameters: ${TEST_COMMAND}\n"
    docker_compose up
    docker_compose down

    # allow overriding CMD too
    export TEST_COMMAND="esrally --pipeline=benchmark-only --test-mode --track=geonames --challenge=append-no-conflicts-index-only --target-hosts=es01:9200"
    printf "* Testing Rally docker image using parameters: ${TEST_COMMAND}\n"
    docker_compose up
    docker_compose down
    unset TEST_COMMAND
}

function push_failed {
    printf "Error while pushing Docker image. Did you \`docker login\`?"

}

export RALLY_VERSION=$1
export RALLY_LICENSE=$(awk 'FNR>=2 && FNR<=2' LICENSE | sed 's/^[ \t]*//')

echo "========================================================"
echo "Building Docker image for Rally release $RALLY_VERSION  "
echo "========================================================"

docker build -t elastic/rally:${RALLY_VERSION} --build-arg RALLY_VERSION --build-arg RALLY_LICENSE -f docker/Dockerfiles/Dockerfile-release $PWD

echo "======================================================="
echo "Testing Docker image for Rally release $RALLY_VERSION  "
echo "======================================================="

test_docker_image

echo "======================================================="
echo "Publishing Docker image elastic/rally:$RALLY_VERSION   "
echo "======================================================="

trap push_failed ERR
# docker push elastic/rally:${RALLY_VERSION}

echo "============================================"
echo "Publishing Docker image elastic/rally:latest"
echo "============================================"

# docker tag elastic/rally:${RALLY_VERSION} elastic/rally:latest
# docker push elastic/rally:latest

trap - ERR

