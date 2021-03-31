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

set -e

readonly MIN_DOCKER_MEM_BYTES=$(expr 6 \* 1024 \* 1024 \* 1024)

function check_prerequisites {
    exit_if_docker_not_running

    DOCKER_MEM_BYTES=$(docker info --format="{{.MemTotal}}")
    if [[ ${DOCKER_MEM_BYTES} -lt ${MIN_DOCKER_MEM_BYTES} ]]; then
        echo "Error: Docker is not configured with enough memory. ($DOCKER_MEM_BYTES bytes)"
        echo "Please increase memory available to Docker to at least ${MIN_DOCKER_MEM_BYTES} bytes"
        exit 1
    fi

    if ! type docker-compose > /dev/null; then
        echo "docker compose is necessary to run the integration tests"
        exit 1
    fi
}

function log {
    local ts=$(date -u "+%Y-%m-%dT%H:%M:%SZ")
    echo "[${ts}] [${1}] ${2}"
}

function info {
    log "INFO" "${1}"
}

function warn {
    log "WARN" "${1}"
}

function error {
    log "ERROR" "${1}"
}

function stop_and_clean_docker_container {
    docker stop ${1} > /dev/null || true
    docker rm ${1} > /dev/null || true
}

function kill_related_es_processes {
    # kill all lingering Rally instances that might still be hanging
    set +e
    # kill all lingering Elasticsearch Docker containers launched by Rally
    RUNNING_DOCKER_CONTAINERS=$(docker ps --filter "label=io.rally.description" --format "{{.ID}}")
    if [ -n "${RUNNING_DOCKER_CONTAINERS}" ]; then
        for container in "${RUNNING_DOCKER_CONTAINERS}"
        do
            stop_and_clean_docker_container ${container}
        done
    fi
    set -e
}

function exit_if_docker_not_running {
    if ! docker ps >/dev/null 2>&1; then
        error "Docker is required to run integration tests. Install and run Docker and try again."
        exit 1
    fi
}

function docker_compose {
    if [[ "$1" == "up" ]]; then
        docker-compose -f docker/docker-compose-tests.yml up --abort-on-container-exit
    elif [[ "$1" == "down" ]]; then
        docker-compose -f docker/docker-compose-tests.yml down -v
    else
        error "Unknown argument [$1] for docker-compose, exiting."
    fi
}

# This function gets called by release-docker.sh and assumes the image has been already built
function test_docker_release_image {
    if [[ -z "${RALLY_VERSION}" ]]; then
        error "Environment variable [RALLY_VERSION] needs to be set to test the release image; exiting."
    elif [[ -z "${RALLY_LICENSE}" ]]; then
        error "Environment variable [RALLY_LICENSE] needs to be set to test the release image; exiting."
    fi

    docker_compose down

    info "Testing Rally docker image uses the right version"
    actual_version=$(docker run --rm elastic/rally:${RALLY_VERSION} esrally --version | cut -d ' ' -f 2,2)
    if [[ ${actual_version} != ${RALLY_VERSION} ]]; then
        echo "Rally version in Docker image: [${actual_version}] doesn't match the expected version [${RALLY_VERSION}]"
        exit 1
    fi

    info "Testing Rally docker image version label is correct"
    actual_version=$(docker inspect --format '{{ index .Config.Labels "org.label-schema.version"}}' elastic/rally:${RALLY_VERSION})
    if [[ ${actual_version} != ${RALLY_VERSION} ]]; then
        echo "org.label-schema.version label in Rally Docker image: [${actual_version}] doesn't match the expected version [${RALLY_VERSION}]"
        exit 1
    fi

    info "Testing Rally docker image license label is correct"
    actual_license=$(docker inspect --format '{{ index .Config.Labels "license"}}' elastic/rally:${RALLY_VERSION})
    if [[ ${actual_license} != ${RALLY_LICENSE} ]]; then
        echo "license label in Rally Docker image: [${actual_license}] doesn't match the expected license [${RALLY_LICENSE}]"
        exit 1
    fi

    export TEST_COMMAND="race --pipeline=benchmark-only --test-mode --track=geonames --challenge=append-no-conflicts-index-only --target-hosts=es01:9200"
    info "Testing Rally docker image using parameters: ${TEST_COMMAND}"
    docker_compose up
    docker_compose down

    # list should work
    export TEST_COMMAND="list tracks"
    info "Testing Rally docker image using parameters: ${TEST_COMMAND}"
    docker_compose up
    docker_compose down

    # --help should work
    export TEST_COMMAND="--help"
    info "Testing Rally docker image using parameters: ${TEST_COMMAND}"
    docker_compose up
    docker_compose down

    # allow overriding CMD too
    export TEST_COMMAND="esrally race --pipeline=benchmark-only --test-mode --track=geonames --challenge=append-no-conflicts-index-only --target-hosts=es01:9200"
    info "Testing Rally docker image using parameters: ${TEST_COMMAND}"
    docker_compose up
    docker_compose down
    unset TEST_COMMAND
}

function main {
  check_prerequisites
  test_docker_release_image
}


function tear_down {
    info "tearing down"
    kill_related_es_processes
}

trap "tear_down" EXIT

main
