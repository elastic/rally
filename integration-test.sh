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

readonly CONFIGURATIONS=(integration-test es-integration-test)

readonly DISTRIBUTIONS=(2.4.6 5.6.16 6.8.0 7.1.1)
readonly BUILD_TYPES=(tar docker)
readonly TRACKS=(geonames nyc_taxis http_logs nested)

readonly ES_METRICS_STORE_JAVA_HOME="${JAVA8_HOME}"
readonly ES_METRICS_STORE_VERSION="6.2.1"
readonly ES_METRICS_STORE_HTTP_PORT="62200"
readonly ES_METRICS_STORE_TRANSPORT_PORT="63200"
readonly ES_ARTIFACT_PATH="elasticsearch-${ES_METRICS_STORE_VERSION}"
readonly ES_ARTIFACT="${ES_ARTIFACT_PATH}.tar.gz"
readonly MIN_CURL_VERSION=(7 12 3)
readonly MIN_DOCKER_MEM_BYTES=$(expr 6 \* 1024 \* 1024 \* 1024)
readonly RALLY_HOME="${RALLY_HOME:-$HOME}"
readonly RALLY_LOG="${RALLY_HOME}/.rally/logs/rally.log"
readonly RALLY_LOG_BACKUP="${RALLY_HOME}/.rally/logs/rally.log.it.bak"

ES_PID=-1
PROXY_CONTAINER_ID=-1
PROXY_SERVER_AVAILABLE=0
TEST_SUCCESS=0

function check_prerequisites {
    exit_if_docker_not_running

    DOCKER_MEM_BYTES=$(docker info --format="{{.MemTotal}}")
    if [[ ${DOCKER_MEM_BYTES} -lt ${MIN_DOCKER_MEM_BYTES} ]]; then
        echo "Error: Docker is not configured with enough memory. ($DOCKER_MEM_BYTES bytes)"
        echo "Please increase memory available to Docker to at least ${MIN_DOCKER_MEM_BYTES} bytes"
        exit 1
    fi

    local curl_major_version=$(curl --version | head -1 | cut -d ' ' -f 2,2 | cut -d '.' -f 1,1)
    local curl_minor_version=$(curl --version | head -1 | cut -d ' ' -f 2,2 | cut -d '.' -f 2,2)
    local curl_patch_release=$(curl --version | head -1 | cut -d ' ' -f 2,2 | cut -d '.' -f 3,3)

    if [[ $curl_major_version < ${MIN_CURL_VERSION[0]} ]] || \
       [[ $curl_major_version == ${MIN_CURL_VERSION[0]} && $curl_minor_version < ${MIN_CURL_VERSION[1]} ]] || \
       [[ $curl_major_version == ${MIN_CURL_VERSION[0]} && $curl_minor_version == ${MIN_CURL_VERSION[1]} && $curl_patch_release < ${MIN_CURL_VERSION[2]} ]]
    then
        echo "Minimum curl version required is ${MIN_CURL_VERSION[0]}.${MIN_CURL_VERSION[1]}.${MIN_CURL_VERSION[2]} ; please upgrade your curl."
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

function backup_rally_log {
    set +e
    mv -f ${RALLY_LOG} "${RALLY_LOG_BACKUP}"
    set -e
}

function restore_rally_log {
    set +e
    mv -f ${RALLY_LOG_BACKUP} "${RALLY_LOG}"
    set -e
}

function stop_and_clean_docker_container {
    docker stop ${1} > /dev/null || true
    docker rm ${1} > /dev/null || true
}

function wait_for_free_es_port {
  es_port=39200
  while nc -z localhost ${es_port}; do sleep 1; echo "Port ${es_port} occupied waiting"; done
}

function kill_rally_processes {
    # kill all lingering Rally instances that might still be hanging
    set +e
    RUNNING_RALLY_PROCESSES=$(ps -ef | egrep "[e]srally" | awk '{print $2}')
    if [ -n "${RUNNING_RALLY_PROCESSES}" ]; then
        for p in "${RUNNING_RALLY_PROCESSES}"
        do
            kill -9 ${p}
        done
    fi
    set -e
}

function kill_related_es_processes {
    # kill all lingering Rally instances that might still be hanging
    set +e
    # killall matching ES instances - we cannot use killall as we also want to ensure "rally" is "somewhere" in the command line.
    # TODO: exclude Docker containers; in Linux, the following may result in Docker Elasticsearch containers getting killed too.
    RUNNING_RALLY_ES_PROCESSES=$(jps -v | egrep ".*java.*rally" | awk '{print $1}')
    if [ -n "${RUNNING_RALLY_ES_PROCESSES}" ]; then
        for p in "${RUNNING_RALLY_ES_PROCESSES}"
        do
            kill -9 ${p}
        done
    fi
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

function set_up_metrics_store {
    local in_memory_config_file_path="${RALLY_HOME}/.rally/rally-integration-test.ini"
    local es_config_file_path="${RALLY_HOME}/.rally/rally-es-integration-test.ini"

    # configure Elasticsearch instead of in-memory after the fact
    # this is more portable than using sed's in-place editing which requires "-i" on GNU and "-i ''" elsewhere.
    perl -i -pe "s/datastore\.type.*/datastore.type = elasticsearch/g" ${es_config_file_path}
    perl -i -pe "s/datastore\.host.*/datastore.host = localhost/g"  ${es_config_file_path}
    perl -i -pe "s/datastore\.port.*/datastore.port = ${ES_METRICS_STORE_HTTP_PORT}/g"  ${es_config_file_path}
    perl -i -pe "s/datastore\.secure.*/datastore.secure = False/g"  ${es_config_file_path}

    info "Final configuration for ${in_memory_config_file_path}:"
    cat "${in_memory_config_file_path}"

    info "Final configuration for ${es_config_file_path}:"
    cat "${es_config_file_path}"

    # Download and run Elasticsearch metrics store
    pushd .
    mkdir -p .rally_it/cache
    cd .rally_it/cache
    if [[ ! -f $ES_ARTIFACT ]]; then
        # If curl fails immediately, executing all retries will take up to (2**retries)-1 seconds.
        curl --retry 8 -O https://artifacts.elastic.co/downloads/elasticsearch/"${ES_ARTIFACT}" || { rm -f "${ES_ARTIFACT}"; exit 1; }
    fi
    # Delete and exit if archive is somehow corrupted, despite getting downloaded correctly.
    tar -xzf "${ES_ARTIFACT}" || { rm -f "${ES_ARTIFACT}"; exit 1; }
    cd "${ES_ARTIFACT_PATH}"
    export JAVA_HOME=${ES_METRICS_STORE_JAVA_HOME}
    bin/elasticsearch -Ehttp.port=${ES_METRICS_STORE_HTTP_PORT} -Etransport.tcp.port=${ES_METRICS_STORE_TRANSPORT_PORT} &
    # store PID so we can kill ES later
    ES_PID=$!

    # Wait for ES cluster to be up and running
    while true
    do
        curl "http://localhost:${ES_METRICS_STORE_HTTP_PORT}/_cluster/health?wait_for_status=yellow&timeout=5s" > /dev/null 2>&1 && break
        info "Waiting for ES metrics store..."
        sleep 1
    done ;
    info "ES metrics store is up and running."
    popd > /dev/null
}

function exit_if_docker_not_running {
    if ! docker ps >/dev/null 2>&1; then
        error "Docker is required to run integration tests. Install and run Docker and try again."
        exit 1
    fi
}

function set_up_proxy_server {
    local config_dir="$PWD/.rally_it/proxy_tmp"
    mkdir -p ${config_dir}

    cat > ${config_dir}/squid.conf <<"EOF"
auth_param basic program /usr/lib/squid/basic_ncsa_auth /etc/squid/squidpasswords
auth_param basic realm proxy
acl authenticated proxy_auth REQUIRED
http_access allow authenticated
http_port 3128
EOF

    cat > ${config_dir}/squidpasswords <<"EOF"
testuser:$apr1$GcQaaItl$lhi4JoDsWBpZbkXVbI51O/
EOF
    PROXY_CONTAINER_ID=$(docker run --rm --name squid -d -v ${config_dir}/squidpasswords:/etc/squid/squidpasswords -v ${config_dir}/squid.conf:/etc/squid/squid.conf -p 3128:3128 datadog/squid)
    PROXY_SERVER_AVAILABLE=1
}

function set_up {
    info "setting up"
    kill_rally_processes
    kill_related_es_processes

    # configure for tests with an in-memory metrics store
    esrally configure --assume-defaults --configuration-name="integration-test"
    # configure for tests with an Elasticsearch metrics store
    esrally configure --assume-defaults --configuration-name="es-integration-test"

    set_up_metrics_store
    set_up_proxy_server
}

function random_configuration {
    local num_configs=${#CONFIGURATIONS[*]}
    # we cannot simply return string values in a bash script
    eval "$1='${CONFIGURATIONS[$((RANDOM%num_configs))]}'"
}

function random_track {
    local num_tracks=${#TRACKS[*]}
    eval "$1='${TRACKS[$((RANDOM%num_tracks))]}'"
}

function random_distribution {
    local num_distributions=${#DISTRIBUTIONS[*]}
    eval "$1='${DISTRIBUTIONS[$((RANDOM%num_distributions))]}'"
}

function random_build_type {
    local num_build_types=${#BUILD_TYPES[*]}
    eval "$1='${BUILD_TYPES[$((RANDOM%num_build_types))]}'"
}

function test_proxy_connection {
    local cfg

    random_configuration cfg

    # isolate invocations so we see only the log output from the current invocation
    backup_rally_log

    set +e
    esrally list tracks --configuration-name="${cfg}"
    unset http_proxy
    set -e

    if grep -F -q "Connecting directly to the Internet" "$RALLY_LOG"; then
        info "Successfully checked that direct internet connection is used."
        rm -f ${RALLY_LOG}
    else
        error "Could not find indication that direct internet connection is used. Check ${RALLY_LOG}."
        exit 1
    fi

    # test that we cannot connect to the Internet if the proxy authentication is missing
    export http_proxy=http://127.0.0.1:3128
    # this invocation *may* lead to an error but this is ok
    set +e
    esrally list tracks --configuration-name="${cfg}"
    unset http_proxy
    set -e
    if grep -F -q "Connecting via proxy URL [http://127.0.0.1:3128] to the Internet" "$RALLY_LOG"; then
        info "Successfully checked that proxy is used."
    else
        error "Could not find indication that proxy access is used. Check ${RALLY_LOG}."
        exit 1
    fi

    if grep -F -q "No Internet connection detected" "$RALLY_LOG"; then
        info "Successfully checked that unauthenticated proxy access is prevented."
        rm -f ${RALLY_LOG}
    else
        error "Could not find indication that unauthenticated proxy access is prevented. Check ${RALLY_LOG}."
        exit 1
    fi

    # test that we can connect to the Internet if the proxy authentication is set

    export http_proxy=http://testuser:testuser@127.0.0.1:3128
    # this invocation *may* lead to an error but this is ok
    set +e
    esrally list tracks --configuration-name="${cfg}"
    unset http_proxy
    set -e

    if grep -F -q "Connecting via proxy URL [http://testuser:testuser@127.0.0.1:3128] to the Internet" "$RALLY_LOG"; then
        info "Successfully checked that proxy is used."
    else
        error "Could not find indication that proxy access is used. Check ${RALLY_LOG}."
        exit 1
    fi

    if grep -F -q "Detected a working Internet connection" "$RALLY_LOG"; then
        info "Successfully checked that authenticated proxy access is allowed."
        rm -f ${RALLY_LOG}
    else
        error "Could not find indication that authenticated proxy access is allowed. Check ${RALLY_LOG}."
        exit 1
    fi
    # restore original file (but only on success so we keep the test's Rally log file for inspection on errors).
    restore_rally_log
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

function tests_for_all_docker_images {
    export TEST_COMMAND="--pipeline=benchmark-only --test-mode --track=geonames --challenge=append-no-conflicts-index-only --target-hosts=es01:9200"
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
    export TEST_COMMAND="esrally --pipeline=benchmark-only --test-mode --track=geonames --challenge=append-no-conflicts-index-only --target-hosts=es01:9200"
    info "Testing Rally docker image using parameters: ${TEST_COMMAND}"
    docker_compose up
    docker_compose down
    unset TEST_COMMAND
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

    tests_for_all_docker_images
}

function run_test {
    if [ "${PROXY_SERVER_AVAILABLE}" == "1" ]; then
        echo "**************************************** TESTING PROXY CONNECTIONS *********************************"
        test_proxy_connection
    fi
    echo "**************************************** TESTING RALLY DOCKER IMAGE ********************************************"
    test_docker_dev_image
    TEST_SUCCESS=1
}

function tear_down {
    info "tearing down"
    # just let tear down finish
    set +e
    if [ "${TEST_SUCCESS}" != "1" ]; then
      error "Tests have failed - Printing last 200 lines of logs"
      error "===================== LOG FILE START =============================="
      tail -n 200 "${RALLY_LOG}"
      error "====================== LOG FILE END ==============================="
    fi

    # terminate metrics store
    if [ "${ES_PID}" != "-1" ]; then
        info "Stopping Elasticsearch metrics store with PID [${ES_PID}]"
        kill -9 ${ES_PID} > /dev/null
    fi
    # stop Docker container for tests
    if [ "${PROXY_CONTAINER_ID}" != "-1" ]; then
        info "Stopping Docker container [${PROXY_CONTAINER_ID}]"
        stop_and_clean_docker_container ${PROXY_CONTAINER_ID}
    fi

    rm -f ${RALLY_HOME}/.rally/rally*integration-test.ini
    rm -rf .rally_it/cache/"${ES_ARTIFACT_PATH}"
    rm -rf .rally_it/proxy_tmp
    set -e
    kill_rally_processes
    # run this after the metrics store has been stopped otherwise we might forcefully terminate our metrics store.
    kill_related_es_processes
}

function main {
    set_up
    run_test
}

check_prerequisites

trap "tear_down" EXIT

# allow invocation from release-docker.sh
if [[ $1 == "test_docker_release_image" ]]; then
    test_docker_release_image
    exit
# if argument is the name of any other function, set up and call it
elif declare -f "$1" > /dev/null; then
    set_up
    $1
    TEST_SUCCESS=1
    exit
# otherwise run all functions
else
  main
fi
