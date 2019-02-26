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

readonly DISTRIBUTIONS=(1.7.6 2.4.6 5.6.9)
readonly TRACKS=(geonames nyc_taxis http_logs nested)

readonly ES_METRICS_STORE_JAVA_HOME="${JAVA8_HOME}"
readonly ES_METRICS_STORE_VERSION="6.2.1"
readonly ES_METRICS_STORE_HTTP_PORT="62200"
readonly ES_METRICS_STORE_TRANSPORT_PORT="63200"
readonly ES_ARTIFACT_PATH="elasticsearch-${ES_METRICS_STORE_VERSION}"
readonly ES_ARTIFACT="${ES_ARTIFACT_PATH}.tar.gz"
readonly MIN_CURL_VERSION=(7 12 3)

ES_PID=-1
PROXY_CONTAINER_ID=-1
PROXY_SERVER_AVAILABLE=0

function check_prerequisites {
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
    RUNNING_RALLY_ES_PROCESSES=$(jps -v | egrep ".*java.*rally" | awk '{print $1}')
    if [ -n "${RUNNING_RALLY_ES_PROCESSES}" ]; then
        for p in "${RUNNING_RALLY_ES_PROCESSES}"
        do
            kill -9 ${p}
        done
    fi
    set -e
}

function set_up_metrics_store {
    local in_memory_config_file_path="${HOME}/.rally/rally-integration-test.ini"
    local es_config_file_path="${HOME}/.rally/rally-es-integration-test.ini"

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

function set_up_proxy_server {
    # we want to see output to stderr for diagnosing problems
    if docker ps > /dev/null; then
        info "Docker is available. Proxy-related tests will be run"
        # Portably create a temporary config directory for Squid on Linux or MacOS
        local config_dir=$(mktemp -d 2>/dev/null || mktemp -d -t 'tmp_squid_cfg')

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
    else
        warn "Docker is not available. Skipping proxy-related tests."
    fi
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

function test_configure {
    info "test configure()"
    # just run to test the configuration procedure, don't use this configuration in other tests.
    esrally configure --assume-defaults --configuration-name="config-integration-test"
}

function test_list {
    local cfg
    random_configuration cfg

    info "test list races [${cfg}]"
    esrally list races --configuration-name="${cfg}"
    info "test list cars [${cfg}]"
    esrally list cars --configuration-name="${cfg}"
    info "test list Elasticsearch plugins [${cfg}]"
    esrally list elasticsearch-plugins --configuration-name="${cfg}"
    info "test list tracks [${cfg}]"
    esrally list tracks --configuration-name="${cfg}"
    info "test list telemetry [${cfg}]"
    esrally list telemetry --configuration-name="${cfg}"
}

function test_sources {
    local cfg
    random_configuration cfg

    # build Elasticsearch and a core plugin
    info "test sources [--configuration-name=${cfg}], [--revision=latest], [--track=geonames], [--challenge=append-no-conflicts], [--car=4gheap] [--elasticsearch-plugins=analysis-icu]"
    kill_rally_processes
    esrally --configuration-name="${cfg}" --on-error=abort --revision=latest --track=geonames --test-mode --challenge=append-no-conflicts --car=4gheap --elasticsearch-plugins=analysis-icu
    info "test sources [--configuration-name=${cfg}], [--pipeline=from-sources-skip-build], [--track=geonames], [--challenge=append-no-conflicts-index-only], [--car=4gheap,ea] [--laps=2]"
    kill_rally_processes
    esrally --configuration-name="${cfg}" --on-error=abort --pipeline=from-sources-skip-build --track=geonames --test-mode --challenge=append-no-conflicts-index-only --car="4gheap,ea" --laps=2
}

function test_distributions {
    local cfg

    for dist in "${DISTRIBUTIONS[@]}"
    do
        for track in "${TRACKS[@]}"
        do
            random_configuration cfg
            info "test distributions [--configuration-name=${cfg}], [--distribution-version=${dist}], [--track=${track}], [--car=4gheap]"
            kill_rally_processes
            esrally --configuration-name="${cfg}" --on-error=abort --distribution-version="${dist}" --track="${track}" --test-mode --car=4gheap
        done
    done
}

function test_benchmark_only {
    # we just use our metrics cluster for these benchmarks. It's not ideal but simpler.
    local cfg
    local dist
    random_configuration cfg

    info "test benchmark-only [--configuration-name=${cfg}]"
    kill_rally_processes
    esrally --target-host="localhost:${ES_METRICS_STORE_HTTP_PORT}" \
            --configuration-name="${cfg}" \
            --on-error=abort \
            --pipeline=benchmark-only \
            --track=geonames \
            --test-mode \
            --challenge=append-no-conflicts-index-only \
            --track-params="cluster_health:'yellow'"
}

function test_proxy_connection {
    readonly rally_log="${HOME}/.rally/logs/rally.log"
    readonly rally_log_backup="${HOME}/.rally/logs/rally.log.it.bak"
    # isolate invocations so we see only the log output from the current invocation
    set +e
    mv -f ${rally_log} "${rally_log_backup}"
    set -e

    set +e
    esrally list tracks
    unset http_proxy
    set -e

    if grep -F -q "Connecting directly to the Internet" "$rally_log"; then
        info "Successfully checked that direct internet connection is used."
        rm -f ${rally_log}
    else
        error "Could not find indication that direct internet connection is used. Please check ${rally_log}."
        exit 1
    fi

    # test that we cannot connect to the Internet if the proxy authentication is missing
    export http_proxy=http://127.0.0.1:3128
    # this invocation *may* lead to an error but this is ok
    set +e
    esrally list tracks
    unset http_proxy
    set -e
    if grep -F -q "Connecting via proxy URL [http://127.0.0.1:3128] to the Internet" "$rally_log"; then
        info "Successfully checked that proxy is used."
    else
        error "Could not find indication that proxy access is used. Please check ${rally_log}."
        exit 1
    fi

    if grep -F -q "No Internet connection detected" "$rally_log"; then
        info "Successfully checked that unauthenticated proxy access is prevented."
        rm -f ${rally_log}
    else
        error "Could not find indication that unauthenticated proxy access is prevented. Please check ${rally_log}."
        exit 1
    fi

    # test that we can connect to the Internet if the proxy authentication is set

    export http_proxy=http://testuser:testuser@127.0.0.1:3128
    # this invocation *may* lead to an error but this is ok
    set +e
    esrally list tracks
    unset http_proxy
    set -e

    if grep -F -q "Connecting via proxy URL [http://testuser:testuser@127.0.0.1:3128] to the Internet" "$rally_log"; then
        info "Successfully checked that proxy is used."
    else
        error "Could not find indication that proxy access is used. Please check ${rally_log}."
        exit 1
    fi

    if grep -F -q "Detected a working Internet connection" "$rally_log"; then
        info "Successfully checked that authenticated proxy access is allowed."
        rm -f ${rally_log}
    else
        error "Could not find indication that authenticated proxy access is allowed. Please check ${rally_log}."
        exit 1
    fi
    # restore original file (but only on success so we keep the test's Rally log file for inspection on errors).
    set +e
    mv -f ${rally_log_backup} "${rally_log}"
    set -e

}

function run_test {
    if [ "${PROXY_SERVER_AVAILABLE}" == "1" ]; then
        echo "**************************************** TESTING PROXY CONNECTIONS *********************************"
        test_proxy_connection
    fi
    echo "**************************************** TESTING CONFIGURATION OF RALLY ****************************************"
    test_configure
    echo "**************************************** TESTING RALLY LIST COMMANDS *******************************************"
    test_list
    echo "**************************************** TESTING RALLY WITH ES FROM SOURCES ************************************"
    test_sources
    echo "**************************************** TESTING RALLY WITH ES DISTRIBUTIONS ***********************************"
    test_distributions
    echo "**************************************** TESTING RALLY BENCHMARK-ONLY PIPELINE *********************************"
    test_benchmark_only
}

function tear_down {
    info "tearing down"
    # just let tear down finish
    set +e
    # terminate metrics store
    if [ "${ES_PID}" != "-1" ]; then
        info "Stopping Elasticsearch metrics store with PID [${ES_PID}]"
        kill -9 ${ES_PID} > /dev/null
    fi
    # stop Docker container for tests
    if [ "${PROXY_CONTAINER_ID}" != "-1" ]; then
        info "Stopping Docker container [${PROXY_CONTAINER_ID}]"
        docker stop ${PROXY_CONTAINER_ID} > /dev/null
    fi

    rm -f ~/.rally/rally*integration-test.ini
    rm -rf .rally_it/cache/"${ES_ARTIFACT_PATH}"
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

main
