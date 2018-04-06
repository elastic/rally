#!/usr/bin/env bash

set -e

readonly CONFIGURATIONS=(integration-test es-integration-test)

# we will not test ES 1.x anymore because it does not work with (out of the box) with Java 9 anymore (Java 8 is still fine). On startup it
# fails with:
#
#       java.lang.UnsupportedOperationException: Boot class path mechanism is not supported
#
#readonly DISTRIBUTIONS=(2.4.6 5.6.7)
readonly DISTRIBUTIONS=(6.2.3)
readonly TRACKS=(geonames nyc_taxis http_logs nested)

readonly ES_METRICS_STORE_VERSION="6.2.1"
readonly ES_ARTIFACT_PATH="elasticsearch-${ES_METRICS_STORE_VERSION}"
readonly ES_ARTIFACT="${ES_ARTIFACT_PATH}.tar.gz"
readonly MIN_CURL_VERSION=(7 12 3)

ES_PID=-1

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

function kill_rally_processes {
    # kill all lingering Rally instances that might still be hanging
    set +e
    killall -9 esrally
    set -e
}

function kill_related_es_processes {
    # kill all lingering Rally instances that might still be hanging
    set +e
    # killall matching ES instances - we cannot use killall as we also want to ensure "rally" is "somewhere" in the command line.
    RUNNING_RALLY_ES_PROCESSES=$(jps -v | egrep ".*java.*rally" | awk '{print $1}')
    for p in "${RUNNING_RALLY_ES_PROCESSES}"
    do
        kill -9 ${p}
    done
    set -e
}

function set_up {
    info "setting up"
    kill_rally_processes
    kill_related_es_processes

    local in_memory_config_file_path="${HOME}/.rally/rally-integration-test.ini"
    local es_config_file_path="${HOME}/.rally/rally-es-integration-test.ini"

    # if the build defines these variables we'll explicitly use them instead of auto-detection
    if [ -n "${JAVA_HOME}" ] && [ -n "${RUNTIME_JAVA_HOME}" ]; then
        # configure for tests with an in-memory metrics store
        esrally configure --java-home="${JAVA_HOME}" --runtime-java-home="${RUNTIME_JAVA_HOME}" --assume-defaults --configuration-name="integration-test"
        # configure for tests with an Elasticsearch metrics store
        esrally configure --java-home="${JAVA_HOME}" --runtime-java-home="${RUNTIME_JAVA_HOME}" --assume-defaults --configuration-name="es-integration-test"
    else
        # configure for tests with an in-memory metrics store
        esrally configure --assume-defaults --configuration-name="integration-test"
        # configure for tests with an Elasticsearch metrics store
        esrally configure --assume-defaults --configuration-name="es-integration-test"

    fi

    # configure Elasticsearch instead of in-memory after the fact
    # this is more portable than using sed's in-place editing which requires "-i" on GNU and "-i ''" elsewhere.
    perl -i -pe 's/datastore\.type.*/datastore.type = elasticsearch/g' ${es_config_file_path}
    perl -i -pe 's/datastore\.host.*/datastore.host = localhost/g'  ${es_config_file_path}
    perl -i -pe 's/datastore\.port.*/datastore.port = 9200/g'  ${es_config_file_path}
    perl -i -pe 's/datastore\.secure.*/datastore.secure = False/g'  ${es_config_file_path}

    info "Final configuration for ${in_memory_config_file_path}:"
    cat "${in_memory_config_file_path}"

    info "Final configuration for ${es_config_file_path}:"
    cat "${es_config_file_path}"

    # Download and Elasticsearch metrics store
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
    bin/elasticsearch &
    # store PID so we can kill ES later
    ES_PID=$!

    # Wait for ES cluster to be up and running
    while true
    do
        curl "http://localhost:9200/_cluster/health?wait_for_status=yellow&timeout=5s" > /dev/null 2>&1 && break
        info "Waiting for ES metrics store..."
        sleep 1
    done ;
    info "ES metrics store is up and running."
    popd
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
    esrally --logging=console --configuration-name="${cfg}" --revision=latest --track=geonames --test-mode --challenge=append-no-conflicts --car=4gheap --elasticsearch-plugins=analysis-icu
    info "test sources [--configuration-name=${cfg}], [--pipeline=from-sources-skip-build], [--track=geonames], [--challenge=append-no-conflicts-index-only], [--car=verbose_iw], [--laps=2]"
    kill_rally_processes
    esrally --logging=console --configuration-name="${cfg}" --pipeline=from-sources-skip-build --track=geonames --test-mode --challenge=append-no-conflicts-index-only --car=verbose_iw --laps=2
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
            esrally --logging=console --configuration-name="${cfg}" --distribution-version="${dist}" --track="${track}" --test-mode --car=4gheap
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
    esrally --logging=console --configuration-name="${cfg}" --pipeline=benchmark-only --track=geonames --test-mode --challenge=append-no-conflicts-index-only --track-params="cluster_health:'yellow'"
}

function run_test {
    test_configure
    test_list
    test_sources
    test_distributions
    test_benchmark_only
}

function tear_down {
    info "tearing down"
    # just let tear down finish
    set +e
    # terminate metrics store
    if [ "${ES_PID}" != "-1" ]; then
        kill -9 ${ES_PID}
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
