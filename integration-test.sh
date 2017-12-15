#!/usr/bin/env bash

set -e

readonly CONFIGURATIONS=(integration-test es-integration-test)

# we will not test ES 1.x anymore because it does not work with (out of the box) with Java 9 anymore (Java 8 is still fine). On startup it
# fails with:
#
#       java.lang.UnsupportedOperationException: Boot class path mechanism is not supported
#
readonly DISTRIBUTIONS=(2.4.5 5.5.2)
readonly TRACKS=(geonames nyc_taxis http_logs nested)

readonly ES_METRICS_STORE_VERSION="5.0.0"

ES_PID=-1


function log() {
    local ts=$(date -u "+%Y-%m-%dT%H:%M:%SZ")
    echo "[${ts}] [${1}] ${2}"
}

function info() {
    log "INFO" "${1}"
}

function kill_rally_processes() {
    # kill all lingering Rally instances that might still be hanging
    set +e
    killall -9 esrally
    set -e
}

function kill_related_es_processes() {
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


function set_up() {
    info "setting up"
    kill_rally_processes
    kill_related_es_processes

    # configure for tests with an Elasticsearch metrics store
    esrally configure --assume-defaults --configuration-name="es-integration-test"
    # configure Elasticsearch instead of in-memory after the fact
    local config_file_path="${HOME}/.rally/rally-es-integration-test.ini"
    # this is more portable than using sed's in-place editing which requires "-i" on GNU and "-i ''" elsewhere.
    perl -i -pe 's/datastore\.type.*/datastore.type = elasticsearch/g' ${config_file_path}
    perl -i -pe 's/datastore\.host.*/datastore.host = localhost/g'  ${config_file_path}
    perl -i -pe 's/datastore\.port.*/datastore.port = 9200/g'  ${config_file_path}
    perl -i -pe 's/datastore\.secure.*/datastore.secure = False/g'  ${config_file_path}

    # Download and Elasticsearch metrics store
    pushd .
    mkdir -p .rally_it/cache
    cd .rally_it/cache
    if [ ! -f elasticsearch-"${ES_METRICS_STORE_VERSION}".tar.gz ]; then
        curl -O https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-"${ES_METRICS_STORE_VERSION}".tar.gz
    fi
    tar -xzf elasticsearch-"${ES_METRICS_STORE_VERSION}".tar.gz
    cd elasticsearch-"${ES_METRICS_STORE_VERSION}"
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

function random_configuration() {
    local num_configs=${#CONFIGURATIONS[*]}
    # we cannot simply return string values in a bash script
    eval "$1='${CONFIGURATIONS[$((RANDOM%num_configs))]}'"
}

function test_configure() {
    info "test configure()"
    esrally configure --assume-defaults --configuration-name=integration-test
}

function test_list() {
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

function test_sources() {
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

function test_distributions() {
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

function test_benchmark_only() {
    # we just use our metrics cluster for these benchmarks. It's not ideal but simpler.
    local cfg
    local dist
    random_configuration cfg

    info "test benchmark-only [--configuration-name=${cfg}]"
    kill_rally_processes
    esrally --logging=console --configuration-name="${cfg}" --pipeline=benchmark-only --track=geonames --test-mode --challenge=append-no-conflicts-index-only --cluster-health=yellow
}

function run_test() {
    test_configure
    test_list
    test_sources
    test_distributions
    test_benchmark_only
}

function tear_down() {
    info "tearing down"
    # just let tear down finish
    set +e
    # terminate metrics store
    kill -9 ${ES_PID}

    rm -f ~/.rally/rally*integration-test.ini
    rm -rf .rally_it/cache/elasticsearch-"${ES_METRICS_STORE_VERSION}"
    set -e
    kill_rally_processes
    # run this after the metrics store has been stopped otherwise we might forcefully terminate our metrics store.
    kill_related_es_processes
}

function main {
    set_up
    run_test
}

trap "tear_down" EXIT

main