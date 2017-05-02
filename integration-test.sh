#!/usr/bin/env bash

set -e

readonly CONFIGURATIONS=(integration-test es-integration-test)

readonly DISTRIBUTIONS=(1.7.6 2.4.4 5.2.0)
# TODO: Should we just derive the tracks with Rally itself?
readonly TRACKS=(geonames geopoint nyc_taxis pmc logging nested)

ES_PID=-1

function log() {
    local ts=$(date -u "+%Y-%m-%dT%H:%M:%SZ")
    echo "[${ts}] [${1}] ${2}"
}

function info() {
    log "INFO" "${1}"
}

function set_up() {
    info "setting up"
    # configure for tests with an Elasticsearch metrics store
    esrally configure --assume-defaults --configuration-name=es-integration-test
    # configure Elasticsearch instead of in-memory after the fact
    local config_file_path=~/.rally/rally-es-integration-test.ini
    sed -i -e 's/datastore\.type.*/datastore.type = elasticsearch/g' ${config_file_path}
    sed -i -e 's/datastore\.host.*/datastore.host = localhost/g'  ${config_file_path}
    sed -i -e 's/datastore\.port.*/datastore.port = 9200/g'  ${config_file_path}
    sed -i -e 's/datastore\.secure.*/datastore.secure = False/g'  ${config_file_path}

    # Download and Elasticsearch metrics store
    pushd .
    mkdir -p .rally_it/cache
    cd .rally_it/cache
    if [ ! -f elasticsearch-5.0.0.tar.gz ]; then
        curl -O https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.0.0.tar.gz
    fi
    tar -xzf elasticsearch-5.0.0.tar.gz
    cd elasticsearch-5.0.0
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
    info "test list tracks [${cfg}]"
    esrally list tracks --configuration-name="${cfg}"
    info "test list telemetry [${cfg}]"
    esrally list telemetry --configuration-name="${cfg}"
}

function test_sources() {
    local cfg
    random_configuration cfg

    info "test sources [--configuration-name=${cfg}], [--revision=latest], [--track=geonames], [--challenge=append-no-conflicts], [--car=4gheap]"
    esrally --logging=console --configuration-name="${cfg}" --revision=latest --track=geonames --test-mode --challenge=append-no-conflicts --car=4gheap
    info "test sources [--configuration-name=${cfg}], [--pipeline=from-sources-skip-build], [--track=geonames], [--challenge=append-no-conflicts-index-only], [--car=two_nodes], [--laps=2]"
    esrally --logging=console --configuration-name="${cfg}" --pipeline=from-sources-skip-build --track=geonames --test-mode --challenge=append-no-conflicts-index-only --car=two_nodes --laps=2
}

function test_distributions() {
    local cfg

    for dist in "${DISTRIBUTIONS[@]}"
    do
        for track in "${TRACKS[@]}"
        do
            random_configuration cfg
            info "test distributions [--configuration-name=${cfg}], [--distribution-version=${dist}], [--track=${track}], [--car=4gheap]"
            esrally --logging=console --configuration-name="${cfg}" --distribution-version="${dist}" --track="${track}" --test-mode --car=4gheap
        done
    done
}

function run_test() {
    test_configure
    test_list
    test_sources
    test_distributions
}

function tear_down() {
    info "tearing down"
    # just let tear down finish
    set +e
    # terminate metrics store
    kill -9 ${ES_PID}

    rm -f ~/.rally/rally*integration-test.ini
    rm -rf .rally_it/cache/elasticsearch-5.0.0
    set -e
}

function main {
    set_up
    run_test
}

trap "tear_down" EXIT

main