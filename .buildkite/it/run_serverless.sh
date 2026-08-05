#!/usr/bin/env bash

set -exo pipefail

source .buildkite/retry.sh

function upload_logs {
    echo "--- Upload artifacts"
    buildkite-agent artifact upload "${RALLY_HOME}/.rally/logs/*.log"
}

export TERM=dumb
export LC_ALL=en_US.UTF-8
export TZ=Etc/UTC
export DEBIAN_FRONTEND=noninteractive
# https://askubuntu.com/questions/1367139/apt-get-upgrade-auto-restart-services
sudo mkdir -p /etc/needrestart
echo "\$nrconf{restart} = 'a';" | sudo tee -a /etc/needrestart/needrestart.conf > /dev/null

PY_SHORT_VERSION="$1"
TEST_NAME="$2"

echo "--- System dependencies"

retry 5 sudo apt-get update
retry 5 sudo apt-get install -y \
    make jq \
    dnsutils # provides nslookup

export PY_VERSION=$(jq -r ".python_versions.PY$(echo "${PY_SHORT_VERSION}" | tr -d '.')" .ci/variables.json)

echo "--- Install UV"

curl -LsSf https://astral.sh/uv/0.11.19/install.sh | env UV_UNMANAGED_INSTALL="${HOME}/.local/bin" sh
export PATH="${HOME}/.local/bin:${PATH}"

echo "--- Install Python ${PY_VERSION}"

uv python install "${PY_VERSION}"

echo "--- Create virtual environment"

make venv

echo "--- Run IT serverless test \"$TEST_NAME\" :pytest:"

export RALLY_HOME=$HOME
export THESPLOG_FILE="${THESPLOG_FILE:-${RALLY_HOME}/.rally/logs/actor-system-internal.log}"
# this value is in bytes, the default is 50kB. We increase it to 200kiB.
export THESPLOG_FILE_MAXSIZE=${THESPLOG_FILE_MAXSIZE:-204800}
# adjust the default log level from WARNING
export THESPLOG_THRESHOLD="INFO"

trap upload_logs ERR


case $TEST_NAME in
    "user")
        make -s it_serverless
        ;;
    "operator")
        make -s it_serverless "ARGS=--operator"
        ;;
    *)
        echo "Unknown test type."
        exit 1
        ;;
esac

upload_logs
