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

echo "--- Check resources"

free
cat /proc/cpuinfo

echo "--- System dependencies"

export PY_VERSION="$1"
retry 5 sudo add-apt-repository --yes ppa:deadsnakes/ppa
retry 5 sudo apt-get update
retry 5 sudo apt-get install -y \
    "python${PY_VERSION}" "python${PY_VERSION}-dev" "python${PY_VERSION}-venv" \
    git make jq docker \
    openjdk-21-jdk-headless openjdk-11-jdk-headless
export JAVA11_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export JAVA21_HOME=/usr/lib/jvm/java-21-openjdk-amd64

echo "--- Install UV"

curl -LsSf https://astral.sh/uv/install.sh | sh
source "${HOME}/.local/bin/env"

echo "--- Create virtual environment"

make venv

echo "--- Run IT test :pytest:"

export RALLY_HOME=$HOME
export THESPLOG_FILE="${THESPLOG_FILE:-${RALLY_HOME}/.rally/logs/actor-system-internal.log}"
# this value is in bytes, the default is 50kB. We increase it to 200kiB.
export THESPLOG_FILE_MAXSIZE=${THESPLOG_FILE_MAXSIZE:-204800}
# adjust the default log level from WARNING
export THESPLOG_THRESHOLD="INFO"
export TERM=dumb
export LC_ALL=en_US.UTF-8

trap upload_logs ERR

make it

upload_logs
