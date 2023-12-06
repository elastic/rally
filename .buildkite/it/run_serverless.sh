#!/usr/bin/env bash

set -eo pipefail

source .buildkite/retry.sh

export TERM=dumb
export LC_ALL=en_US.UTF-8
export TZ=Etc/UTC
export DEBIAN_FRONTEND=noninteractive
# https://askubuntu.com/questions/1367139/apt-get-upgrade-auto-restart-services
sudo mkdir -p /etc/needrestart
echo "\$nrconf{restart} = 'a';" | sudo tee -a /etc/needrestart/needrestart.conf > /dev/null

PYTHON_VERSION="$1"
TEST_NAME="$2"

echo "--- System dependencies"

retry 5 sudo add-apt-repository --yes ppa:deadsnakes/ppa
retry 5 sudo apt-get update
retry 5 sudo apt-get install -y \
    "python${PYTHON_VERSION}" "python${PYTHON_VERSION}-dev" "python${PYTHON_VERSION}-venv" \
    dnsutils  # provides nslookup

echo "--- Python modules"

"python${PYTHON_VERSION}" -m venv .venv
source .venv/bin/activate
pip install nox

echo "--- Run IT serverless test \"$TEST_NAME\" :pytest:"

export RALLY_HOME=$HOME
export THESPLOG_FILE="${THESPLOG_FILE:-${RALLY_HOME}/.rally/logs/actor-system-internal.log}"
# this value is in bytes, the default is 50kB. We increase it to 200kiB.
export THESPLOG_FILE_MAXSIZE=${THESPLOG_FILE_MAXSIZE:-204800}
# adjust the default log level from WARNING
export THESPLOG_THRESHOLD="INFO"

case $TEST_NAME in
    "user")
        nox -s it_serverless
        ;;
    "operator")
        nox -s it_serverless -- --operator
        ;;
    *)
        echo "Unknown test type."
        exit 1
        ;;
esac
