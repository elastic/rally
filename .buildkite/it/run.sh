#!/usr/bin/env bash

set -exo pipefail

function upload_logs {
    echo "--- Upload artifacts"
    tar zcf "${RALLY_HOME}/rally-logs.tar.gz" "${RALLY_HOME}/.rally/logs"
    buildkite-agent artifact upload "${RALLY_HOME}/rally-logs.tar.gz"
    tar zcf "${RALLY_HOME}/rally-es-logs.tar.gz" "${RALLY_HOME}/.rally/benchmarks/races"/*/*/logs
    buildkite-agent artifact upload "${RALLY_HOME}/rally-es-logs.tar.gz"
}

export TERM=dumb
export LC_ALL=en_US.UTF-8
export TZ=Etc/UTC

echo "--- Check resources"

free
cat /proc/cpuinfo

PY_SHORT_VERSION="$1"

# Provided by the family/es-perf-ubuntu-2604 image (elastic/ci-agent-images, vm-images/es-perf)
export JAVA21_HOME=/usr/lib/jvm/java-21-openjdk-amd64

export PY_VERSION=$(jq -r ".python_versions.PY$(echo "${PY_SHORT_VERSION}" | tr -d '.')" .ci/variables.json)

echo "--- Install UV"

curl -LsSf https://astral.sh/uv/0.11.19/install.sh | env UV_UNMANAGED_INSTALL="${HOME}/.local/bin" sh
export PATH="${HOME}/.local/bin:${PATH}"

echo "--- Install Python ${PY_VERSION}"

uv python install "${PY_VERSION}"

echo "--- Create virtual environment"

make venv

echo "--- Run IT test :pytest:"

export RALLY_HOME=$HOME
export THESPLOG_FILE="${THESPLOG_FILE:-${RALLY_HOME}/.rally/logs/actor-system-internal.log}"
# this value is in bytes, the default is 50kB. We increase it to 10MiB.
export THESPLOG_FILE_MAXSIZE=${THESPLOG_FILE_MAXSIZE:-10485760}
# adjust the default log level from WARNING
export THESPLOG_THRESHOLD="INFO"
export TERM=dumb
export LC_ALL=en_US.UTF-8

trap upload_logs ERR

make it

upload_logs
