#!/usr/bin/env bash

set -eo pipefail

# based on https://gist.github.com/sj26/88e1c6584397bb7c13bd11108a579746?permalink_comment_id=4155247#gistcomment-4155247
function retry {
  local retries=$1
  shift
  local cmd=($@)
  local cmd_string="${@}"
  local count=0

  # be lenient with non-zero exit codes, to allow retries
  set +o errexit
  set +o pipefail
  until "${cmd[@]}"; do
    retcode=$?
    wait=$(( 2 ** count ))
    count=$(( count + 1))
    if [[ $count -le $retries ]]; then
      printf "Command [%s] failed. Retry [%d/%d] in [%d] seconds.\n" "$cmd_string" $count $retries $wait
      sleep $wait
    else
      printf "Exhausted all [%s] retries for command [%s]. Exiting.\n" "$cmd_string" $retries
      # restore settings to fail immediately on error
      set -o errexit
      set -o pipefail
      return $retcode
    fi
  done
  # restore settings to fail immediately on error
  set -o errexit
  set -o pipefail
  return 0
}
echo "--- System dependencies"

PYTHON_VERSION="$1"
retry 5 sudo add-apt-repository --yes ppa:deadsnakes/ppa
retry 5 sudo apt-get update
retry 5 sudo apt-get install -y \
    "python${PYTHON_VERSION}" "python${PYTHON_VERSION}-dev" "python${PYTHON_VERSION}-venv" \
    git make jq docker \
    openjdk-17-jdk-headless openjdk-11-jdk-headless
export JAVA11_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export JAVA17_HOME=/usr/lib/jvm/java-17-openjdk-amd64

echo "--- Run IT test :pytest:"

export RALLY_HOME=$HOME
export THESPLOG_FILE="${THESPLOG_FILE:-${RALLY_HOME}/.rally/logs/actor-system-internal.log}"
# this value is in bytes, the default is 50kB. We increase it to 200kiB.
export THESPLOG_FILE_MAXSIZE=${THESPLOG_FILE_MAXSIZE:-204800}
# adjust the default log level from WARNING
export THESPLOG_THRESHOLD="INFO"
export TERM=dumb
export LC_ALL=en_US.UTF-8

"python${PYTHON_VERSION}" -m venv .venv
source .venv/bin/activate

pip install nox
nox -s "it-${PYTHON_VERSION}"
