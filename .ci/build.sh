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

# fail this script immediately if any command fails with a non-zero exit code
set -e
# fail on pipeline errors, e.g. when grepping
set -o pipefail
# fail on any unset environment variables
set -u

function update_pyenv {
  # need to have the latest pyenv version to ensure latest patch releases are installable
  cd $HOME/.pyenv/plugins/python-build/../.. && git pull origin master --rebase && cd -
}

function build {
  export THESPLOG_FILE="${THESPLOG_FILE:-${RALLY_HOME}/.rally/logs/actor-system-internal.log}"
  # this value is in bytes, the default is 50kB. We increase it to 200kiB.
  export THESPLOG_FILE_MAXSIZE=${THESPLOG_FILE_MAXSIZE:-204800}
  # adjust the default log level from WARNING
  export THESPLOG_THRESHOLD="INFO"

  # turn nounset off because some of the following commands fail if nounset is turned on
  set +u

  export PATH="$HOME/.pyenv/bin:$PATH"
  export TERM=dumb
  export LC_ALL=en_US.UTF-8
  update_pyenv
  eval "$(pyenv init -)"
  # ensure pyenv shims are added to PATH, see https://github.com/pyenv/pyenv/issues/1906
  eval "$(pyenv init --path)"
  eval "$(pyenv virtualenv-init -)"

  make prereq
  make install
  make precommit
  make it
}

function license-scan {
  # turn nounset off because some of the following commands fail if nounset is turned on
  set +u

  export PATH="$HOME/.pyenv/bin:$PATH"
  eval "$(pyenv init -)"
  # ensure pyenv shims are added to PATH, see https://github.com/pyenv/pyenv/issues/1906
  eval "$(pyenv init --path)"
  eval "$(pyenv virtualenv-init -)"

  make prereq
  # only install depdencies that are needed by end users
  make install-user
  fossa analyze
}

function archive {
  # Treat unset env variables as an error, but only in this function as there are other functions that allow unset variables
  set -u

  # this will only be done if the build number variable is present
  RALLY_DIR=${RALLY_HOME}/.rally
  if [[ -d ${RALLY_DIR} ]]; then
    find ${RALLY_DIR} -name "*.log" -printf "%P\\0" | tar -cvjf ${RALLY_DIR}/${BUILD_NUMBER}.tar.bz2 -C ${RALLY_DIR} --transform "s,^,ci-${BUILD_NUMBER}/," --null -T -
  else
    echo "Rally directory [${RALLY_DIR}] not present. Ensure the RALLY_DIR environment variable is correct"
    exit 1
  fi
}

if declare -F "$1" > /dev/null; then
    $1
    exit
else
    echo "Please specify a function to run"
    exit 1
fi
