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

function build {
  export PATH="$HOME/.pyenv/bin:$PATH"
  export TERM=dumb
  export LC_ALL=en_US.UTF-8
  eval "$(pyenv init -)"
  eval "$(pyenv virtualenv-init -)"
  make prereq
  make install
  make precommit
  make it
}

function archive {
  # Treat unset env variables as an error, but only in this function as there are other functions that allow unset variables
  set -u

  # this will only be done if the build number variable is present
  RALLY_DIR=${RALLY_HOME}/.rally
  if [[ -d ${RALLY_DIR} ]]; then
    find ${RALLY_DIR} -name "*.log" -printf "%P\\0" | tar -cvjf ${RALLY_DIR}/${BUILD_NUMBER}.tar.bz2 -C ${RALLY_DIR} --transform "s,^,ci-${BUILD_NUMBER}/," --null -T -
  else
    echo "Rally directory not present, this should not happen"
    exit 1
  fi
}

if declare -f "$1" > /dev/null; then
    $1
    exit
else
    echo "Please specify a function to run"
    exit 1
fi
