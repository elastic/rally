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

##########################################################################################
#
# Internal helper script to actually run either Rally or Rally daemon.
#
# Do not invoke directly but rather use the `rally` and `rallyd` scripts.
#
##########################################################################################

readonly BINARY_NAME="${__RALLY_INTERNAL_BINARY_NAME}"
readonly HUMAN_NAME="${__RALLY_INTERNAL_HUMAN_NAME}"

install_esrally_with_setuptools () {
    # Check if optional parameter with Rally binary path, points to an existing executable file.
    if [[ $# -ge 1 && -n $1 ]]; then
        if [[ -f $1 && -x $1 ]]; then return; fi
    fi

    if [[ ${IN_VIRTUALENV} == 0 ]]; then
        # https://setuptools.readthedocs.io/en/latest/setuptools.html suggests not invoking setup.py directly
        # Also workaround system pip conflicts, https://github.com/pypa/pip/issues/5599
        python3 -m pip install --quiet --user --upgrade --editable .[develop]
    else
        python3 -m pip install --quiet --upgrade --editable .[develop]
    fi
}

# Attempt to update Rally itself by default but allow user to skip it.
SELF_UPDATE=YES
# Assume that the "main remote" is called "origin"
REMOTE="origin"

# While we could also check via the presence of `VIRTUAL_ENV` this is a bit more reliable.
# Check for both pyvenv and normal venv environments
# https://www.python.org/dev/peps/pep-0405/
if python3 -c 'import os, sys; sys.exit(0) if "VIRTUAL_ENV" in os.environ else sys.exit(1)' >/dev/null 2>&1
then
    IN_VIRTUALENV=1
else
    IN_VIRTUALENV=0
fi

# Check for parameters that are intended for this script. Note that they only work if they're specified at the beginning (due to how
# the shell builtin `shift` works. We could make it work for arbitrary positions but that's not worth the complexity for such an
# edge case).
for i in "$@"
do
case ${i} in
    --update-from-remote=*)
    REMOTE="${i#*=}"
    shift # past argument=value
    ;;
    --skip-update)
    SELF_UPDATE=NO
    shift # past argument with no value
    ;;
    # inspect Rally's command line options and skip update also if the user has specified --offline.
    #
    # Note that we do NOT consume this option as it needs to be passed to Rally.
    --offline)
    SELF_UPDATE=NO
    # DO NOT CONSUME!!
    ;;
    # Do not consume unknown parameters; they should still be passed to the actual Rally script
    #*)
esac
done

if [[ $SELF_UPDATE == YES ]]
then
    # see http://unix.stackexchange.com/a/155077
    if output=$(git status --porcelain) && [ -z "$output" ] && on_master=$(git rev-parse --abbrev-ref HEAD) && [ "$on_master" == "master" ]
    then
      # Working directory clean -> we assume this is a user that is not actively developing Rally and just upgrade it every time it is invoked
      set +e
      # this will fail if the user is offline
      git fetch ${REMOTE} --quiet >/dev/null 2>&1
      exit_code=$?
      set -e
      if [[ $exit_code == 0 ]]
      then
        echo "Auto-updating Rally from ${REMOTE}"
        git rebase ${REMOTE}/master --quiet
        install_esrally_with_setuptools
      #else
      # offline - skipping update
      fi
    else
      >&2 echo "There are uncommitted changes. Please cleanup your working copy or specify --skip-update."
      exit 1
    fi
#else -> No self update
fi

popd >/dev/null 2>&1

# write the actor system's log file to a well-known location (but let the user override it with the same env variable)
export THESPLOG_FILE="${THESPLOG_FILE:-${HOME}/.rally/logs/actor-system-internal.log}"
# this value is in bytes, the default is 50kB. We increase it to 200kiB.
export THESPLOG_FILE_MAXSIZE=${THESPLOG_FILE_MAXSIZE:-204800}
# adjust the default log level from WARNING
export THESPLOG_THRESHOLD="INFO"

# Provide a consistent binary name to the user and hide the fact that we call another binary under the hood.
export RALLY_ALTERNATIVE_BINARY_NAME=$(basename "$0")
if [[ $IN_VIRTUALENV == 0 ]]
then
    RALLY_ROOT=$(python3 -c "import site; print(site.USER_BASE)")
    RALLY_BIN=${RALLY_ROOT}/bin/${BINARY_NAME}
    install_esrally_with_setuptools "${RALLY_BIN}"
    if [[ -x $RALLY_BIN ]]; then
        ${RALLY_BIN} "$@"
    else
        echo "Cannot execute ${HUMAN_NAME} in ${RALLY_BIN}."
    fi
else
    install_esrally_with_setuptools "${BINARY_NAME}"

    ${BINARY_NAME} "$@"
fi
