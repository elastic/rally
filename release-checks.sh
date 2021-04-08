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
set -eu
RELEASE_VERSION=$1

KERNEL_NAME=$(uname -s)
if [[ ${KERNEL_NAME} != *"Linux"* ]]
then
  echo "Error: release needs to be run on a Linux workstation but you are running on ${KERNEL_NAME}."
  echo "Switch to a Linux workstation and try again."
  exit 1
fi

# test number of parameters
if [[ $# != 2 ]]
then
    echo "Usage: make release release_version=RELEASE_VERSION next_version=NEXT_VERSION"
    exit 1
fi

if ! git config user.signingkey >/dev/null
then
    echo "Error: the variable user.signingkey is not configured for git on this system."
    echo "The release process requires a valid gpg key configured both locally and on GitHub."
    echo "Please follow the instructions in https://git-scm.com/book/id/v2/Git-Tools-Signing-Your-Work"
    echo "to set your gpg for git."
    exit 1
fi

if [[ ! -f ~/.github/rally_release_changelog.token ]]
then
    echo "Error: didn't find a valid GitHub token in ~/.github/rally_release_changelog.token."
    echo "The release process requires a valid GitHub token."
    exit 1
fi

if [[ ! -v GPG_TTY ]]
then
    echo "Error: env variable GPG_TTY is not set. Please execute export GPG_TTY=\$(tty)"
    exit 1
fi

ORIGIN_URL=$(git remote get-url --push origin)
if [[ ${ORIGIN_URL} != *"elastic/rally"* ]]
then
    echo "Error: the git remote [origin] does not point to Rally's main repo at elastic/rally but to [${ORIGIN_URL}]."
    exit 1
fi

# Check if there will be any errors during CHANGELOG.md generation
CHANGELOG="$(python3 changelog.py ${RELEASE_VERSION})"

