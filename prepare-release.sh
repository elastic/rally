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
__NOTICE_OUTPUT_FILE="NOTICE.txt"


echo "============================="
echo "Preparing Rally release $RELEASE_VERSION"
echo "============================="

echo "Preparing ${__NOTICE_OUTPUT_FILE}"
source create-notice.sh

echo "Updating author information"
git log --format='%aN' | LC_ALL=en_US.UTF-8 sort -u > AUTHORS
# This will produce a non-zero exit code iff there are changes.
# Obviously we should disable exiting on error temporarily.
set +e
git diff --exit-code
set -e

echo "Updating changelog"
# For exit on error to work we have to separate
#  CHANGELOG.md generation into two steps.
CHANGELOG="$(python3 changelog.py ${RELEASE_VERSION})"
printf "$CHANGELOG\n\n$(cat CHANGELOG.md)" > CHANGELOG.md

echo "Updating release version number"
printf '__version__ = "%s"\n' $RELEASE_VERSION > esrally/_version.py
git commit -a -m "Bump version to $RELEASE_VERSION"

pip install --editable .

# Check version
if ! [[ $(esrally --version) =~ "esrally ${RELEASE_VERSION} (git revision" ]]
then
    echo "ERROR: Rally version string [$(esrally --version)] does not start with expected version string [esrally $RELEASE_VERSION]"
    exit 2
fi

echo ""
echo "===================="
echo "Please open a pull request for ${RELEASE_VERSION}"
echo "===================="
