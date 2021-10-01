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

# Prerequisites for releasing:

# pip3 install twine sphinx sphinx_rtd_theme
# pip3 install --pre github3.py (see changelog.py)

# fail this script immediately if any command fails with a non-zero exit code
set -eu

RELEASE_VERSION=$1
NEXT_RELEASE="$2.dev0"
__NOTICE_OUTPUT_FILE="NOTICE.txt"


echo "============================="
echo "Preparing Rally release $RELEASE_VERSION"
echo "============================="

echo "Preparing ${__NOTICE_OUTPUT_FILE}"
source create-notice.sh

echo "Updating author information"
git log --format='%aN' | sort -u > AUTHORS
# This will produce a non-zero exit code iff there are changes.
# Obviously we should disable exiting on error temporarily.
set +e
git diff --exit-code
set -e
exit_code=$?
if [[ ${exit_code} != "0" ]]
then
    git commit -a -m "Update AUTHORS for Rally release $RELEASE_VERSION"
fi

echo "Updating changelog"
# For exit on error to work we have to separate 
#  CHANGELOG.md generation into two steps.
CHANGELOG="$(python3 changelog.py ${RELEASE_VERSION})"
printf "$CHANGELOG\n\n$(cat CHANGELOG.md)" > CHANGELOG.md
git commit -a -m "Update changelog for Rally release $RELEASE_VERSION"

# * Update version in `setup.py` and `docs/conf.py`
echo "Updating release version number"
echo "$RELEASE_VERSION" > version.txt
git commit -a -m "Bump version to $RELEASE_VERSION"

# --upgrade is required for virtualenv
python3 setup.py develop --upgrade

# Check version
if ! [[ $(esrally --version) =~ "esrally ${RELEASE_VERSION} (git revision" ]]
then
    echo "ERROR: Rally version string [$(esrally --version)] does not start with expected version string [esrally $RELEASE_VERSION]"
    exit 2
fi

# Build new version
python3 setup.py bdist_wheel
# Upload to PyPI
printf "\033[0;31mUploading to PyPI. Please enter your credentials ...\033[0m\n"
twine upload dist/esrally-${RELEASE_VERSION}-*.whl

# Create (signed) release tag
git tag -s "${RELEASE_VERSION}" -m "Rally release $RELEASE_VERSION"
git push --tags

# Update version to next dev version
echo "$NEXT_RELEASE" > version.txt

# Install locally for development
python3 setup.py develop --upgrade

git commit -a -m "Continue in $NEXT_RELEASE"
git push origin master

# Prepare offline installation package
# source scripts/offline-install.sh "${RELEASE_VERSION}"

echo ""
echo "===================="
echo "Released Rally ${RELEASE_VERSION}"
echo "===================="
echo ""
echo "Manual tasks:"
echo ""
echo "* Close milestone on Github: https://github.com/elastic/rally/milestones"
echo "* Upload offline install package to Github: https://github.com/elastic/rally/releases/edit/$RELEASE_VERSION"
echo "* Build and publish Docker image using: https://elasticsearch-ci.elastic.co/view/All/job/elastic+rally-release-docker+master specifying $RELEASE_VERSION"
echo "* Announce on Discuss: https://discuss.elastic.co/c/annoucements"
