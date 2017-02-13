#!/usr/bin/env bash

# Prerequisites for releasing:

# pip3 install twine sphinx sphinx_rtd_theme
# pip3 install --pre github3.py (see changelog.py)

# fail this script immediately if any command fails with a non-zero exit code
set -e

# test number of parameters
if [ $# != 2 ]
then
    echo "Usage: $0 RELEASE_VERSION NEXT_VERSION"
    exit 1
fi

RELEASE_VERSION=$1
NEXT_RELEASE="$2.dev0"

echo "============================="
echo "Preparing Rally release $RELEASE_VERSION"
echo "============================="

echo "Running tests"
cd docs && make html && cd -
# run integration tests, note that this requires that tox is properly set up
tox

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
echo -e "$(python3 changelog.py ${RELEASE_VERSION})\n\n$(cat CHANGELOG.md)" > CHANGELOG.md
git commit -a -m "Update changelog for Rally release $RELEASE_VERSION"

# * Update version in `setup.py` and `docs/conf.py`
echo "Updating release version number"
echo "$RELEASE_VERSION" > version.txt
git commit -a -m "Bump version to $RELEASE_VERSION"

python3 setup.py develop

# Check version
if ! [[ "`esrally --version`" =~ "esrally ${RELEASE_VERSION} (git revision" ]]
then
    echo "ERROR: Rally version string [`esrally --version`] does not start with expected version string [esrally $RELEASE_VERSION]"
    exit 2
fi

# Build new version
python3 setup.py bdist_wheel
# Upload to PyPI
twine upload dist/esrally-${RELEASE_VERSION}-*.whl

# Create (signed) release tag
git tag -s ${RELEASE_VERSION} -m "Rally release $RELEASE_VERSION"
git push --tags

# Update version to next dev version
echo "$NEXT_RELEASE" > version.txt

# Install locally for development
python3 setup.py develop

git commit -a -m "Continue in $NEXT_RELEASE"
git push origin master

echo ""
echo "===================="
echo "Released Rally $RELEASE_VERSION"
echo "===================="
echo ""
echo "Manual tasks:"
echo ""
echo "* Activate version $RELEASE_VERSION: https://readthedocs.org/dashboard/esrally/version/$RELEASE_VERSION/"
echo "* Close milestone on Github: https://github.com/elastic/rally/milestones/$RELEASE_VERSION"
echo "* Announce on Discuss: https://discuss.elastic.co/c/annoucements"
