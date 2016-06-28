#!/usr/bin/env bash

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

# * Update version in `setup.py` and `docs/conf.py`
echo "$RELEASE_VERSION" > version.txt
git commit -a -m 'Bump version to $RELEASE_VERSION'

cd docs && make html && cd -
# run integration tests, note that this requires that tox is properly set up
tox

python3 setup.py develop

# Check version
if [ "`esrally --version`" != "esrally $RELEASE_VERSION" ]
then
    echo "ERROR: Rally version string [`esrally --version`] does not match expected version [esrally $RELEASE_VERSION]"
    exit 2
fi

# Build new version
python3 setup.py bdist_wheel
# Upload to PyPI
twine upload dist/esrally-${RELEASE_VERSION}-*.whl

# Create release tag
git tag -a ${RELEASE_VERSION} -m 'Rally release $RELEASE_VERSION'
git push --tags

# Update version to next dev version
echo "$NEXT_RELEASE" > version.txt

# Install locally for development
python3 setup.py develop

git commit -a -m 'Continue in $NEXT_RELEASE'
git push origin master

echo ""
echo "===================="
echo "Released Rally $RELEASE_VERSION"
echo "===================="
echo ""
echo "Manual tasks:"
echo ""
echo "* Close milestone on Github: https://github.com/elastic/rally/milestones/$RELEASE_VERSION"
echo "* Announce on Discuss: https://discuss.elastic.co/c/annoucements"
