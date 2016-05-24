#!/usr/bin/env bash

# fail this script immediately if any command fails with a non-zero exit code
set -e

# We assume here that this script will stay in the Rally git root directory (it does not make sense in any other place anyway)

# see http://stackoverflow.com/a/246128
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
RALLY_SRC_HOME="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

pushd . >/dev/null 2>&1
cd ${RALLY_SRC_HOME} >/dev/null 2>&1

# see http://unix.stackexchange.com/a/155077
if output=$(git status --porcelain) && [ -z "$output" ] && on_master=$(git rev-parse --abbrev-ref HEAD) && [ "$on_master" == "master" ]
then
  # Working directory clean -> we assume this is a user that is not actively developing Rally and just upgrade it every time it is invoked
  echo "Auto-updating Rally"

  # this will fail if the user is offline
  git fetch origin --quiet
  git rebase origin/master --quiet
  python3 setup.py -q develop --user
# else
  # Uncommitted changes - don't upgrade, just run
fi

popd >/dev/null 2>&1

RALLY_ROOT=$(python3 -c "import site; print(site.USER_BASE)")
${RALLY_ROOT}/bin/esrally "$@"