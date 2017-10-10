#!/usr/bin/env bash

##########################################################################################
#
# Internal helper script to actually run either Rally or Rally daemon.
#
# Do not invoke directly but rather use the `rally` and `rallyd` scripts.
#
##########################################################################################

BINARY_NAME="${__RALLY_INTERNAL_BINARY_NAME}"
HUMAN_NAME="${__RALLY_INTERNAL_HUMAN_NAME}"

# Attempt to update Rally itself by default but allow user to skip it.
SELF_UPDATE=YES
# Assume that the "main remote" is called "origin"
REMOTE="origin"

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

if [ ${SELF_UPDATE} == YES ]
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
      if [ ${exit_code} == 0 ]
      then
        echo "Auto-updating Rally from ${REMOTE}"
        git rebase ${REMOTE}/master --quiet
        python3 setup.py -q develop --user
      #else
      # offline - skipping update
      fi
    # else
      # Uncommitted changes - don't upgrade, just run
    fi
#else -> No self update
fi

popd >/dev/null 2>&1

# Provide a consistent binary name to the user and hide the fact that we call another binary under the hood.
export RALLY_ALTERNATIVE_BINARY_NAME=$(basename "$0")
RALLY_ROOT=$(python3 -c "import site; print(site.USER_BASE)")
RALLY_BIN=${RALLY_ROOT}/bin/${BINARY_NAME}
if [ -x "$RALLY_BIN" ]
then
    ${RALLY_BIN} "$@"
else
    echo "Cannot execute ${HUMAN_NAME} in ${RALLY_BIN}."
fi