#!/usr/bin/env bash

# fail this script immediately if any command fails with a non-zero exit code
set -eu
RELEASE_VERSION=$1

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
    echo "The release process requires a valid GitHub token. See RELEASE.md for details."
    exit 1
fi

if [[ $(uname) == "Darwin" && -z "${GPG_TTY+set}" ]]
then
    echo "Error: to allow git to create signed commits on Mac OS you need to set \"export GPG_TTY=\$(tty)\"."
    exit 1
fi

# Check if there will be any errors during CHANGELOG.md generation
CHANGELOG="$(python3 changelog.py ${RELEASE_VERSION})"

