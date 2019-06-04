#!/usr/bin/env bash
set -Eeo pipefail

if [[ $1 == *"bash"* || $1 == *"sh"* ]]; then
    : # noop
elif [[ "$*" == *"--help"* || "$*" == *"list "* ]]; then
    set -- esrally "$@" # allow --help, --list commands to be passed as is
elif [[ $1 != "esrally" ]]; then
    set -- esrally "$@"
fi

exec "$@"
