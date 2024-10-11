#!/usr/bin/env bash
set -Eeo pipefail

if [[ $1 == *"bash"* || $1 == *"sh"* ]]; then
    : # noop
elif [[ $1 != "esrally" ]]; then
    set -- esrally "$@"
fi

exec "$@"
