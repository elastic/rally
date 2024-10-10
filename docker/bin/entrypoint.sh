#!/usr/bin/env bash
set -Eeo pipefail

if [[ $1 == *"bash"* || $1 == *"sh"* ]]; then
    : # noop
elif [[ $1 == "esrallyd" && $2 == "start" ]]; then
    # We want to wait for esrallyd's child processes to exit before we exit
    set -x
    "$@" &
    pid=$!
    ps -e
    child_pids=$(pgrep -P $pid)
    echo "Started $1 with PID $pid. Waiting for child processes $child_pids to exit."
    wait "$child_pids"
    exit 0
elif [[ $1 != "esrally" ]]; then
    set -- esrally "$@"
fi

exec "$@"
