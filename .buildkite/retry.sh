#!/usr/bin/env bash

# based on https://gist.github.com/sj26/88e1c6584397bb7c13bd11108a579746?permalink_comment_id=4155247#gistcomment-4155247
function retry {
  local retries=$1
  shift
  local cmd=("$@")
  local cmd_string="$*"
  local count=0

  # be lenient with non-zero exit codes, to allow retries
  set +o errexit
  set +o pipefail
  until "${cmd[@]}"; do
    retcode=$?
    wait=$(( 2 ** count ))
    count=$(( count + 1))
    if [[ $count -le $retries ]]; then
      printf "Command [%s] failed. Retry [%d/%d] in [%d] seconds.\n" "${cmd_string}" "${count}" "${retries}" "${wait}"
      sleep $wait
    else
      printf "Exhausted all [%s] retries for command [%s]. Exiting.\n" "${cmd_string}" "${retries}"
      # restore settings to fail immediately on error
      set -o errexit
      set -o pipefail
      return ${retcode}
    fi
  done
  # restore settings to fail immediately on error
  set -o errexit
  set -o pipefail
  return 0
}
