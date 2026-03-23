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

# fail this script immediately if any command fails with a non-zero exit code
set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

warn_skipped_release_checks() {
	# macOS / Linux-in-Docker: only changelog + token are validated here.
	echo "" >&2
	if command -v tput >/dev/null 2>&1 && [[ -t 2 ]]; then
		printf '%srelease-checks:%s skipping GPG signing, GPG_TTY, and git origin checks on this platform.\n' "$(tput bold)" "$(tput sgr0)" >&2
	else
		echo "release-checks: skipping GPG signing, GPG_TTY, and git origin checks on this platform." >&2
	fi
	echo "Run 'make release-checks RELEASE_VERSION=...' on Linux outside Docker before tagging if you rely on those checks." >&2
	echo "See https://codex.elastic.dev/r/elasticsearch-team/teams/performance/runbooks/rally-release-process and docs/developing.rst (Preparing a release)." >&2
	echo "" >&2
}

if [[ $# != 1 ]]; then
	echo "Usage: make release-checks RELEASE_VERSION=x.z.y"
	exit 1
fi

RELEASE_VERSION=$1

# Same path as scripts/release/changelog.py: optional override for host or release-checks.
: "${RALLY_CHANGELOG_TOKEN:=$HOME/.github/rally_release_changelog.token}"
export RALLY_CHANGELOG_TOKEN

KERNEL_NAME=$(uname -s)
if [[ ${KERNEL_NAME} != *"Linux"* ]]; then
	warn_skipped_release_checks
	if [[ ! -f "$RALLY_CHANGELOG_TOKEN" ]]; then
		echo "Error: didn't find a valid GitHub token at ${RALLY_CHANGELOG_TOKEN}."
		echo "The release process requires a valid GitHub token (set RALLY_CHANGELOG_TOKEN or use the default under ~/.github/)."
		exit 1
	fi
	python3 "${SCRIPT_DIR}/changelog.py" "${RELEASE_VERSION}" >/dev/null
	exit 0
fi

# Linux in Docker (e.g. prepare-docker.sh): token + changelog only; no GPG on host.
if [[ -f /.dockerenv ]]; then
	warn_skipped_release_checks
	if [[ ! -f "$RALLY_CHANGELOG_TOKEN" ]]; then
		echo "Error: didn't find a valid GitHub token at ${RALLY_CHANGELOG_TOKEN}."
		echo "The release process requires a valid GitHub token (set RALLY_CHANGELOG_TOKEN or use the default under ~/.github/)."
		exit 1
	fi
	python3 "${SCRIPT_DIR}/changelog.py" "${RELEASE_VERSION}" >/dev/null
	exit 0
fi

if ! git config user.signingkey >/dev/null; then
	echo "Error: the variable user.signingkey is not configured for git on this system."
	echo "The release process requires a valid gpg key configured both locally and on GitHub."
	echo "Please follow the instructions in https://git-scm.com/book/id/v2/Git-Tools-Signing-Your-Work"
	echo "to set your gpg for git."
	exit 1
fi

if [[ ! -f "$RALLY_CHANGELOG_TOKEN" ]]; then
	echo "Error: didn't find a valid GitHub token at ${RALLY_CHANGELOG_TOKEN}."
	echo "The release process requires a valid GitHub token (set RALLY_CHANGELOG_TOKEN or use the default under ~/.github/)."
	exit 1
fi

if [[ -z "${GPG_TTY:-}" ]]; then
	echo "Error: env variable GPG_TTY is not set. Please execute export GPG_TTY=\$(tty)"
	exit 1
fi

ORIGIN_URL=$(git remote get-url --push origin)
if [[ ${ORIGIN_URL} != *"elastic/rally"* ]]; then
	echo "Error: the git remote [origin] does not point to Rally's main repo at elastic/rally but to [${ORIGIN_URL}]."
	exit 1
fi

# Check if there will be any errors during CHANGELOG.md generation
python3 "${SCRIPT_DIR}/changelog.py" "${RELEASE_VERSION}" >/dev/null
