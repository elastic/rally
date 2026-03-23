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

# Builds scripts/release/Dockerfile and runs scripts/release/prepare.sh in a container
# with the repo bind-mounted. The container sets PREPARE_RELEASE_NO_VERIFY so the
# version bump commit skips git hooks inside the container.
# Release workflow (tokens, milestones, PyPI, release-checks expectations): https://codex.elastic.dev/r/elasticsearch-team/teams/performance/runbooks/rally-release-process
#
# Image: scripts/release/Dockerfile (Python 3.13, jq, uv, make, git, …).
#
# Usage:
#   ./scripts/release/prepare-docker.sh <release_version>
#
# Prerequisites:
#   - GitHub milestone on elastic/rally titled exactly <release_version>
#     (scripts/release/changelog.py opens, reopens, or creates one as needed; see Codex runbook).
#   - Token file for changelog.py: default path ~/.github/rally_release_changelog.token
#     Override with RALLY_CHANGELOG_TOKEN_FILE=/path/to/file (same env var as changelog.py / checks.sh on the host)
#
# Git identity for the commit step:
#   Host ~/.gitconfig is bind-mounted read-only to ${CONTAINER_HOME}/.gitconfig when
#   that file exists (same as $HOME inside the container). Override host path with
#   RALLY_GITCONFIG if needed. You may still export GIT_AUTHOR_* / GIT_COMMITTER_* to
#   override author for the commit.
#
# Optional environment:
#   DOCKER_IMAGE   — image tag to build/run (default rally-prepare-release:local)
#   RALLY_PREPARE_RELEASE_SKIP_BUILD — if set (non-empty), skip docker build and use
#                                      existing DOCKER_IMAGE
#   CONTAINER_HOME — default /tmp; changelog.py reads $HOME/.github/... so the token
#                    is mounted under ${CONTAINER_HOME}/.github/...
#   RALLY_GITCONFIG — host path to gitconfig (default ~/.gitconfig); mounted to
#                     ${CONTAINER_HOME}/.gitconfig when the file exists
#   DOCKER_USER    — uid:gid for `scripts/release/prepare.sh` inside the container (default:
#                    host $(id -u):$(id -g) so bind-mounted files are not root-owned).
#                    The container starts as root only long enough to create/chown the venv volume.
#   A named Docker volume is mounted at /workspace/.venv so the host .venv is not used
#   (avoids broken cross-OS Python symlinks). The entrypoint installs github3.py into that
#   venv (same pin as scripts/release/Dockerfile) before prepare.sh runs changelog.py.
#   Remove the volume for a fresh env:
#     docker volume rm rally-prepare-release-venv
#
# TTY: uses docker -it only when stdout is a terminal; otherwise -i (e.g. CI).
#
# fail this script immediately if any command fails with a non-zero exit code
set -e
# Treat unset env variables as an error
set -u
# fail on pipeline errors, e.g. when grepping
set -o pipefail

usage() {
	echo "Usage: $0 <release_version>" >&2
	echo "Example: $0 2.13.0" >&2
	echo "Requires changelog token file at ~/.github/rally_release_changelog.token (or set RALLY_CHANGELOG_TOKEN_FILE to a path)." >&2
	exit 1
}

if [[ $# -ne 1 ]]; then
	usage
fi

VERSION="$1"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TOKEN_FILE="${RALLY_CHANGELOG_TOKEN_FILE:-$HOME/.github/rally_release_changelog.token}"
GITCONFIG_HOST="${RALLY_GITCONFIG:-$HOME/.gitconfig}"
DOCKER_IMAGE="${DOCKER_IMAGE:-rally-prepare-release:local}"
CONTAINER_HOME="${CONTAINER_HOME:-/tmp}"
DOCKERFILE_PREPARE_RELEASE="${REPO_ROOT}/scripts/release/Dockerfile"

if [[ ! -f "$TOKEN_FILE" ]]; then
	echo "error: token file not found: $TOKEN_FILE" >&2
	exit 1
fi

DOCKER_USER="${DOCKER_USER:-$(id -u):$(id -g)}"
HOST_UID="${DOCKER_USER%%:*}"
HOST_GID="${DOCKER_USER#*:}"

DOCKER_ARGS=(--rm)
# -t requires a real TTY; omit in CI / piped environments
if [[ -t 1 ]]; then
	DOCKER_ARGS+=(-it)
else
	DOCKER_ARGS+=(-i)
fi
DOCKER_ARGS+=(
	-e "HOME=${CONTAINER_HOME}"
	-e PREPARE_RELEASE_NO_VERIFY=1
	-e GIT_AUTHOR_NAME
	-e GIT_AUTHOR_EMAIL
	-e GIT_COMMITTER_NAME
	-e GIT_COMMITTER_EMAIL
	-v "${REPO_ROOT}:/workspace"
	-v rally-prepare-release-venv:/workspace/.venv
	-v "${TOKEN_FILE}:${CONTAINER_HOME}/.github/rally_release_changelog.token:ro"
	-w /workspace
)

if [[ -f "$GITCONFIG_HOST" ]]; then
	DOCKER_ARGS+=(-v "${GITCONFIG_HOST}:${CONTAINER_HOME}/.gitconfig:ro")
else
	echo "warning: gitconfig not found at $GITCONFIG_HOST; git commit may fail without user.name/user.email" >&2
fi

if [[ -z "${RALLY_PREPARE_RELEASE_SKIP_BUILD:-}" ]]; then
	docker build -f "${DOCKERFILE_PREPARE_RELEASE}" -t "${DOCKER_IMAGE}" "${REPO_ROOT}"
fi

# github3.py version must match pyproject.toml develop extra and scripts/release/Dockerfile.
docker run "${DOCKER_ARGS[@]}" --user root "${DOCKER_IMAGE}" \
	sh -c "mkdir -p /workspace/.venv && \
		if [ ! -x /workspace/.venv/bin/python ]; then python3 -m venv /workspace/.venv; fi && \
		/workspace/.venv/bin/pip install \"github3.py==3.2.0\" && \
		chown -R \"${HOST_UID}:${HOST_GID}\" /workspace/.venv && \
		cd /workspace && \
		exec setpriv --reuid=\"${HOST_UID}\" --regid=\"${HOST_GID}\" --clear-groups -- \
			env VIRTUAL_ENV=/workspace/.venv PATH=/workspace/.venv/bin:\$PATH scripts/release/prepare.sh \"${VERSION}\""
