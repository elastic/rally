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

# Runs `make pre-commit` on the host first, then prepare-release.sh in Docker.
# The container sets PREPARE_RELEASE_NO_VERIFY so the version bump commit skips hooks
# (hooks already ran on the host).
#
# Image: scripts/Dockerfile.prepare-release (Python 3.13, jq, uv, make, git, …).
#
# Usage:
#   ./scripts/prepare-release-docker.sh <release_version>
#
# Prerequisites (same as on the host):
#   - Open GitHub milestone on elastic/rally titled exactly <release_version>
#     (see changelog.py); otherwise prepare-release.sh fails.
#   - Token file for changelog.py: default path ~/.github/rally_release_changelog.token
#     Override with RALLY_CHANGELOG_TOKEN=/path/to/file
#
# Git identity for the commit step:
#   Host ~/.gitconfig is bind-mounted read-only to ${CONTAINER_HOME}/.gitconfig when
#   that file exists (same as $HOME inside the container). Override host path with
#   RALLY_GITCONFIG if needed. You may still export GIT_AUTHOR_* / GIT_COMMITTER_* to
#   override author for the commit.
#
# Optional environment:
#   RALLY_PREPARE_RELEASE_SKIP_HOST_PRE_COMMIT — if set, skip `make pre-commit` on the host
#   DOCKER_IMAGE   — image tag to build/run (default rally-prepare-release:local)
#   RALLY_PREPARE_RELEASE_SKIP_BUILD — if set (non-empty), skip docker build and use
#                                      existing DOCKER_IMAGE
#   CONTAINER_HOME — default /tmp; changelog.py reads $HOME/.github/... so the token
#                    is mounted under ${CONTAINER_HOME}/.github/...
#   RALLY_GITCONFIG — host path to gitconfig (default ~/.gitconfig); mounted to
#                     ${CONTAINER_HOME}/.gitconfig when the file exists
#   DOCKER_USER    — container user as uid:gid for `docker run --user` (default:
#                    host $(id -u):$(id -g) so bind-mounted files are not root-owned).
#                    Release prep uses a venv under ${CONTAINER_HOME} so non-root installs
#                    work. Set to e.g. 0:0 only if you intentionally want root in the container.
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
	echo "Requires changelog token at ~/.github/rally_release_changelog.token (or RALLY_CHANGELOG_TOKEN)." >&2
	exit 1
}

if [[ $# -ne 1 ]]; then
	usage
fi

VERSION="$1"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TOKEN_FILE="${RALLY_CHANGELOG_TOKEN:-$HOME/.github/rally_release_changelog.token}"
GITCONFIG_HOST="${RALLY_GITCONFIG:-$HOME/.gitconfig}"
DOCKER_IMAGE="${DOCKER_IMAGE:-rally-prepare-release:local}"
CONTAINER_HOME="${CONTAINER_HOME:-/tmp}"
DOCKERFILE_PREPARE_RELEASE="${REPO_ROOT}/scripts/Dockerfile.prepare-release"

if [[ ! -f "$TOKEN_FILE" ]]; then
	echo "error: token file not found: $TOKEN_FILE" >&2
	exit 1
fi

if [[ -z "${RALLY_PREPARE_RELEASE_SKIP_HOST_PRE_COMMIT:-}" ]]; then
	(cd "${REPO_ROOT}" && make pre-commit)
fi

DOCKER_ARGS=(--rm)
# -t requires a real TTY; omit in CI / piped environments
if [[ -t 1 ]]; then
	DOCKER_ARGS+=(-it)
else
	DOCKER_ARGS+=(-i)
fi
DOCKER_ARGS+=(
	-e "HOME=${CONTAINER_HOME}"
	-e "RELEASE_VERSION=${VERSION}"
	-e PREPARE_RELEASE_NO_VERIFY=1
	-e GIT_AUTHOR_NAME
	-e GIT_AUTHOR_EMAIL
	-e GIT_COMMITTER_NAME
	-e GIT_COMMITTER_EMAIL
	-v "${REPO_ROOT}:/workspace"
	-v "${TOKEN_FILE}:${CONTAINER_HOME}/.github/rally_release_changelog.token:ro"
	-w /workspace
)

if [[ -f "$GITCONFIG_HOST" ]]; then
	DOCKER_ARGS+=(-v "${GITCONFIG_HOST}:${CONTAINER_HOME}/.gitconfig:ro")
else
	echo "warning: gitconfig not found at $GITCONFIG_HOST; git commit may fail without user.name/user.email" >&2
fi

# Default to host UID/GID so editable installs and egg-info on the bind mount are owned by you.
DOCKER_USER="${DOCKER_USER:-$(id -u):$(id -g)}"
DOCKER_ARGS+=(--user "${DOCKER_USER}")

if [[ -z "${RALLY_PREPARE_RELEASE_SKIP_BUILD:-}" ]]; then
	docker build -f "${DOCKERFILE_PREPARE_RELEASE}" -t "${DOCKER_IMAGE}" "${REPO_ROOT}"
fi

docker run "${DOCKER_ARGS[@]}" "${DOCKER_IMAGE}" bash -lc '
set -eux
# Pin matches project.optional-dependencies.develop (pyproject.toml).
GITHUB3_PY_PIN="github3.py==3.2.0"
VENV="${HOME}/.prepare-release-venv"
rm -rf "${VENV}"
uv venv "${VENV}"
# shellcheck source=/dev/null
. "${VENV}/bin/activate"
uv pip install --no-cache "${GITHUB3_PY_PIN}"
bash -x prepare-release.sh "${RELEASE_VERSION}"
'
