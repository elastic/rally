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

# Installs Rally with uv, using exactly the dependency versions Rally was tested with.
#
#   curl -sSf https://raw.githubusercontent.com/elastic/rally/master/scripts/install.sh | bash
#   curl -sSf https://raw.githubusercontent.com/elastic/rally/master/scripts/install.sh | bash -s -- --version 2.14.0
#   curl -sSf https://raw.githubusercontent.com/elastic/rally/master/scripts/install.sh | bash -s -- --uninstall
#
# See docs/install.rst for details.

# fail this script immediately if any command fails with a non-zero exit code
set -e
# Treat unset env variables as an error
set -u
# fail on pipeline errors, e.g. when grepping
set -o pipefail

# Python that Rally runs with, the one Rally's Docker image ships
readonly PYTHON_VERSION="3.13"
# uv that this script downloads and uses, whether or not uv is already installed
readonly UV_VERSION="0.12.23"

RALLY_VERSION=""
UNINSTALL="false"

function usage {
    cat <<EOF
Usage: $0 [--version RALLY_VERSION]
       $0 --uninstall

Installs Rally (esrally) as a uv tool, with Python ${PYTHON_VERSION} and all dependencies pinned to
the versions the release was tested with. It uses a temporary uv that it deletes afterwards, so
it does not change an existing uv installation or your shell configuration.

Options:
  --version VERSION  Rally release to install, e.g. 2.14.0 (default: latest release on PyPI)
  --uninstall        Uninstall Rally. Keeps Rally's configuration and data in ~/.rally
  --help             Show this help

To upgrade Rally, run this script again.
EOF
}

function info {
    echo "==> $*"
}

function die {
    echo "Error: $*" >&2
    exit 1
}

function parse_args {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --version)
                if [[ $# -lt 2 ]]; then
                    die "--version needs a value. See --help."
                fi
                RALLY_VERSION="$2"
                shift 2
                ;;
            --uninstall)
                UNINSTALL="true"
                shift
                ;;
            --help)
                usage
                exit 0
                ;;
            *)
                usage >&2
                die "unknown option: $1"
                ;;
        esac
    done
}

# Downloads uv into WORK_DIR with uv's installer and sets UV. With UV_UNMANAGED_INSTALL, the
# installer neither touches an existing uv installation nor the user's shell configuration.
# UV_INSTALL_DIR would take precedence, so it is unset.
function download_uv {
    info "Downloading uv ${UV_VERSION} to a temporary directory"
    curl -LsSf "https://astral.sh/uv/${UV_VERSION}/install.sh" |
        env -u UV_INSTALL_DIR UV_UNMANAGED_INSTALL="${WORK_DIR}/uv" sh -s -- --quiet
    UV="${WORK_DIR}/uv/uv"
}

# Downloads esrally from PyPI and extracts its constraints. Sets RALLY_VERSION and CONSTRAINTS_FILE.
# The version and the constraints come from the very same wheel, so they cannot disagree.
function fetch_constraints {
    local spec="esrally${RALLY_VERSION:+==${RALLY_VERSION}}"
    local extract_dir="${WORK_DIR}/wheel"

    info "Downloading ${spec} to read its dependency constraints"
    # Downloads the managed Python if necessary, without adding python3.X executables to the PATH
    "${UV}" pip install --quiet --managed-python --python "${PYTHON_VERSION}" --no-deps --only-binary :all: \
        --target "${extract_dir}" "${spec}"

    local dist_info=("${extract_dir}"/esrally-*.dist-info)
    RALLY_VERSION="${dist_info[0]##*/esrally-}"
    RALLY_VERSION="${RALLY_VERSION%.dist-info}"

    CONSTRAINTS_FILE="${extract_dir}/esrally/resources/rally-constraints.txt"
    if [[ ! -s "${CONSTRAINTS_FILE}" ]]; then
        die "esrally ${RALLY_VERSION} does not ship its dependency constraints, so its dependencies cannot be pinned. Releases newer than 2.13.0 ship them; see https://esrally.readthedocs.io/en/stable/install.html for installation methods that do not pin dependencies."
    fi
}

function uninstall_rally {
    "${UV}" tool uninstall esrally
    info "Rally is uninstalled."
    cat <<EOF
These are kept, in case you still need them:
  Rally's configuration and data: ${RALLY_HOME:-${HOME}}/.rally
  Pythons downloaded by uv: $("${UV}" python dir)
EOF
}

function install_rally {
    local bin_dir shell_config
    fetch_constraints

    info "Installing esrally ${RALLY_VERSION} with Python ${PYTHON_VERSION}"
    # uv records the pinned version and the constraints in the tool's receipt, and installs again
    # when they differ from an existing esrally tool's, for example one installed without them.
    # --quiet hides uv's PATH hint, which suggests a temporary uv; this script gives its own.
    "${UV}" tool install --quiet --managed-python --python "${PYTHON_VERSION}" \
        --constraints "${CONSTRAINTS_FILE}" "esrally==${RALLY_VERSION}"
    bin_dir="$("${UV}" tool dir --bin)"
    "${bin_dir}/esrally" --version

    echo ""
    info "Rally ${RALLY_VERSION} is installed. Run 'esrally --help' to get started."
    if [[ ":${PATH}:" != *":${bin_dir}:"* ]]; then
        case "$(basename "${SHELL:-}")" in
            zsh) shell_config="${HOME}/.zshrc" ;;
            bash) shell_config="${HOME}/.bashrc" ;;
            *) shell_config="${HOME}/.profile" ;;
        esac
        echo "${bin_dir} is not on your PATH. To add it, run this command and open a new shell:"
        echo ""
        echo "    echo 'export PATH=\"${bin_dir}:\$PATH\"' >> ${shell_config}"
        echo ""
    fi
    echo "To upgrade Rally, run this script again. To uninstall it, run this script with --uninstall."
}

function main {
    parse_args "$@"

    # Elasticsearch refuses to start as root, and so Rally cannot provision it.
    if [[ "$(id -u)" -eq 0 ]]; then
        die "do not run this script as root. Rally cannot start Elasticsearch as root; run it as a regular user."
    fi

    WORK_DIR="$(mktemp -d)"
    trap 'rm -rf "${WORK_DIR}"' EXIT

    download_uv
    # Run uv from an empty directory: 'uv pip' would otherwise pick up the uv.toml, pyproject.toml
    # or .venv of wherever the user runs this script, while 'uv tool' ignores them.
    cd "${WORK_DIR}"

    if [[ "${UNINSTALL}" == "true" ]]; then
        uninstall_rally
    else
        install_rally
    fi
}

main "$@"
