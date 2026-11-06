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
#   curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/elastic/rally/master/scripts/install.sh | bash
#   curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/elastic/rally/master/scripts/install.sh | bash -s -- --version 2.14.0
#   curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/elastic/rally/master/scripts/install.sh | bash -s -- --uninstall
#
# See docs/install.rst for details.

# fail this script immediately if any command fails with a non-zero exit code
set -e
# Treat unset env variables as an error
set -u
# fail on pipeline errors, e.g. when grepping
set -o pipefail

# 3.13 is what Rally's Docker image ships.
readonly DEFAULT_PYTHON="3.13"
# uv to install if it is not installed yet
readonly UV_VERSION="0.12.23"
# Oldest uv known to work: older versions lack --managed-python.
readonly MIN_UV_VERSION="0.6.17"
# Where uv's installer puts uv
readonly BIN_DIR="${UV_INSTALL_DIR:-${XDG_BIN_HOME:-${HOME}/.local/bin}}"

RALLY_VERSION=""
PYTHON_VERSION="${DEFAULT_PYTHON}"
UNINSTALL="false"

function usage {
    cat <<EOF
Usage: $0 [--version RALLY_VERSION] [--python PYTHON_VERSION]
       $0 --uninstall

Installs Rally (esrally) as a uv tool with all dependencies pinned to the versions
the release was tested with. Installs uv first if it is not available.

Options:
  --version VERSION  Rally release to install, e.g. 2.14.0 (default: latest release on PyPI)
  --python VERSION   Python to run Rally with, 3.10 to 3.13 (default: ${DEFAULT_PYTHON})
  --uninstall        Uninstall Rally. Keeps uv and Rally's configuration and data in ~/.rally
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
            --version | --python)
                if [[ $# -lt 2 ]]; then
                    die "$1 needs a value. See --help."
                fi
                if [[ "$1" == "--version" ]]; then
                    RALLY_VERSION="$2"
                else
                    PYTHON_VERSION="$2"
                fi
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

    # Checked here because uv would also accept newer Pythons that Rally does not support yet
    if ! [[ ${PYTHON_VERSION} =~ ^3\.(10|11|12|13)(\.[0-9]+)?$ ]]; then
        die "unsupported Python version '${PYTHON_VERSION}'. Rally supports Python 3.10 to 3.13."
    fi
}

# Installs uv as its documentation recommends. The installer verifies the checksum of the uv it
# downloads and adds its directory to the PATH in the user's shell configuration.
function install_uv {
    info "Installing uv ${UV_VERSION}"
    curl -LsSf "https://astral.sh/uv/${UV_VERSION}/install.sh" | sh
    if ! UV="$(find_uv)"; then
        die "uv was installed, but not to ${BIN_DIR}. Open a new shell and run this script again."
    fi
}

# Prints the uv executable to use: the one on PATH, else the one this script installs.
function find_uv {
    if command -v uv; then
        return 0
    elif [[ -x "${BIN_DIR}/uv" ]]; then
        echo "${BIN_DIR}/uv"
    else
        return 1
    fi
}

# Sets UV to the uv executable to use, installing it if necessary.
function ensure_uv {
    if UV="$(find_uv)"; then
        local installed oldest
        installed="$("${UV}" --version | cut -d ' ' -f 2)"
        oldest="$(printf '%s\n' "${MIN_UV_VERSION}" "${installed}" | sort -V | head -n 1)"
        if [[ "${oldest}" != "${MIN_UV_VERSION}" ]]; then
            die "uv ${installed} at ${UV} is too old, Rally's installation needs at least ${MIN_UV_VERSION}. Update it with 'uv self update' or your package manager and run this script again."
        fi
        info "Using uv ${installed} at ${UV}"
    else
        install_uv
    fi
}

# Downloads esrally from PyPI and extracts its constraints. Sets RALLY_VERSION and CONSTRAINTS_FILE.
# The version and the constraints come from the very same wheel, so they cannot disagree.
function fetch_constraints {
    local spec="esrally${RALLY_VERSION:+==${RALLY_VERSION}}"
    local extract_dir="${WORK_DIR}/wheel"

    info "Downloading ${spec} to read its dependency constraints"
    # 'uv venv' downloads the managed Python if necessary, which older 'uv pip' does not do, and
    # unlike 'uv python install' it does not add python3.X executables to the user's PATH.
    "${UV}" venv --quiet --managed-python --python "${PYTHON_VERSION}" "${WORK_DIR}/venv"
    "${UV}" pip install --quiet --python "${WORK_DIR}/venv/bin/python" --no-deps --only-binary :all: \
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
    if ! UV="$(find_uv)" || [[ ! -d "$("${UV}" tool dir)/esrally" ]]; then
        info "Rally is not installed as a uv tool, nothing to uninstall."
        return
    fi
    "${UV}" tool uninstall esrally
    info "Rally is uninstalled."
    cat <<EOF
These are kept, in case you still need them:
  Rally's configuration and data: ${RALLY_HOME:-${HOME}}/.rally
  uv: ${UV}
  Pythons downloaded by uv: $("${UV}" python dir)
EOF
}

function main {
    parse_args "$@"

    # Elasticsearch refuses to start as root, and so Rally cannot provision it.
    if [[ "$(id -u)" -eq 0 ]]; then
        die "do not run this script as root. Rally cannot start Elasticsearch as root; run it as a regular user."
    fi

    if [[ "${UNINSTALL}" == "true" ]]; then
        uninstall_rally
        exit 0
    fi

    WORK_DIR="$(mktemp -d)"
    trap 'rm -rf "${WORK_DIR}"' EXIT

    ensure_uv
    # Run uv from an empty directory: 'uv pip' would otherwise pick up the uv.toml, pyproject.toml
    # or .venv of wherever the user runs this script, while 'uv tool' ignores them.
    cd "${WORK_DIR}"
    fetch_constraints

    info "Installing esrally ${RALLY_VERSION} with Python ${PYTHON_VERSION}"
    # uv records the pinned version and the constraints in the tool's receipt. --reinstall makes
    # this script idempotent, even over an esrally tool that was installed without constraints.
    "${UV}" tool install --managed-python --python "${PYTHON_VERSION}" --reinstall \
        --constraints "${CONSTRAINTS_FILE}" "esrally==${RALLY_VERSION}"
    "$("${UV}" tool dir --bin)/esrally" --version

    echo ""
    info "Rally ${RALLY_VERSION} is installed. Run 'esrally --help' to get started."
    echo "To upgrade Rally, run this script again. To uninstall it, run this script with --uninstall."
}

main "$@"
