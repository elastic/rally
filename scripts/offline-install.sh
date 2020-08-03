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
set -e
# Treat unset env variables as an error
set -u
# fail on pipeline errors, e.g. when grepping
set -o pipefail

# test number of parameters
if [ $# != 1 ]
then
    echo "Usage: $0 RALLY_RELEASE"
    exit 1
fi

KERNEL_NAME=$(uname -s)
if [[ ${KERNEL_NAME} != *"Linux"* ]]
then
  echo "Error: this script needs to be run on a Linux workstation but you are running on ${KERNEL_NAME}."
  echo "Switch to a Linux workstation and try again."
  exit 1
fi

readonly RALLY_VERSION=$1

readonly WD=$(pwd)
readonly RELATIVE_DOWNLOAD_DIR="esrally-dist-${RALLY_VERSION}"
readonly ABSOLUTE_DOWNLOAD_DIR="${WD}/${RELATIVE_DOWNLOAD_DIR}"
readonly ABSOLUTE_DOWNLOAD_BIN_DIR="${ABSOLUTE_DOWNLOAD_DIR}/bin"
readonly PYTHON_INSTALL_LINK="https://esrally.readthedocs.io/en/stable/install.html#python"
readonly PYTHON_ERROR_MSG="is required but not installed. Follow the instructions in ${PYTHON_INSTALL_LINK} and try again."

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SCRIPT_SRC_HOME="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

function main {
    local archive_name="esrally-dist-linux-${RALLY_VERSION}.tar.gz"
    local install_script_file="install.sh"
    local install_script="${ABSOLUTE_DOWNLOAD_DIR}/${install_script_file}"

    echo "Preparing offline distribution for Rally ${RALLY_VERSION}"

    mkdir -p "${ABSOLUTE_DOWNLOAD_BIN_DIR}"
    # Prepare install
    pip3 download esrally=="${RALLY_VERSION}" --dest "${ABSOLUTE_DOWNLOAD_BIN_DIR}" --no-binary=MarkupSafe


    echo "Preparing NOTICE file"
    __NOTICE_OUTPUT_FILE="${ABSOLUTE_DOWNLOAD_BIN_DIR}/NOTICE.txt"
    source "${SCRIPT_SRC_HOME}/../create-notice.sh"

    # create an offline install script
    cat >"${install_script}" <<EOL
#!/usr/bin/env bash

# fail this script immediately if any command fails with a non-zero exit code
set -e
# Treat unset env variables as an error
set -u
# fail on pipeline errors, e.g. when grepping
set -o pipefail

SOURCE="\${BASH_SOURCE[0]}"
while [ -h "\$SOURCE" ]; do # resolve \$SOURCE until the file is no longer a symlink
  DIR="\$( cd -P "\$( dirname "\$SOURCE" )" && pwd )"
  SOURCE="\$(readlink "\$SOURCE")"
  [[ \$SOURCE != /* ]] && SOURCE="\$DIR/\$SOURCE" # if \$SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
SRC_HOME="\$( cd -P "\$( dirname "\$SOURCE" )" && pwd )"

echo "Installing Rally ${RALLY_VERSION}..."

# Check if mandatory prerequisites are installed
command -v python3 >/dev/null 2>&1 || { echo >&2 "Python3 ${PYTHON_ERROR_MSG}"; exit 1; }
command -v pip3 >/dev/null 2>&1 || { echo >&2 "pip3 ${PYTHON_ERROR_MSG}"; exit 1; }

pip3 install esrally==${RALLY_VERSION} --no-index --find-links file://\${SRC_HOME}/bin
EOL
    chmod u+x ${install_script}

    #then create an archive with all dependencies
    tar -czf "${archive_name}" "${RELATIVE_DOWNLOAD_DIR}"

    echo "Successfully created ${archive_name}."
    echo ""
    echo "Next steps:"
    echo ""
    echo "1. copy it to the target machine(s)"
    echo "2. Uncompress: tar -xzf ${archive_name}"
    echo "3. Run the install script: sudo ./${RELATIVE_DOWNLOAD_DIR}/${install_script_file}"
}

function cleanup {
    rm -rf "${ABSOLUTE_DOWNLOAD_DIR}"
}

trap "cleanup" EXIT

main
