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

readonly RALLY_VERSION=$1

readonly WD=$(pwd)
readonly RELATIVE_DOWNLOAD_DIR="esrally-dist-${RALLY_VERSION}"
readonly ABSOLUTE_DOWNLOAD_DIR="${WD}/${RELATIVE_DOWNLOAD_DIR}"
readonly ABSOLUTE_DOWNLOAD_BIN_DIR="${ABSOLUTE_DOWNLOAD_DIR}/bin"

function main {
    local archive_name="esrally-dist-${RALLY_VERSION}.tar.gz"
    local install_script_file="install.sh"
    local install_script="${ABSOLUTE_DOWNLOAD_DIR}/${install_script_file}"

    echo "Preparing offline distribution for Rally ${RALLY_VERSION}"

    mkdir -p "${ABSOLUTE_DOWNLOAD_BIN_DIR}"
    # Prepare install
    pip3 download esrally=="${RALLY_VERSION}" --dest "${ABSOLUTE_DOWNLOAD_BIN_DIR}"

    # grab all license files
    echo "Downloading license files for all dependencies"
    # direct dependencies
    curl --silent -o "${ABSOLUTE_DOWNLOAD_BIN_DIR}/certifi-LICENSE.txt" https://raw.githubusercontent.com/certifi/python-certifi/master/LICENSE
    curl --silent -o "${ABSOLUTE_DOWNLOAD_BIN_DIR}/elasticsearch-LICENSE.txt" https://raw.githubusercontent.com/elastic/elasticsearch-py/master/LICENSE
    curl --silent -o "${ABSOLUTE_DOWNLOAD_BIN_DIR}/jinja2-LICENSE.txt" https://raw.githubusercontent.com/pallets/jinja/master/LICENSE
    curl --silent -o "${ABSOLUTE_DOWNLOAD_BIN_DIR}/jsonschema-LICENSE.txt" https://raw.githubusercontent.com/Julian/jsonschema/master/COPYING
    curl --silent -o "${ABSOLUTE_DOWNLOAD_BIN_DIR}/psutil-LICENSE.txt" https://raw.githubusercontent.com/giampaolo/psutil/master/LICENSE
    curl --silent -o "${ABSOLUTE_DOWNLOAD_BIN_DIR}/py-cpuinfo-LICENSE.txt" https://raw.githubusercontent.com/workhorsy/py-cpuinfo/master/LICENSE
    curl --silent -o "${ABSOLUTE_DOWNLOAD_BIN_DIR}/tabulate-LICENSE.txt" https://bitbucket.org/astanin/python-tabulate/raw/03182bf9b8a2becbc54d17aa7e3e7dfed072c5f5/LICENSE
    curl --silent -o "${ABSOLUTE_DOWNLOAD_BIN_DIR}/thespian-LICENSE.txt" https://raw.githubusercontent.com/kquick/Thespian/master/LICENSE.txt
    # transitive dependencies
    # Jinja2 -> Markupsafe
    curl --silent -o "${ABSOLUTE_DOWNLOAD_BIN_DIR}/markupsafe-LICENSE.txt" https://raw.githubusercontent.com/pallets/markupsafe/master/LICENSE
    # elasticsearch -> urllib3
    curl --silent -o "${ABSOLUTE_DOWNLOAD_BIN_DIR}/urllib3-LICENSE.txt" https://raw.githubusercontent.com/shazow/urllib3/master/LICENSE.txt

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
command -v python3 >/dev/null 2>&1 || { echo >&2 "Python3 is required but not installed."; exit 1; }
command -v pip3 >/dev/null 2>&1 || { echo >&2 "pip3 is required but not installed."; exit 1; }

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
