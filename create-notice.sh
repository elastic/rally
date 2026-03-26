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

readonly OUTPUT_FILE="${__NOTICE_OUTPUT_FILE}"


function add_license {
    local dep_name=$1
    local download_url=$2

    printf '\n======================================\n%s LICENSE\n======================================\n' "${dep_name}" >> "${OUTPUT_FILE}"
    # Transient 5xx / timeouts: retry a few times (still fail-fast on 4xx or exhausted retries).
    curl --fail --show-error --silent \
        --retry 3 --retry-delay 2 --retry-connrefused \
        "${download_url}" >> "${OUTPUT_FILE}"
}

function main {
    cat NOTICE > "${OUTPUT_FILE}"
    # direct dependencies (keep in sync with pyproject.toml [project.dependencies])
    printf "\n======================================\ncertifi\n======================================\n\n" >> "${OUTPUT_FILE}"
    # link to a URL providing the MPL-covered source code
    printf "The source code can be obtained at https://github.com/certifi/python-certifi\n" >> "${OUTPUT_FILE}"
    add_license "certifi" "https://raw.githubusercontent.com/certifi/python-certifi/master/LICENSE"
    add_license "elasticsearch" "https://raw.githubusercontent.com/elastic/elasticsearch-py/main/LICENSE"
    add_license "elastic-transport" "https://raw.githubusercontent.com/elastic/elastic-transport-python/main/LICENSE"
    add_license "jinja2" "https://raw.githubusercontent.com/pallets/jinja/main/LICENSE.txt"
    add_license "jsonschema" "https://raw.githubusercontent.com/Julian/jsonschema/main/COPYING"
    add_license "psutil" "https://raw.githubusercontent.com/giampaolo/psutil/master/LICENSE"
    add_license "py-cpuinfo" "https://raw.githubusercontent.com/workhorsy/py-cpuinfo/master/LICENSE"
    add_license "tabulate" "https://raw.githubusercontent.com/astanin/python-tabulate/master/LICENSE"
    add_license "thespian" "https://raw.githubusercontent.com/kquick/Thespian/master/LICENSE.txt"
    add_license "yappi" "https://raw.githubusercontent.com/sumerc/yappi/master/LICENSE"
    add_license "ijson" "https://raw.githubusercontent.com/ICRAR/ijson/master/LICENSE.txt"
    add_license "aiosignal" "https://raw.githubusercontent.com/aio-libs/aiosignal/master/LICENSE"
    add_license "docker" "https://raw.githubusercontent.com/docker/docker-py/main/LICENSE"
    add_license "requests" "https://raw.githubusercontent.com/psf/requests/main/LICENSE"
    add_license "google-resumable-media" "https://raw.githubusercontent.com/googleapis/google-resumable-media-python/main/LICENSE"
    add_license "google-auth" "https://raw.githubusercontent.com/googleapis/google-auth-library-python/main/LICENSE"
    add_license "zstandard" "https://raw.githubusercontent.com/indygreg/python-zstandard/main/LICENSE"
    add_license "typing-extensions" "https://raw.githubusercontent.com/python/typing_extensions/main/LICENSE"
    add_license "python-json-logger" "https://raw.githubusercontent.com/nhairs/python-json-logger/main/LICENSE"
    add_license "ecs-logging" "https://raw.githubusercontent.com/elastic/ecs-logging-python/main/LICENSE.txt"
    add_license "hatch" "https://raw.githubusercontent.com/pypa/hatch/master/LICENSE.txt"
    add_license "hatchling" "https://raw.githubusercontent.com/pypa/hatch/master/backend/LICENSE.txt"
    add_license "wheel" "https://raw.githubusercontent.com/pypa/wheel/main/LICENSE.txt"
    add_license "pip" "https://raw.githubusercontent.com/pypa/pip/main/LICENSE.txt"
    add_license "boto3" "https://raw.githubusercontent.com/boto/boto3/develop/LICENSE"

    # transitive dependencies
    # Jinja2 dependencies
    add_license "Markupsafe" "https://raw.githubusercontent.com/pallets/markupsafe/main/LICENSE.txt"
    # elasticsearch dependencies
    add_license "urllib3" "https://raw.githubusercontent.com/urllib3/urllib3/main/LICENSE.txt"
    # elasticsearch[async] dependencies
    add_license "aiohttp" "https://raw.githubusercontent.com/aio-libs/aiohttp/master/LICENSE.txt"
    # aiohttp dependencies (aiohttp 3.13; aiosignal listed as direct above)
    add_license "async_timeout" "https://raw.githubusercontent.com/aio-libs/async-timeout/master/LICENSE"
    add_license "attrs" "https://raw.githubusercontent.com/python-attrs/attrs/main/LICENSE"
    add_license "aiohappyeyeballs" "https://raw.githubusercontent.com/aio-libs/aiohappyeyeballs/main/LICENSE"
    add_license "frozenlist" "https://raw.githubusercontent.com/aio-libs/frozenlist/master/LICENSE"
    add_license "propcache" "https://raw.githubusercontent.com/aio-libs/propcache/master/LICENSE"
    add_license "multidict" "https://raw.githubusercontent.com/aio-libs/multidict/master/LICENSE"
    add_license "yarl" "https://raw.githubusercontent.com/aio-libs/yarl/master/LICENSE"
    # yarl dependencies
    add_license "idna" "https://raw.githubusercontent.com/kjd/idna/master/LICENSE.md"
    # yarl dependency "multidict" is already covered above
    # requests dependencies
    add_license "charset-normalizer" "https://raw.githubusercontent.com/jawah/charset_normalizer/master/LICENSE"
    # docker dependencies
    add_license "packaging" "https://raw.githubusercontent.com/pypa/packaging/main/LICENSE"
    add_license "websocket-client" "https://raw.githubusercontent.com/websocket-client/websocket-client/master/LICENSE"
    # boto3 dependencies
    add_license "s3transfer" "https://raw.githubusercontent.com/boto/s3transfer/develop/LICENSE.txt"
    add_license "jmespath" "https://raw.githubusercontent.com/jmespath/jmespath.py/develop/LICENSE"
    add_license "botocore" "https://raw.githubusercontent.com/boto/botocore/develop/LICENSE.txt"
    # google-resumable-media dependencies (google-crc32c is also a direct dependency in pyproject.toml)
    add_license "google-crc32c" "https://raw.githubusercontent.com/googleapis/python-crc32c/main/LICENSE"
}

main
