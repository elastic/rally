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

import collections
import os

import pytest

import it
from esrally.utils import process

HttpProxy = collections.namedtuple("HttpProxy", ["authenticated_url", "anonymous_url"])


@pytest.fixture(scope="module")
def http_proxy():
    config_dir = os.path.join(os.path.dirname(__file__), "resources", "squid")

    lines = process.run_subprocess_with_output(f"docker run --rm --name squid -d "
                                               f"-v {config_dir}/squidpasswords:/etc/squid/squidpasswords "
                                               f"-v {config_dir}/squid.conf:/etc/squid/squid.conf "
                                               f"-p 3128:3128 datadog/squid")
    proxy_container_id = lines[0].strip()
    proxy = HttpProxy(authenticated_url=f"http://testuser:testuser@127.0.0.1:3128",
                      anonymous_url=f"http://127.0.0.1:3128")
    yield proxy
    process.run_subprocess(f"docker stop {proxy_container_id}")
    process.run_subprocess(f"docker rm {proxy_container_id}")


@it.rally_in_mem
def test_run_with_direct_internet_connection(cfg, http_proxy):
    assert it.esrally(cfg, "list tracks") == 0
    # TODO: add proper assertions
    # if grep -F -q "Connecting directly to the Internet" "$RALLY_LOG"; then
    # info "Successfully checked that direct internet connection is used."
    # rm -f ${RALLY_LOG}
    # else
    # error "Could not find indication that direct internet connection is used. Check ${RALLY_LOG}."
    # exit 1
    # fi


@it.rally_in_mem
def test_anonymous_proxy_no_connection(cfg, http_proxy):
    # TODO: Use http_proxy.anonymous_url
    assert it.esrally(cfg, "list tracks") == 0
    # TODO: add proper assertions
    # if grep -F -q "Connecting via proxy URL [http://127.0.0.1:3128] to the Internet" "$RALLY_LOG"; then
    # info "Successfully checked that proxy is used."
    # else
    # error "Could not find indication that proxy access is used. Check ${RALLY_LOG}."
    # exit 1
    # fi
    #
    # if grep -F -q "No Internet connection detected" "$RALLY_LOG"; then
    # info "Successfully checked that unauthenticated proxy access is prevented."
    # rm -f ${RALLY_LOG}
    # else
    # error "Could not find indication that unauthenticated proxy access is prevented. Check ${RALLY_LOG}."
    # exit 1
    # fi


@it.rally_in_mem
def test_authenticated_proxy_user_can_connect(cfg, http_proxy):
    # TODO: Use http_proxy.authenticated_url
    assert it.esrally(cfg, "list tracks") == 0
    # TODO: add proper assertions
    # export http_proxy=http://testuser:testuser@127.0.0.1:3128
    # # this invocation *may* lead to an error but this is ok
    # set +e
    # esrally list tracks --configuration-name="${cfg}"
    # unset http_proxy
    # set -e
    #
    # if grep -F -q "Connecting via proxy URL [http://testuser:testuser@127.0.0.1:3128] to the Internet" "$RALLY_LOG"; then
    # info "Successfully checked that proxy is used."
    # else
    # error "Could not find indication that proxy access is used. Check ${RALLY_LOG}."
    # exit 1
    # fi
    #
    # if grep -F -q "Detected a working Internet connection" "$RALLY_LOG"; then
    # info "Successfully checked that authenticated proxy access is allowed."
    # rm -f ${RALLY_LOG}
    # else
    # error "Could not find indication that authenticated proxy access is allowed. Check ${RALLY_LOG}."
    # exit 1
