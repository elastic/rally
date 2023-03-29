# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
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

# pylint: disable=unused-import
from it import fresh_log_file

HttpProxy = collections.namedtuple("HttpProxy", ["authenticated_url", "anonymous_url"])


@pytest.fixture(scope="module")
def http_proxy():
    config_dir = os.path.join(os.path.dirname(__file__), "resources", "squid")

    lines = process.run_subprocess_with_output(
        f"docker run --rm --name squid -d "
        f"-v {config_dir}/squidpasswords:/etc/squid/squidpasswords "
        f"-v {config_dir}/squid.conf:/etc/squid/squid.conf "
        f"-p 3128:3128 ubuntu/squid"
    )
    proxy_container_id = lines[-1].strip()
    proxy = HttpProxy(authenticated_url="http://testuser:testuser@127.0.0.1:3128", anonymous_url="http://127.0.0.1:3128")
    yield proxy
    process.run_subprocess(f"docker stop {proxy_container_id}")


@it.rally_in_mem
def test_run_with_direct_internet_connection(cfg, http_proxy, fresh_log_file):
    assert it.esrally(cfg, "list tracks") == 0
    assert it.check_log_line_present(fresh_log_file, "Connecting directly to the Internet")


@it.rally_in_mem
def test_anonymous_proxy_no_connection(cfg, http_proxy):
    env = dict(os.environ)
    env["http_proxy"] = http_proxy.anonymous_url
    env["https_proxy"] = http_proxy.anonymous_url
    lines = process.run_subprocess_with_output(it.esrally_command_line_for(cfg, "list tracks"), env=env)
    output = "\n".join(lines)
    # there should be a warning because we can't connect
    assert "[WARNING] Could not update tracks." in output
    # still, the command succeeds because of local state
    assert "[INFO] SUCCESS" in output


@it.rally_in_mem
def test_authenticated_proxy_user_can_connect(cfg, http_proxy):
    env = dict(os.environ)
    env["http_proxy"] = http_proxy.authenticated_url
    env["https_proxy"] = http_proxy.authenticated_url
    lines = process.run_subprocess_with_output(it.esrally_command_line_for(cfg, "list tracks"), env=env)
    output = "\n".join(lines)
    # rally should be able to connect, no warning
    assert "[WARNING] Could not update tracks." not in output
    # the command should succeed
    assert "[INFO] SUCCESS" in output
