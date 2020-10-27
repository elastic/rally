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
import shutil
import tempfile

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
    proxy = HttpProxy(authenticated_url="http://testuser:testuser@127.0.0.1:3128",
                      anonymous_url="http://127.0.0.1:3128")
    yield proxy
    process.run_subprocess(f"docker stop {proxy_container_id}")


# ensures that a fresh log file is available
@pytest.fixture(scope="function")
def fresh_log_file():
    cfg = it.ConfigFile(config_name=None)
    log_file = os.path.join(cfg.rally_home, "logs", "rally.log")

    if os.path.exists(log_file):
        bak = os.path.join(tempfile.mkdtemp(), "rally.log")
        shutil.move(log_file, bak)
        yield log_file
        # append log lines to the original file and move it back to its original
        with open(log_file, "r") as src:
            with open(bak, "a") as dst:
                dst.write(src.read())
        shutil.move(bak, log_file)
    else:
        yield log_file


def assert_log_line_present(log_file, text):
    with open(log_file, "r") as f:
        assert any(text in line for line in f), f"Could not find [{text}] in [{log_file}]."


@it.rally_in_mem
def test_run_with_direct_internet_connection(cfg, http_proxy, fresh_log_file):
    assert it.esrally(cfg, "list tracks") == 0
    assert_log_line_present(fresh_log_file, "Connecting directly to the Internet")


@it.rally_in_mem
def test_anonymous_proxy_no_connection(cfg, http_proxy, fresh_log_file):
    env = dict(os.environ)
    env["http_proxy"] = http_proxy.anonymous_url
    assert process.run_subprocess_with_logging(it.esrally_command_line_for(cfg, "list tracks"), env=env) == 0
    assert_log_line_present(fresh_log_file, f"Connecting via proxy URL [{http_proxy.anonymous_url}] to the Internet")
    # unauthenticated proxy access is prevented
    assert_log_line_present(fresh_log_file, "No Internet connection detected")


@it.rally_in_mem
def test_authenticated_proxy_user_can_connect(cfg, http_proxy, fresh_log_file):
    env = dict(os.environ)
    env["http_proxy"] = http_proxy.authenticated_url
    assert process.run_subprocess_with_logging(it.esrally_command_line_for(cfg, "list tracks"), env=env) == 0
    assert_log_line_present(fresh_log_file,
                            f"Connecting via proxy URL [{http_proxy.authenticated_url}] to the Internet")
    # authenticated proxy access is allowed
    assert_log_line_present(fresh_log_file, "Detected a working Internet connection")
