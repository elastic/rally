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
import os
import subprocess

import pytest

import it
from esrally import actor


@pytest.fixture(autouse=True)
def setup_esrallyd():
    it.wait_until_port_is_free(actor.RAY_GCS_PORT)
    assert it.shell_cmd("esrallyd start --node-ip 127.0.0.1 --coordinator-ip 127.0.0.1") == 0
    yield
    assert it.shell_cmd("esrallyd stop") == 0
    assert not actor.is_daemon_running_locally()


def test_esrallyd_status():
    def status():
        return subprocess.run("esrallyd status", shell=True, check=True, capture_output=True, text=True).stdout.strip()

    assert status() == "Running"


@it.rally_in_mem
def test_race_on_load_driver_host(cfg):
    """
    Races with load driver hosts given by IP address: Rally places its workers on the Ray node with that IP.
    """
    responses = os.path.join(os.path.dirname(__file__), "resources", "static-responses.json")
    assert (
        it.race(
            cfg,
            f'--pipeline=benchmark-only --distribution-version="{it.DISTRIBUTIONS[-1]}" '
            f"--client-options=\"static_responses:'{responses}'\" "
            "--track=geonames --challenge=append-no-conflicts-index-only --test-mode --load-driver-hosts=127.0.0.1",
        )
        == 0
    )
    # --kill-running-processes must not kill the daemon
    assert actor.is_daemon_running_locally()


@it.rally_in_mem
def test_elastic_transport_module_does_not_log_at_info_level(cfg, fresh_log_file, free_benchmark_http_port):
    """
    The 'elastic_transport' module logs at 'INFO' by default and is _very_ noisy, so we explicitly set the threshold to
    'WARNING' to avoid perturbing benchmarking results due to the high volume of logging calls by the client itself.

    Actors run in processes started by the Rally daemon, which configure logging themselves. Eager top level imports
    (i.e at the top of a module) of this module can reset its logger threshold to the default 'INFO' level.

    Therefore, we try to tightly control the imports of `elastic_transport` and `elasticsearch` throughout the codebase, but
    it is very easy to reintroduce this 'bug' by simply putting the import statement in the 'wrong' spot, thus this IT
    attempts to ensure this doesn't happen.

    See https://github.com/elastic/rally/pull/1669#issuecomment-1442783985 for more details.
    """
    dist = it.DISTRIBUTIONS[-1]
    it.race(
        cfg,
        f'--distribution-version={dist} --track="geonames" --include-tasks=delete-index '
        f"--test-mode --car=4gheap,trial-license --target-hosts=127.0.0.1:{free_benchmark_http_port} ",
    )
    assert it.find_log_line(fresh_log_file, "elastic_transport.transport INFO") is None
