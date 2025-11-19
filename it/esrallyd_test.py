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
import pytest

import it
from it import ensure_prerequisites, fresh_log_file  # pylint: disable=unused-import


@pytest.fixture(autouse=True)
def setup_esrallyd():
    it.wait_until_port_is_free(1900)
    it.shell_cmd("esrallyd start --node-ip 127.0.0.1 --coordinator-ip 127.0.0.1")
    yield
    it.shell_cmd("esrallyd stop")


@it.rally_in_mem
def test_elastic_transport_module_does_not_log_at_info_level(cfg, fresh_log_file):
    """
    The 'elastic_transport' module logs at 'INFO' by default and is _very_ noisy, so we explicitly set the threshold to
    'WARNING' to avoid perturbing benchmarking results due to the high volume of logging calls by the client itself.

    Unfortunately, due to the underlying double-fork behaviour of the ActorSystem, it's possible for this module's logger
    threshold to be overridden and reset to the default 'INFO' level via eager top level imports (i.e at the top of a module).

    Therefore, we try to tightly control the imports of `elastic_transport` and `elasticsearch` throughout the codebase, but
    it is very easy to reintroduce this 'bug' by simply putting the import statement in the 'wrong' spot, thus this IT
    attempts to ensure this doesn't happen.

    See https://github.com/elastic/rally/pull/1669#issuecomment-1442783985 for more details.
    """
    port = 19200
    it.wait_until_port_is_free(port_number=port)
    dist = it.DISTRIBUTIONS[-1]
    it.race(
        cfg,
        f'--distribution-version={dist} --track="geonames" --include-tasks=delete-index '
        f"--test-mode --car=4gheap,trial-license --target-hosts=127.0.0.1:{port} ",
    )
    assert it.find_log_line(fresh_log_file, "elastic_transport.transport INFO") is None
