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

import it


@it.random_rally_config
def test_static_responses(cfg):
    port = 19200
    it.wait_until_port_is_free(port_number=port)
    dist_version = it.DISTRIBUTIONS[-1]
    cwd = os.path.dirname(__file__)
    responses = os.path.join(cwd, "resources", "static-responses.json")

    assert (
        it.race(
            cfg,
            f'--pipeline=benchmark-only --distribution-version="{dist_version}" '
            f"--client-options=\"static_responses:'{responses}'\" "
            "--track=geonames --challenge=append-no-conflicts-index-only",
        )
        == 0
    )
