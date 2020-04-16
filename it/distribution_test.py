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

import it


@it.random_rally_config
def test_tar_distributions(cfg):
    for dist in it.DISTRIBUTIONS:
        for track in it.TRACKS:
            it.wait_until_port_is_free()
            assert it.esrally(cfg, f"--on-error=abort --distribution-version=\"{dist}\" --track=\"{track}\" "
                                   f"--test-mode --car=4gheap") == 0


@it.random_rally_config
def test_docker_distribution(cfg):
    # only test the most recent Docker distribution
    dist = it.DISTRIBUTIONS[-1]
    it.wait_until_port_is_free(port_number=19200)
    assert it.esrally(cfg, f"--on-error=abort --pipeline=\"docker\" --distribution-version=\"{dist}\" "
                           f"--track=\"geonames\" --challenge=\"append-no-conflicts-index-only\" --test-mode "
                           f"--car=4gheap --target-hosts=127.0.0.1:19200") == 0
