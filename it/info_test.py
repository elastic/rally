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
from esrally.utils import process


@it.rally_in_mem
def test_track_info_with_challenge(cfg):
    assert it.esrally(cfg, "info --track=geonames --challenge=append-no-conflicts") == 0


@it.rally_in_mem
def test_track_info_with_track_repo(cfg):
    assert it.esrally(cfg, "info --track-repository=default --track=geonames") == 0


@it.rally_in_mem
def test_track_info_with_task_filter(cfg):
    assert it.esrally(cfg, "info --track=geonames --challenge=append-no-conflicts --include-tasks=\"type:search\"") == 0


@it.rally_in_mem
def test_track_info_fails_with_wrong_track_params(cfg):
    # simulate a typo in track parameter
    cmd = it.esrally_command_line_for(cfg, "info --track=geonames --track-params='conflict_probability:5,number-of-replicas:1'")
    output = process.run_subprocess_with_output(cmd)
    expected = "Some of your track parameter(s) \"number-of-replicas\" are not used by this track; " \
               "perhaps you intend to use \"number_of_replicas\" instead.\n\nAll track parameters you " \
               "provided are:\n- conflict_probability\n- number-of-replicas\n\nAll parameters exposed by this track"

    assert expected in "\n".join(output)
