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

import os

import it


@it.rally_in_mem
def test_track_info_with_challenge(cfg, tmp_path):
    cwd = os.path.dirname(__file__)
    chart_spec_path = os.path.join(cwd, "resources", "sample-race-config.json")
    output_path = os.path.join(tmp_path, "nightly-charts.ndjson")
    assert it.esrally(cfg, f"generate charts "
                           f"--chart-spec-path={chart_spec_path} "
                           f"--chart-type=time-series "
                           f"--output-path={output_path}") == 0

@it.rally_in_mem
def test_fails_when_spec_not_found(cfg, tmp_path):
    chart_spec_path = "/non/existent/path"
    output_path = os.path.join(tmp_path, "nightly-charts.ndjson")
    assert it.esrally(cfg, f"generate charts "
                           f"--chart-spec-path={chart_spec_path} "
                           f"--chart-type=time-series "
                           f"--output-path={output_path}") !=0
