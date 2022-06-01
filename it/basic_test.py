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

import it
from esrally.utils import process


@it.rally_in_mem
def test_run_without_arguments(cfg):
    cmd = it.esrally_command_line_for(cfg, "")
    output = process.run_subprocess_with_output(cmd)
    expected = "usage: esrally [-h] [--version]"
    assert expected in "\n".join(output)


@it.rally_in_mem
def test_run_with_help(cfg):
    cmd = it.esrally_command_line_for(cfg, "--help")
    output = process.run_subprocess_with_output(cmd)
    expected = "usage: esrally [-h] [--version]"
    assert expected in "\n".join(output)


@it.rally_in_mem
def test_run_without_http_connection(cfg):
    cmd = it.esrally_command_line_for(cfg, "list races")
    output = process.run_subprocess_with_output(cmd, {"http_proxy": "http://invalid"})
    expected = "No Internet connection detected. Specify --offline"
    assert expected in "\n".join(output)
