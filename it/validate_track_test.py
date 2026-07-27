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

import it

TRACK_PATH = os.path.join(os.path.dirname(__file__), "resources", "track_with_validator")


def _run(cfg, command_line):
    cmd = it.esrally_command_line_for(cfg, command_line)
    return subprocess.run(cmd, shell=True, check=False, capture_output=True, text=True)


@it.rally_in_mem
def test_validate_track_succeeds_with_valid_params(cfg):
    result = _run(
        cfg,
        f"validate-track --track-path={TRACK_PATH} --challenge=validated --track-params='ok:1' --no-quiet",
    )
    assert result.returncode == 0
    assert "Track parameters for challenge [validated] are valid." in result.stdout
    assert "validator" in result.stdout


@it.rally_in_mem
def test_validate_track_uses_default_challenge_when_omitted(cfg):
    result = _run(cfg, f"validate-track --track-path={TRACK_PATH} --track-params='ok:1'")
    assert result.returncode == 0


@it.rally_in_mem
def test_validate_track_fails_on_invalid_params(cfg):
    result = _run(cfg, f"validate-track --track-path={TRACK_PATH} --challenge=validated --track-params='ok:0'")
    assert result.returncode != 0
    combined = f"{result.stdout}\n{result.stderr}"
    assert "ok" in combined


@it.rally_in_mem
def test_validate_track_fails_on_unknown_challenge(cfg):
    result = _run(cfg, f"validate-track --track-path={TRACK_PATH} --challenge=does-not-exist")
    assert result.returncode != 0
    combined = f"{result.stdout}\n{result.stderr}"
    assert "does-not-exist" in combined
