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

from pathlib import Path

import pytest

from esrally import exceptions, rally


def test_creates_default_configuration_when_missing(tmp_path, monkeypatch):
    monkeypatch.setenv("RALLY_HOME", str(tmp_path))

    cfg = rally.load_configuration(None)

    assert cfg.config_present()
    assert Path(cfg.config_file.location) == tmp_path / ".rally" / "rally.ini"


def test_loads_existing_named_configuration(tmp_path, monkeypatch):
    monkeypatch.setenv("RALLY_HOME", str(tmp_path))
    config_file = tmp_path / ".rally" / "rally-existing.ini"
    config_file.parent.mkdir()
    config_file.write_text("[meta]\nconfig.version = 17\n", encoding="utf-8")

    cfg = rally.load_configuration("existing")

    assert cfg.config_present()
    assert Path(cfg.config_file.location) == config_file


def test_rejects_missing_named_configuration(tmp_path, monkeypatch):
    monkeypatch.setenv("RALLY_HOME", str(tmp_path))
    config_file = tmp_path / ".rally" / "rally-missing.ini"

    with pytest.raises(exceptions.ConfigError, match=rf"Configuration file \[{config_file}\] does not exist\."):
        rally.load_configuration("missing")

    assert not config_file.exists()


def test_prepare_track_parser_accepts_arguments():
    arg_parser = rally.create_arg_parser()
    args = arg_parser.parse_args(
        [
            "prepare-track",
            "--track-path=/path/to/track",
            "--track-params=k:v",
            "--ignore-unused-track-params",
            "--challenge=my-challenge",
            "--build-flavor=serverless",
            "--serverless-operator",
            "--test-mode",
            "--kill-running-processes",
        ]
    )
    assert args.subcommand == "prepare-track"
    assert args.track_path == "/path/to/track"
    assert args.track_params == "k:v"
    assert args.ignore_unused_track_params is True
    assert args.challenge == "my-challenge"
    assert args.build_flavor == "serverless"
    assert args.serverless_operator is True
    assert args.test_mode is True
    assert args.kill_running_processes is True

