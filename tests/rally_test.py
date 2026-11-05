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
from unittest import mock

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


class TestWaitForActorSystemShutdown:
    @mock.patch("esrally.rally.time.sleep")
    @mock.patch("esrally.actor.actor_system_already_running", return_value=False)
    def test_returns_immediately_when_already_stopped(self, already_running, sleep):
        assert rally.wait_for_actor_system_shutdown() is True
        already_running.assert_called_once()
        sleep.assert_not_called()

    @mock.patch("esrally.rally.time.sleep")
    @mock.patch("esrally.actor.actor_system_already_running", side_effect=[True, True, False])
    def test_polls_at_short_interval_until_stopped(self, already_running, sleep):
        assert rally.wait_for_actor_system_shutdown(timeout=15, poll_interval=0.05) is True
        assert already_running.call_count == 3
        assert sleep.call_args_list == [mock.call(0.05), mock.call(0.05)]

    @mock.patch("esrally.rally.time.monotonic", side_effect=[0.0, 0.0, 0.05, 0.1])
    @mock.patch("esrally.rally.time.sleep")
    @mock.patch("esrally.actor.actor_system_already_running", return_value=True)
    def test_gives_up_after_timeout(self, already_running, sleep, monotonic):
        assert rally.wait_for_actor_system_shutdown(timeout=0.1, poll_interval=0.05) is False
        assert sleep.call_count == 2


class TestWithActorSystem:
    @mock.patch("esrally.rally.time.sleep")
    @mock.patch("esrally.actor.bootstrap_actor_system")
    @mock.patch("esrally.actor.actor_system_already_running", return_value=False)
    def test_shuts_down_local_actor_system_without_fixed_delay(self, already_running, bootstrap, sleep):
        cfg = mock.Mock()
        cfg.opts.return_value = None
        runnable = mock.Mock()

        rally.with_actor_system(runnable, cfg)

        runnable.assert_called_once_with(cfg)
        bootstrap.return_value.shutdown.assert_called_once()
        sleep.assert_not_called()

    @mock.patch("esrally.rally.time.sleep")
    @mock.patch("esrally.actor.bootstrap_actor_system")
    @mock.patch("esrally.actor.actor_system_already_running", return_value=True)
    def test_does_not_shut_down_actor_system_that_was_already_running(self, already_running, bootstrap, sleep):
        cfg = mock.Mock()
        cfg.opts.return_value = None

        rally.with_actor_system(mock.Mock(), cfg)

        bootstrap.return_value.shutdown.assert_not_called()
        sleep.assert_not_called()

    @mock.patch("esrally.rally.console")
    @mock.patch("esrally.rally.wait_for_actor_system_shutdown", return_value=True)
    @mock.patch("esrally.actor.bootstrap_actor_system")
    @mock.patch("esrally.actor.actor_system_already_running", return_value=False)
    def test_retries_shutdown_after_user_interrupt(self, already_running, bootstrap, wait, console):
        cfg = mock.Mock()
        cfg.opts.return_value = None
        bootstrap.return_value.shutdown.side_effect = [KeyboardInterrupt, None]

        rally.with_actor_system(mock.Mock(), cfg)

        assert bootstrap.return_value.shutdown.call_count == 2
        wait.assert_called_once()

    @mock.patch("esrally.rally.console")
    @mock.patch("esrally.rally.wait_for_actor_system_shutdown", return_value=True)
    @mock.patch("esrally.actor.bootstrap_actor_system")
    @mock.patch("esrally.actor.actor_system_already_running", return_value=False)
    def test_gives_up_after_two_user_interrupts(self, already_running, bootstrap, wait, console):
        cfg = mock.Mock()
        cfg.opts.return_value = None
        bootstrap.return_value.shutdown.side_effect = KeyboardInterrupt

        with pytest.raises(exceptions.UserInterrupted):
            rally.with_actor_system(mock.Mock(), cfg)

        assert bootstrap.return_value.shutdown.call_count == 2
        wait.assert_not_called()
