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

import argparse
import time
from pathlib import Path
from unittest import mock

import pytest

from esrally import config, exceptions, rally
from esrally.utils import process


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


@pytest.fixture
def fake_time(monkeypatch):
    # Replace only esrally.rally's reference to the time module. Patching "esrally.rally.time.sleep" would patch
    # time.sleep globally, and calls from threads leaked by other tests would end up in the mock.
    fake = mock.Mock(wraps=time)
    fake.sleep = mock.Mock()
    monkeypatch.setattr(rally, "time", fake)
    return fake


class TestWaitForActorSystemShutdown:
    @mock.patch("esrally.actor.actor_system_already_running", return_value=False)
    def test_returns_immediately_when_already_stopped(self, already_running, fake_time):
        sleep = fake_time.sleep
        assert rally.wait_for_actor_system_shutdown() is True
        already_running.assert_called_once()
        sleep.assert_not_called()

    @mock.patch("esrally.actor.actor_system_already_running", side_effect=[True, True, False])
    def test_polls_at_short_interval_until_stopped(self, already_running, fake_time):
        sleep = fake_time.sleep
        assert rally.wait_for_actor_system_shutdown(timeout=15, poll_interval=0.05) is True
        assert already_running.call_count == 3
        assert sleep.call_args_list == [mock.call(0.05), mock.call(0.05)]

    @mock.patch("esrally.actor.actor_system_already_running", return_value=True)
    def test_gives_up_after_timeout(self, already_running, fake_time):
        fake_time.monotonic = mock.Mock(side_effect=[0.0, 0.0, 0.05, 0.1])
        sleep = fake_time.sleep
        assert rally.wait_for_actor_system_shutdown(timeout=0.1, poll_interval=0.05) is False
        assert sleep.call_count == 2


class TestWithActorSystem:
    @mock.patch("esrally.actor.bootstrap_actor_system")
    @mock.patch("esrally.actor.actor_system_already_running", return_value=False)
    def test_shuts_down_local_actor_system_without_fixed_delay(self, already_running, bootstrap, fake_time):
        sleep = fake_time.sleep
        cfg = mock.Mock()
        cfg.opts.return_value = None
        runnable = mock.Mock()

        rally.with_actor_system(runnable, cfg)

        runnable.assert_called_once_with(cfg)
        bootstrap.return_value.shutdown.assert_called_once()
        sleep.assert_not_called()

    @mock.patch("esrally.actor.bootstrap_actor_system")
    @mock.patch("esrally.actor.actor_system_already_running", return_value=True)
    def test_does_not_shut_down_actor_system_that_was_already_running(self, already_running, bootstrap, fake_time):
        sleep = fake_time.sleep
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


def test_race_telemetry_client_options_are_stored_without_default_timeout():
    arg_parser = rally.create_arg_parser()
    args = arg_parser.parse_args(
        [
            "race",
            "--pipeline=benchmark-only",
            "--client-options=use_ssl:true,timeout:90,api_key:'bench'",
            "--telemetry-client-options=api_key:'telemetry'",
        ]
    )
    cfg = config.Config()

    rally.configure_connection_params(arg_parser, args, cfg)

    assert cfg.opts("client", "telemetry.options").default_or_first == {"api_key": "telemetry"}
    benchmark = cfg.opts("client", "options").default_or_first
    assert benchmark["api_key"] == "bench"
    assert benchmark["timeout"] == 90
    assert benchmark["use_ssl"] is True


def test_create_track_does_not_require_telemetry_client_options():
    arg_parser = rally.create_arg_parser()
    args = arg_parser.parse_args(["create-track", "--track=t", "--indices=i", "--target-hosts=localhost:9200"])
    cfg = config.Config()

    rally.configure_connection_params(arg_parser, args, cfg)

    assert not hasattr(args, "telemetry_client_options")
    assert cfg.opts("client", "telemetry.options", mandatory=False) is None


def test_telemetry_client_options_reject_unknown_cluster_keys():
    arg_parser = rally.create_arg_parser()
    args = arg_parser.parse_args(
        [
            "race",
            "--pipeline=benchmark-only",
            "--target-hosts=localhost:9200",
            "--client-options=timeout:60",
            '--telemetry-client-options={"defaut":{"api_key":"telemetry-key"}}',
        ]
    )
    cfg = config.Config()

    with pytest.raises(SystemExit):
        rally.configure_connection_params(arg_parser, args, cfg)


def test_telemetry_client_options_may_omit_clusters():
    arg_parser = rally.create_arg_parser()
    args = arg_parser.parse_args(
        [
            "race",
            "--pipeline=benchmark-only",
            '--target-hosts={"default":["127.0.0.1:9200"],"remote":["10.0.0.1:9200"]}',
            '--client-options={"default":{"timeout":60},"remote":{"timeout":60}}',
            '--telemetry-client-options={"default":{"api_key":"telemetry"}}',
        ]
    )
    cfg = config.Config()

    rally.configure_connection_params(arg_parser, args, cfg)

    assert set(cfg.opts("client", "telemetry.options").all_client_options) == {"default"}


def test_redact_namespace_masks_client_credentials_without_changing_args():
    args = argparse.Namespace(
        client_options="api_key:'secret'",
        telemetry_client_options="basic_auth_password:'pw'",
        track="geonames",
    )

    redacted = rally.redact_namespace(args)

    assert args.client_options == "api_key:'secret'"
    assert args.telemetry_client_options == "basic_auth_password:'pw'"
    assert redacted.client_options == "*****"
    assert redacted.telemetry_client_options == "*****"
    assert redacted.track == "geonames"
    assert "secret" not in str(redacted)
    assert "pw" not in str(redacted)


@pytest.mark.parametrize("flag, dest", [("--client-options", "client_options"), ("--telemetry-client-options", "telemetry_client_options")])
def test_redact_cmdline_hides_every_form_argparse_accepts_for_credential_flags(flag, dest):
    arg_parser = rally.create_arg_parser()
    secret = "api_key:'top-secret'"
    base = ["race", "--pipeline=benchmark-only", "--target-hosts=localhost:9200"]
    accepted = 0

    # every prefix of the flag that argparse resolves to it, plus the full flag
    for end in range(len("--c"), len(flag) + 1):
        option = flag[:end]
        for tokens in ([f"{option}={secret}"], [option, secret]):
            try:
                args = arg_parser.parse_args(base + tokens)
            except SystemExit:
                continue  # ambiguous or unknown prefix: Rally rejects the command
            if getattr(args, dest, None) != secret:
                continue  # the prefix resolved to a different option
            accepted += 1
            redacted = process.redact_cmdline(["esrally"] + base + tokens)
            assert "top-secret" not in " ".join(redacted), f"credentials leaked for {tokens}"
    assert accepted >= 2, "expected at least the full flag in both spellings to be accepted"
