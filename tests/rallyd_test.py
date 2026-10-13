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
import sys

import pytest

from esrally import actor, exceptions, rallyd
from esrally.utils import console


class FakeRayCli:
    def __init__(self, returncodes=None):
        self.calls: list[tuple[str, ...]] = []
        self.returncodes = returncodes or {}

    def __call__(self, *args: str, capture_output: bool = True) -> int:
        self.calls.append(args)
        return self.returncodes.get(args[0], 0)


@pytest.fixture
def ray_cli(monkeypatch: pytest.MonkeyPatch) -> FakeRayCli:
    cli = FakeRayCli()
    monkeypatch.setattr(rallyd, "run_ray", cli)
    monkeypatch.setattr(rallyd.net, "resolve", lambda host: host)
    monkeypatch.setattr(console, "RALLY_RUNNING_IN_DOCKER", False)
    monkeypatch.delenv("RAY_AUTH_MODE", raising=False)
    monkeypatch.delenv("RAY_AUTH_TOKEN", raising=False)
    return cli


@pytest.fixture
def no_daemon(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(actor, "is_daemon_running_locally", lambda: False)


@pytest.fixture
def daemon(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(actor, "is_daemon_running_locally", lambda: True)


def start_args(node_ip: str, coordinator_ip: str) -> argparse.Namespace:
    return argparse.Namespace(node_ip=node_ip, coordinator_ip=coordinator_ip)


@pytest.mark.usefixtures("no_daemon")
class TestStart:
    def test_starts_head_on_coordinator(self, ray_cli: FakeRayCli):
        rallyd.start(start_args("10.5.5.5", "10.5.5.5"))

        assert ray_cli.calls == [
            ("get-auth-token", "--generate"),
            (
                "start",
                "--head",
                "--port",
                "1900",
                "--include-dashboard",
                "false",
                "--node-ip-address",
                "10.5.5.5",
                "--disable-usage-stats",
            ),
        ]

    def test_joins_coordinator_on_other_nodes(self, ray_cli: FakeRayCli, tmp_path, monkeypatch: pytest.MonkeyPatch):
        token = tmp_path / "auth_token"
        token.write_text("secret")
        monkeypatch.setenv("RAY_AUTH_TOKEN_PATH", str(token))

        rallyd.start(start_args("10.5.5.6", "10.5.5.5"))

        assert ray_cli.calls == [("start", "--address", "10.5.5.5:1900", "--node-ip-address", "10.5.5.6", "--disable-usage-stats")]

    def test_requires_token_of_coordinator_on_other_nodes(self, ray_cli: FakeRayCli, tmp_path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("RAY_AUTH_TOKEN_PATH", str(tmp_path / "missing"))

        with pytest.raises(exceptions.RallyError, match=r"Copy the file \[~/.ray/auth_token\] from the coordinator node \[10.5.5.5\]"):
            rallyd.start(start_args("10.5.5.6", "10.5.5.5"))

        assert ray_cli.calls == []

    def test_does_not_require_token_when_authentication_is_disabled(self, ray_cli: FakeRayCli, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("RAY_AUTH_MODE", "disabled")

        rallyd.start(start_args("10.5.5.6", "10.5.5.5"))

        assert [c[0] for c in ray_cli.calls] == ["start"]

    def test_blocks_in_docker(self, ray_cli: FakeRayCli, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(console, "RALLY_RUNNING_IN_DOCKER", True)

        rallyd.start(start_args("10.5.5.5", "10.5.5.5"))

        assert ray_cli.calls[-1][-1] == "--block"

    def test_fails_if_ray_cannot_start(self, ray_cli: FakeRayCli):
        ray_cli.returncodes["start"] = 1

        with pytest.raises(exceptions.RallyError, match="Could not start the Rally daemon"):
            rallyd.start(start_args("10.5.5.5", "10.5.5.5"))


@pytest.mark.usefixtures("daemon")
def test_start_fails_if_already_running(ray_cli: FakeRayCli):
    with pytest.raises(exceptions.RallyError, match="already running"):
        rallyd.start(start_args("10.5.5.5", "10.5.5.5"))
    assert ray_cli.calls == []


@pytest.mark.usefixtures("daemon")
def test_stop(ray_cli: FakeRayCli):
    rallyd.stop()
    assert ray_cli.calls == [("stop",)]


@pytest.mark.usefixtures("no_daemon")
def test_stop_fails_if_not_running(ray_cli: FakeRayCli):
    with pytest.raises(SystemExit):
        rallyd.stop()
    # but restart does not care
    rallyd.stop(raise_errors=False)
    assert ray_cli.calls == []


@pytest.mark.usefixtures("daemon")
def test_status_running(capsys: pytest.CaptureFixture):
    rallyd.status()
    assert capsys.readouterr().out.strip() == "Running"


@pytest.mark.usefixtures("no_daemon")
def test_status_stopped(capsys: pytest.CaptureFixture):
    rallyd.status()
    assert capsys.readouterr().out.strip() == "Stopped"


def test_runs_ray_with_same_interpreter():
    assert rallyd.ray_command("status") == [sys.executable, "-m", "ray.scripts.scripts", "status"]
