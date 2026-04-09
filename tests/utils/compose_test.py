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
from __future__ import annotations

import logging
import os
import subprocess
from dataclasses import dataclass
from unittest import mock

import pytest
import tabulate

from esrally import config
from esrally.paths import rally_root
from esrally.utils.cases import cases
from esrally.utils.compose import (
    ComposeConfig,
    build_image,
    decode,
    list_tracks,
    parse_tabulate_simple_table,
    rally_race,
    remove_service,
    run_compose,
    run_rally,
    run_service,
    start_elasticsearch,
    start_service,
    teardown_project,
)


def _compose_cfg(
    *,
    cmd: str | list[str] | object | None = None,
    compose_file: str = "/tmp/compose.yaml",
    compose_dir: str = "/tmp/compose",
) -> ComposeConfig:
    c = config.Config()
    if cmd is not None:
        c.add(config.Scope.applicationOverride, "compose", "compose.cmd", cmd)
    c.add(config.Scope.applicationOverride, "compose", "compose.file", compose_file)
    c.add(config.Scope.applicationOverride, "compose", "compose.dir", compose_dir)
    return ComposeConfig.from_config(c)


def _silent_logger() -> logging.Logger:
    return mock.create_autospec(logging.Logger, instance=True)


@dataclass
class DecodeCase:
    given: bytes | None
    want: str


@cases(
    none=DecodeCase(given=None, want=""),
    empty=DecodeCase(given=b"", want=""),
    one_line=DecodeCase(given=b"hello\n", want="\n  hello\n"),
    two_lines=DecodeCase(given=b"a \n b  \n", want="\n  a\n   b\n"),
)
def test_decode(case: DecodeCase):
    assert decode(case.given) == case.want


@dataclass
class ComposeCmdCase:
    cmd_opt: str | list[str] | object | None
    env_cmd: str | None
    want: list[str] | type[BaseException]


@cases(
    from_list=ComposeCmdCase(cmd_opt=["podman", "compose"], env_cmd=None, want=["podman", "compose"]),
    from_string=ComposeCmdCase(cmd_opt="docker-compose -f x", env_cmd=None, want=["docker-compose", "-f", "x"]),
    default_env=ComposeCmdCase(cmd_opt=None, env_cmd="nerdctl compose", want=["nerdctl", "compose"]),
    bad_type=ComposeCmdCase(cmd_opt=42, env_cmd=None, want=TypeError),
)
def test_compose_config_compose_cmd(case: ComposeCmdCase, monkeypatch: pytest.MonkeyPatch):
    if case.env_cmd is not None:
        monkeypatch.setenv("COMPOSE_COMMAND", case.env_cmd)
    else:
        monkeypatch.delenv("COMPOSE_COMMAND", raising=False)
    c = config.Config()
    if case.cmd_opt is not None:
        c.add(config.Scope.applicationOverride, "compose", "compose.cmd", case.cmd_opt)
    cc = ComposeConfig.from_config(c)
    if isinstance(case.want, list):
        assert cc.compose_cmd == case.want
    else:
        with pytest.raises(case.want):
            _ = cc.compose_cmd


@dataclass
class ComposePathsCase:
    compose_file: str
    compose_dir: str | None
    want_file: str
    want_dir: str


@cases(
    explicit_dir=ComposePathsCase(
        compose_file="/proj/compose.yaml",
        compose_dir="/custom",
        want_file="/proj/compose.yaml",
        want_dir="/custom",
    ),
    dir_from_file=ComposePathsCase(
        compose_file="/proj/sub/compose.yaml",
        compose_dir=None,
        want_file="/proj/sub/compose.yaml",
        want_dir=os.path.dirname(rally_root()),
    ),
)
def test_compose_config_paths(case: ComposePathsCase):
    c = config.Config()
    c.add(config.Scope.applicationOverride, "compose", "compose.file", case.compose_file)
    if case.compose_dir is not None:
        c.add(config.Scope.applicationOverride, "compose", "compose.dir", case.compose_dir)
    cc = ComposeConfig.from_config(c)
    assert cc.compose_file == case.want_file
    assert cc.compose_dir == case.want_dir


def test_compose_config_default_file_and_dir_from_class_default(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(ComposeConfig, "_default_compose_file", staticmethod(lambda: "/stack/compose.yaml"))
    c = config.Config()
    cc = ComposeConfig.from_config(c)
    assert cc.compose_file == "/stack/compose.yaml"
    assert cc.compose_dir == os.path.dirname(rally_root())


def test_compose_config_compose_file_from_rally_compose_file_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("RALLY_COMPOSE_FILE", "/env/compose.yaml")
    c = config.Config()
    cc = ComposeConfig.from_config(c)
    assert cc.compose_file == "/env/compose.yaml"


@dataclass
class RunComposeCase:
    command: str
    service: str | None
    args: list[str] | None
    compose_options: list[str] | None
    compose_file: str | None
    compose_dir: str | None
    want_argv_suffix: list[str]
    want_cwd: str
    cfg_compose_file: str = "/tmp/compose.yaml"
    cfg_compose_dir: str = "/tmp/compose"


@cases(
    minimal=RunComposeCase(
        command="ps",
        service=None,
        args=None,
        compose_options=None,
        compose_file=None,
        compose_dir=None,
        want_argv_suffix=["--file", "/tmp/compose.yaml", "ps"],
        want_cwd="/tmp/compose",
    ),
    with_service_and_args=RunComposeCase(
        command="run",
        service="rally",
        args=["list", "tracks"],
        compose_options=["--rm"],
        compose_file=None,
        compose_dir=None,
        want_argv_suffix=["--file", "/tmp/compose.yaml", "run", "--rm", "rally", "list", "tracks"],
        want_cwd="/tmp/compose",
    ),
    no_compose_file=RunComposeCase(
        command="version",
        service=None,
        args=None,
        compose_options=None,
        compose_file=None,
        compose_dir="/other",
        want_argv_suffix=["version"],
        want_cwd="/other",
        cfg_compose_file="",
        cfg_compose_dir="/other",
    ),
)
@mock.patch("esrally.utils.compose.subprocess.run")
def test_run_compose(mock_run: mock.MagicMock, case: RunComposeCase, monkeypatch: pytest.MonkeyPatch) -> None:
    mock_run.return_value = mock.Mock(spec=["returncode", "stdout", "stderr"], returncode=0, stdout=b"", stderr=b"")
    monkeypatch.delenv("COMPOSE_PROJECT_NAME", raising=False)
    cfg = _compose_cfg(compose_file=case.cfg_compose_file, compose_dir=case.cfg_compose_dir)
    run_compose(
        case.command,
        case.service,
        case.args,
        cfg=cfg,
        compose_options=case.compose_options,
        compose_file=case.compose_file,
        compose_dir=case.compose_dir,
        check=True,
    )
    mock_run.assert_called_once()
    call_kw = mock_run.call_args.kwargs
    assert call_kw["cwd"] == case.want_cwd
    # Mirrors compose_dir for build.context (Compose resolves relative context from the compose file dir).
    assert call_kw["env"]["RALLY_DOCKER_DIR"] == case.want_cwd
    assert call_kw["check"] is True
    (argv,) = mock_run.call_args[0]
    assert argv == ["docker", "compose"] + case.want_argv_suffix


@mock.patch("esrally.utils.compose.subprocess.run")
def test_run_compose_injects_project_name_from_compose_project_name_env(mock_run: mock.MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    mock_run.return_value = mock.Mock(spec=["returncode", "stdout", "stderr"], returncode=0, stdout=b"", stderr=b"")
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "rally_it_tracks_gw0")
    cfg = _compose_cfg()
    run_compose("ps", cfg=cfg, check=True)
    (argv,) = mock_run.call_args[0]
    assert argv == [
        "docker",
        "compose",
        "--project-name",
        "rally_it_tracks_gw0",
        "--file",
        "/tmp/compose.yaml",
        "ps",
    ]


@mock.patch("esrally.utils.compose.subprocess.run")
def test_run_compose_skips_injected_project_name_when_compose_options_has_dash_p(
    mock_run: mock.MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    mock_run.return_value = mock.Mock(spec=["returncode", "stdout", "stderr"], returncode=0, stdout=b"", stderr=b"")
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "from_env_should_not_duplicate")
    cfg = _compose_cfg()
    run_compose("ps", cfg=cfg, compose_options=["-p", "explicit"], check=True)
    (argv,) = mock_run.call_args[0]
    assert argv == ["docker", "compose", "--file", "/tmp/compose.yaml", "ps", "-p", "explicit"]


@mock.patch("esrally.utils.compose.subprocess.run")
def test_run_compose_skips_injected_project_name_when_compose_options_has_long_form(
    mock_run: mock.MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    mock_run.return_value = mock.Mock(spec=["returncode", "stdout", "stderr"], returncode=0, stdout=b"", stderr=b"")
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "from_env_should_not_duplicate")
    cfg = _compose_cfg()
    run_compose("ps", cfg=cfg, compose_options=["--project-name", "explicit"], check=True)
    (argv,) = mock_run.call_args[0]
    assert argv == ["docker", "compose", "--file", "/tmp/compose.yaml", "ps", "--project-name", "explicit"]


@dataclass
class WrapperCase:
    func: object
    kwargs: dict
    want_command: str
    want_compose_options_tail: list[str]


@cases(
    run_service_no_rm=WrapperCase(
        func=run_service,
        kwargs={"service": "app", "remove": False},
        want_command="run",
        want_compose_options_tail=["app"],
    ),
    start_detach=WrapperCase(
        func=start_service,
        kwargs={"service": "es", "detach": True},
        want_command="up",
        want_compose_options_tail=["--detach", "es"],
    ),
    remove_force_volumes=WrapperCase(
        func=remove_service,
        kwargs={"service": "es", "force": True, "volumes": True, "check": False},
        want_command="rm",
        want_compose_options_tail=["--force", "--volumes", "es"],
    ),
    remove_stop_force=WrapperCase(
        func=remove_service,
        kwargs={"service": "rally", "force": True, "stop": True, "check": False},
        want_command="rm",
        want_compose_options_tail=["--force", "--stop", "rally"],
    ),
    build=WrapperCase(
        func=build_image,
        kwargs={"service": "rally"},
        want_command="build",
        want_compose_options_tail=["rally"],
    ),
)
@mock.patch("esrally.utils.compose.subprocess.run")
def test_compose_wrappers(mock_run: mock.MagicMock, case: WrapperCase):
    mock_run.return_value = mock.Mock(spec=["returncode", "stdout", "stderr"], returncode=0, stdout=b"", stderr=b"")
    cfg = _compose_cfg(compose_file="", compose_dir="/w")
    kwargs = dict(case.kwargs)
    kwargs["cfg"] = cfg
    kwargs.setdefault("logger", _silent_logger())
    case.func(**kwargs)
    assert mock_run.call_args.kwargs["env"]["RALLY_DOCKER_DIR"] == "/w"
    (argv,) = mock_run.call_args[0]
    assert argv[0:2] == ["docker", "compose"]
    assert case.want_command in argv
    tail_start = argv.index(case.want_command) + 1
    assert argv[tail_start:] == case.want_compose_options_tail


@mock.patch("esrally.utils.compose.subprocess.run")
def test_run_service_remove_true_invokes_rm_after_run(mock_run: mock.MagicMock):
    """``run_service(remove=True)`` ends with ``compose kill`` + ``compose ps -a -q`` (``rm`` excludes one-off run containers)."""
    mock_run.return_value = mock.Mock(spec=["returncode", "stdout", "stderr"], returncode=0, stdout=b"", stderr=b"")
    cfg = _compose_cfg(compose_file="/stack.yaml", compose_dir="/w")
    run_service("rally", ["list", "tracks"], cfg=cfg, compose_options=["-T"], logger=_silent_logger())
    assert mock_run.call_count == 3
    (run_argv,) = mock_run.call_args_list[0][0]
    assert run_argv[run_argv.index("run") + 1 :] == ["-T", "--rm", "rally", "list", "tracks"]
    (kill_argv,) = mock_run.call_args_list[1][0]
    assert kill_argv[kill_argv.index("kill") + 1 :] == ["rally"]
    (ps_argv,) = mock_run.call_args_list[2][0]
    assert ps_argv[ps_argv.index("ps") + 1 :] == ["-a", "-q", "rally"]


@mock.patch("esrally.utils.compose.subprocess.run")
def test_run_service_timeout_still_invokes_rm(mock_run: mock.MagicMock):
    """Even when ``run`` hits ``TimeoutExpired``, ``finally`` still runs kill + ps + ``docker rm -f`` for listed IDs."""
    mock_run.side_effect = [
        subprocess.TimeoutExpired(cmd=["docker", "compose"], timeout=1),
        mock.Mock(spec=["returncode", "stdout", "stderr"], returncode=0, stdout=b"", stderr=b""),
        mock.Mock(spec=["returncode", "stdout", "stderr"], returncode=0, stdout=b"deadbeef\ncafebabe\n", stderr=b""),
        mock.Mock(spec=["returncode", "stdout", "stderr"], returncode=0, stdout=b"", stderr=b""),
    ]
    cfg = _compose_cfg(compose_file="", compose_dir="/w")
    with pytest.raises(subprocess.TimeoutExpired):
        run_service("rally", ["x"], cfg=cfg, logger=_silent_logger(), timeout=1)
    assert mock_run.call_count == 4
    (kill_argv,) = mock_run.call_args_list[1][0]
    assert kill_argv[kill_argv.index("kill") + 1 :] == ["rally"]
    (ps_argv,) = mock_run.call_args_list[2][0]
    assert ps_argv[ps_argv.index("ps") + 1 :] == ["-a", "-q", "rally"]
    (docker_rm_argv,) = mock_run.call_args_list[3][0]
    assert docker_rm_argv == ["docker", "rm", "-f", "deadbeef", "cafebabe"]


@dataclass
class ParseTracksCase:
    stdout: str
    want: list[dict[str, str]]


def _tracks_stdout(headers: list[str], rows: list[list[str]]) -> str:
    return "Available tracks:\n\n" + tabulate.tabulate(rows, headers=headers)


@cases(
    empty=ParseTracksCase(
        stdout=_tracks_stdout(["Name", "Description", "Documents"], []),
        want=[],
    ),
    two_rows=ParseTracksCase(
        stdout=_tracks_stdout(
            ["Name", "Description", "Documents", "Compressed Size", "Uncompressed Size"],
            [
                ["geonames", "Rally track for geonames", "1", "2.3 GB", "3.4 GB"],
                ["nyc_taxis", "NYC taxi rides", "2", "4 GB", "5 GB"],
            ],
        ),
        want=[
            {
                "Name": "geonames",
                "Description": "Rally track for geonames",
                "Documents": "1",
                "Compressed Size": "2.3 GB",
                "Uncompressed Size": "3.4 GB",
            },
            {
                "Name": "nyc_taxis",
                "Description": "NYC taxi rides",
                "Documents": "2",
                "Compressed Size": "4 GB",
                "Uncompressed Size": "5 GB",
            },
        ],
    ),
    with_challenges=ParseTracksCase(
        stdout=_tracks_stdout(
            [
                "Name",
                "Description",
                "Documents",
                "Compressed Size",
                "Uncompressed Size",
                "Default Challenge",
                "All Challenges",
            ],
            [["t1", "d1", "1", "1 B", "2 B", "c1", "c1,c2"]],
        ),
        want=[
            {
                "Name": "t1",
                "Description": "d1",
                "Documents": "1",
                "Compressed Size": "1 B",
                "Uncompressed Size": "2 B",
                "Default Challenge": "c1",
                "All Challenges": "c1,c2",
            }
        ],
    ),
    no_table=ParseTracksCase(stdout="Nothing to see\n", want=[]),
)
def test_parse_tabulate_simple_table(case: ParseTracksCase):
    assert parse_tabulate_simple_table(case.stdout) == case.want


@mock.patch("esrally.utils.compose.run_rally")
def test_list_tracks(mock_run_rally: mock.MagicMock):
    headers = ["Name", "Description", "Documents", "Compressed Size", "Uncompressed Size"]
    rows = [["a", "b", "1", "1 B", "2 B"]]
    text = _tracks_stdout(headers, rows)
    mock_run_rally.return_value = mock.Mock(stdout=text.encode("utf-8"))
    got = list_tracks(logger=_silent_logger())
    assert got == [{"Name": "a", "Description": "b", "Documents": "1", "Compressed Size": "1 B", "Uncompressed Size": "2 B"}]


@dataclass
class RallyRaceCase:
    test_mode: bool
    target_hosts: list[str] | None
    pipeline: str | None
    want_rally_options: list[str]
    challenge: str | None = None
    it_tracks_root: str | None = None
    track_name: str = "t"


@cases(
    minimal=RallyRaceCase(
        test_mode=False,
        target_hosts=None,
        pipeline="benchmark-only",
        want_rally_options=["--track", "t", "--pipeline", "benchmark-only"],
    ),
    test_mode=RallyRaceCase(
        test_mode=True,
        target_hosts=None,
        pipeline=None,
        want_rally_options=["--track", "t", "--test-mode"],
    ),
    targets=RallyRaceCase(
        test_mode=False,
        target_hosts=["h1:9200", "h2:9200"],
        pipeline="p",
        want_rally_options=["--track", "t", "--target-hosts", "h1:9200,h2:9200", "--pipeline", "p"],
    ),
    with_challenge=RallyRaceCase(
        test_mode=False,
        target_hosts=None,
        pipeline="benchmark-only",
        want_rally_options=["--track", "t", "--pipeline", "benchmark-only", "--challenge", "append-no-conflicts"],
        challenge="append-no-conflicts",
    ),
    bundled_track_path=RallyRaceCase(
        test_mode=False,
        target_hosts=None,
        pipeline="benchmark-only",
        want_rally_options=["--track-path", "/tracks/elastic/apm", "--pipeline", "benchmark-only"],
        it_tracks_root="/tracks",
        track_name="elastic/apm",
    ),
)
@mock.patch("esrally.utils.compose.run_rally")
def test_rally_race(mock_run: mock.MagicMock, case: RallyRaceCase, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ES_VERSION", "8.0.0")
    if case.it_tracks_root is None:
        monkeypatch.delenv("RALLY_IT_TRACKS_ROOT", raising=False)
    else:
        monkeypatch.setenv("RALLY_IT_TRACKS_ROOT", case.it_tracks_root)
    log = _silent_logger()
    rally_race(
        case.track_name,
        test_mode=case.test_mode,
        target_hosts=case.target_hosts,
        pipeline=case.pipeline,
        challenge=case.challenge,
        logger=log,
    )
    mock_run.assert_called_once()
    args, kwargs = mock_run.call_args
    assert args[0] == "race"
    assert kwargs["rally_options"] == case.want_rally_options


@mock.patch("esrally.utils.compose.start_service")
def test_start_elasticsearch(mock_start: mock.MagicMock, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ES_VERSION", "8.0.0")
    log = _silent_logger()
    start_elasticsearch("es01", detach=True, logger=log)
    mock_start.assert_called_once_with("es01", detach=True, logger=log)


@mock.patch("esrally.utils.compose.run_compose")
@mock.patch("esrally.utils.compose._cleanup_compose_run_service")
def test_teardown_project(mock_cleanup: mock.MagicMock, mock_run_compose: mock.MagicMock) -> None:
    cfg = _compose_cfg()
    log = _silent_logger()
    teardown_project(cfg=cfg, logger=log)
    mock_cleanup.assert_called_once_with(None, cfg=cfg, logger=log)
    mock_run_compose.assert_called_once()
    assert mock_run_compose.call_args[0][0] == "down"
    assert mock_run_compose.call_args[1]["check"] is False


@mock.patch("esrally.utils.compose.subprocess.run")
def test_run_rally_invokes_run_service(mock_run: mock.MagicMock):
    mock_run.return_value = mock.Mock(spec=["returncode", "stdout", "stderr"], returncode=0, stdout=b"", stderr=b"")
    cfg = _compose_cfg(compose_file="", compose_dir="/w")
    log = _silent_logger()
    run_rally("list", ["tracks"], cfg=cfg, logger=log)
    assert mock_run.call_count == 3
    assert mock_run.call_args_list[0].kwargs["env"]["RALLY_DOCKER_DIR"] == "/w"
    (argv,) = mock_run.call_args_list[0][0]
    assert "run" in argv
    assert "rally" in argv
    assert argv[-2:] == ["list", "tracks"]
