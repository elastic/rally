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
import dataclasses
import json
import logging
import os
import subprocess
import sys
from typing import Any, Literal

import pytest

from esrally import storage
from esrally.utils import cases

LOG = logging.getLogger(__name__)

COMMAND = [sys.executable, "-m", storage.__name__]

BASE_URL = storage.StorageConfig.DEFAULT_BASE_URL.rstrip("/")

LOGGER_NAME = "esrally.storage._cli"

FIRST_PATH = "apm/documents-1k.ndjson.bz2"
FIRST_URL = f"{BASE_URL}/{FIRST_PATH}"

SECOND_PATH = "apm/documents.ndjson.bz2"
SECOND_URL = f"{BASE_URL}/{SECOND_PATH}"

SUCCESS_GET_STDERR_LINES = [f"INFO {LOGGER_NAME} All transfers done."]
SUCCESS_LS_STDERR_LINES = []


@pytest.fixture(autouse=True)
def local_dir(monkeypatch: pytest.MonkeyPatch, tmpdir) -> str:
    local_dir = str(tmpdir.mkdir("local"))
    monkeypatch.setattr(storage.StorageConfig, "DEFAULT_LOCAL_DIR", local_dir)
    os.environ["RALLY_STORAGE_LOCAL_DIR"] = local_dir
    return local_dir


RESOURCES_DIR = os.path.join(os.path.dirname(__file__), "resources")
GOOD_MIRROR_FILES = os.path.join(RESOURCES_DIR, "good-mirror.json")
GOOD_MIRROR_URL = "https://storage.googleapis.com/rally-tracks/apm/documents-1k.ndjson.bz2"
BAD_MIRROR_FILES = os.path.join(RESOURCES_DIR, "bad-mirror.json")
BAD_MIRROR_URL = "https://storage.googleapis.com/invalid-rally-tracks/apm/documents-1k.ndjson.bz2"


@pytest.fixture(autouse=True)
def mirror_files(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(storage.StorageConfig, "DEFAULT_MIRROR_FILES", "")
    os.environ["RALLY_STORAGE_MIRROR_FILES"] = ""


@pytest.fixture()
def cfg() -> storage.StorageConfig:
    cfg = storage.StorageConfig()
    return cfg


@pytest.fixture()
def client(cfg: storage.StorageConfig) -> storage.Client:
    return storage.Client.from_config(cfg)


@dataclasses.dataclass
class LsCase:
    args: list[str]
    mirror_files: list[str] | None = None
    after_get_params: dict[str, Any] | None = None
    want_format: Literal["json", "ndjson"] = "json"
    want_return_code: int = 0
    want_output: dict[str, dict[str, Any]] | None = None
    want_stderr_lines: list[str] = dataclasses.field(default_factory=lambda: list(SUCCESS_LS_STDERR_LINES))


@cases.cases(
    no_args=LsCase(
        [],
        want_return_code=1,
        want_stderr_lines=[
            f"INFO {LOGGER_NAME} No transfers found.",
        ],
    ),
    no_urls=LsCase(
        ["ls"],
        want_return_code=1,
        want_stderr_lines=[
            f"INFO {LOGGER_NAME} No transfers found.",
        ],
    ),
    no_args_after_get=LsCase(
        [],
        after_get_params={"url": FIRST_URL, "todo": storage.Range(0, 1024)},
        want_output={
            FIRST_URL: {
                "status": "INITIALIZED",
                "done": "0-1023",
                "finished": False,
            }
        },
        want_stderr_lines=[
            f"INFO {LOGGER_NAME} Found 1 transfer(s).",
        ],
    ),
    no_urls_after_get=LsCase(
        ["ls"],
        after_get_params={"url": FIRST_URL, "todo": storage.Range(0, 1024)},
        want_output={
            FIRST_URL: {
                "status": "INITIALIZED",
                "done": "0-1023",
                "finished": False,
            }
        },
        want_stderr_lines=[
            f"INFO {LOGGER_NAME} Found 1 transfer(s).",
        ],
    ),
    path_after_get=LsCase(
        ["ls", FIRST_PATH],
        after_get_params={"url": FIRST_URL, "todo": storage.Range(0, 64)},
        want_output={
            FIRST_URL: {
                "status": "INITIALIZED",
                "done": "0-63",
                "finished": False,
            }
        },
        want_stderr_lines=[
            f"INFO {LOGGER_NAME} Found 1 transfer(s).",
        ],
    ),
    url_after_get=LsCase(
        ["ls", FIRST_URL],
        after_get_params={"url": FIRST_URL, "todo": storage.Range(0, 64)},
        want_output={
            FIRST_URL: {
                "status": "INITIALIZED",
                "done": "0-63",
                "finished": False,
            }
        },
        want_stderr_lines=[
            f"INFO {LOGGER_NAME} Found 1 transfer(s).",
        ],
    ),
    after_get_with_bad_mirrors=LsCase(
        ["ls", FIRST_URL],
        mirror_files=[BAD_MIRROR_FILES],
        after_get_params={"url": FIRST_URL, "todo": storage.Range(0, 64)},
        want_stderr_lines=[
            f"INFO {LOGGER_NAME} Found 1 transfer(s).",
        ],
        want_output={
            FIRST_URL: {
                "finished": False,
                "status": "INITIALIZED",
                "done": "0-63",
                "mirror_failures": {BAD_MIRROR_URL: f"FileNotFoundError:Can't get file head: {BAD_MIRROR_URL}"},
            }
        },
    ),
    ndjson_after_get_files=LsCase(
        ["ls", "--ndjson", FIRST_URL],
        after_get_params={"url": FIRST_URL, "todo": storage.Range(0, 64)},
        want_output={
            FIRST_URL: {
                "status": "INITIALIZED",
                "done": "0-63",
                "finished": False,
            }
        },
        want_format="ndjson",
        want_stderr_lines=[
            f"INFO {LOGGER_NAME} Found 1 transfer(s).",
        ],
    ),
)
def test_ls(case: LsCase, tmpdir, cfg: storage.StorageConfig):
    if case.mirror_files:
        cfg.mirror_files = case.mirror_files

    if case.after_get_params is not None:
        with storage.TransferManager.from_config(cfg) as manager:
            manager.get(**case.after_get_params).wait(timeout=15)

    cwd = str(tmpdir.mkdir("cwd"))
    result = subprocess.run(COMMAND + case.args, cwd=cwd, capture_output=True, check=not case.want_return_code)
    assert result.returncode == case.want_return_code, f"STDERR: {result.stderr}"
    for line in case.want_stderr_lines:
        assert line.encode("utf-8") in result.stderr

    if not case.want_output:
        assert b"" == result.stdout
        return

    got_output: dict[str, dict] = {}
    match case.want_format:
        case "json":
            got_output.update((got["url"], got) for got in json.loads(result.stdout))
        case "ndjson":
            for line in result.stdout.splitlines():
                got = json.loads(line)
                got_output[got["url"]] = got
        case _:
            pytest.fail(f"Unexpected output format: {case.want_format}")

    assert set(got_output) == set(case.want_output)
    for want_url, want in case.want_output.items():
        assert want_url in got_output
        got = got_output[want_url]

        assert got["path"] == cfg.transfer_file_path(want_url)
        assert got["mirror_failures"] == want.get("mirror_failures", {})
        assert got["done"] == want["done"]
        assert got["status"] == want["status"]
        assert got["finished"] == want["finished"]


@dataclasses.dataclass
class GetCase:
    args: list[str]
    after_get_params: dict[str, Any] | None = None
    want_return_code: int = 0
    want_stdout: bytes = b""
    want_stderr_lines: list[str] = dataclasses.field(default_factory=lambda: list(SUCCESS_GET_STDERR_LINES))
    want_status: dict[str, dict] = dataclasses.field(default_factory=dict)


@cases.cases(
    no_urls=GetCase(["get"]),
    one_path=GetCase(
        ["get", FIRST_PATH],
        want_stderr_lines=SUCCESS_GET_STDERR_LINES + [f"INFO {LOGGER_NAME} Download terminated: {FIRST_URL}"],
        want_status={FIRST_URL: {"done": "0-63457"}},
    ),
    one_url=GetCase(
        ["get", FIRST_URL],
        want_stderr_lines=SUCCESS_GET_STDERR_LINES + [f"INFO {LOGGER_NAME} Download terminated: {FIRST_URL}"],
        want_status={FIRST_URL: {"done": "0-63457"}},
    ),
    two_urls=GetCase(
        ["get", "--range=0-1023", FIRST_URL, SECOND_PATH],
        want_stderr_lines=SUCCESS_GET_STDERR_LINES
        + [
            f"INFO {LOGGER_NAME} Download terminated: {FIRST_URL}",
            f"INFO {LOGGER_NAME} Download terminated: {SECOND_URL}",
        ],
        want_status={FIRST_URL: {"done": "0-1023"}, SECOND_URL: {"done": "0-1023"}},
    ),
    range=GetCase(
        ["get", "--range=1024-2043", FIRST_URL],
        want_stderr_lines=SUCCESS_GET_STDERR_LINES + [f"INFO {LOGGER_NAME} Download terminated: {FIRST_URL}"],
        want_status={FIRST_URL: {"done": "1024-2043"}},
    ),
    resume=GetCase(
        ["get", "--resume"],
        after_get_params={"url": FIRST_URL, "todo": storage.Range(1024, 2048)},
        want_stderr_lines=SUCCESS_GET_STDERR_LINES + [f"INFO {LOGGER_NAME} Download terminated: {FIRST_URL}"],
        want_status={FIRST_URL: {"done": "0-63457"}},
    ),
    good_mirrors=GetCase(
        ["-v", "get", "--mirrors", GOOD_MIRROR_FILES, "--range=0-63,128-255", FIRST_URL],
        want_stderr_lines=SUCCESS_GET_STDERR_LINES
        + [
            f"DEBUG esrally.storage._transfer Downloading file fragment from '{GOOD_MIRROR_URL}'",
            f"INFO {LOGGER_NAME} Download terminated: {FIRST_URL}",
        ],
        want_status={FIRST_URL: {"done": "0-63,128-255"}},
    ),
    bad_mirrors=GetCase(
        ["get", "--mirrors", BAD_MIRROR_FILES, "--range=-63", FIRST_URL],
        want_stderr_lines=SUCCESS_GET_STDERR_LINES
        + [
            f"WARNING esrally.storage._client Failed to get head from mirror URL: '{BAD_MIRROR_URL}'",
            f"INFO {LOGGER_NAME} Download terminated: {FIRST_URL}",
        ],
        want_status={
            FIRST_URL: {"done": "0-63", "mirror_failures": {BAD_MIRROR_URL: f"FileNotFoundError:Can't get file head: {BAD_MIRROR_URL}"}}
        },
    ),
)
def test_get(case: GetCase, tmpdir, local_dir: str, cfg: storage.StorageConfig, client: storage.Client):
    if case.after_get_params is not None:
        with storage.TransferManager.from_config(cfg) as manager:
            manager.get(**case.after_get_params).wait(timeout=15)

    cwd = str(tmpdir.mkdir("cwd"))
    try:
        result = subprocess.run(COMMAND + case.args, cwd=cwd, capture_output=True, check=not case.want_return_code)
    except subprocess.CalledProcessError as ex:
        LOG.critical("Command '%s' returned non-zero exit status %d", COMMAND, ex.returncode)
        LOG.critical("STDERR:\n%s", ex.stderr.decode("utf-8"))
        raise

    assert result.returncode == case.want_return_code
    assert result.stdout == case.want_stdout
    for line in case.want_stderr_lines:
        assert line.encode("utf-8") in result.stderr

    for want_url, want in case.want_status.items():
        want_path = cfg.transfer_file_path(want_url)
        assert os.path.isfile(want_path)

        assert storage.rangeset(want["done"]).end == os.path.getsize(want_path)

        with open(cfg.transfer_status_path(want_url), "rb") as fd:
            got = json.load(fd)

        head = client.head(want_url)
        assert got["url"] == want_url
        assert got["path"] == want_path
        assert got["document_length"] == head.content_length
        assert got["done"] == want["done"]
        assert got["mirror_failures"] == want.get("mirror_failures", {})


@dataclasses.dataclass
class PutCase:
    args: list[str]
    mirror_files: list[str] | None = None
    after_get_params: dict[str, Any] | None = None
    want_return_code: int = 0
    want_stdout: bytes = b""
    want_stderr_lines: list[str] = dataclasses.field(default_factory=list)
    want_files: list[str] = dataclasses.field(default_factory=list)


@cases.cases(
    no_urls=PutCase(
        ["put", "target"],
        want_return_code=1,
    ),
    no_urls_after_get=PutCase(
        ["put", "target"],
        after_get_params={"url": FIRST_URL},
        want_return_code=0,
        want_files=[f"./target/{FIRST_PATH}"],
    ),
)
def test_put(case, cfg: storage.StorageConfig, client: storage.Client, tmpdir):
    try:
        subprocess.run(["which", "rclone"], check=True)
    except subprocess.CalledProcessError:
        LOG.critical("rclone is not installed")
        pytest.skip("rclone is not installed")

    if case.after_get_params is not None:
        with storage.TransferManager.from_config(cfg) as manager:
            manager.get(**case.after_get_params).wait(timeout=15)

    cwd = str(tmpdir.mkdir("cwd"))
    try:
        result = subprocess.run(COMMAND + case.args, cwd=cwd, capture_output=True, check=not case.want_return_code)
    except subprocess.CalledProcessError as ex:
        LOG.critical("Command '%s' returned non-zero exit status %d", COMMAND, ex.returncode)
        LOG.critical("STDERR:\n%s", ex.stderr.decode("utf-8"))
        raise

    assert result.returncode == case.want_return_code
    assert result.stdout == case.want_stdout
    for line in case.want_stderr_lines:
        assert line.encode("utf-8") in result.stderr

    try:
        find_result = subprocess.run(["find", ".", "-type", "f"], cwd=cwd, capture_output=True, check=not case.want_files)
    except subprocess.CalledProcessError as ex:
        LOG.critical("Command '%s' returned non-zero exit status %d", COMMAND, ex.returncode)
        LOG.critical("STDERR:\n%s", ex.stderr.decode("utf-8"))
        raise

    assert find_result.stdout.decode("utf-8").splitlines() == case.want_files
