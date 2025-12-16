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

import argparse
import json
import logging
import os.path
import shlex
import subprocess
import sys
import time
import urllib
from collections.abc import Generator
from typing import Literal, TypedDict
from urllib.parse import urlparse

from typing_extensions import NotRequired

from esrally import storage

LOG = logging.getLogger(__name__)


LsFormat = Literal["json", "filebeat", "pretty", "filenames"]


def main():
    cfg = storage.StorageConfig()
    try:
        cfg.load_config()
    except FileNotFoundError:
        LOG.info("No configuration file found, using default configuration")

    parser = argparse.ArgumentParser(description="Interacts with ES Rally remote storage services.")

    subparsers = parser.add_subparsers(dest="command")
    ls_parser = subparsers.add_parser("ls", help="It lists file(s) downloaded from ES Rally remote storage services.")
    get_parser = subparsers.add_parser("get", help="It downloads file(s) from ES Rally rem1ote storage services.")
    put_parser = subparsers.add_parser("put", help="It uploads file(s) to mirror server.")
    prune_parser = subparsers.add_parser("prune", help="It deletes transfer files from local directories.")

    for p in (parser, ls_parser, get_parser, put_parser, prune_parser):
        p.add_argument("-v", "--verbose", action="count", required=False, default=0, help="It increases the verbosity level.")
        p.add_argument("-q", "--quiet", action="count", required=False, default=0, help="It decreases the verbosity level.")
        p.add_argument(
            "--local-dir", type=str, default=cfg.local_dir, help="It specifies local destination directory for downloading files."
        )
        p.add_argument("--base-url", type=str, default=None, help="It specifies the base URL for remote storage.")

    # It defines ls sub-command output options.
    for p in (parser, ls_parser):
        p.add_argument("--filebeat", action="store_true", help="It prints a JSON entry for each file, each separated by a newline.")
        p.add_argument("--json", action="store_true", help="It prints a pretty entry for each file.")
        p.add_argument("--stats", action="store_true", help="It shows connectivity statistics in produced output.")
        p.add_argument("--mirror-failures", action="store_true", help="It shows mirror failures in produced output.")
        p.add_argument("--filenames", action="store_true", help="It shows downloaded file names.")
        p.add_argument("--status-filenames", action="store_true", help="It shows status file names.")

    # It defines get sub-command options.
    get_parser.add_argument("--range", type=str, default="", help="It will only download given range of each file.")
    get_parser.add_argument("--mirrors", type=str, default="", nargs="*", help="It will look for mirror services in given mirror file.")
    get_parser.add_argument(
        "--monitor-interval",
        type=float,
        default=cfg.monitor_interval,
        help="It specify the period of time (in seconds) for monitoring ongoing transfers.",
    )

    # It defines prune sub-command options.
    prune_parser.add_argument("--status-filenames", action="store_true", help="It deletes only status files.")
    prune_parser.add_argument("--filenames", action="store_true", help="It deletes downloaded files.")

    # It defines positional arguments.
    for p in (ls_parser, get_parser, put_parser, prune_parser):
        p.add_argument("urls", type=str, nargs="*")

    # It defines put sub-command options.
    put_parser.add_argument("target_dir", type=str)

    args = parser.parse_args()
    logging_level = (args.quiet - args.verbose) * (logging.INFO - logging.DEBUG) + logging.INFO
    logging.basicConfig(level=logging_level, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    if args.local_dir:
        cfg.local_dir = args.local_dir

    if args.base_url:
        cfg.base_url = args.base_url

    if args.command == "get":
        if args.mirrors:
            cfg.mirror_files = args.mirrors
        if args.monitor_interval:
            cfg.monitor_interval = args.monitor_interval

    manager = storage.init_transfer_manager(cfg=cfg)
    urls: list[str] = []
    if args.command and args.urls:
        urls.extend(normalise_url(url, base_url=cfg.base_url) for u in (args.urls or []) if (url := u.strip()))
    try:
        transfers: list[storage.Transfer] = manager.list(urls=urls, start=False)
    except FileNotFoundError as ex:
        if args.command and args.urls:
            LOG.critical("Failed to list transfers: %s", ex)
            sys.exit(1)
        LOG.info("No transfers found.")
        return
    except Exception as ex:
        LOG.critical("Failed to list transfers: %s", ex)
        sys.exit(2)

    if LOG.isEnabledFor(logging.DEBUG):
        LOG.debug("Found %d transfer(s):\n%s", len(transfers), "\n".join(tr.info() for tr in transfers))

    if args.base_url:
        transfers = [tr for tr in transfers if tr.url.startswith(args.base_url.rstrip("/"))]
        if not transfers:
            LOG.info("No transfers with base URL: %s.", args.base_url)
            return

    file_types: set[storage.TransferFileType] | None = None
    if args.status_filenames or args.filenames:
        file_types = set[storage.TransferFileType]()
        if args.filenames:
            file_types.add("data")
        if args.status_filenames:
            file_types.add("status")

    match args.command:
        case None | "ls":
            fmt: LsFormat = "pretty"
            if file_types:
                fmt = "filenames"
            elif args.json:
                fmt = "json"
            elif args.filebeat:
                fmt = "filebeat"
            ls(transfers, fmt=fmt, stats=args.stats, mirror_failures=args.mirror_failures, file_types=file_types)
        case "get":
            get(transfers, todo=storage.rangeset(args.range), monitor_interval=cfg.monitor_interval)
        case "put":
            put(transfers, args.target_dir, base_url=cfg.base_url)
        case "prune":
            prune(transfers, file_types=file_types)
        case _:
            LOG.critical("Invalid command: %r", args.command)
            sys.exit(3)


def ls(
    transfers: list[storage.Transfer],
    *,
    fmt: LsFormat = "json",
    stats: bool = False,
    mirror_failures: bool = False,
    file_types: set[storage.TransferFileType] | None = None,
) -> None:
    LOG.info("Found %d transfer(s).", len(transfers))
    match fmt:
        case "filenames":
            filenames: set[str] = set()
            for tr in transfers:
                filenames.update(tr.ls_files(file_types=file_types))
            sys.stdout.write("\n".join(sorted(filenames)) + "\n")
        case "json":
            json.dump(
                [transfer_to_dict(tr, stats=stats, mirror_failures=mirror_failures) for tr in transfers],
                sys.stdout,
                indent=2,
                sort_keys=True,
            )
        case "filebeat":
            for tr in transfers:
                # Filebeat format is made of the root object (without stats), plus a separate object for each transfer stat.
                for d in transfer_to_filebeat(tr, stats=stats, mirror_failures=mirror_failures):
                    line = json.dumps({"rally": {"storage": d}})
                    sys.stdout.write(f"{line}\n")
        case "pretty":
            json.dump([t.pretty(stats=stats, mirror_failures=mirror_failures) for t in transfers], sys.stdout, indent=2)


class BaseTransferDict(TypedDict):
    url: str
    path: str


class TransferDict(BaseTransferDict):
    progress: float
    errors: list[str]
    document_length: int | None
    crc32c: str | None
    done: str
    todo: str
    finished: bool
    mirror_failures: NotRequired[list[MirrorFailureDict]]
    stats: NotRequired[list[StatsDict]]


class FilebeatStatsDict(BaseTransferDict):
    stats: StatsDict


class FilebeatMirrorFailureDict(BaseTransferDict):
    mirror_failures: MirrorFailureDict


class MirrorFailureDict(TypedDict):
    url: str
    error: str


class StatsDict(TypedDict):
    url: str
    request_count: int
    transferred_bytes: int
    response_time: float
    read_time: float
    write_time: float


def transfer_to_dict(tr: storage.Transfer, *, stats: bool = False, mirror_failures: bool = False) -> TransferDict:
    """It obtains dictionaries from transfer status in the format to be serialized as JSON."""
    d = TransferDict(
        url=tr.url,
        path=tr.path,
        progress=tr.progress,
        errors=[str(e) for e in tr.errors],
        document_length=tr.document_length,
        crc32c=tr.crc32c,
        done=str(tr.done),
        todo=str(tr.todo),
        finished=tr.finished,
    )
    if mirror_failures:
        d["mirror_failures"] = [MirrorFailureDict(url=f.url, error=f.error) for f in tr.mirror_failures]
    if stats:
        d["stats"] = [
            StatsDict(
                url=s.url,
                request_count=s.request_count,
                transferred_bytes=s.transferred_bytes,
                response_time=s.response_time,
                read_time=s.read_time,
                write_time=s.write_time,
            )
            for s in tr.stats
        ]
    return d


def transfer_to_filebeat(
    tr: storage.Transfer, stats: bool = False, mirror_failures: bool = False
) -> Generator[TransferDict | FilebeatStatsDict | FilebeatMirrorFailureDict]:
    """It obtains dictionaries from transfer in the format to be ingested to filebeat.

    After getting a TransferDict as transfer_to_dict it yields:
      - the TransferDict without 'mirror_failures' and 'stats' items;
      - a FilebeatMirrorFailureDict for every MirrorFailureDict in 'mirror_failures' list.
      - a FilebeatStatsDict for every TransferStatsDict in 'stats' list.
    """
    root: TransferDict = transfer_to_dict(tr, stats=stats, mirror_failures=mirror_failures)
    _mirror_failures: list[MirrorFailureDict] = root.pop("mirror_failures", [])
    _stats: list[StatsDict] = root.pop("stats", [])
    yield root
    for f in _mirror_failures:
        yield FilebeatMirrorFailureDict(url=tr.url, path=tr.path, mirror_failures=f)
    for s in _stats:
        yield FilebeatStatsDict(url=tr.url, path=tr.path, stats=s)


def get(
    transfers: list[storage.Transfer],
    *,
    todo: storage.RangeSet = storage.NO_RANGE,
    monitor_interval: float = storage.StorageConfig.DEFAULT_MONITOR_INTERVAL,
) -> None:
    errors: dict[str, list[str]] = {}

    transferring: dict[str, storage.Transfer] = {}
    for tr in transfers:
        tr.start(todo=todo or storage.Range())
        if tr.finished and not tr.errors:
            LOG.debug("Transfer finished: '%s'.", tr.url)
            continue

        transferring[tr.url] = tr
        LOG.debug("Transfer started: '%s'.", tr.url)

    while transferring:
        LOG.info("Downloading files: \n - %s", "\n - ".join(f"{url}: {tr.progress or 0.:.2f}%" for url, tr in transferring.items()))
        for url, tr in list(transferring.items()):
            if tr.finished:
                transferring.pop(url)
                if tr.errors:
                    LOG.error("Transfer failed: %s. Errors: %s", url, tr.errors)
                    errors[url] = [str(e) for e in tr.errors]
                    continue
                LOG.info("Transfer finished: %s", url)
                continue
        if transferring:
            time.sleep(monitor_interval)
    if errors:
        LOG.critical("Files download failed. Errors:\n%s", json.dumps(errors, indent=2, sort_keys=True))
        sys.exit(1)
    LOG.info("All transfers finished.")


def put(transfers: list[storage.Transfer], target_dir: str, *, base_url: str | None = None) -> None:
    target_dir = os.path.normpath(os.path.expanduser(target_dir))
    commands: dict[str, list[str]] = {}
    for tr in transfers:
        url = tr.url
        if base_url and url.startswith(base_url):
            subdir = os.path.dirname(url[len(base_url) :]).strip("/")
        else:
            subdir = os.path.dirname(urlparse(url).path).strip("/")
        if subdir:
            target_dir += f"/{subdir}"

        commands[tr.url] = ["rclone", "copy", tr.path, target_dir]
    if not commands:
        LOG.info("No files to transfer.")
        return

    failures: dict[str, str] = {}
    for url, command in commands.items():
        cmd = " ".join(shlex.quote(s) for s in command)
        LOG.debug("Running command: '%s' ...", cmd)
        try:
            subprocess.run(command, check=True)
        except subprocess.CalledProcessError as ex:
            LOG.error("Failed running command '%s': %s", cmd, ex)
            failures[url] = f"'{cmd}' -> {ex}"

    if failures:
        LOG.critical("Failed transfers: %s", json.dumps(failures, indent=2, sort_keys=True))
        sys.exit(3)

    LOG.info("All transfers finished.")


def prune(transfers: list[storage.Transfer], *, file_types: set[storage.TransferFileType] | None = None) -> None:
    errors: dict[str, str] = {}
    for tr in transfers:
        try:
            tr.prune(file_types=file_types)
        except Exception as ex:
            errors[tr.url] = str(ex)

    if errors:
        LOG.critical("Failed to prune transfer files:\n%s", json.dumps(errors, indent=2, sort_keys=True))
        sys.exit(1)

    LOG.info("All transfers files pruned.")


def normalise_url(url: str, *, base_url: str | None = None) -> str:
    u = urllib.parse.urlparse(url, "https")
    if not u.netloc and base_url:
        u = urllib.parse.urlparse(f"{base_url.rstrip('/')}/{u.path}")
    return u.geturl()
