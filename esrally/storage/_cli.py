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
import dataclasses
import json
import logging
import os.path
import shlex
import subprocess
import sys
import time
import urllib
from typing import Any, Literal
from urllib.parse import urlparse

from esrally import storage

LOG = logging.getLogger(__name__)


LsFormat = Literal["json", "filebeat", "pretty"]


def main():
    cfg = storage.StorageConfig()
    try:
        cfg.load_config()
    except FileNotFoundError:
        LOG.info("No configuration file found, using default configuration")

    parser = argparse.ArgumentParser(description="Interacts with ES Rally remote storage services.")

    subparsers = parser.add_subparsers(dest="command")
    ls_parser = subparsers.add_parser("ls", help="List file(s) downloaded from ES Rally remote storage services.")
    get_parser = subparsers.add_parser("get", help="Download file(s) from ES Rally rem1ote storage services.")
    put_parser = subparsers.add_parser("put", help="Upload file(s) to mirror server.")

    for p in (ls_parser, get_parser, put_parser):
        p.add_argument("-v", "--verbose", action="count", required=False, default=0, help="Increase verbosity level.")
        p.add_argument("-q", "--quiet", action="count", required=False, default=0, help="Decrease verbosity level.")
        p.add_argument("--local-dir", type=str, default=cfg.local_dir, help="destination directory for downloading files")
        p.add_argument("--base-url", type=str, default=cfg.base_url, help="base URL for remote storage.")
        p.add_argument("--mirror-failures", action="store_true", help="It upload only files that have recorded mirror failures.")

    ls_parser.add_argument("urls", type=str, nargs="*")
    ls_parser.add_argument("--filebeat", action="store_true", help="It prints a JSON entry for each file, each separated by a newline.")
    ls_parser.add_argument("--json", action="store_true", help="It prints a pretty entry for each file.")
    ls_parser.add_argument("--stats", action="store_true", help="It prints a pretty entry for each file.")

    get_parser.add_argument("urls", type=str, nargs="*")
    get_parser.add_argument("--range", type=str, default="", help="It will only download given range of each file.")
    get_parser.add_argument("--mirrors", type=str, default="", nargs="*", help="It will look for mirror services in given mirror file.")

    put_parser.add_argument("urls", type=str, nargs="*")
    put_parser.add_argument("target_dir", type=str)

    args = parser.parse_args()
    logging_level = (args.quiet - args.verbose) * (logging.INFO - logging.DEBUG) + logging.INFO
    logging.basicConfig(level=logging_level, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    if args.local_dir:
        cfg.local_dir = args.local_dir

    if args.base_url:
        cfg.base_url = args.base_url

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

    if args.mirror_failures:
        transfers = [tr for tr in transfers if tr.mirror_failures]
        if not transfers:
            LOG.info("No transfers with mirror failures.")
            return

    match args.command:
        case None | "ls":
            fmt: LsFormat = "pretty"
            if args.json:
                fmt = "json"
            elif args.filebeat:
                fmt = "filebeat"
            ls(transfers, fmt=fmt, stats=args.stats, mirror_failures=args.mirror_failures)
        case "get":
            if args.mirrors:
                cfg.mirror_files = args.mirrors
            get(transfers, todo=storage.rangeset(args.range))
        case "put":
            put(transfers, args.target_dir, base_url=cfg.base_url)
        case _:
            LOG.critical("Invalid command: %r", args.command)
            sys.exit(3)


def ls(transfers: list[storage.Transfer], *, fmt: LsFormat = "json", stats: bool = False, mirror_failures: bool = False) -> None:
    LOG.info("Found %d transfer(s).", len(transfers))

    match fmt:
        case "filebeat":
            for o in transfers_as_dictionaries(transfers):
                line = json.dumps({"rally": {"storage": o}})
                sys.stdout.write(f"{line}\n")
        case "json":
            json.dump(transfers_as_dictionaries(transfers), sys.stdout, indent=2, sort_keys=True)
        case "pretty":
            for t in transfers:
                print(t.info(stats=stats, mirror_failures=mirror_failures))


def transfers_as_dictionaries(transfers: list[storage.Transfer]) -> list[dict[str, Any]]:
    return [
        {
            "url": tr.url,
            "path": tr.path,
            "status": str(tr.status),
            "progress": tr.progress,
            "errors": [str(e) for e in tr.errors],
            "document_length": tr.document_length,
            "crc32": tr.crc32c,
            "done": str(tr.done),
            "mirror_failures": tr.mirror_failures,
            "has_mirror_failures": bool(tr.mirror_failures),
            "todo": str(tr.todo),
            "finished": tr.finished,
            "stats": {u: dataclasses.asdict(s) for u, s in tr.stats.items()},
        }
        for tr in transfers
    ]


def get(transfers: list[storage.Transfer], *, todo: storage.RangeSet = storage.NO_RANGE) -> None:
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
            time.sleep(2.0)
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


def normalise_url(url: str, *, base_url: str | None = None) -> str:
    u = urllib.parse.urlparse(url, "https")
    if not u.netloc and base_url:
        u = urllib.parse.urlparse(f"{base_url.rstrip('/')}/{u.path}")
    return u.geturl()
