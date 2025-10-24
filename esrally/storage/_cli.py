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
import sys
import time
import urllib

from esrally import storage, types

LOG = logging.getLogger(__name__)


def main():
    cfg = storage.StorageConfig()
    try:
        cfg.load_config()
    except FileNotFoundError:
        LOG.info("No configuration file found, using default configuration")

    parser = argparse.ArgumentParser(description="Interacts with ES Rally remote storage services.")
    subparsers = parser.add_subparsers(dest="command")
    parser.add_argument("-v", "--verbose", action="count", required=False, default=0, help="Increase verbosity level.")
    parser.add_argument("-q", "--quiet", action="count", required=False, default=0, help="Decrease verbosity level.")
    parser.add_argument(
        "--local-dir", dest="local_dir", type=str, default=cfg.local_dir, help="destination directory for files to be downloaded"
    )

    ls_parser = subparsers.add_parser("ls", help="List file(s) downloaded from ES Rally remote storage services.")
    ls_parser.add_argument("urls", type=str, nargs="*")
    ls_parser.add_argument("--ndjson", action="store_true", help="It prints a JSON entry for each file, each separated by a newline.")

    get_parser = subparsers.add_parser("get", help="Download file(s) from ES Rally remote storage services.")
    get_parser.add_argument("urls", type=str, nargs="*")
    get_parser.add_argument("--range", type=str, default="", help="It will only download given range of each file.")
    get_parser.add_argument("--resume", action="store_true", help="It resumes interrupted downloads")
    get_parser.add_argument("--mirrors", type=str, default="", nargs="*", help="It will look for mirror services in given mirror file.")

    args = parser.parse_args()
    logging_level = (args.quiet - args.verbose) * (logging.INFO - logging.DEBUG) + logging.INFO
    logging.basicConfig(level=logging_level, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    if args.local_dir:
        cfg.local_dir = args.local_dir

    match args.command:
        case "ls" | None:
            ls(cfg, args)
        case "get":
            get(cfg, args)
        case _:
            LOG.critical("Invalid command: %r", args.command)
            sys.exit(3)


def ls(cfg: types.Config, args: argparse.Namespace) -> None:
    cfg = storage.StorageConfig.from_config(cfg)

    manager = storage.init_transfer_manager(cfg=cfg)
    urls: list[str] = []
    if args.command == "ls":
        urls.extend(normalise_url(url, base_url=cfg.base_url) for u in (args.urls or []) if (url := u.strip()))
    try:
        transfers: list[storage.Transfer] = manager.list(urls=urls, start=False)
    except FileNotFoundError as ex:
        LOG.warning("Failed to list transfers: %s", ex)
        LOG.info("No transfers found.")
        sys.exit(1)
    except Exception as ex:
        LOG.critical("Failed to list transfers: %s", ex)
        sys.exit(2)

    LOG.info("Found %d transfer(s).", len(transfers))
    output = [
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
            "todo": str(tr.todo),
            "finished": tr.finished,
        }
        for tr in transfers
    ]

    if args.command == "ls" and args.ndjson:
        for o in output:
            sys.stdout.write(f"{json.dumps(o)}\n")
        return

    json.dump(output, sys.stdout, indent=2, sort_keys=True)


def get(cfg: types.Config, args: argparse.Namespace) -> None:
    cfg = storage.StorageConfig.from_config(cfg)
    if args.mirrors:
        cfg.mirror_files = args.mirrors

    manager = storage.init_transfer_manager(cfg=cfg)

    todo: storage.RangeSet = storage.rangeset(args.range) if args.range else storage.NO_RANGE

    transfers: dict[str, storage.Transfer] = {}
    if args.resume:
        try:
            transfers.update((tr.url, tr) for tr in manager.list(start=True, todo=todo))
        except FileNotFoundError as ex:
            LOG.critical("Unable to find any status file in local dir: %s. Error: %s", cfg.local_dir, ex)
            sys.exit(1)

    if args.urls:
        urls = [normalise_url(url, base_url=cfg.base_url) for u in args.urls if (url := u.strip())]
        try:
            transfers.update((tr.url, tr) for tr in manager.list(urls=urls, start=True, todo=todo))
        except FileNotFoundError as ex:
            LOG.critical("Unable to start new transfers. Error: %s", ex)

    errors: dict[str, list[str]] = {}
    while transfers:
        LOG.info("Downloading files: \n - %s", "\n - ".join(f"{url}: {tr.progress or 0.:.2f}%" for url, tr in transfers.items()))
        for url, tr in list(transfers.items()):
            if tr.finished:
                transfers.pop(url)
                if tr.errors:
                    LOG.error("Download failed: %s. Errors: %s", url, tr.errors)
                    errors[url] = [str(e) for e in tr.errors]
                    continue
                LOG.info("Download terminated: %s", url)
                continue
        if transfers:
            time.sleep(2.0)
    if errors:
        LOG.critical("Files download failed. Errors:\n%s", json.dumps(errors, indent=2, sort_keys=True))
        sys.exit(1)
    LOG.info("All transfers done.")


def normalise_url(url: str, *, base_url: str | None = None) -> str:
    u = urllib.parse.urlparse(url, "https")
    if not u.netloc and base_url:
        u = urllib.parse.urlparse(f"{base_url.rstrip('/')}/{u.path}")
    return u.geturl()
