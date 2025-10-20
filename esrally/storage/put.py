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
import logging
import os
import subprocess
import sys
import urllib

from esrally.storage import (
    Range,
    StorageConfig,
    Transfer,
    init_transfer_manager,
    rangeset,
    transfer_status_path,
)

LOG = logging.getLogger("esrally.storage")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    cfg = StorageConfig()
    cfg.load_config()

    parser = argparse.ArgumentParser(description="Files to remote storage services.")
    parser.add_argument(
        "--local-dir", dest="local_dir", type=str, default=cfg.local_dir, help="destination directory for files to be downloaded"
    )
    parser.add_argument("urls", type=str, nargs="*")
    args = parser.parse_args()

    if args.local_dir:
        cfg.local_dir = args.local_dir

    manager = init_transfer_manager(cfg=cfg)
    local_dir = os.path.expanduser(cfg.local_dir)
    urls: list[str] = [normalise_url(url) for u in args.urls if (url := u.strip())]
    if not urls:
        find_status_files_command = ["find", "./", "-name", "*.status"]
        try:
            found_status_files = (
                subprocess.run(find_status_files_command, check=True, cwd=local_dir, capture_output=True, shell=False)
                .stdout.decode("utf-8")
                .strip()
                .split("\n")
            )
        except subprocess.CalledProcessError as e:
            LOG.critical("Unable to find any files to download: %s.\nError: %s", e, e.stderr)
            sys.exit(e.returncode)
        urls = [
            url[len("./") :][: -len(".status")].replace(":/", "://") for f in found_status_files if (url := f.strip()).endswith(".status")
        ]

    all_ok = True
    for url in urls:
        LOG.info("Synchronizing file: %s...", url)
        transfer: Transfer = manager.get(url, start=False)
        assert isinstance(transfer, Transfer)
        if transfer.todo or not transfer.finished:
            LOG.error("Transfer not terminated: %s", url)
            all_ok = False
            continue

        if transfer.errors:
            LOG.error("Transfer has errors: %s, errors: %s", url, transfer.errors)
            all_ok = False
            continue

        for mirror_url, reason in transfer.mirror_failures.items():
            LOG.info("Synchronize mirror URL: %s -> %s (reason=%s)", transfer.url, mirror_url, reason)
            command = ["rclone", "copy", "-vv", transfer.path, os.path.dirname(mirror_url)]
            try:
                subprocess.run(command, check=True, shell=False)
            except subprocess.CalledProcessError as e:
                LOG.error("Unable to synchronize transfer: %s", e)
                all_ok = False
            else:
                LOG.info("Successfully synchronized transfer: %s -> %s", transfer.url, mirror_url)
        LOG.info("Finished synchronizing file: %s", url)
    if not all_ok:
        LOG.error("Some synchronization failed.")
        sys.exit(1)

    LOG.error("Synchronization succeeded.")


def normalise_url(url: str) -> str:
    u = urllib.parse.urlparse(url, "https")
    if not u.netloc:
        u = urllib.parse.urlparse(f"{u.scheme}://rally-tracks.elastic.co/{u.path}")
    return u.geturl()


if __name__ == "__main__":
    main()
