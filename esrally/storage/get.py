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
import time
import urllib

from esrally.storage import StorageConfig, Transfer, init_transfer_manager

LOG = logging.getLogger("esrally.storage")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    cfg = StorageConfig()
    cfg.load_config()

    parser = argparse.ArgumentParser(description="Download files from remote storage service.")
    parser.add_argument(
        "--local-dir", dest="local_dir", type=str, default=cfg.local_dir, help="destination directory for files to be downloaded"
    )
    parser.add_argument("urls", type=str, nargs="+")
    args = parser.parse_args()

    if args.local_dir:
        cfg.local_dir = args.local_dir

    manager = init_transfer_manager(cfg=cfg)

    all: dict[str, Transfer | None] = {normalise_url(url): None for url in args.urls}
    try:
        unfinished = set(all)
        while unfinished:
            LOG.info("Downloading files: \n - %s", "\n - ".join(f"{u}: {(s := all.get(u)) and s.progress or 0.:.2f}%" for u in unfinished))
            for url in list(unfinished):
                transfer: Transfer = manager.get(url)
                assert isinstance(transfer, Transfer)
                all[url] = transfer
                if transfer.finished:
                    unfinished.remove(url)
                    LOG.info("Download terminated: %s", transfer.url)
                    continue
            if unfinished:
                time.sleep(2.0)
    finally:
        LOG.info("Downloaded files: \n - %s", "\n - ".join(f"{u}: {s and s.progress or '?'}%" for u, s in all.items()))


def normalise_url(url: str) -> str:
    u = urllib.parse.urlparse(url, "https")
    if not u.netloc:
        u = urllib.parse.urlparse(f"{u.scheme}://rally-tracks.elastic.co/{u.path}")
    return u.geturl()


if __name__ == "__main__":
    main()
