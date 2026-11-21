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

"""
Points Rally at the latest prebuilt Elasticsearch snapshot of a branch, so that the
rally-tracks compatibility tests do not need to build Elasticsearch from source.

It resolves the newest Elasticsearch-only snapshot build published to
artifacts-snapshot.elastic.co (typically a couple of hours behind the branch head),
registers it as the ``es-snapshot`` distribution repository in the ``[distributions]``
section of ``rally.ini`` and prints the pytest-rally options that select it, e.g.::

    --distribution-version=9.7.0-SNAPSHOT --distribution-repository=es-snapshot

The build id is part of the download URL, so all test clusters of one test session use
the same build, and Rally downloads it only once.
"""

import argparse
import configparser
import json
import sys
import urllib.request

from esrally import config

REPOSITORY_NAME = "es-snapshot"
LATEST_URL = "https://artifacts-snapshot.elastic.co/elasticsearch/latest/{branch}.json"
DOWNLOAD_URL = (
    "https://artifacts-snapshot.elastic.co/elasticsearch/{build_id}/downloads/elasticsearch/"
    "elasticsearch-{{{{VERSION}}}}-{{{{OSNAME}}}}-{{{{ARCH}}}}.tar.gz"
)


def fetch_json(url):
    with urllib.request.urlopen(url, timeout=60) as response:
        return json.load(response)


def latest_build(branch):
    latest = fetch_json(LATEST_URL.format(branch=branch))
    manifest = fetch_json(latest["manifest_url"])
    commit = manifest["projects"]["elasticsearch"]["commit_hash"]
    return latest["version"], latest["build_id"], commit, manifest["start_time"]


def register_repository(build_id):
    config_file = config.ConfigFile()
    if config_file.present:
        config_file.backup()
    else:
        config_file.store_default_config()
    # Rally substitutes ${...} placeholders when loading the file; keep them as they are.
    cfg = configparser.ConfigParser(interpolation=None)
    cfg.read(config_file.location, encoding="utf-8")
    if not cfg.has_section("distributions"):
        cfg.add_section("distributions")
    cfg.set("distributions", f"{REPOSITORY_NAME}.url", DOWNLOAD_URL.format(build_id=build_id))
    cfg.set("distributions", f"{REPOSITORY_NAME}.cache", "true")
    with open(config_file.location, "w", encoding="utf-8") as f:
        cfg.write(f)
    return config_file.location


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--branch",
        default="master",
        help="Elasticsearch branch whose latest snapshot should be used ('master' is Elasticsearch's main branch; default: master)",
    )
    args = parser.parse_args()

    version, build_id, commit, build_time = latest_build(args.branch)
    location = register_repository(build_id)
    print(
        f"Using Elasticsearch {version} snapshot build [{build_id}] of commit [{commit}] from [{build_time}] "
        f"via distribution repository [{REPOSITORY_NAME}] in [{location}].",
        file=sys.stderr,
    )
    print(f"--distribution-version={version} --distribution-repository={REPOSITORY_NAME}")


if __name__ == "__main__":
    main()
