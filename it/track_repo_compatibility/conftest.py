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

import os
import shlex
import subprocess

import pytest

RALLY_HOME = os.getenv("RALLY_HOME", os.path.expanduser("~"))
RALLY_CONFIG_DIR = os.path.join(RALLY_HOME, ".rally")
TRACK_REPO_PATH = os.path.join(RALLY_CONFIG_DIR, "benchmarks", "tracks", "rally-tracks-compat")
REMOTE_TRACK_REPO = "https://github.com/elastic/rally-tracks"

@pytest.hookimpl(tryfirst=True)
def pytest_addoption(parser):
    group = parser.getgroup("rally")
    group.addoption(
        "--track-repository",
        action="store",
        default=TRACK_REPO_PATH,
        help=("Path to a local track repository\n" f"(default: {TRACK_REPO_PATH})"),
    )
    group.addoption("--track-revision", action="store", default="master", help=("Track repository revision to test\n" "default: `master`"))
    group.addoption(
        "--track-repository-test-directory",
        action="store",
        dest="track_repo_test_dir",
        default="it",
        help=("Name of the directory containing the track repo's integration tests\n" "(default: `it`)"),
    )


@pytest.hookimpl(tryfirst=True)
def pytest_cmdline_main(config):
    repo = config.option.track_repository
    if not os.path.isdir(repo):
        # we're using the defaults, so perform an initial clone of rally-tracks
        if repo == TRACK_REPO_PATH:
            try:
                subprocess.run(shlex.split(f"git clone {REMOTE_TRACK_REPO} {repo}"), text=True, capture_output=True, check=True)
            except subprocess.CalledProcessError as e:
                raise AssertionError(f"Unable to clone {REMOTE_TRACK_REPO} into {repo}: {e.stderr}")
        # user has provided a non-default track repository, but it's not a valid path, so we raise an error
        else:
            raise AssertionError(f"Track repository not found: [{repo}]")

    test_dir = config.option.track_repo_test_dir
    config.args.append(os.path.join(repo, test_dir))


@pytest.hookimpl
def pytest_report_header(config):
    return f"rally: track-repository={config.option.track_repository}, track-revision={config.option.track_revision}"
