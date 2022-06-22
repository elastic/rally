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

import logging
import os
import unittest.mock as mock

import pytest

from esrally import exceptions
from esrally.utils import git


class TestGit:
    def test_is_git_working_copy(self):
        test_dir = os.path.dirname(os.path.dirname(__file__))
        # this test is assuming that nobody stripped the git repo info in their Rally working copy
        assert not git.is_working_copy(test_dir)
        assert git.is_working_copy(os.path.dirname(test_dir))

    @mock.patch("esrally.utils.process.run_subprocess_with_output")
    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_git_version_too_old(self, run_subprocess_with_logging, run_subprocess):
        # any non-zero return value will do
        run_subprocess_with_logging.return_value = 64
        run_subprocess.return_value = "1.0.0"

        with pytest.raises(exceptions.SystemSetupError) as exc:
            git.head_revision("/src")
        assert exc.value.args[0] == "Your git version is [1.0.0] but Rally requires at least git 1.9. Please update git."
        run_subprocess_with_logging.assert_called_with("git -C /src --version", level=logging.DEBUG)

    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_clone_successful(self, run_subprocess_with_logging, ensure_dir):
        run_subprocess_with_logging.return_value = 0
        src = "/src"
        remote = "http://github.com/some/project"

        git.clone(src, remote=remote)

        ensure_dir.assert_called_with(src)
        run_subprocess_with_logging.assert_called_with("git clone http://github.com/some/project /src")

    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_clone_with_error(self, run_subprocess_with_logging, ensure_dir):
        run_subprocess_with_logging.return_value = 128
        src = "/src"
        remote = "http://github.com/some/project"

        with pytest.raises(exceptions.SupplyError) as exc:
            git.clone(src, remote=remote)
        assert exc.value.args[0] == "Could not clone from [http://github.com/some/project] to [/src]"

        ensure_dir.assert_called_with(src)
        run_subprocess_with_logging.assert_called_with("git clone http://github.com/some/project /src")

    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_fetch_successful(self, run_subprocess_with_logging):
        run_subprocess_with_logging.return_value = 0
        git.fetch("/src", remote="my-origin")
        run_subprocess_with_logging.assert_called_with("git -C /src fetch --prune --tags my-origin")

    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_fetch_with_error(self, run_subprocess_with_logging):
        # first call is to check the git version (0 -> succeeds), the second call is the failing checkout (1 -> fails)
        run_subprocess_with_logging.side_effect = [0, 1]
        with pytest.raises(exceptions.SupplyError) as exc:
            git.fetch("/src", remote="my-origin")
        assert exc.value.args[0] == "Could not fetch source tree from [my-origin]"
        run_subprocess_with_logging.assert_called_with("git -C /src fetch --prune --tags my-origin")

    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_checkout_successful(self, run_subprocess_with_logging):
        run_subprocess_with_logging.return_value = 0
        git.checkout("/src", branch="feature-branch")
        run_subprocess_with_logging.assert_called_with("git -C /src checkout feature-branch")

    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_checkout_with_error(self, run_subprocess_with_logging):
        # first call is to check the git version (0 -> succeeds), the second call is the failing checkout (1 -> fails)
        run_subprocess_with_logging.side_effect = [0, 1]
        with pytest.raises(exceptions.SupplyError) as exc:
            git.checkout("/src", branch="feature-branch")
        assert exc.value.args[0] == "Could not checkout [feature-branch]. Do you have uncommitted changes?"
        run_subprocess_with_logging.assert_called_with("git -C /src checkout feature-branch")

    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_rebase(self, run_subprocess_with_logging):
        run_subprocess_with_logging.return_value = 0
        git.rebase("/src", remote="my-origin", branch="feature-branch")
        calls = [
            mock.call("git -C /src checkout feature-branch"),
            mock.call("git -C /src rebase my-origin/feature-branch"),
        ]
        run_subprocess_with_logging.assert_has_calls(calls)

    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_pull(self, run_subprocess_with_logging):
        run_subprocess_with_logging.return_value = 0
        git.pull("/src", remote="my-origin", branch="feature-branch")
        calls = [
            # pull
            mock.call("git -C /src --version", level=logging.DEBUG),
            # fetch
            mock.call("git -C /src --version", level=logging.DEBUG),
            mock.call("git -C /src fetch --prune --tags my-origin"),
            # rebase
            mock.call("git -C /src --version", level=logging.DEBUG),
            # checkout
            mock.call("git -C /src --version", level=logging.DEBUG),
            mock.call("git -C /src checkout feature-branch"),
            mock.call("git -C /src rebase my-origin/feature-branch"),
        ]
        run_subprocess_with_logging.assert_has_calls(calls)

    @mock.patch("esrally.utils.process.run_subprocess")
    @mock.patch("esrally.utils.process.run_subprocess_with_output")
    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_pull_ts(self, run_subprocess_with_logging, run_subprocess_with_output, run_subprocess):
        run_subprocess_with_logging.return_value = 0
        run_subprocess_with_output.return_value = ["3694a07"]
        run_subprocess.side_effect = [False, False]
        git.pull_ts("/src", "20160101T110000Z", remote="origin", branch="master")

        run_subprocess_with_output.assert_called_with('git -C /src rev-list -n 1 --before="20160101T110000Z" --date=iso8601 origin/master')
        run_subprocess.has_calls(
            [
                mock.call("git -C /src fetch --prune --tags --quiet origin"),
                mock.call("git -C /src checkout 3694a07"),
            ]
        )

    @mock.patch("esrally.utils.process.run_subprocess")
    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_pull_revision(self, run_subprocess_with_logging, run_subprocess):
        run_subprocess_with_logging.return_value = 0
        run_subprocess.side_effect = [False, False]
        git.pull_revision("/src", remote="origin", revision="3694a07")
        run_subprocess.has_calls(
            [
                mock.call("git -C /src fetch --prune --tags --quiet origin"),
                mock.call("git -C /src checkout --quiet 3694a07"),
            ]
        )

    @mock.patch("esrally.utils.process.run_subprocess_with_output")
    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_head_revision(self, run_subprocess_with_logging, run_subprocess):
        run_subprocess_with_logging.return_value = 0
        run_subprocess.return_value = ["3694a07"]
        assert git.head_revision("/src") == "3694a07"
        run_subprocess.assert_called_with("git -C /src rev-parse --short HEAD")

    @mock.patch("esrally.utils.process.run_subprocess_with_output")
    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_list_remote_branches(self, run_subprocess_with_logging, run_subprocess):
        run_subprocess_with_logging.return_value = 0
        run_subprocess.return_value = [
            "  origin/HEAD",
            "  origin/master",
            "  origin/5.0.0-alpha1",
            "  origin/5",
        ]
        assert git.branches("/src", remote=True) == ["master", "5.0.0-alpha1", "5"]
        run_subprocess.assert_called_with("git -C /src for-each-ref refs/remotes/ --format='%(refname:short)'")

    @mock.patch("esrally.utils.process.run_subprocess_with_output")
    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_list_local_branches(self, run_subprocess_with_logging, run_subprocess):
        run_subprocess_with_logging.return_value = 0
        run_subprocess.return_value = [
            "  HEAD",
            "  master",
            "  5.0.0-alpha1",
            "  5",
        ]
        assert git.branches("/src", remote=False) == ["master", "5.0.0-alpha1", "5"]
        run_subprocess.assert_called_with("git -C /src for-each-ref refs/heads/ --format='%(refname:short)'")

    @mock.patch("esrally.utils.process.run_subprocess_with_output")
    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_list_tags_with_tags_present(self, run_subprocess_with_logging, run_subprocess):
        run_subprocess_with_logging.return_value = 0
        run_subprocess.return_value = [
            "  v1",
            "  v2",
        ]
        assert git.tags("/src") == ["v1", "v2"]
        run_subprocess.assert_called_with("git -C /src tag")

    @mock.patch("esrally.utils.process.run_subprocess_with_output")
    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_list_tags_no_tags_available(self, run_subprocess_with_logging, run_subprocess):
        run_subprocess_with_logging.return_value = 0
        run_subprocess.return_value = ""
        assert git.tags("/src") == []
        run_subprocess.assert_called_with("git -C /src tag")
