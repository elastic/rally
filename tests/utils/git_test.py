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
import shutil
from unittest import mock

import pytest

from esrally import exceptions
from esrally.utils import git, io, process


# pylint: disable=too-many-public-methods
class TestGit:
    @classmethod
    def setup_class(cls):
        cls.local_branch = "rally-unit-test-local-only-branch"
        cls.remote_branch = "rally-unit-test-remote-only-branch"
        # this is assuming that nobody stripped the git repo info in their Rally working copy
        cls.src_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        cls.tmp_src_dir = io.escape_path("/tmp/rally-unit-test-dir")
        # location for 'clone' tests
        cls.tmp_clone_dir = io.escape_path("/tmp/rally-unit-test-clone-dir")

        # delete any pre-existing dirs (maybe last test setup cancelled early)
        shutil.rmtree(TestGit.tmp_src_dir, ignore_errors=True)
        # create a copy to perform git operations in
        shutil.copytree(TestGit.src_dir, TestGit.tmp_src_dir)

        # stash any current changes in copied dir
        process.run_subprocess_with_logging(f"git -C {TestGit.tmp_src_dir} stash")

        cls.current_branch = git.current_branch(TestGit.tmp_src_dir)

    @classmethod
    def teardown_class(cls):
        # delete copy of git dir
        shutil.rmtree(TestGit.tmp_src_dir)

    @pytest.fixture(autouse=True)
    def checkout_previous_branch(self):
        yield
        # checkout the 'original' branch after each test
        git.checkout(TestGit.tmp_src_dir, branch=TestGit.current_branch)

    @pytest.fixture
    def teardown_clone(self):
        yield
        shutil.rmtree(TestGit.tmp_clone_dir)

    @pytest.fixture
    def setup_teardown_local_branch(self):
        process.run_subprocess_with_logging(f"git -C {TestGit.tmp_src_dir} branch {TestGit.local_branch}")
        yield
        process.run_subprocess_with_logging(f"git -C {TestGit.tmp_src_dir} branch -D {TestGit.local_branch}")

    @pytest.fixture
    def setup_teardown_checkout_branch(self):
        """
        The test branch we use is only available at elastic/rally.git, and because 'origin' is relative to where the test
        is invoked we call this fixture to add a consistently named remote for both CI and local test invocations.
        """
        process.run_subprocess_with_logging(f"git -C {TestGit.tmp_src_dir} remote add esrally git@github.com:elastic/rally.git")
        git.fetch(TestGit.tmp_src_dir, remote="esrally")
        yield
        process.run_subprocess_with_logging(f"git -C {TestGit.tmp_src_dir} remote remove esrally")

    @pytest.fixture
    def delete_local_tags(self):
        # delete tags, locally
        process.run_subprocess(f"git -C {TestGit.tmp_src_dir} tag | xargs git -C {TestGit.tmp_src_dir} tag -d")
        yield
        # reinstate local tags from remote
        git.fetch(TestGit.tmp_src_dir, remote="origin")

    def test_is_git_working_copy(self):
        # this test is assuming that nobody stripped the git repo info in their Rally working copy
        assert not git.is_working_copy(os.path.dirname(TestGit.tmp_src_dir))
        assert git.is_working_copy(TestGit.tmp_src_dir)

    def test_is_branch(self, setup_teardown_local_branch):
        # only remote
        assert git.is_branch(TestGit.tmp_src_dir, identifier=TestGit.remote_branch)

        # only local
        assert git.is_branch(TestGit.tmp_src_dir, identifier=TestGit.local_branch)

        # both remote, and local
        assert git.is_branch(TestGit.tmp_src_dir, identifier="master")

    def test_is_not_branch_tags(self):
        assert not git.is_branch(TestGit.tmp_src_dir, identifier="2.6.0")

    def test_is_not_branch_commit_hash(self):
        # rally's initial commit :-)
        assert not git.is_branch(TestGit.tmp_src_dir, identifier="bd368741951c643f9eb1958072c316e493c15b96")

    def test_list_remote_branches(self):
        assert TestGit.remote_branch in git.branches(TestGit.tmp_src_dir, remote=True)

    def test_list_local_branches(self, setup_teardown_local_branch):
        assert TestGit.local_branch in git.branches(TestGit.tmp_src_dir, remote=False)

    def test_list_tags_with_tags_present(self):
        expected_tags = ["2.6.0", "2.5.0", "2.4.0", "2.3.1", "2.3.0"]
        tags = git.tags(TestGit.tmp_src_dir)
        for tag in expected_tags:
            assert tag in tags

    def test_list_tags_no_tags_available(self, delete_local_tags):
        assert git.tags(TestGit.tmp_src_dir) == []

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

    def test_clone_successful(self, teardown_clone):
        remote = "https://github.com/elastic/rally"
        git.clone(TestGit.tmp_clone_dir, remote=remote)
        assert git.is_working_copy(TestGit.tmp_clone_dir)

    def test_clone_with_error(self):
        remote = "https://github.com/elastic/this-project-doesnt-actually-exist"
        with pytest.raises(exceptions.SupplyError) as exc:
            git.clone(TestGit.tmp_clone_dir, remote=remote)
        assert exc.value.args[0] == f"Could not clone from [{remote}] to [{TestGit.tmp_clone_dir}]"

    def test_fetch_successful(self):
        before_time = os.path.getmtime(os.path.join(TestGit.tmp_src_dir, ".git/FETCH_HEAD"))
        git.fetch(TestGit.tmp_src_dir, remote="origin")
        after_time = os.path.getmtime(os.path.join(TestGit.tmp_src_dir, ".git/FETCH_HEAD"))
        fetch_head_modified = after_time > before_time
        assert fetch_head_modified

    def test_fetch_with_error(self):
        with pytest.raises(exceptions.SupplyError) as exc:
            git.fetch(TestGit.tmp_src_dir, remote="this-remote-doesnt-actually-exist")
        assert exc.value.args[0] == "Could not fetch source tree from [this-remote-doesnt-actually-exist]"

    def test_checkout_successful(self, setup_teardown_local_branch):
        git.checkout(TestGit.tmp_src_dir, branch=TestGit.local_branch)
        assert git.current_branch(TestGit.tmp_src_dir) == TestGit.local_branch

    def test_checkout_with_error(self):
        branch = "this-branch-doesnt-actually-exist"
        with pytest.raises(exceptions.SupplyError) as exc:
            git.checkout(TestGit.tmp_src_dir, branch=branch)
        assert exc.value.args[0] == f"Could not checkout [{branch}]. Do you have uncommitted changes?"

    def test_checkout_revision(self):
        git.checkout_revision(TestGit.tmp_src_dir, revision="bd368741951c643f9eb1958072c316e493c15b96")
        assert "bd368741951c643f9eb1958072c316e493c15b96" == git.head_revision(TestGit.tmp_src_dir)

    def test_checkout_branch(self, setup_teardown_checkout_branch):
        git.checkout_branch(TestGit.tmp_src_dir, remote="esrally", branch=TestGit.remote_branch)
        assert "be3eeda86e12616fa744f6cde7fcfe41b3a1c963" == git.head_revision(TestGit.tmp_src_dir)

    def test_head_revision(self):
        # Apple Git 'core.abbrev' defaults to return 7 char prefixes (09980cd)
        # Linux defaults to return 8 char prefixes (09980cd5)
        git.checkout(TestGit.tmp_src_dir, branch="2.6.0")
        assert git.head_revision(TestGit.tmp_src_dir).startswith("09980cd")

    def test_pull_ts(self):
        git.pull_ts(TestGit.tmp_src_dir, "20160101T110000Z", remote="origin", branch="master")
        assert "28474f4f097106ff3507be35958db0c3c8be0fc6" == git.head_revision(TestGit.tmp_src_dir)

    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_rebase(self, run_subprocess_with_logging):
        run_subprocess_with_logging.return_value = 0
        git.rebase("/src", remote="my-origin", branch="feature-branch")
        calls = [
            mock.call("git -C /src checkout feature-branch"),
            mock.call(
                '/bin/bash -c "for i in {1..5}; do git -C /src rebase '
                'my-origin/feature-branch && s=0 && break || s=1 && sleep 1; done; (exit $s)"'
            ),
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
            mock.call(
                '/bin/bash -c "for i in {1..5}; do git -C /src fetch --prune '
                '--tags my-origin && s=0 && break || s=1 && sleep 1; done; (exit $s)"'
            ),
            # rebase
            mock.call("git -C /src --version", level=logging.DEBUG),
            # checkout
            mock.call("git -C /src --version", level=logging.DEBUG),
            mock.call("git -C /src checkout feature-branch"),
            mock.call(
                '/bin/bash -c "for i in {1..5}; do git -C /src rebase '
                'my-origin/feature-branch && s=0 && break || s=1 && sleep 1; done; (exit $s)"'
            ),
        ]
        run_subprocess_with_logging.assert_has_calls(calls)
