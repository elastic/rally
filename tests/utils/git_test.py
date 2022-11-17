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
from esrally.utils import git, process


@pytest.fixture(scope="class", autouse=True)
def setup(request, tmp_path_factory):
    request.cls.remote_repo = "esrally"
    request.cls.local_branch = "rally-unit-test-local-only-branch"
    request.cls.remote_branch = "rally-unit-test-remote-only-branch"
    request.cls.rebase_branch = "rally-unit-test-rebase-branch"

    # this is assuming that nobody stripped the git repo info in their Rally working copy
    request.cls.original_src_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    request.cls.local_tmp_src_dir = str(tmp_path_factory.mktemp("rally-unit-test-local-dir"))
    request.cls.remote_tmp_src_dir = str(tmp_path_factory.mktemp("rally-unit-test-remote-dir"))
    request.cls.tmp_clone_dir = str(tmp_path_factory.mktemp("rally-unit-test-clone-dir"))

    # create tmp git repos
    shutil.copytree(request.cls.original_src_dir, request.cls.local_tmp_src_dir, dirs_exist_ok=True)
    # stash any current changes in copied dir
    process.run_subprocess_with_logging(f"git -C {request.cls.local_tmp_src_dir} stash")
    shutil.copytree(request.cls.original_src_dir, request.cls.remote_tmp_src_dir, dirs_exist_ok=True)
    # so we can restore the working tree after some tests
    request.cls.starting_branch = git.current_branch(request.cls.local_tmp_src_dir)

    # setup test branches
    process.run_subprocess_with_logging(f"git -C {request.cls.local_tmp_src_dir} branch {request.cls.local_branch}")
    process.run_subprocess_with_logging(f"git -C {request.cls.remote_tmp_src_dir} branch {request.cls.remote_branch}")
    request.cls.remote_branch_hash = process.run_subprocess_with_output(
        f"git -C {request.cls.remote_tmp_src_dir} rev-parse {request.cls.remote_branch}"
    )[0]
    # setup remote
    process.run_subprocess_with_logging(
        f"git -C {request.cls.local_tmp_src_dir} remote add {request.cls.remote_repo} "
        f"{os.path.join(request.cls.remote_tmp_src_dir, '.git')}"
    )

    # fetch branches from remote
    git.fetch(request.cls.local_tmp_src_dir, remote=request.cls.remote_repo)


# pylint: disable=too-many-public-methods
class TestGit:
    @pytest.fixture(autouse=True)
    def checkout_previous_branch(self):
        yield
        # checkout the 'original' branch after each test
        git.checkout(self.local_tmp_src_dir, branch=self.starting_branch)

    @pytest.fixture
    def delete_local_tags(self):
        # delete tags, locally
        process.run_subprocess(f"git -C {self.local_tmp_src_dir} tag | xargs git -C {self.local_tmp_src_dir} tag -d")
        yield
        # reinstate local tags from remote
        git.fetch(self.local_tmp_src_dir, remote=self.remote_repo)

    @pytest.fixture
    def setup_teardown_rebase(self):
        # create branches on local and remote
        process.run_subprocess_with_logging(f"git -C {self.local_tmp_src_dir} branch {self.rebase_branch}")
        process.run_subprocess_with_logging(f"git -C {self.remote_tmp_src_dir} checkout -b {self.rebase_branch}")
        # create remote commit from which to rebase on in local
        process.run_subprocess_with_logging(f"git -C {self.remote_tmp_src_dir} commit --allow-empty -m 'Rally rebase/pull test'")
        self.remote_commit_hash = git.head_revision(self.remote_tmp_src_dir)
        # run rebase
        yield
        # undo rebase
        process.run_subprocess_with_logging(f"git -C {self.local_tmp_src_dir} reset --hard ORIG_HEAD")

        # checkout starting branches
        git.checkout(self.local_tmp_src_dir, branch=self.starting_branch)
        git.checkout(self.remote_tmp_src_dir, branch=self.starting_branch)

        # delete branches
        process.run_subprocess_with_logging(f"git -C {self.local_tmp_src_dir} branch -D {self.rebase_branch}")
        process.run_subprocess_with_logging(f"git -C {self.remote_tmp_src_dir} branch -D {self.rebase_branch}")

    def test_is_git_working_copy(self):
        # this test is assuming that nobody stripped the git repo info in their Rally working copy
        assert not git.is_working_copy(os.path.dirname(self.local_tmp_src_dir))
        assert git.is_working_copy(self.local_tmp_src_dir)

    def test_is_branch(self):
        # only remote
        assert git.is_branch(self.local_tmp_src_dir, identifier=self.remote_branch)

        # only local
        assert git.is_branch(self.local_tmp_src_dir, identifier=self.local_branch)

        # both remote, and local
        assert git.is_branch(self.local_tmp_src_dir, identifier="master")

    def test_is_not_branch_tags(self):
        assert not git.is_branch(self.local_tmp_src_dir, identifier="2.6.0")

    def test_is_not_branch_commit_hash(self):
        # rally's initial commit :-)
        assert not git.is_branch(self.local_tmp_src_dir, identifier="bd368741951c643f9eb1958072c316e493c15b96")

    def test_list_remote_branches(self):
        assert self.remote_branch in git.branches(self.local_tmp_src_dir, remote=True)

    def test_list_local_branches(self):
        assert self.local_branch in git.branches(self.local_tmp_src_dir, remote=False)

    def test_list_tags_with_tags_present(self):
        expected_tags = ["2.6.0", "2.5.0", "2.4.0", "2.3.1", "2.3.0"]
        tags = git.tags(self.local_tmp_src_dir)
        for tag in expected_tags:
            assert tag in tags

    def test_list_tags_no_tags_available(self, delete_local_tags):
        assert git.tags(self.local_tmp_src_dir) == []

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

    def test_clone_successful(self):
        git.clone(self.tmp_clone_dir, remote=self.remote_tmp_src_dir)
        assert git.is_working_copy(self.tmp_clone_dir)

    def test_clone_with_error(self):
        remote = "/this/remote/doesnt/exist.git"
        with pytest.raises(exceptions.SupplyError) as exc:
            git.clone(self.tmp_clone_dir, remote=remote)
        assert exc.value.args[0] == f"Could not clone from [{remote}] to [{self.tmp_clone_dir}]"

    def test_fetch_successful(self):
        git.fetch(self.local_tmp_src_dir, remote=self.remote_repo)

    def test_fetch_with_error(self):
        with pytest.raises(exceptions.SupplyError) as exc:
            git.fetch(self.local_tmp_src_dir, remote="this-remote-doesnt-actually-exist")
        assert exc.value.args[0] == "Could not fetch source tree from [this-remote-doesnt-actually-exist]"

    def test_checkout_successful(self):
        git.checkout(self.local_tmp_src_dir, branch=self.local_branch)
        assert git.current_branch(self.local_tmp_src_dir) == self.local_branch

    def test_checkout_with_error(self):
        branch = "this-branch-doesnt-actually-exist"
        with pytest.raises(exceptions.SupplyError) as exc:
            git.checkout(self.local_tmp_src_dir, branch=branch)
        assert exc.value.args[0] == f"Could not checkout [{branch}]. Do you have uncommitted changes?"

    def test_checkout_revision(self):
        # minimum 'core.abbrev' is to return 7 char prefixes
        git.checkout_revision(self.local_tmp_src_dir, revision="bd368741951c643f9eb1958072c316e493c15b96")
        assert git.head_revision(self.local_tmp_src_dir).startswith("bd36874")

    def test_checkout_branch(self):
        git.checkout_branch(self.local_tmp_src_dir, remote=self.remote_repo, branch=self.remote_branch)
        assert git.head_revision(self.local_tmp_src_dir).startswith(self.remote_branch_hash[0:7])

    def test_head_revision(self):
        # minimum 'core.abbrev' is to return 7 char prefixes
        git.checkout(self.local_tmp_src_dir, branch="2.6.0")
        assert git.head_revision(self.local_tmp_src_dir).startswith("09980cd")

    def test_pull_ts(self):
        # results in commit 28474f4f097106ff3507be35958db0c3c8be0fc6
        # minimum 'core.abbrev' is to return 7 char prefixes
        git.pull_ts(self.local_tmp_src_dir, "2016-01-01T110000Z", remote=self.remote_repo, branch=self.remote_branch)
        assert git.head_revision(self.local_tmp_src_dir).startswith("28474f4")

    def test_rebase(self, setup_teardown_rebase):
        # fetch required first to get remote branch
        git.fetch(self.local_tmp_src_dir, remote=self.remote_repo)
        git.rebase(self.local_tmp_src_dir, remote=self.remote_repo, branch=self.rebase_branch)
        # minimum 'core.abbrev' is to return 7 char prefixes
        assert git.head_revision(self.local_tmp_src_dir).startswith(self.remote_commit_hash[0:7])

    def test_pull_rebase(self, setup_teardown_rebase):
        git.pull(self.local_tmp_src_dir, remote=self.remote_repo, branch=self.rebase_branch)
        # minimum 'core.abbrev' is to return 7 char prefixes
        assert git.head_revision(self.local_tmp_src_dir).startswith(self.remote_commit_hash[0:7])
