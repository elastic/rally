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
from unittest import mock

import pytest
from git import Repo

from esrally import exceptions
from esrally.utils import git


def commit(repo, *, date=None):
    file_name = os.path.join(repo.working_dir, "test")
    # creates an empty file
    with open(file_name, "wb"):
        pass
    repo.index.add([file_name])
    repo.index.commit("initial commit", commit_date=date)


@pytest.fixture(scope="class", autouse=True)
def setup(request, tmp_path_factory):
    cls = request.cls

    cls.local_remote_name = "remote_repo"
    cls.local_branch = "rally-unit-test-local-only-branch"
    cls.remote_branch = "rally-unit-test-remote-only-branch"
    cls.rebase_branch = "rally-unit-test-rebase-branch"

    cls.local_tmp_src_dir = str(tmp_path_factory.mktemp("rally-unit-test-local-dir"))
    cls.remote_tmp_src_dir = str(tmp_path_factory.mktemp("rally-unit-test-remote-dir"))
    cls.tmp_clone_dir = str(tmp_path_factory.mktemp("rally-unit-test-clone-dir"))

    # create tmp git repos
    # Some recent git versions default to `main` but old versions don't accept
    # --initial-branch. Until we migrate off Jenkins, let's default recent versions to
    # master too.
    try:
        cls.local_repo = Repo.init(cls.local_tmp_src_dir, initial_branch="master")
    except Exception:
        cls.local_repo = Repo.init(cls.local_tmp_src_dir)
    commit(cls.local_repo)
    cls.local_revision = cls.local_repo.heads["master"].commit.hexsha
    cls.local_repo.create_tag("local-tag-1", "HEAD")
    cls.local_repo.create_tag("local-tag-2", "HEAD")
    cls.local_repo.create_head(cls.local_branch, "HEAD")

    try:
        cls.remote_repo = Repo.init(cls.remote_tmp_src_dir, initial_branch="master")
    except Exception:
        cls.remote_repo = Repo.init(cls.remote_tmp_src_dir)
    commit(cls.remote_repo, date="2016-01-01 00:00:00+0000")
    cls.old_revision = cls.remote_repo.heads["master"].commit.hexsha
    commit(cls.remote_repo)
    cls.remote_branch_hash = cls.remote_repo.heads["master"].commit.hexsha

    cls.remote_repo.create_head(cls.remote_branch, "HEAD")
    cls.local_repo.create_remote(cls.local_remote_name, cls.remote_tmp_src_dir)
    cls.local_repo.remotes[cls.local_remote_name].fetch()


# pylint: disable=too-many-public-methods
class TestGit:
    @pytest.fixture
    def setup_teardown_rebase(self):
        # create branches on local and remote
        self.local_repo.create_head(self.rebase_branch, "HEAD")
        self.remote_repo.head.reference = self.remote_repo.create_head(self.rebase_branch, "HEAD")
        # create remote commit from which to rebase on in local
        commit(self.remote_repo)
        self.remote_commit_hash = self.remote_repo.heads[self.rebase_branch].commit.hexsha
        # run rebase
        yield
        # undo rebase
        self.local_repo.head.reset("ORIG_HEAD", hard=True)
        # checkout starting branches
        self.local_repo.head.reference = "master"
        self.remote_repo.head.reference = "master"

        # delete branches
        self.local_repo.delete_head(self.rebase_branch, force=True)
        self.remote_repo.delete_head(self.rebase_branch, force=True)

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

    def test_list_tags(self):
        assert git.tags(self.local_tmp_src_dir) == ["local-tag-1", "local-tag-2"]

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
        remote = "/this/remote/doesnt/exist"
        with pytest.raises(exceptions.SupplyError) as exc:
            git.clone(self.tmp_clone_dir, remote=remote)
        assert exc.value.args[0] == f"Could not clone from [{remote}] to [{self.tmp_clone_dir}]"

    def test_fetch_successful(self):
        git.fetch(self.local_tmp_src_dir, remote=self.local_remote_name)

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
        git.checkout_revision(self.local_tmp_src_dir, revision=self.local_revision)
        assert git.head_revision(self.local_tmp_src_dir).startswith(self.local_revision[:7])

    def test_checkout_branch(self):
        git.checkout_branch(self.local_tmp_src_dir, remote=self.local_remote_name, branch=self.remote_branch)
        assert git.head_revision(self.local_tmp_src_dir).startswith(self.remote_branch_hash[0:7])

    def test_head_revision(self):
        # minimum 'core.abbrev' is to return 7 char prefixes
        git.checkout(self.local_tmp_src_dir, branch="master")
        assert git.head_revision(self.local_tmp_src_dir).startswith(self.local_revision[:7])

    def test_pull_ts(self):
        # minimum 'core.abbrev' is to return 7 char prefixes
        git.pull_ts(self.local_tmp_src_dir, "2016-01-01T110000Z", remote=self.local_remote_name, branch=self.remote_branch)
        assert git.head_revision(self.local_tmp_src_dir).startswith(self.old_revision)

    def test_rebase(self, setup_teardown_rebase):
        # fetch required first to get remote branch
        git.fetch(self.local_tmp_src_dir, remote=self.local_remote_name)
        git.rebase(self.local_tmp_src_dir, remote=self.local_remote_name, branch=self.rebase_branch)
        # minimum 'core.abbrev' is to return 7 char prefixes
        assert git.head_revision(self.local_tmp_src_dir).startswith(self.remote_commit_hash[0:7])

    def test_pull_rebase(self, setup_teardown_rebase):
        git.pull(self.local_tmp_src_dir, remote=self.local_remote_name, branch=self.rebase_branch)
        # minimum 'core.abbrev' is to return 7 char prefixes
        assert git.head_revision(self.local_tmp_src_dir).startswith(self.remote_commit_hash[0:7])
