import os
import unittest.mock as mock
from unittest import TestCase

from esrally import exceptions
from esrally.utils import git


class GitTests(TestCase):
    def test_is_git_working_copy(self):
        test_dir = os.path.dirname(os.path.dirname(__file__))
        # this test is assuming that nobody stripped the git repo info in their Rally working copy
        self.assertFalse(git.is_working_copy(test_dir))
        self.assertTrue(git.is_working_copy(os.path.dirname(test_dir)))

    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("esrally.utils.process.run_subprocess")
    def test_clone_successful(self, run_subprocess, ensure_dir):
        run_subprocess.return_value = False
        src = "/src"
        remote = "http://github.com/some/project"

        git.clone(src, remote)

        ensure_dir.assert_called_with(src)
        run_subprocess.assert_called_with("git clone http://github.com/some/project /src")

    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("esrally.utils.process.run_subprocess")
    def test_clone_with_error(self, run_subprocess, ensure_dir):
        run_subprocess.return_value = True
        src = "/src"
        remote = "http://github.com/some/project"

        with self.assertRaises(exceptions.SupplyError) as ctx:
            git.clone(src, remote)
        self.assertEqual("Could not clone from 'http://github.com/some/project' to '/src'", ctx.exception.args[0])

        ensure_dir.assert_called_with(src)
        run_subprocess.assert_called_with("git clone http://github.com/some/project /src")

    @mock.patch("esrally.utils.process.run_subprocess")
    def test_fetch_successful(self, run_subprocess):
        run_subprocess.return_value = False
        git.fetch("/src", remote="my-origin")
        run_subprocess.assert_called_with("git -C /src fetch --quiet my-origin")

    @mock.patch("esrally.utils.process.run_subprocess")
    def test_fetch_with_error(self, run_subprocess):
        run_subprocess.return_value = True
        with self.assertRaises(exceptions.SupplyError) as ctx:
            git.fetch("/src", remote="my-origin")
        self.assertEqual("Could not fetch source tree from 'my-origin'", ctx.exception.args[0])
        run_subprocess.assert_called_with("git -C /src fetch --quiet my-origin")

    @mock.patch("esrally.utils.process.run_subprocess")
    def test_checkout_successful(self, run_subprocess):
        run_subprocess.return_value = False
        git.checkout("/src", "feature-branch")
        run_subprocess.assert_called_with("git -C /src checkout --quiet feature-branch")

    @mock.patch("esrally.utils.process.run_subprocess")
    def test_checkout_with_error(self, run_subprocess):
        run_subprocess.return_value = True
        with self.assertRaises(exceptions.SupplyError) as ctx:
            git.checkout("/src", "feature-branch")
        self.assertEqual("Could not checkout 'feature-branch'", ctx.exception.args[0])
        run_subprocess.assert_called_with("git -C /src checkout --quiet feature-branch")

    @mock.patch("esrally.utils.process.run_subprocess")
    def test_rebase(self, run_subprocess):
        run_subprocess.return_value = False
        git.rebase("/src", remote="my-origin", branch="feature-branch")
        calls = [
            mock.call("git -C /src checkout --quiet feature-branch"),
            mock.call("git -C /src rebase --quiet my-origin/feature-branch")
        ]
        run_subprocess.assert_has_calls(calls)

    @mock.patch("esrally.utils.process.run_subprocess")
    def test_pull(self, run_subprocess):
        run_subprocess.return_value = False
        git.pull("/src", remote="my-origin", branch="feature-branch")
        calls = [
            mock.call("git -C /src fetch --quiet my-origin"),
            mock.call("git -C /src checkout --quiet feature-branch"),
            mock.call("git -C /src rebase --quiet my-origin/feature-branch")
        ]
        run_subprocess.assert_has_calls(calls)

    @mock.patch("esrally.utils.process.run_subprocess")
    def test_pull_ts(self, run_subprocess):
        run_subprocess.return_value = False
        git.pull_ts("/src", "20160101T110000Z")
        run_subprocess.assert_called_with(
                "git -C /src fetch --quiet origin && git -C /src checkout "
                "--quiet `git -C /src rev-list -n 1 --before=\"20160101T110000Z\" --date=iso8601 origin/master`")

    @mock.patch("esrally.utils.process.run_subprocess")
    def test_pull_revision(self, run_subprocess):
        run_subprocess.return_value = False
        git.pull_revision("/src", "3694a07")
        run_subprocess.assert_called_with("git -C /src fetch --quiet origin && git -C /src checkout --quiet 3694a07")

    @mock.patch("esrally.utils.process.run_subprocess_with_output")
    def test_head_revision(self, run_subprocess):
        run_subprocess.return_value = ["3694a07"]
        self.assertEqual("3694a07", git.head_revision("/src"))
        run_subprocess.assert_called_with("git -C /src rev-parse --short HEAD")

    @mock.patch("esrally.utils.process.run_subprocess_with_output")
    def test_list_remote_branches(self, run_subprocess):
        run_subprocess.return_value = ["  origin/HEAD",
                                       "  origin/master",
                                       "  origin/5.0.0-alpha1",
                                       "  origin/5"]
        self.assertEqual(["master", "5.0.0-alpha1", "5"], git.branches("/src", remote=True))
        run_subprocess.assert_called_with("git -C /src for-each-ref refs/remotes/ --format='%(refname:short)'")

    @mock.patch("esrally.utils.process.run_subprocess_with_output")
    def test_list_local_branches(self, run_subprocess):
        run_subprocess.return_value = ["  HEAD",
                                       "  master",
                                       "  5.0.0-alpha1",
                                       "  5"]
        self.assertEqual(["master", "5.0.0-alpha1", "5"], git.branches("/src", remote=False))
        run_subprocess.assert_called_with("git -C /src for-each-ref refs/heads/ --format='%(refname:short)'")