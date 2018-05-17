from unittest import TestCase
import unittest.mock as mock

from esrally import exceptions
from esrally.utils import repo


class RallyRepositoryTests(TestCase):
    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    def test_fails_in_offline_mode_if_not_a_git_repo(self, is_working_copy):
        is_working_copy.return_value = False

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            repo.RallyRepository(
                remote_url=None,
                root_dir="/rally-resources",
                repo_name="unit-test",
                resource_name="unittest-resources",
                offline=True)

        self.assertEqual("[/rally-resources/unit-test] must be a git repository.\n\n"
                         "Please run:\ngit -C /rally-resources/unit-test init", ctx.exception.args[0])

    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    def test_does_nothing_if_working_copy_present(self, is_working_copy):
        is_working_copy.return_value = True

        r = repo.RallyRepository(
                remote_url=None,
                root_dir="/rally-resources",
                repo_name="unit-test",
                resource_name="unittest-resources",
                offline=True)

        self.assertFalse(r.remote)

    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    @mock.patch("esrally.utils.git.clone", autospec=True)
    def test_clones_initially(self, clone, is_working_copy):
        is_working_copy.return_value = False

        r = repo.RallyRepository(
            remote_url="git@gitrepos.example.org/rally-resources",
            root_dir="/rally-resources",
            repo_name="unit-test",
            resource_name="unittest-resources",
            offline=False)

        self.assertTrue(r.remote)

        clone.assert_called_with(src="/rally-resources/unit-test", remote="git@gitrepos.example.org/rally-resources")

    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    @mock.patch("esrally.utils.git.fetch", autospec=True)
    def test_fetches_if_already_cloned(self, fetch, is_working_copy):
        is_working_copy.return_value = True

        repo.RallyRepository(
            remote_url="git@gitrepos.example.org/rally-resources",
            root_dir="/rally-resources",
            repo_name="unit-test",
            resource_name="unittest-resources",
            offline=False)

        fetch.assert_called_with(src="/rally-resources/unit-test")

    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    @mock.patch("esrally.utils.git.fetch")
    def test_does_not_fetch_if_suppressed(self, fetch, is_working_copy):
        is_working_copy.return_value = True

        r = repo.RallyRepository(
            remote_url="git@gitrepos.example.org/rally-resources",
            root_dir="/rally-resources",
            repo_name="unit-test",
            resource_name="unittest-resources",
            offline=False,
            fetch=False)

        self.assertTrue(r.remote)

        self.assertEqual(0, fetch.call_count)

    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    @mock.patch("esrally.utils.git.fetch")
    def test_ignores_fetch_errors(self, fetch, is_working_copy):
        fetch.side_effect = exceptions.SupplyError("Testing error")
        is_working_copy.return_value = True

        r = repo.RallyRepository(
            remote_url="git@gitrepos.example.org/rally-resources",
            root_dir="/rally-resources",
            repo_name="unit-test",
            resource_name="unittest-resources",
            offline=False)
        # no exception during the call - we reach this here
        self.assertTrue(r.remote)

        fetch.assert_called_with(src="/rally-resources/unit-test")

    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    @mock.patch("esrally.utils.git.fetch", autospec=True)
    @mock.patch("esrally.utils.git.branches", autospec=True)
    @mock.patch("esrally.utils.git.checkout", autospec=True)
    @mock.patch("esrally.utils.git.rebase", autospec=True)
    def test_updates_from_remote(self, rebase, checkout, branches, fetch, is_working_copy):
        branches.return_value = ["1", "2", "5", "master"]
        is_working_copy.return_value = True

        r = repo.RallyRepository(
            remote_url="git@gitrepos.example.org/rally-resources",
            root_dir="/rally-resources",
            repo_name="unit-test",
            resource_name="unittest-resources",
            offline=False)

        r.update(distribution_version="1.7.3")

        branches.assert_called_with("/rally-resources/unit-test", remote=True)
        rebase.assert_called_with("/rally-resources/unit-test", branch="1")
        checkout.assert_called_with("/rally-resources/unit-test", branch="1")

    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    @mock.patch("esrally.utils.git.fetch", autospec=True)
    @mock.patch("esrally.utils.git.branches", autospec=True)
    @mock.patch("esrally.utils.git.checkout", autospec=True)
    @mock.patch("esrally.utils.git.rebase")
    def test_updates_locally(self, rebase, checkout, branches, fetch, is_working_copy):
        branches.return_value = ["1", "2", "5", "master"]
        is_working_copy.return_value = True

        r = repo.RallyRepository(
            remote_url=None,
            root_dir="/rally-resources",
            repo_name="unit-test",
            resource_name="unittest-resources",
            offline=False)

        r.update(distribution_version="6.0.0")

        branches.assert_called_with("/rally-resources/unit-test", remote=False)
        self.assertEqual(0, rebase.call_count)
        checkout.assert_called_with("/rally-resources/unit-test", branch="master")

    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    @mock.patch("esrally.utils.git.fetch", autospec=True)
    @mock.patch("esrally.utils.git.branches", autospec=True)
    @mock.patch("esrally.utils.git.checkout")
    @mock.patch("esrally.utils.git.rebase")
    def test_does_not_update_unknown_branch_remotely(self, rebase, checkout, branches, fetch, is_working_copy):
        branches.return_value = ["1", "2", "5", "master"]
        is_working_copy.return_value = True

        r = repo.RallyRepository(
            remote_url="git@gitrepos.example.org/rally-resources",
            root_dir="/rally-resources",
            repo_name="unit-test",
            resource_name="unittest-resources",
            offline=False)

        self.assertTrue(r.remote)

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            r.update(distribution_version="4.0.0")

        self.assertEqual("Cannot find unittest-resources for distribution version 4.0.0", ctx.exception.args[0])

        calls = [
            # first try to find it remotely...
            mock.call("/rally-resources/unit-test", remote=True),
            # ... then fallback to local
            mock.call("/rally-resources/unit-test", remote=False),
        ]

        branches.assert_has_calls(calls)
        self.assertEqual(0, checkout.call_count)
        self.assertEqual(0, rebase.call_count)

    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    @mock.patch("esrally.utils.git.fetch", autospec=True)
    @mock.patch("esrally.utils.git.branches", autospec=True)
    @mock.patch("esrally.utils.git.checkout", autospec=True)
    @mock.patch("esrally.utils.git.rebase")
    def test_does_not_update_unknown_branch_remotely_local_fallback(self, rebase, checkout, branches, fetch, is_working_copy):
        # we have only "master" remotely but a few more branches locally
        branches.side_effect = ["5", ["1", "2", "5", "master"]]
        is_working_copy.return_value = True

        r = repo.RallyRepository(
            remote_url="git@gitrepos.example.org/rally-resources",
            root_dir="/rally-resources",
            repo_name="unit-test",
            resource_name="unittest-resources",
            offline=False)

        r.update(distribution_version="1.7.3")

        calls = [
            # first try to find it remotely...
            mock.call("/rally-resources/unit-test", remote=True),
            # ... then fallback to local
            mock.call("/rally-resources/unit-test", remote=False),
        ]

        branches.assert_has_calls(calls)
        checkout.assert_called_with("/rally-resources/unit-test", branch="1")
        self.assertEqual(0, rebase.call_count)

    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    @mock.patch("esrally.utils.git.fetch", autospec=True)
    @mock.patch("esrally.utils.git.branches", autospec=True)
    @mock.patch("esrally.utils.git.checkout")
    @mock.patch("esrally.utils.git.rebase")
    def test_does_not_update_unknown_branch_locally(self, rebase, checkout, branches, fetch, is_working_copy):
        branches.return_value = ["1", "2", "5", "master"]
        is_working_copy.return_value = True

        r = repo.RallyRepository(
            remote_url=None,
            root_dir="/rally-resources",
            repo_name="unit-test",
            resource_name="unittest-resources",
            offline=False)

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            r.update(distribution_version="4.0.0")

        self.assertEqual("Cannot find unittest-resources for distribution version 4.0.0", ctx.exception.args[0])

        branches.assert_called_with("/rally-resources/unit-test", remote=False)
        self.assertEqual(0, checkout.call_count)
        self.assertEqual(0, rebase.call_count)


