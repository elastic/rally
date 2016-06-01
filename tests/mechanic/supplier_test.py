from unittest import TestCase
import unittest.mock as mock

from esrally import config
from esrally.mechanic import supplier


class SupplierTests(TestCase):
    @mock.patch("esrally.utils.git.head_revision", autospec=True)
    @mock.patch("esrally.utils.git.pull", autospec=True)
    @mock.patch("esrally.utils.git.clone", autospec=True)
    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    def test_intial_checkout_latest(self, mock_is_working_copy, mock_clone, mock_pull, mock_head_revision):
        cfg = config.Config()
        cfg.add(config.Scope.application, "source", "local.src.dir", "/src")
        cfg.add(config.Scope.application, "source", "remote.repo.url", "some-github-url")
        cfg.add(config.Scope.application, "source", "revision", "latest")

        mock_is_working_copy.return_value = False
        mock_head_revision.return_value = "HEAD"

        s = supplier.Supplier(cfg)
        s.fetch()

        mock_is_working_copy.assert_called_with("/src")
        mock_clone.assert_called_with("/src", "some-github-url")
        mock_pull.assert_called_with("/src")
        mock_head_revision.assert_called_with("/src")

    @mock.patch("esrally.utils.git.head_revision", autospec=True)
    @mock.patch("esrally.utils.git.pull")
    @mock.patch("esrally.utils.git.clone")
    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    def test_checkout_current(self, mock_is_working_copy, mock_clone, mock_pull, mock_head_revision):
        cfg = config.Config()
        cfg.add(config.Scope.application, "source", "local.src.dir", "/src")
        cfg.add(config.Scope.application, "source", "remote.repo.url", "some-github-url")
        cfg.add(config.Scope.application, "source", "revision", "current")

        mock_is_working_copy.return_value = True
        mock_head_revision.return_value = "HEAD"

        s = supplier.Supplier(cfg)
        s.fetch()

        mock_is_working_copy.assert_called_with("/src")
        mock_clone.assert_not_called()
        mock_pull.assert_not_called()
        mock_head_revision.assert_called_with("/src")

    @mock.patch("esrally.utils.git.head_revision", autospec=True)
    @mock.patch("esrally.utils.git.pull_ts", autospec=True)
    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    def test_checkout_ts(self, mock_is_working_copy, mock_pull_ts, mock_head_revision):
        cfg = config.Config()
        cfg.add(config.Scope.application, "source", "local.src.dir", "/src")
        cfg.add(config.Scope.application, "source", "remote.repo.url", "some-github-url")
        cfg.add(config.Scope.application, "source", "revision", "@2015-01-01-01:00:00")

        mock_is_working_copy.return_value = True
        mock_head_revision.return_value = "HEAD"

        s = supplier.Supplier(cfg)
        s.fetch()

        mock_is_working_copy.assert_called_with("/src")
        mock_pull_ts.assert_called_with("/src", "2015-01-01-01:00:00")
        mock_head_revision.assert_called_with("/src")

    @mock.patch("esrally.utils.git.head_revision", autospec=True)
    @mock.patch("esrally.utils.git.pull_revision", autospec=True)
    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    def test_checkout_revision(self, mock_is_working_copy, mock_pull_revision, mock_head_revision):
        cfg = config.Config()
        cfg.add(config.Scope.application, "source", "local.src.dir", "/src")
        cfg.add(config.Scope.application, "source", "remote.repo.url", "some-github-url")
        cfg.add(config.Scope.application, "source", "revision", "67c2f42")

        mock_is_working_copy.return_value = True
        mock_head_revision.return_value = "HEAD"

        s = supplier.Supplier(cfg)
        s.fetch()

        mock_is_working_copy.assert_called_with("/src")
        mock_pull_revision.assert_called_with("/src", "67c2f42")
        mock_head_revision.assert_called_with("/src")
