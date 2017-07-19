from unittest import TestCase
import unittest.mock as mock

from esrally import exceptions
from esrally.mechanic import supplier


class SourceRepositoryTests(TestCase):
    @mock.patch("esrally.utils.git.head_revision", autospec=True)
    @mock.patch("esrally.utils.git.pull", autospec=True)
    @mock.patch("esrally.utils.git.clone", autospec=True)
    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    def test_intial_checkout_latest(self, mock_is_working_copy, mock_clone, mock_pull, mock_head_revision):
        mock_is_working_copy.return_value = False
        mock_head_revision.return_value = "HEAD"

        s = supplier.SourceRepository(remote_url="some-github-url", src_dir="/src")
        s.fetch("latest")

        mock_is_working_copy.assert_called_with("/src")
        mock_clone.assert_called_with("/src", "some-github-url")
        mock_pull.assert_called_with("/src")
        mock_head_revision.assert_called_with("/src")

    @mock.patch("esrally.utils.git.head_revision", autospec=True)
    @mock.patch("esrally.utils.git.pull")
    @mock.patch("esrally.utils.git.clone")
    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    def test_checkout_current(self, mock_is_working_copy, mock_clone, mock_pull, mock_head_revision):
        mock_is_working_copy.return_value = True
        mock_head_revision.return_value = "HEAD"

        s = supplier.SourceRepository(remote_url="some-github-url", src_dir="/src")
        s.fetch("current")

        mock_is_working_copy.assert_called_with("/src")
        mock_clone.assert_not_called()
        mock_pull.assert_not_called()
        mock_head_revision.assert_called_with("/src")

    @mock.patch("esrally.utils.git.head_revision", autospec=True)
    @mock.patch("esrally.utils.git.pull_ts", autospec=True)
    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    def test_checkout_ts(self, mock_is_working_copy, mock_pull_ts, mock_head_revision):
        mock_is_working_copy.return_value = True
        mock_head_revision.return_value = "HEAD"

        s = supplier.SourceRepository(remote_url="some-github-url", src_dir="/src")
        s.fetch("@2015-01-01-01:00:00")

        mock_is_working_copy.assert_called_with("/src")
        mock_pull_ts.assert_called_with("/src", "2015-01-01-01:00:00")
        mock_head_revision.assert_called_with("/src")

    @mock.patch("esrally.utils.git.head_revision", autospec=True)
    @mock.patch("esrally.utils.git.pull_revision", autospec=True)
    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    def test_checkout_revision(self, mock_is_working_copy, mock_pull_revision, mock_head_revision):
        mock_is_working_copy.return_value = True
        mock_head_revision.return_value = "HEAD"

        s = supplier.SourceRepository(remote_url="some-github-url", src_dir="/src")
        s.fetch("67c2f42")

        mock_is_working_copy.assert_called_with("/src")
        mock_pull_revision.assert_called_with("/src", "67c2f42")
        mock_head_revision.assert_called_with("/src")


class BuilderTests(TestCase):

    @mock.patch("esrally.utils.process.run_subprocess")
    def test_build(self, mock_run_subprocess):
        mock_run_subprocess.return_value = False

        b = supplier.Builder(src_dir="/src", gradle="/usr/local/gradle", java_home="/opt/jdk8", log_dir="logs")
        b.build()

        calls = [
            # Actual call
            mock.call("export JAVA_HOME=/opt/jdk8; cd /src; /usr/local/gradle clean >> logs/build.log 2>&1"),
            # Return value check
            mock.call("export JAVA_HOME=/opt/jdk8; cd /src; /usr/local/gradle :distribution:tar:assemble >> logs/build.log 2>&1"),
        ]

        mock_run_subprocess.assert_has_calls(calls)

    @mock.patch("glob.glob", lambda p: ["elasticsearch.zip"])
    def test_binary(self):
        b = supplier.Builder(src_dir="/src")
        self.assertEqual(b.binary, "elasticsearch.zip")


class DistributionRepositoryTests(TestCase):
    def test_release_repo_config_with_default_url(self):
        repo = supplier.DistributionRepository(name="release", distribution_config={
            "release.2.url": "https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/"
                             "{{VERSION}}/elasticsearch-{{VERSION}}.tar.gz",
            "release.url": "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-{{VERSION}}.tar.gz",
            "release.cache": "true"
        }, version="5.5.0")
        self.assertEqual("https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.5.0.tar.gz", repo.download_url)
        self.assertTrue(repo.cache)

    def test_release_repo_config_with_version_url(self):
        repo = supplier.DistributionRepository(name="release", distribution_config={
            "release.2.url": "https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/"
                             "{{VERSION}}/elasticsearch-{{VERSION}}.tar.gz",
            "release.url": "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-{{VERSION}}.tar.gz",
            "release.cache": "false"
        }, version="2.4.3")
        self.assertEqual("https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.4.3/"
                         "elasticsearch-2.4.3.tar.gz", repo.download_url)
        self.assertFalse(repo.cache)

    def test_missing_url(self):
        repo = supplier.DistributionRepository(name="miss", distribution_config={
            "release.url": "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-{{VERSION}}.tar.gz",
            "release.cache": "true"
        }, version="2.4.3")
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            # noinspection PyStatementEffect
            repo.download_url
        self.assertEqual(
            "Neither version specific distribution config key [miss.2.url] nor a default distribution config key [miss.url] is defined.",
            ctx.exception.args[0])

    def test_missing_cache(self):
        repo = supplier.DistributionRepository(name="release", distribution_config={
            "release.url": "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-{{VERSION}}.tar.gz",
        }, version="2.4.3")
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            # noinspection PyStatementEffect
            repo.cache
        self.assertEqual("Mandatory config key [release.cache] is undefined.", ctx.exception.args[0])

    def test_invalid_cache_value(self):
        repo = supplier.DistributionRepository(name="release", distribution_config={
            "release.url": "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-{{VERSION}}.tar.gz",
            "release.cache": "Invalid"
        }, version="2.4.3")
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            # noinspection PyStatementEffect
            repo.cache
        self.assertEqual("Value [Invalid] for config key [release.cache] is not a valid boolean value.", ctx.exception.args[0])
