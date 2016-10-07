import urllib.error

from unittest import TestCase
import unittest.mock as mock

from esrally import config, exceptions
from esrally.mechanic import supplier


class SourceRepositoryTests(TestCase):
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

        s = supplier.SourceRepository(cfg)
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

        s = supplier.SourceRepository(cfg)
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

        s = supplier.SourceRepository(cfg)
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

        s = supplier.SourceRepository(cfg)
        s.fetch()

        mock_is_working_copy.assert_called_with("/src")
        mock_pull_revision.assert_called_with("/src", "67c2f42")
        mock_head_revision.assert_called_with("/src")


class BuilderTests(TestCase):

    @mock.patch("esrally.utils.process.run_subprocess")
    def test_build(self, mock_run_subprocess):
        mock_run_subprocess.return_value = False

        cfg = config.Config()
        cfg.add(config.Scope.application, "source", "local.src.dir", "/src")
        cfg.add(config.Scope.application, "runtime", "java8.home", "/opt/jdk8")
        cfg.add(config.Scope.application, "build", "gradle.bin", "/usr/local/gradle")
        cfg.add(config.Scope.application, "build", "gradle.tasks.clean", "clean")
        cfg.add(config.Scope.application, "build", "gradle.tasks.package", "assemble")
        cfg.add(config.Scope.application, "system", "log.dir", "logs")
        cfg.add(config.Scope.application, "build", "log.dir", "build")

        b = supplier.Builder(cfg)
        b.build()

        calls = [
            # Actual call
            mock.call("export JAVA_HOME=/opt/jdk8; cd /src; /usr/local/gradle clean > logs/build/build.gradle.tasks.clean.log 2>&1"),
            # Return value check
            mock.call("export JAVA_HOME=/opt/jdk8; cd /src; /usr/local/gradle assemble > logs/build/build.gradle.tasks.package.log 2>&1"),
        ]

        mock_run_subprocess.assert_has_calls(calls)

    @mock.patch("glob.glob", lambda p: ["elasticsearch.zip"])
    def test_add_binary_to_config(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "source", "local.src.dir", "/src")
        b = supplier.Builder(cfg)
        b.add_binary_to_config()
        self.assertEqual(cfg.opts("builder", "candidate.bin.path"), "elasticsearch.zip")


class SnapshotDistributionRepositoryTests(TestCase):
    @mock.patch("esrally.utils.net.retrieve_content_as_string")
    def test_download_url_for_valid_version(self, content_as_string):
        content_as_string.return_value = """
<metadata modelVersion="1.1.0">
    <groupId>org.elasticsearch.distribution.tar</groupId>
    <artifactId>elasticsearch</artifactId>
    <version>5.0.0-SNAPSHOT</version>
    <versioning>
        <snapshot>
            <timestamp>20160613.162731</timestamp>
            <buildNumber>397</buildNumber>
        </snapshot>
        <lastUpdated>20160616030717</lastUpdated>
        <snapshotVersions>
            <snapshotVersion>
                <extension>pom</extension>
                <value>5.0.0-20160613.162731-397</value>
                <updated>20160613162731</updated>
            </snapshotVersion>
            <snapshotVersion>
                <extension>tar.gz</extension>
                <value>5.0.0-20160613.162731-397</value>
                <updated>20160613162731</updated>
            </snapshotVersion>
        </snapshotVersions>
    </versioning>
</metadata>
"""
        repo = supplier.SnapshotDistributionRepo()
        self.assertEqual("https://oss.sonatype.org/content/repositories/snapshots/org/elasticsearch/distribution/tar/elasticsearch/"
                         "5.0.0-SNAPSHOT/elasticsearch-5.0.0-20160613.162731-397.tar.gz", repo.download_url("5.0.0-SNAPSHOT"))

    @mock.patch("esrally.utils.net.retrieve_content_as_string")
    def test_download_url_for_invalid_metadata(self, content_as_string):
        content_as_string.return_value = """
<metadata modelVersion="1.1.0">
    <groupId>org.elasticsearch.distribution.tar</groupId>
    <artifactId>elasticsearch</artifactId>
    <version>5.0.0-SNAPSHOT</version>
    <versioning>
        <snapshot>
            <timestamp>20160613.162731</timestamp>
            <buildNumber>397</buildNumber>
        </snapshot>
        <lastUpdated>20160616030717</lastUpdated>
        <snapshotVersions>
            <snapshotVersion>
                <extension>pom</extension>
                <value>5.0.0-20160613.162731-397</value>
                <updated>20160613162731</updated>
            </snapshotVersion>
        </snapshotVersions>
    </versioning>
</metadata>
"""
        repo = supplier.SnapshotDistributionRepo()
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            repo.download_url("5.0.0-SNAPSHOT")
        self.assertEqual("Cannot derive download URL for Elasticsearch 5.0.0-SNAPSHOT", ctx.exception.args[0])

    @mock.patch("esrally.utils.net.retrieve_content_as_string")
    def test_download_url_for_corrupt_metadata(self, content_as_string):
        content_as_string.return_value = """
<metadata modelVersion="1.1
"""
        repo = supplier.SnapshotDistributionRepo()
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            repo.download_url("5.0.0-SNAPSHOT")
        self.assertEqual("Cannot derive download URL for Elasticsearch 5.0.0-SNAPSHOT", ctx.exception.args[0])

    @mock.patch("esrally.utils.net.retrieve_content_as_string")
    def test_download_url_for_unavailable_metadata(self, content_as_string):
        content_as_string.side_effect = urllib.error.HTTPError("url", 404, "", "", None)
        repo = supplier.SnapshotDistributionRepo()
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            repo.download_url("10.0.0-SNAPSHOT")
        self.assertEqual("Cannot derive download URL for Elasticsearch 10.0.0-SNAPSHOT", ctx.exception.args[0])


class ReleaseDistributionRepositoryTests(TestCase):
    def test_download_url_for_5_0_beta_or_later_valid_versions(self):
        root_url = "https://artifacts.elastic.co/downloads/elasticsearch"
        repo = supplier.ReleaseDistributionRepo()
        self.assertEqual("%s/elasticsearch-5.0.0-beta1.tar.gz" % root_url, repo.download_url("5.0.0-beta1"))
        self.assertEqual("%s/elasticsearch-6.0.0-alpha1.tar.gz" % root_url, repo.download_url("6.0.0-alpha1"))

    def test_download_url_for_2_until_5_0_alpha_valid_versions(self):
        root_url = "https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch"
        repo = supplier.ReleaseDistributionRepo()
        self.assertEqual("%s/5.0.0-alpha1/elasticsearch-5.0.0-alpha1.tar.gz" % root_url, repo.download_url("5.0.0-alpha1"))
        self.assertEqual("%s/2.0.0/elasticsearch-2.0.0.tar.gz" % root_url, repo.download_url("2.0.0"))

    def test_download_url_for_ancient_valid_versions(self):
        repo = supplier.ReleaseDistributionRepo()
        self.assertEqual("https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.7.2.tar.gz",
                         repo.download_url("1.7.2"))


