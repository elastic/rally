from unittest import TestCase
import unittest.mock as mock

from esrally import exceptions
from esrally.mechanic import supplier


class RevisionExtractorTests(TestCase):
    def test_single_revision(self):
        self.assertDictEqual({"elasticsearch": "67c2f42", "all": "67c2f42"}, supplier.extract_revisions("67c2f42"))
        self.assertDictEqual({"elasticsearch": "current", "all": "current"}, supplier.extract_revisions("current"))
        self.assertDictEqual({"elasticsearch": "@2015-01-01-01:00:00", "all": "@2015-01-01-01:00:00"},
                             supplier.extract_revisions("@2015-01-01-01:00:00"))

    def test_multiple_revisions(self):
        self.assertDictEqual({"elasticsearch": "67c2f42", "x-pack": "@2015-01-01-01:00:00", "some-plugin": "current"},
                             supplier.extract_revisions("elasticsearch:67c2f42,x-pack:@2015-01-01-01:00:00,some-plugin:current"))

    def test_invalid_revisions(self):
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            supplier.extract_revisions("elasticsearch 67c2f42,x-pack:current")
        self.assertEqual("Revision [elasticsearch 67c2f42] does not match expected format [name:revision].", ctx.exception.args[0])


class SourceRepositoryTests(TestCase):
    @mock.patch("esrally.utils.git.head_revision", autospec=True)
    @mock.patch("esrally.utils.git.pull", autospec=True)
    @mock.patch("esrally.utils.git.clone", autospec=True)
    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    def test_intial_checkout_latest(self, mock_is_working_copy, mock_clone, mock_pull, mock_head_revision):
        # before cloning, it is not a working copy, afterwards it is
        mock_is_working_copy.side_effect = [False, True]
        mock_head_revision.return_value = "HEAD"

        s = supplier.SourceRepository(name="Elasticsearch", remote_url="some-github-url", src_dir="/src")
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

        s = supplier.SourceRepository(name="Elasticsearch", remote_url="some-github-url", src_dir="/src")
        s.fetch("current")

        mock_is_working_copy.assert_called_with("/src")
        mock_clone.assert_not_called()
        mock_pull.assert_not_called()
        mock_head_revision.assert_called_with("/src")\


    @mock.patch("esrally.utils.git.head_revision", autospec=True)
    @mock.patch("esrally.utils.git.checkout")
    @mock.patch("esrally.utils.git.pull")
    @mock.patch("esrally.utils.git.clone")
    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    def test_checkout_revision_for_local_only_repo(self, mock_is_working_copy, mock_clone, mock_pull, mock_checkout, mock_head_revision):
        mock_is_working_copy.return_value = True
        mock_head_revision.return_value = "HEAD"

        # local only, we dont specify a remote
        s = supplier.SourceRepository(name="Elasticsearch", remote_url=None, src_dir="/src")
        s.fetch("67c2f42")

        mock_is_working_copy.assert_called_with("/src")
        mock_clone.assert_not_called()
        mock_pull.assert_not_called()
        mock_checkout.assert_called_with("/src", "67c2f42")
        mock_head_revision.assert_called_with("/src")

    @mock.patch("esrally.utils.git.head_revision", autospec=True)
    @mock.patch("esrally.utils.git.pull_ts", autospec=True)
    @mock.patch("esrally.utils.git.is_working_copy", autospec=True)
    def test_checkout_ts(self, mock_is_working_copy, mock_pull_ts, mock_head_revision):
        mock_is_working_copy.return_value = True
        mock_head_revision.return_value = "HEAD"

        s = supplier.SourceRepository(name="Elasticsearch", remote_url="some-github-url", src_dir="/src")
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

        s = supplier.SourceRepository(name="Elasticsearch", remote_url="some-github-url", src_dir="/src")
        s.fetch("67c2f42")

        mock_is_working_copy.assert_called_with("/src")
        mock_pull_revision.assert_called_with("/src", "67c2f42")
        mock_head_revision.assert_called_with("/src")

    def test_is_commit_hash(self):
        self.assertTrue(supplier.SourceRepository.is_commit_hash("67c2f42"))

    def test_is_not_commit_hash(self):
        self.assertFalse(supplier.SourceRepository.is_commit_hash("latest"))
        self.assertFalse(supplier.SourceRepository.is_commit_hash("current"))
        self.assertFalse(supplier.SourceRepository.is_commit_hash("@2015-01-01-01:00:00"))


class BuilderTests(TestCase):
    @mock.patch("esrally.utils.process.run_subprocess")
    @mock.patch("esrally.utils.jvm.major_version")
    def test_build_on_jdk_8(self, jvm_major_version, mock_run_subprocess):
        jvm_major_version.return_value = 8
        mock_run_subprocess.return_value = False

        b = supplier.Builder(src_dir="/src", gradle="/usr/local/gradle", java_home="/opt/jdk8", log_dir="logs")
        b.build([supplier.CLEAN_TASK, supplier.ASSEMBLE_TASK])

        calls = [
            # Actual call
            mock.call("export JAVA_HOME=/opt/jdk8; cd /src; /usr/local/gradle clean >> logs/build.log 2>&1"),
            # Return value check
            mock.call("export JAVA_HOME=/opt/jdk8; cd /src; /usr/local/gradle :distribution:tar:assemble >> logs/build.log 2>&1"),
        ]

        mock_run_subprocess.assert_has_calls(calls)

    @mock.patch("esrally.utils.process.run_subprocess")
    @mock.patch("esrally.utils.jvm.major_version")
    def test_build_on_jdk_9(self, jvm_major_version, mock_run_subprocess):
        jvm_major_version.return_value = 9
        mock_run_subprocess.return_value = False

        b = supplier.Builder(src_dir="/src", gradle="/usr/local/gradle", java_home="/opt/jdk9", log_dir="logs")
        b.build([supplier.CLEAN_TASK, supplier.ASSEMBLE_TASK])

        calls = [
            # Actual call
            mock.call("export GRADLE_OPTS=\"%s\"; export JAVA_HOME=/opt/jdk9; cd /src; /usr/local/gradle clean >> logs/build.log 2>&1" %
                      supplier.Builder.JAVA_9_GRADLE_OPTS),
            # Return value check
            mock.call("export GRADLE_OPTS=\"%s\"; export JAVA_HOME=/opt/jdk9; cd /src; /usr/local/gradle :distribution:tar:assemble "
                      ">> logs/build.log 2>&1" % supplier.Builder.JAVA_9_GRADLE_OPTS),
        ]

        mock_run_subprocess.assert_has_calls(calls)


class ArtifactResolverTests(TestCase):
    @mock.patch("glob.glob", lambda p: ["elasticsearch.tar.gz"])
    def test_resolve_elasticsearch_binary(self):
        self.assertEqual(supplier.resolve_es_binary("/src"), "elasticsearch.tar.gz")

    @mock.patch("glob.glob", lambda p: ["/src/elasticsearch-extra/some-plugin/plugin/build/distributions/some-plugin.zip"])
    def test_resolve_elasticsearch_binary(self):
        self.assertEqual(supplier.resolve_plugin_binary("some-plugin", "/src", {
            "plugin.some-plugin.src.subdir": "elasticsearch-extra/some-plugin",
            "plugin.some-plugin.build.artifact.subdir": "plugin/build/distributions"
        }), "file:///src/elasticsearch-extra/some-plugin/plugin/build/distributions/some-plugin.zip")


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
