import unittest.mock as mock
from unittest import TestCase
import urllib.error

from esrally import racecontrol, exceptions


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
        repo = racecontrol.SnapshotDistributionRepo()
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
        repo = racecontrol.SnapshotDistributionRepo()
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            repo.download_url("5.0.0-SNAPSHOT")
        self.assertEqual("Cannot derive download URL for Elasticsearch 5.0.0-SNAPSHOT", ctx.exception.args[0])

    @mock.patch("esrally.utils.net.retrieve_content_as_string")
    def test_download_url_for_corrupt_metadata(self, content_as_string):
        content_as_string.return_value = """
<metadata modelVersion="1.1
"""
        repo = racecontrol.SnapshotDistributionRepo()
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            repo.download_url("5.0.0-SNAPSHOT")
        self.assertEqual("Cannot derive download URL for Elasticsearch 5.0.0-SNAPSHOT", ctx.exception.args[0])

    @mock.patch("esrally.utils.net.retrieve_content_as_string")
    def test_download_url_for_unavailable_metadata(self, content_as_string):
        content_as_string.side_effect = urllib.error.HTTPError("url", 404, "", "", None)
        repo = racecontrol.SnapshotDistributionRepo()
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            repo.download_url("10.0.0-SNAPSHOT")
        self.assertEqual("Cannot derive download URL for Elasticsearch 10.0.0-SNAPSHOT", ctx.exception.args[0])


class ReleaseDistributionRepositoryTests(TestCase):
    def test_download_url_for_5_0_beta_or_later_valid_versions(self):
        root_url = "https://artifacts.elastic.co/downloads/elasticsearch"
        repo = racecontrol.ReleaseDistributionRepo()
        self.assertEqual("%s/elasticsearch-5.0.0-beta1.tar.gz" % root_url, repo.download_url("5.0.0-beta1"))
        self.assertEqual("%s/elasticsearch-6.0.0-alpha1.tar.gz" % root_url, repo.download_url("6.0.0-alpha1"))

    def test_download_url_for_2_until_5_0_alpha_valid_versions(self):
        root_url = "https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch"
        repo = racecontrol.ReleaseDistributionRepo()
        self.assertEqual("%s/5.0.0-alpha1/elasticsearch-5.0.0-alpha1.tar.gz" % root_url, repo.download_url("5.0.0-alpha1"))
        self.assertEqual("%s/2.0.0/elasticsearch-2.0.0.tar.gz" % root_url, repo.download_url("2.0.0"))

    def test_download_url_for_ancient_valid_versions(self):
        repo = racecontrol.ReleaseDistributionRepo()
        self.assertEqual("https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.7.2.tar.gz",
                         repo.download_url("1.7.2"))


