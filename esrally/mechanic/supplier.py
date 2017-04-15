import os
import glob
import logging
import urllib.error
import xml.etree.ElementTree

from esrally import exceptions
from esrally.utils import git, console, io, process, net, versions
from esrally.exceptions import BuildError, SystemSetupError

logger = logging.getLogger("rally.supplier")


def from_sources(remote_url, src_dir, revision, gradle, java_home, log_dir, build=True):
    if build:
        console.info("Preparing for race ...", end="", flush=True)
    try:
        SourceRepository(remote_url, src_dir).fetch(revision)

        builder = Builder(src_dir, gradle, java_home, log_dir)

        if build:
            builder.build()
            console.println(" [OK]")
        return builder.binary
    except BaseException:
        if build:
            console.println(" [FAILED]")
        raise


def from_distribution(version, repo_name, distributions_root):
    if version.strip() == "":
        raise exceptions.SystemSetupError("Could not determine version. Please specify the Elasticsearch distribution "
                                          "to download with the command line parameter --distribution-version. "
                                          "E.g. --distribution-version=5.0.0")
    io.ensure_dir(distributions_root)
    distribution_path = "%s/elasticsearch-%s.tar.gz" % (distributions_root, version)

    try:
        repo = distribution_repos[repo_name]
    except KeyError:
        raise exceptions.SystemSetupError("Unknown distribution repository [%s]. Valid values are: [%s]"
                                          % (repo_name, ",".join(distribution_repos.keys())))

    download_url = repo.download_url(version)
    logger.info("Resolved download URL [%s] for version [%s]" % (download_url, version))
    if not os.path.isfile(distribution_path) or repo.must_download:
        try:
            logger.info("Starting download of Elasticsearch [%s]" % version)
            progress = net.Progress("[INFO] Downloading Elasticsearch %s" % version)
            net.download(download_url, distribution_path, progress_indicator=progress)
            progress.finish()
            logger.info("Successfully downloaded Elasticsearch [%s]." % version)
        except urllib.error.HTTPError:
            console.println("[FAILED]")
            logging.exception("Cannot download Elasticsearch distribution for version [%s] from [%s]." % (version, download_url))
            raise exceptions.SystemSetupError("Cannot download Elasticsearch distribution from [%s]. Please check that the specified "
                                              "version [%s] is correct." % (download_url, version))
    else:
        logger.info("Skipping download for version [%s]. Found an existing binary locally at [%s]." % (version, distribution_path))
    return distribution_path


class SourceRepository:
    """
    Supplier fetches the benchmark candidate source tree from the remote repository.
    """

    def __init__(self, remote_url, src_dir):
        self.remote_url = remote_url
        self.src_dir = src_dir

    def fetch(self, revision):
        # assume fetching of latest version for now
        self._try_init()
        self._update(revision)

    def _try_init(self):
        if not git.is_working_copy(self.src_dir):
            console.println("Downloading sources from %s to %s." % (self.remote_url, self.src_dir))
            git.clone(self.src_dir, self.remote_url)

    def _update(self, revision):
        if revision == "latest":
            logger.info("Fetching latest sources from origin.")
            git.pull(self.src_dir)
        elif revision == "current":
            logger.info("Skip fetching sources")
        elif revision.startswith("@"):
            # convert timestamp annotated for Rally to something git understands -> we strip leading and trailing " and the @.
            git.pull_ts(self.src_dir, revision[1:])
        else:  # assume a git commit hash
            git.pull_revision(self.src_dir, revision)
        git_revision = git.head_revision(self.src_dir)
        logger.info("Specified revision [%s] on command line results in git revision [%s]" % (revision, git_revision))


class Builder:
    """
    A builder is responsible for creating an installable binary from the source files.

    It is not intended to be used directly but should be triggered by its mechanic.
    """

    def __init__(self, src_dir, gradle=None, java_home=None, log_dir=None):
        self.src_dir = src_dir
        self.gradle = gradle
        self.java_home = java_home
        self.log_dir = log_dir

    def build(self):
        self.run("clean")
        self.run(":distribution:tar:assemble")

    @property
    def binary(self):
        try:
            return glob.glob("%s/distribution/tar/build/distributions/*.tar.gz" % self.src_dir)[0]
        except IndexError:
            raise SystemSetupError("Couldn't find a tar.gz distribution. Please run Rally with the pipeline 'from-sources-complete'.")

    def run(self, task):
        logger.info("Building Elasticsearch from sources in [%s]." % self.src_dir)
        logger.info("Executing %s %s..." % (self.gradle, task))
        io.ensure_dir(self.log_dir)
        log_file = "%s/build.log" % self.log_dir

        # we capture all output to a dedicated build log file

        if process.run_subprocess("export JAVA_HOME=%s; cd %s; %s %s >> %s 2>&1" %
                                          (self.java_home, self.src_dir, self.gradle, task, log_file)):
            msg = "Executing '%s %s' failed. The last 20 lines in the build log file are:\n" % (self.gradle, task)
            msg += "=========================================================================================================\n"
            with open(log_file, "r") as f:
                msg += "\t"
                msg += "\t".join(f.readlines()[-20:])
            msg += "=========================================================================================================\n"
            msg += "The full build log is available at [%s]." % log_file
            raise BuildError(msg)


class ReleaseDistributionRepo:
    def __init__(self):
        self.must_download = False

    def download_url(self, version):
        major_version, _, _, _ = versions.components(version)
        if major_version > 1 and not self.on_or_after_5_0_0_beta1(version):
            download_url = "https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/%s/" \
                           "elasticsearch-%s.tar.gz" % (version, version)
        elif self.on_or_after_5_0_0_beta1(version):
            download_url = "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-%s.tar.gz" % version
        else:
            download_url = "https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-%s.tar.gz" % version
        return download_url

    def on_or_after_5_0_0_beta1(self, version):
        major, minor, patch, suffix = versions.components(version)
        if major < 5:
            return False
        elif major == 5 and minor == 0 and patch == 0 and suffix and suffix.startswith("alpha"):
            return False
        return True


class SnapshotDistributionRepo:
    def __init__(self):
        self.must_download = True

    def download_url(self, version):
        root_path = "https://oss.sonatype.org/content/repositories/snapshots/org/elasticsearch/distribution/tar/elasticsearch/%s" % version
        metadata_url = "%s/maven-metadata.xml" % root_path
        try:
            metadata = net.retrieve_content_as_string(metadata_url)
            x = xml.etree.ElementTree.fromstring(metadata)
            found_snapshot_versions = x.findall("./versioning/snapshotVersions/snapshotVersion/[extension='tar.gz']/value")
        except Exception:
            logger.exception("Could not retrieve a valid metadata.xml file from remote URL [%s]." % metadata_url)
            raise exceptions.SystemSetupError("Cannot derive download URL for Elasticsearch %s" % version)

        if len(found_snapshot_versions) == 1:
            snapshot_id = found_snapshot_versions[0].text
            return "%s/elasticsearch-%s.tar.gz" % (root_path, snapshot_id)
        else:
            logger.error("Found [%d] version identifiers in [%s]. Contents: %s" % (len(found_snapshot_versions), metadata_url, metadata))
            raise exceptions.SystemSetupError("Cannot derive download URL for Elasticsearch %s" % version)

distribution_repos = {
    "release": ReleaseDistributionRepo(),
    "snapshot": SnapshotDistributionRepo()
}
