import os
import glob
import logging
import urllib.error
import xml.etree.ElementTree

from esrally import config, exceptions, PROGRAM_NAME
from esrally.utils import git, console, io, process, net, versions
from esrally.exceptions import BuildError, SystemSetupError

logger = logging.getLogger("rally.supplier")


def from_sources(cfg, build=True):
    console.println("Preparing for race (might take a few moments) ...")
    builder = Builder(cfg)
    SourceRepository(cfg).fetch()

    if build:
        builder.build()
    builder.add_binary_to_config()


def from_distribution(cfg):
    version = cfg.opts("source", "distribution.version")
    repo_name = cfg.opts("source", "distribution.repository")
    if version.strip() == "":
        raise exceptions.SystemSetupError("Could not determine version. Please specify the Elasticsearch distribution "
                                          "to download with the command line parameter --distribution-version. "
                                          "E.g. --distribution-version=5.0.0")
    distributions_root = "%s/%s" % (cfg.opts("system", "root.dir"), cfg.opts("source", "distribution.dir"))
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
            console.println("Downloading Elasticsearch %s ..." % version, logger=logger.info)
            net.download(download_url, distribution_path)
        except urllib.error.HTTPError:
            logging.exception("Cannot download Elasticsearch distribution for version [%s] from [%s]." % (version, download_url))
            raise exceptions.SystemSetupError("Cannot download Elasticsearch distribution from [%s]. Please check that the specified "
                                              "version [%s] is correct." % (download_url, version))
    else:
        logger.info("Skipping download for version [%s]. Found an existing binary locally at [%s]." % (version, distribution_path))

    cfg.add(config.Scope.invocation, "builder", "candidate.bin.path", distribution_path)


class SourceRepository:
    """
    Supplier fetches the benchmark candidate source tree from the remote repository.
    """

    def __init__(self, cfg):
        self.cfg = cfg

    def fetch(self):
        # assume fetching of latest version for now
        self._try_init()
        self._update()

    def _try_init(self):
        if not git.is_working_copy(self.src_dir):
            console.println("Downloading sources from %s to %s." % (self.remote_url, self.src_dir))
            git.clone(self.src_dir, self.remote_url)

    def _update(self):
        revision = self.cfg.opts("source", "revision")
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

    @property
    def src_dir(self):
        return self.cfg.opts("source", "local.src.dir")

    @property
    def remote_url(self):
        return self.cfg.opts("source", "remote.repo.url")


class Builder:
    """
    A builder is responsible for creating an installable binary from the source files.

    It is not intended to be used directly but should be triggered by its mechanic.
    """

    def __init__(self, cfg):
        self._config = cfg

    def build(self):
        self._exec("gradle.tasks.clean")
        console.println("  Building from sources ...")
        self._exec("gradle.tasks.package")

    def add_binary_to_config(self):
        src_dir = self._config.opts("source", "local.src.dir")
        try:
            binary = glob.glob("%s/distribution/tar/build/distributions/*.tar.gz" % src_dir)[0]
        except IndexError:
            raise SystemSetupError("Couldn't find a tar.gz distribution. Please run Rally with the pipeline 'from-sources-complete'.")
        self._config.add(config.Scope.invocation, "builder", "candidate.bin.path", binary)

    def _exec(self, task_key):
        try:
            src_dir = self._config.opts("source", "local.src.dir")
        except config.ConfigError:
            logging.exception("Rally is not configured to build from sources")
            raise SystemSetupError("Rally is not setup to build from sources. You can either benchmark a binary distribution or "
                                   "install the required software and reconfigure Rally with %s --configure." % PROGRAM_NAME)

        logger.info("Building Elasticsearch from sources in [%s]." % src_dir)
        gradle = self._config.opts("build", "gradle.bin")
        task = self._config.opts("build", task_key)
        java_home = self._config.opts("runtime", "java8.home")

        log_root = self._config.opts("system", "log.dir")
        build_log_dir = self._config.opts("build", "log.dir")
        log_dir = "%s/%s" % (log_root, build_log_dir)

        logger.info("Executing %s %s..." % (gradle, task))
        io.ensure_dir(log_dir)
        log_file = "%s/build.%s.log" % (log_dir, task_key)

        # we capture all output to a dedicated build log file

        if process.run_subprocess("export JAVA_HOME=%s; cd %s; %s %s > %s 2>&1" % (java_home, src_dir, gradle, task, log_file)):
            msg = "Executing '%s %s' failed. Here are the last 20 lines in the build log file:\n" % (gradle, task)
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
        major_version = int(versions.components(version)["major"])
        if major_version > 1 and not self.on_or_after_5_0_0_beta1(version):
            download_url = "https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/%s/" \
                           "elasticsearch-%s.tar.gz" % (version, version)
        elif self.on_or_after_5_0_0_beta1(version):
            download_url = "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-%s.tar.gz" % version
        else:
            download_url = "https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-%s.tar.gz" % version
        return download_url

    def on_or_after_5_0_0_beta1(self, version):
        components = versions.components(version)
        major_version = int(components["major"])
        minor_version = int(components["minor"])
        patch_version = int(components["patch"])
        suffix = components["suffix"] if "suffix" in components else None

        if major_version < 5:
            return False
        elif major_version == 5 and minor_version == 0 and patch_version == 0 and suffix and suffix.startswith("alpha"):
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
