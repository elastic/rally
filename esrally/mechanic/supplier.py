import os
import glob
import logging
import urllib.error

from esrally import exceptions
from esrally.utils import git, console, io, process, net, versions, convert
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
        return {"elasticsearch": builder.binary}
    except BaseException:
        if build:
            console.println(" [FAILED]")
        raise


def from_distribution(version, repo_name, distribution_config, distributions_root, plugins):
    if version.strip() == "":
        raise exceptions.SystemSetupError("Could not determine version. Please specify the Elasticsearch distribution "
                                          "to download with the command line parameter --distribution-version. "
                                          "E.g. --distribution-version=5.0.0")
    io.ensure_dir(distributions_root)
    distribution_path = "%s/elasticsearch-%s.tar.gz" % (distributions_root, version)

    repo = DistributionRepository(repo_name, distribution_config, version)

    download_url = repo.download_url
    logger.info("Resolved download URL [%s] for version [%s]" % (download_url, version))
    if not os.path.isfile(distribution_path) or not repo.cache:
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

    binaries = {"elasticsearch": distribution_path}
    for plugin in plugins:
        # if we have multiple plugin configurations for a plugin we will override entries here but as this is always the same
        # key-value pair this is ok.
        plugin_url = repo.plugin_download_url(plugin.name)
        if plugin_url:
            binaries[plugin.name] = plugin_url

    return binaries


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
    # Tested with Gradle 4.1 on Java 9-ea+161
    JAVA_9_GRADLE_OPTS = "--add-opens=java.base/java.io=ALL-UNNAMED " \
                         "--add-opens=java.base/java.lang=ALL-UNNAMED " \
                         "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED " \
                         "--add-opens=java.base/java.util=ALL-UNNAMED " \
                         "--add-opens=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED " \
                         "--add-opens=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED"
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
        from esrally.utils import jvm

        logger.info("Building Elasticsearch from sources in [%s]." % self.src_dir)
        logger.info("Executing %s %s..." % (self.gradle, task))
        io.ensure_dir(self.log_dir)
        log_file = "%s/build.log" % self.log_dir

        # we capture all output to a dedicated build log file
        jvm_major_version = jvm.major_version(self.java_home)
        if jvm_major_version > 8:
            logger.info("Detected JVM with major version [%d]. Adjusting JDK module access options for the build." % jvm_major_version)
            gradle_opts = "export GRADLE_OPTS=\"%s\"; " % Builder.JAVA_9_GRADLE_OPTS
        else:
            gradle_opts = ""

        if process.run_subprocess("%sexport JAVA_HOME=%s; cd %s; %s %s >> %s 2>&1" %
                                  (gradle_opts, self.java_home, self.src_dir, self.gradle, task, log_file)):
            msg = "Executing '%s %s' failed. The last 20 lines in the build log file are:\n" % (self.gradle, task)
            msg += "=========================================================================================================\n"
            with open(log_file, "r") as f:
                msg += "\t"
                msg += "\t".join(f.readlines()[-20:])
            msg += "=========================================================================================================\n"
            msg += "The full build log is available at [%s]." % log_file
            if jvm_major_version > 8:
                msg += "Please check"

            raise BuildError(msg)


class DistributionRepository:
    def __init__(self, name, distribution_config, version):
        self.name = name
        self.cfg = distribution_config
        self.version = version

    @property
    def download_url(self):
        major_version = versions.major_version(self.version)
        version_url_key = "%s.%s.url" % (self.name, str(major_version))
        default_url_key = "%s.url" % self.name
        return self._url_for(version_url_key, default_url_key)

    def plugin_download_url(self, plugin_name):
        major_version = versions.major_version(self.version)
        version_url_key = "plugin.%s.%s.%s.url" % (plugin_name, self.name, str(major_version))
        default_url_key = "plugin.%s.%s.url" % (plugin_name, self.name)
        return self._url_for(version_url_key, default_url_key, mandatory=False)

    def _url_for(self, version_url_key, default_url_key, mandatory=True):
        try:
            if version_url_key in self.cfg:
                url_template = self.cfg[version_url_key]
            else:
                url_template = self.cfg[default_url_key]
        except KeyError:
            if mandatory:
                raise exceptions.SystemSetupError("Neither version specific distribution config key [%s] nor a default distribution "
                                                  "config key [%s] is defined." % (version_url_key, default_url_key))
            else:
                return None
        return url_template.replace("{{VERSION}}", self.version)

    @property
    def cache(self):
        k = "%s.cache" % self.name
        try:
            raw_value = self.cfg[k]
        except KeyError:
            raise exceptions.SystemSetupError("Mandatory config key [%s] is undefined." % k)
        try:
            return convert.to_bool(raw_value)
        except ValueError:
            raise exceptions.SystemSetupError("Value [%s] for config key [%s] is not a valid boolean value." % (raw_value, k))
