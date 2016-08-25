import os
import glob
import logging

from esrally import config
from esrally.utils import io, process
from esrally.exceptions import ImproperlyConfigured, BuildError

logger = logging.getLogger("rally.builder")


class Builder:
    """
    A builder is responsible for creating an installable binary from the source files.

    It is not intended to be used directly but should be triggered by its mechanic.
    """

    def __init__(self, cfg):
        self._config = cfg

    def build(self):
        # just Gradle is supported for now
        self._exec("gradle.tasks.clean")
        print("  Building from sources ...")
        self._exec("gradle.tasks.package")

    def add_binary_to_config(self):
        src_dir = self._config.opts("source", "local.src.dir")
        try:
            binary = glob.glob("%s/distribution/tar/build/distributions/*.tar.gz" % src_dir)[0]
        except IndexError:
            raise ImproperlyConfigured("Couldn't find a tar.gz distribution.")
        self._config.add(config.Scope.invocation, "builder", "candidate.bin.path", binary)

    def _exec(self, task_key):
        src_dir = self._config.opts("source", "local.src.dir")
        logger.info("Building Elasticsearch from sources in [%s]." % src_dir)
        gradle = self._config.opts("build", "gradle.bin")
        task = self._config.opts("build", task_key)

        log_root = self._config.opts("system", "log.dir")
        build_log_dir = self._config.opts("build", "log.dir")
        log_dir = "%s/%s" % (log_root, build_log_dir)

        logger.info("Executing %s %s..." % (gradle, task))
        io.ensure_dir(log_dir)
        log_file = "%s/build.%s.log" % (log_dir, task_key)

        # we capture all output to a dedicated build log file
        if process.run_subprocess("cd %s; %s %s > %s 2>&1" % (src_dir, gradle, task, log_file)):
            msg = "Executing '%s %s' failed. Here are the last 20 lines in the build log file:\n" % (gradle, task)
            msg += "=========================================================================================================\n"
            with open(log_file, "r") as f:
                msg += "\t"
                msg += "\t".join(f.readlines()[-20:])
            msg += "=========================================================================================================\n"
            msg += "The full build log is available at [%s]." % log_file
            raise BuildError(msg)
