import os
import glob

import rally.config
import rally.utils.io as io
import rally.utils.process


class Builder:
  """
  A builder is responsible for creating an installable binary from the source files.

  It is not intended to be used directly but should be triggered by its mechanic.
  """
  def __init__(self, config, logger):
    self._config = config
    self._logger = logger

  def build(self):
    # just Gradle is supported for now
    if not self._config.opts("build", "skip"):
      self._clean()
      self._package()
    else:
      self._logger.info("Skipping build")
    self._add_binary_to_config()

  def _clean(self):
    self._exec("gradle.tasks.clean")

  def _package(self):
    print("  Building from sources ...")
    self._exec("gradle.tasks.package")

  def _add_binary_to_config(self):
    src_dir = self._config.opts("source", "local.src.dir")
    binary = glob.glob("%s/distribution/zip/build/distributions/*.zip" % src_dir)[0]
    self._config.add(rally.config.Scope.invocationScope, "builder", "candidate.bin.path", binary)

  def _exec(self, task_key):
    src_dir = self._config.opts("source", "local.src.dir")
    gradle = self._config.opts("build", "gradle.bin")
    task = self._config.opts("build", task_key)

    log_root = self._config.opts("system", "log.dir")
    build_log_dir = self._config.opts("build", "log.dir")
    log_dir = "%s/%s" % (log_root, build_log_dir)

    self._logger.info("Executing %s %s..." % (gradle, task))
    io.ensure_dir(log_dir)
    log_file = "%s/build.%s.log" % (log_dir, task_key)

    # It's ok to call os.system here; we capture all output to a dedicated build log file
    if not os.system("cd %s; %s %s > %s.tmp 2>&1" % (src_dir, gradle, task, log_file)):
      os.rename(("%s.tmp" % log_file), log_file)
      self._logger.warn("Executing '%s %s' failed" % (gradle, task))
