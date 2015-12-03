import os
import glob

import rally.config
import rally.utils.io as io
import rally.utils.process

# can build an actual source tree
#
# Idea: Think about a "skip-build" flag for local use (or a pre-build check whether there is already a binary (prio: low)
class Builder:
  def __init__(self, config, logger):
    self._config = config
    self._logger = logger

  def build(self):
    # just Gradle is supported for now
    self._clean()
    self._package()
    self._add_binary_to_config()

  def _clean(self):
    # TODO dm: Issue a warning if this task failed (see https://github.com/elastic/dev/commit/04cbcb446f7df929e02f2a3a45dfc4d9ed52f3c6#diff-3516e1c39e49710a814bdbfeb33fa330R46)
    self._exec("gradle.tasks.clean")

  def _package(self):
    # TODO dm: Issue a warning if this task failed (see https://github.com/elastic/dev/commit/04cbcb446f7df929e02f2a3a45dfc4d9ed52f3c6#diff-3516e1c39e49710a814bdbfeb33fa330R46)
    self._exec("gradle.tasks.package")

  def _add_binary_to_config(self):
    src_dir = self._config.opts("source", "local.src.dir")
    binary = glob.glob("%s/distribution/zip/build/distributions/*.zip" % src_dir)[0]
    self._config.add(rally.config.Scope.invocationScope, "builder", "candidate.bin.path", binary)

  def _exec(self, task_key):
    src_dir = self._config.opts("source", "local.src.dir")
    gradle = self._config.opts("build", "gradle.bin")
    task = self._config.opts("build", task_key)
    dry_run = self._config.opts("system", "dryrun")

    log_root = self._config.opts("system", "log.dir")
    build_log_dir = self._config.opts("build", "log.dir")
    log_dir = "%s/%s" % (log_root, build_log_dir)

    self._logger.info("Executing %s %s..." % (gradle, task))
    if not dry_run:
      io.ensure_dir(log_dir)
      log_file = "%s/build.%s.log" % (log_dir, task_key)

      # It's ok to call os.system here; we capture all output to a dedicated build log file
      if not os.system("cd %s; %s %s > %s.tmp 2>&1" % (src_dir, gradle, task, log_file)):
        os.rename(("%s.tmp" % log_file), log_file)
