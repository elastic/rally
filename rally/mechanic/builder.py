import os
import glob
import config

import utils.io as io

# can build an actual source tree
#
# Idea: Think about a "skip-build" flag for local use (or a pre-build check whether there is already a binary (prio: low)
class Builder:
  def __init__(self, config, logger):
    self._config = config
    self._logger = logger

  def build(self):
    # just Gradle is supported for now
    #self._clean()
    #self._package()
    self._add_binary_to_config()

  def _clean(self):
    self._exec("gradle.tasks.clean")

  def _package(self):
    self._exec("gradle.tasks.package")

  def _add_binary_to_config(self):
    src_dir = self._config.opts("source", "local.src.dir")
    binary = glob.glob("%s/distribution/zip/build/distributions/*.zip" % src_dir)[0]
    self._config.add(config.Scope.invocationScope, "builder", "candidate.bin.path", binary)

  def _exec(self, task_key):
    src_dir = self._config.opts("source", "local.src.dir")
    gradle = self._config.opts("build", "gradle.bin")
    task = self._config.opts("build", task_key)
    dry_run = self._config.opts("system", "dryrun")
    # store logs for each invocation in a dedicated directory
    s = self._config.opts("meta", "time.start")
    timestamp = '%04d-%02d-%02d-%02d-%02d-%02d' % (s.year, s.month, s.day, s.hour, s.minute, s.second)
    log_dir = "%s/%s" % (self._config.opts("build", "log.dir"), timestamp)

    self._logger.info("Executing %s %s..." % (gradle, task))
    if not dry_run:
      io.ensure_dir(log_dir)
      # TODO dm: How should this be called?
      log_file = "%s/build.%s.log" % (log_dir, task_key)

      if not os.system("cd %s; %s %s > %s.tmp 2>&1" % (src_dir, gradle, task, log_file)):
        os.rename(("%s.tmp" % log_file), log_file)
