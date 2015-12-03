import os

import rally.utils.io as io
import rally.utils.process


class SupplyError(BaseException):
  pass


# gets the actual source, currently only git is supported (implicitly)
class Supplier:
  def __init__(self, config, logger):
    self._config = config
    self._logger = logger

  def fetch(self):
    # assume fetching of latest version for now
    self._try_init()
    self._update()

  def _try_init(self):
    src_dir = self._src_dir()
    repo_url = self._repo_url()

    io.ensure_dir(src_dir)
    # clone if necessary
    if not os.path.isdir("%s/.git" % src_dir):
      if not rally.utils.process.run_subprocess("git clone %s %s" % (repo_url, src_dir)):
        raise SupplyError("Could not clone from %s to %s" % (repo_url, src_dir))

  def _update(self):
    if self._config.opts("source", "force.update"):
      self._logger.info("Fetching latest sources from %s." % self._repo_url())
      src_dir = self._src_dir()
      if not rally.utils.process.run_subprocess("sh -c 'cd %s; git checkout master && git fetch origin && git rebase origin/master'" % src_dir):
        raise SupplyError("Could not fetch latest source tree")
    else:
      self._logger.info("Skipping fetching sources")

  def _src_dir(self):
    return self._config.opts("source", "local.src.dir")

  def _repo_url(self):
    return self._config.opts("source", "remote.repo.url")
