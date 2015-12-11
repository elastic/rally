import os

import rally.utils.io as io


class SupplyError(BaseException):
  pass


class Supplier:
  """
  Supplier fetches the benchmark candidate source tree from the remote repository. In the current implementation, only git is supported.
  """
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
      print("Downloading sources from %s to %s." % (repo_url, src_dir))
      # Don't swallow subprocess output, user might need to enter credentials...
      if os.system("git clone %s %s" % (repo_url, src_dir)):
        raise SupplyError("Could not clone from %s to %s" % (repo_url, src_dir))

  def _update(self):
    revision = self._config.opts("source", "revision")
    src_dir = self._src_dir()
    if revision == "latest":
      self._logger.info("Fetching latest sources from %s." % self._repo_url())
      # Don't swallow output but silence git at least a bit... (--quiet)
      if os.system("sh -c 'cd %s; git checkout --quiet master && git fetch --quiet origin && git rebase --quiet origin/master'" % src_dir):
        raise SupplyError("Could not fetch latest source tree")
    elif revision == "current":
      self._logger.info("Skip fetching sources")
    elif revision.startswith('@'):
      # concert timestamp annotated for Rally to something git understands -> we strip leading and trailing " and the @.
      ts = revision[1:]
      if os.system("sh -c 'cd %s; git fetch --quiet origin && git checkout --quiet `git rev-list -n 1 --before=\"%s\" master`'" % (src_dir, ts)):
        raise SupplyError("Could not fetch source tree for timestamped revision %s" % ts)
    else: #assume a git commit hash
      if os.system("sh -c 'cd %s; git fetch --quiet origin && git checkout --quiet %s'" % (src_dir, revision)):
        raise SupplyError("Could not fetch source tree for revision %s" % revision)

    git_revision = os.popen("sh -c 'cd %s; git rev-parse --short HEAD'" % src_dir).readline().split()[0]
    self._logger.info("Specified revision [%s] on command line results in git revision [%s]" % (revision, git_revision))

  def _src_dir(self):
    return self._config.opts("source", "local.src.dir")

  def _repo_url(self):
    return self._config.opts("source", "remote.repo.url")
