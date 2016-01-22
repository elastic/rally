import os

import rally.utils.io as io
import rally.utils.process


class SupplyError(BaseException):
    pass


class GitRepository:
    def __init__(self, config):
        self._remote_url = config.opts("source", "remote.repo.url")
        self._src_dir = config.opts("source", "local.src.dir")

    @property
    def src_dir(self):
        return self._src_dir

    @property
    def remote_url(self):
        return self._remote_url

    def is_cloned(self):
        return os.path.isdir("%s/.git" % self._src_dir)

    def clone(self):
        src = self._src_dir
        remote = self._remote_url
        io.ensure_dir(src)
        print("Downloading sources from %s to %s." % (remote, src))
        # Don't swallow subprocess output, user might need to enter credentials...
        if rally.utils.process.run_subprocess("git clone %s %s" % (remote, src)):
            raise SupplyError("Could not clone from %s to %s" % (remote, src))

    def pull(self, remote="origin", branch="master"):
        # Don't swallow output but silence git at least a bit... (--quiet)
        if rally.utils.process.run_subprocess(
                "sh -c 'cd {0}; git checkout --quiet {2} && git fetch --quiet {1} && git rebase --quiet {1}/{2}'" %
                (self._src_dir, remote, branch)):
            raise SupplyError("Could not fetch latest source tree")

    def pull_ts(self, ts):
        if rally.utils.process.run_subprocess(
                "sh -c 'cd %s; git fetch --quiet origin && git checkout --quiet `git rev-list -n 1 --before=\"%s\" master`'" %
                (self._src_dir, ts)):
            raise SupplyError("Could not fetch source tree for timestamped revision %s" % ts)

    def pull_revision(self, revision):
        if rally.utils.process.run_subprocess(
                "sh -c 'cd %s; git fetch --quiet origin && git checkout --quiet %s'" % (self._src_dir, revision)):
            raise SupplyError("Could not fetch source tree for revision %s" % revision)

    def head_revision(self):
        return os.popen("sh -c 'cd %s; git rev-parse --short HEAD'" % self._src_dir).readline().split()[0]


class Supplier:
    """
    Supplier fetches the benchmark candidate source tree from the remote repository. In the current implementation, only git is supported.
    """

    def __init__(self, config, logger, repo):
        self._config = config
        self._logger = logger
        self._repo = repo

    def fetch(self):
        # assume fetching of latest version for now
        self._try_init()
        self._update()

    def _try_init(self):
        if not self._repo.is_cloned():
            self._repo.clone()

    def _update(self):
        revision = self._config.opts("source", "revision")
        if revision == "latest":
            self._logger.info("Fetching latest sources from %s." % self._repo.remote_url)
            self._repo.pull()
        elif revision == "current":
            self._logger.info("Skip fetching sources")
        elif revision.startswith('@'):
            # concert timestamp annotated for Rally to something git understands -> we strip leading and trailing " and the @.
            self._repo.pull_ts(revision[1:])
        else:  # assume a git commit hash
            self._repo.pull_revision(revision)

        git_revision = self._repo.head_revision()
        self._logger.info("Specified revision [%s] on command line results in git revision [%s]" % (revision, git_revision))
