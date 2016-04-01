import os
import logging

from rally.utils import io, process

logger = logging.getLogger("rally.supplier")


class SupplyError(BaseException):
    pass


class GitRepository:
    def __init__(self, cfg):
        self.cfg = cfg

    @property
    def src_dir(self):
        return self.cfg.opts("source", "local.src.dir")

    @property
    def remote_url(self):
        return self.cfg.opts("source", "remote.repo.url")

    def is_cloned(self):
        return os.path.isdir("%s/.git" % self.src_dir)

    def clone(self):
        src = self.src_dir
        remote = self.remote_url
        io.ensure_dir(src)
        print("Downloading sources from %s to %s." % (remote, src))
        # Don't swallow subprocess output, user might need to enter credentials...
        if process.run_subprocess("git clone %s %s" % (remote, src)):
            raise SupplyError("Could not clone from %s to %s" % (remote, src))

    def pull(self, remote="origin", branch="master"):
        # Don't swallow output but silence git at least a bit... (--quiet)
        if process.run_subprocess(
            "sh -c 'cd {0}; git checkout --quiet {2} && git fetch --quiet {1} && git rebase --quiet {1}/{2}'"
                .format(self.src_dir, remote, branch)):
            raise SupplyError("Could not fetch latest source tree")

    def pull_ts(self, ts):
        if process.run_subprocess(
                "sh -c 'cd %s; git fetch --quiet origin && git checkout --quiet `git rev-list -n 1 --before=\"%s\" "
                "--date=iso8601 origin/master`'" %
                (self.src_dir, ts)):
            raise SupplyError("Could not fetch source tree for timestamped revision %s" % ts)

    def pull_revision(self, revision):
        if process.run_subprocess(
                "sh -c 'cd %s; git fetch --quiet origin && git checkout --quiet %s'" % (self.src_dir, revision)):
            raise SupplyError("Could not fetch source tree for revision %s" % revision)

    def head_revision(self):
        return os.popen("sh -c 'cd %s; git rev-parse --short HEAD'" % self.src_dir).readline().split()[0]


class Supplier:
    """
    Supplier fetches the benchmark candidate source tree from the remote repository. In the current implementation, only git is supported.
    """

    def __init__(self, cfg, repo):
        self._config = cfg
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
            logger.info("Fetching latest sources from %s." % self._repo.remote_url)
            self._repo.pull()
        elif revision == "current":
            logger.info("Skip fetching sources")
        elif revision.startswith("@"):
            # concert timestamp annotated for Rally to something git understands -> we strip leading and trailing " and the @.
            self._repo.pull_ts(revision[1:])
        else:  # assume a git commit hash
            self._repo.pull_revision(revision)

        git_revision = self._repo.head_revision()
        logger.info("Specified revision [%s] on command line results in git revision [%s]" % (revision, git_revision))
