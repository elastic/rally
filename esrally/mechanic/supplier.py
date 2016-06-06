import logging

from esrally.utils import git

logger = logging.getLogger("rally.supplier")


class Supplier:
    """
    Supplier fetches the benchmark candidate source tree from the remote repository. In the current implementation, only git is supported.
    """

    def __init__(self, cfg):
        self.cfg = cfg

    def fetch(self):
        # assume fetching of latest version for now
        self._try_init()
        self._update()

    def _try_init(self):
        if not git.is_working_copy(self.src_dir):
            print("Downloading sources from %s to %s." % (self.remote_url, self.src_dir))
            git.clone(self.src_dir, self.remote_url)

    def _update(self):
        revision = self.cfg.opts("source", "revision")
        if revision == "latest":
            logger.info("Fetching latest sources from origin.")
            git.pull(self.src_dir)
        elif revision == "current":
            logger.info("Skip fetching sources")
        elif revision.startswith("@"):
            # concert timestamp annotated for Rally to something git understands -> we strip leading and trailing " and the @.
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

