import os
import sys
import logging

from esrally import exceptions
from esrally.utils import git, console, versions

logger = logging.getLogger("rally.repo")


class RallyRepository:
    """
    Manages Rally resources (e.g. teams or tracks).
    """

    def __init__(self, remote_url, root_dir, repo_name, resource_name, offline, fetch=True):
        # If no URL is found, we consider this a local only repo (but still require that it is a git repo)
        self.url = remote_url
        self.remote = self.url is not None and self.url.strip() != ""
        self.repo_dir = os.path.join(root_dir, repo_name)
        self.resource_name = resource_name
        self.offline = offline
        if self.remote and not self.offline and fetch:
            # a normal git repo with a remote
            if not git.is_working_copy(self.repo_dir):
                git.clone(src=self.repo_dir, remote=self.url)
            else:
                try:
                    git.fetch(src=self.repo_dir)
                except exceptions.SupplyError:
                    console.warn("Could not update %s. Continuing with your locally available state." % self.resource_name, logger=logger)
        else:
            if not git.is_working_copy(self.repo_dir):
                raise exceptions.SystemSetupError("[{src}] must be a git repository.\n\nPlease run:\ngit -C {src} init"
                                                  .format(src=self.repo_dir))

    def update(self, distribution_version):
        try:
            if self.remote and not self.offline:
                branch = versions.best_match(git.branches(self.repo_dir, remote=self.remote), distribution_version)
                if branch:
                    # Allow uncommitted changes iff we do not have to change the branch
                    logger.info(
                        "Checking out [%s] in [%s] for distribution version [%s]." % (branch, self.repo_dir, distribution_version))
                    git.checkout(self.repo_dir, branch=branch)
                    logger.info("Rebasing on [%s] in [%s] for distribution version [%s]." % (branch, self.repo_dir, distribution_version))
                    try:
                        git.rebase(self.repo_dir, branch=branch)
                    except exceptions.SupplyError:
                        logger.exception("Cannot rebase due to local changes in [%s]" % self.repo_dir)
                        console.warn(
                            "Local changes in [%s] prevent %s update from remote. Please commit your changes." %
                            (self.repo_dir, self.resource_name))
                    return
                else:
                    msg = "Could not find %s remotely for distribution version [%s]. Trying to find %s locally." % \
                          (self.resource_name, distribution_version, self.resource_name)
                    logger.warning(msg)
            branch = versions.best_match(git.branches(self.repo_dir, remote=False), distribution_version)
            if branch:
                logger.info("Checking out [%s] in [%s] for distribution version [%s]." % (branch, self.repo_dir, distribution_version))
                git.checkout(self.repo_dir, branch=branch)
            else:
                raise exceptions.SystemSetupError("Cannot find %s for distribution version %s" % (self.resource_name, distribution_version))
        except exceptions.SupplyError as e:
            tb = sys.exc_info()[2]
            raise exceptions.DataError("Cannot update %s in [%s] (%s)." % (self.resource_name, self.repo_dir, e.message)).with_traceback(tb)
