import os

from esrally import exceptions
from esrally.utils import io, process


def is_working_copy(src):
    """
    Checks whether the given directory is a git working copy.
    :param src: A directory. May or may not exist.
    :return: True iff the given directory is a git working copy.
    """
    return os.path.exists(src) and os.path.exists("%s/.git" % src)


def clone(src, remote):
    io.ensure_dir(src)
    # Don't swallow subprocess output, user might need to enter credentials...
    if process.run_subprocess("git clone %s %s" % (remote, src)):
        raise exceptions.SupplyError("Could not clone from '%s' to '%s'" % (remote, src))


def fetch(src, remote="origin"):
    # Don't swallow output but silence git at least a bit... (--quiet)
    if process.run_subprocess(
            "git -C {0} fetch --quiet {1}".format(src, remote)):
        raise exceptions.SupplyError("Could not fetch source tree from '%s'" % remote)


def checkout(src_dir, branch="master"):
    if process.run_subprocess(
            "git -C {0} checkout --quiet {1}".format(src_dir, branch)):
        raise exceptions.SupplyError("Could not checkout '%s'" % branch)


def rebase(src_dir, remote="origin", branch="master"):
    checkout(src_dir, branch)
    if process.run_subprocess("git -C {0} rebase --quiet {1}/{2}".format(src_dir, remote, branch)):
        raise exceptions.SupplyError("Could not rebase on '%s'" % branch)


def pull(src_dir, remote="origin", branch="master"):
    fetch(src_dir, remote)
    rebase(src_dir, remote, branch)


def pull_ts(src_dir, ts):
    if process.run_subprocess(
            "git -C {0} fetch --quiet origin && git -C {0} checkout --quiet `git -C {0} rev-list -n 1 --before=\"{1}\" "
            "--date=iso8601 origin/master`".format(src_dir, ts)):
        raise exceptions.SupplyError("Could not fetch source tree for timestamped revision %s" % ts)


def pull_revision(src_dir, revision):
    if process.run_subprocess(
                    "git -C {0} fetch --quiet origin && git -C {0} checkout --quiet {1}".format(src_dir, revision)):
        raise exceptions.SupplyError("Could not fetch source tree for revision %s" % revision)


def head_revision(src_dir):
    return process.run_subprocess_with_output("git -C {0} rev-parse --short HEAD".format(src_dir))[0]


def branches(src_dir, remote=True):
    if remote:
        # alternatively: git for-each-ref refs/remotes/ --format='%(refname:short)'
        return _cleanup_remote_branch_names(process.run_subprocess_with_output(
                "git -C {src} for-each-ref refs/remotes/ --format='%(refname:short)'".format(src=src_dir)))
    else:
        return _cleanup_local_branch_names(
                process.run_subprocess_with_output(
                        "git -C {src} for-each-ref refs/heads/ --format='%(refname:short)'".format(src=src_dir)))


def _cleanup_remote_branch_names(branch_names):
    return [(b[b.index("/") + 1:]).strip() for b in branch_names if not b.endswith("/HEAD")]


def _cleanup_local_branch_names(branch_names):
    return [b.strip() for b in branch_names if not b.endswith("HEAD")]
