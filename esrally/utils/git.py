# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging
import os
import shutil
import time

from esrally import exceptions
from esrally.utils import io, process

GIT_TIMEOUT = 600
# Number of attempts for network-bound git operations (clone, fetch) before giving up.
GIT_RETRIES = 3
# Base delay for exponential backoff between retries, in seconds (waits ~2s, then ~4s).
GIT_RETRY_BACKOFF_SECONDS = 2


def _with_retries(operation, *, on_retry=None):
    """
    Runs ``operation`` and retries it on ``SupplyError`` up to ``GIT_RETRIES`` times,
    sleeping with exponential backoff between attempts. This is intended for network-bound git
    operations that may hang or fail transiently.

    :param operation: A zero-argument callable that performs the git operation and raises
      ``exceptions.SupplyError`` on failure.
    :param on_retry: An optional zero-argument callable invoked between attempts (e.g. to clean up
      partial state) but not after the final, failing attempt.
    """
    logger = logging.getLogger(__name__)
    for attempt in range(1, GIT_RETRIES + 1):
        try:
            return operation()
        except exceptions.SupplyError:
            if attempt == GIT_RETRIES:
                raise
            wait = GIT_RETRY_BACKOFF_SECONDS * 2 ** (attempt - 1)
            logger.warning("git operation failed (attempt [%d/%d]); retrying in [%d]s.", attempt, GIT_RETRIES, wait)
            if on_retry is not None:
                on_retry()
            time.sleep(wait)


def probed(f):
    def probe(src, *args, **kwargs):
        # Probe for -C
        if not process.exit_status_as_bool(
            lambda: process.run_subprocess_with_logging(
                f"git -C {io.escape_path(src)} --version", level=logging.DEBUG, timeout=GIT_TIMEOUT
            ),
            quiet=True,
        ):
            version = process.run_subprocess_with_output("git --version")
            if version:
                version = str(version).strip()
            else:
                version = "Unknown"
            raise exceptions.SystemSetupError("Your git version is [%s] but Rally requires at least git 1.9. Please update git." % version)
        return f(src, *args, **kwargs)

    return probe


def is_working_copy(src):
    """
    Checks whether the given directory is a git working copy.
    :param src: A directory. May or may not exist.
    :return: True iff the given directory is a git working copy.
    """
    return os.path.exists(src) and os.path.exists(os.path.join(src, ".git"))


@probed
def is_branch(src_dir, identifier):
    show_ref_cmd = f"git -C {src_dir} show-ref {identifier}"
    completed_process = process.run_subprocess_with_logging_and_output(show_ref_cmd)

    # if we get an non-zero exit code, we know that the identifier is not a branch (local or remote)
    if not process.exit_status_as_bool(lambda: completed_process.returncode):
        return False

    # it's possible the identifier could be a tag, so we explicitly check that here
    ref = completed_process.stdout.split("\n")
    if "refs/tags" in ref[0]:
        return False

    return True


def clone(src, *, remote):
    def _clone():
        io.ensure_dir(src)
        # Don't swallow subprocess output, user might need to enter credentials...
        if process.run_subprocess_with_logging("git clone %s %s" % (remote, io.escape_path(src)), timeout=GIT_TIMEOUT):
            raise exceptions.SupplyError("Could not clone from [%s] to [%s]" % (remote, src))

    # A timed-out or failed clone could leave a partial directory behind, remove it so the retry starts clean.
    _with_retries(_clone, on_retry=lambda: shutil.rmtree(src, ignore_errors=True))


@probed
def fetch(src, *, remote):
    def _fetch():
        if process.run_subprocess_with_logging(f"git -C {io.escape_path(src)} fetch --prune --tags {remote}", timeout=GIT_TIMEOUT):
            raise exceptions.SupplyError("Could not fetch source tree from [%s]" % remote)

    _with_retries(_fetch)


@probed
def checkout(src_dir, *, branch):
    if process.run_subprocess_with_logging(f"git -C {io.escape_path(src_dir)} checkout {branch}", timeout=GIT_TIMEOUT):
        raise exceptions.SupplyError("Could not checkout [%s]. Do you have uncommitted changes?" % branch)


@probed
def checkout_branch(src_dir, remote, branch):
    if process.run_subprocess_with_logging(f"git -C {io.escape_path(src_dir)} checkout {remote}/{branch}", timeout=GIT_TIMEOUT):
        raise exceptions.SupplyError("Could not checkout [%s]. Do you have uncommitted changes?" % branch)


@probed
def rebase(src_dir, *, remote, branch):
    checkout(src_dir, branch=branch)
    if process.run_subprocess_with_logging(f"git -C {io.escape_path(src_dir)} rebase {remote}/{branch}", timeout=GIT_TIMEOUT):
        raise exceptions.SupplyError("Could not rebase on branch [%s]" % branch)


@probed
def pull(src_dir, *, remote, branch):
    fetch(src_dir, remote=remote)
    rebase(src_dir, remote=remote, branch=branch)


@probed
def pull_ts(src_dir, ts, *, remote, branch, default_branch):
    fetch(src_dir, remote=remote)
    clean_src = io.escape_path(src_dir)
    # non-default ES branches might receive merges from default ES branch which we want to filter out
    if branch != default_branch:
        rev_list_command = f'git -C {clean_src} rev-list -n 1 --before="{ts}" --date=iso8601 {remote}/{default_branch}..{remote}/{branch}'
    else:
        rev_list_command = f'git -C {clean_src} rev-list -n 1 --before="{ts}" --date=iso8601 {remote}/{branch}'
    revision = process.run_subprocess_with_output(rev_list_command)[0].strip()
    if process.run_subprocess_with_logging(f"git -C {clean_src} checkout {revision}", timeout=GIT_TIMEOUT):
        raise exceptions.SupplyError("Could not checkout source tree for timestamped revision [%s]" % ts)


@probed
def checkout_revision(src_dir, *, revision):
    if process.run_subprocess_with_logging(f"git -C {io.escape_path(src_dir)} checkout {revision}", timeout=GIT_TIMEOUT):
        raise exceptions.SupplyError("Could not checkout source tree for revision [%s]" % revision)


@probed
def head_revision(src_dir):
    return process.run_subprocess_with_output(f"git -C {io.escape_path(src_dir)} rev-parse HEAD")[0].strip()


@probed
def current_branch(src_dir):
    return process.run_subprocess_with_output(f"git -C {io.escape_path(src_dir)} rev-parse --abbrev-ref HEAD")[0].strip()


@probed
def branches(src_dir, remote=True):
    clean_src = io.escape_path(src_dir)
    if remote:
        # alternatively: git for-each-ref refs/remotes/ --format='%(refname:short)'
        return _cleanup_remote_branch_names(
            process.run_subprocess_with_output(f"git -C {clean_src} for-each-ref refs/remotes/ --format='%(refname:short)'")
        )
    else:
        return _cleanup_local_branch_names(
            process.run_subprocess_with_output(f"git -C {clean_src} for-each-ref refs/heads/ --format='%(refname:short)'")
        )


@probed
def tags(src_dir):
    return _cleanup_tag_names(process.run_subprocess_with_output(f"git -C {io.escape_path(src_dir)} tag"))


def _cleanup_remote_branch_names(refs):
    branches = []
    for ref in refs:
        # git >= 2.40.0 reports an `origin` ref without a slash while previous versions
        # reported a `origin/HEAD` ref.
        if "/" in ref and not ref.endswith("/HEAD"):
            branches.append(ref[ref.index("/") + 1 :].strip())
    return branches


def _cleanup_local_branch_names(refs):
    return [ref.strip() for ref in refs if not ref.endswith("HEAD")]


def _cleanup_tag_names(tag_names):
    return [t.strip() for t in tag_names]
