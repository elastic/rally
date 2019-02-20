# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import logging

from esrally import exceptions
from esrally.utils import io, process


def probed(f):
    def probe(src, *args, **kwargs):
        # Probe for -C
        if not process.exit_status_as_bool(lambda: process.run_subprocess_with_logging(
                "git -C {} --version".format(src), level=logging.DEBUG), quiet=True):
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


def clone(src, remote):
    io.ensure_dir(src)
    # Don't swallow subprocess output, user might need to enter credentials...
    if process.run_subprocess_with_logging("git clone %s %s" % (remote, src)):
        raise exceptions.SupplyError("Could not clone from [%s] to [%s]" % (remote, src))


@probed
def fetch(src, remote="origin"):
    # Don't swallow output but silence git at least a bit... (--quiet)
    if process.run_subprocess(
            "git -C {0} fetch --prune --quiet {1}".format(src, remote)):
        raise exceptions.SupplyError("Could not fetch source tree from [%s]" % remote)


@probed
def checkout(src_dir, branch="master"):
    if process.run_subprocess(
            "git -C {0} checkout --quiet {1}".format(src_dir, branch)):
        raise exceptions.SupplyError("Could not checkout branch [%s]. Do you have uncommitted changes?" % branch)


@probed
def rebase(src_dir, remote="origin", branch="master"):
    checkout(src_dir, branch)
    if process.run_subprocess("git -C {0} rebase --quiet {1}/{2}".format(src_dir, remote, branch)):
        raise exceptions.SupplyError("Could not rebase on branch [%s]" % branch)


@probed
def pull(src_dir, remote="origin", branch="master"):
    fetch(src_dir, remote)
    rebase(src_dir, remote, branch)


@probed
def pull_ts(src_dir, ts):
    if process.run_subprocess(
            "git -C {0} fetch --prune --quiet origin && git -C {0} checkout --quiet `git -C {0} rev-list -n 1 --before=\"{1}\" "
            "--date=iso8601 origin/master`".format(src_dir, ts)):
        raise exceptions.SupplyError("Could not fetch source tree for timestamped revision [%s]" % ts)


@probed
def pull_revision(src_dir, revision):
    if process.run_subprocess(
                    "git -C {0} fetch --prune --quiet origin && git -C {0} checkout --quiet {1}".format(src_dir, revision)):
        raise exceptions.SupplyError("Could not fetch source tree for revision [%s]" % revision)


@probed
def head_revision(src_dir):
    return process.run_subprocess_with_output("git -C {0} rev-parse --short HEAD".format(src_dir))[0].strip()


@probed
def current_branch(src_dir):
    return process.run_subprocess_with_output("git -C {0} rev-parse --abbrev-ref HEAD".format(src_dir))[0].strip()


@probed
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
