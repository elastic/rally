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
from itertools import groupby

from esrally import exceptions
from esrally.utils import io, process


def probed(f):
    def probe(src, *args, **kwargs):
        # Probe for -C
        if not process.exit_status_as_bool(
            lambda: process.run_subprocess_with_logging("git -C {} --version".format(io.escape_path(src)), level=logging.DEBUG), quiet=True
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


def is_branch(src_dir, identifier):
    def all_equal(iterable):
        "Returns True if all the elements are equal to each other"
        g = groupby(iterable)
        return next(g, True) and not next(g, False)

    name_rev_command = f"git -C {io.escape_path(src_dir)} name-rev {identifier}"

    # git name-rev returns the symbolic name for a given revision
    #
    # for example, if the identifier is a branch:
    #        $ git name-rev test-branch
    #          test-branch test-branch
    #
    # and if the identifier is a commit hash:
    #       $ git name-rev e9ff502b0c3
    #         e9ff502b0c3 test-branch~6

    names = process.run_subprocess_with_output(name_rev_command)[0].split()
    return all_equal(names)


def clone(src, *, remote):
    io.ensure_dir(src)
    # Don't swallow subprocess output, user might need to enter credentials...
    if process.run_subprocess_with_logging("git clone %s %s" % (remote, io.escape_path(src))):
        raise exceptions.SupplyError("Could not clone from [%s] to [%s]" % (remote, src))


@probed
def fetch(src, *, remote):
    if process.run_subprocess_with_logging("git -C {0} fetch --prune --tags {1}".format(io.escape_path(src), remote)):
        raise exceptions.SupplyError("Could not fetch source tree from [%s]" % remote)


@probed
def checkout(src_dir, *, branch):
    if process.run_subprocess_with_logging("git -C {0} checkout {1}".format(io.escape_path(src_dir), branch)):
        raise exceptions.SupplyError("Could not checkout [%s]. Do you have uncommitted changes?" % branch)


@probed
def checkout_remote(src_dir, remote, branch):
    if process.run_subprocess_with_logging("git -C {0} checkout {1}/{2}".format(io.escape_path(src_dir), remote, branch)):
        raise exceptions.SupplyError("Could not checkout [%s]. Do you have uncommitted changes?" % branch)


@probed
def rebase(src_dir, *, remote, branch):
    checkout(src_dir, branch=branch)
    if process.run_subprocess_with_logging("git -C {0} rebase {1}/{2}".format(io.escape_path(src_dir), remote, branch)):
        raise exceptions.SupplyError("Could not rebase on branch [%s]" % branch)


@probed
def pull(src_dir, *, remote, branch):
    fetch(src_dir, remote=remote)
    rebase(src_dir, remote=remote, branch=branch)


@probed
def pull_revision(src_dir, *, remote, revision):
    fetch(src_dir, remote=remote)
    if process.run_subprocess_with_logging("git -C {0} checkout {1}".format(io.escape_path(src_dir), revision)):
        raise exceptions.SupplyError("Could not checkout source tree for revision [%s]" % revision)


@probed
def pull_ts(src_dir, ts, *, remote, branch):
    fetch(src_dir, remote=remote)
    clean_src = io.escape_path(src_dir)
    rev_list_command = f'git -C {clean_src} rev-list -n 1 --before="{ts}" --date=iso8601 {remote}/{branch}'
    revision = process.run_subprocess_with_output(rev_list_command)[0].strip()
    if process.run_subprocess_with_logging("git -C {0} checkout {1}".format(clean_src, revision)):
        raise exceptions.SupplyError("Could not checkout source tree for timestamped revision [%s]" % ts)


@probed
def head_revision(src_dir):
    return process.run_subprocess_with_output("git -C {0} rev-parse --short HEAD".format(io.escape_path(src_dir)))[0].strip()


@probed
def current_branch(src_dir):
    return process.run_subprocess_with_output("git -C {0} rev-parse --abbrev-ref HEAD".format(io.escape_path(src_dir)))[0].strip()


@probed
def branches(src_dir, remote=True):
    clean_src = io.escape_path(src_dir)
    if remote:
        # alternatively: git for-each-ref refs/remotes/ --format='%(refname:short)'
        return _cleanup_remote_branch_names(
            process.run_subprocess_with_output("git -C {src} for-each-ref refs/remotes/ --format='%(refname:short)'".format(src=clean_src))
        )
    else:
        return _cleanup_local_branch_names(
            process.run_subprocess_with_output("git -C {src} for-each-ref refs/heads/ --format='%(refname:short)'".format(src=clean_src))
        )


@probed
def tags(src_dir):
    return _cleanup_tag_names(process.run_subprocess_with_output("git -C {0} tag".format(io.escape_path(src_dir))))


def _cleanup_remote_branch_names(branch_names):
    return [(b[b.index("/") + 1 :]).strip() for b in branch_names if not b.endswith("/HEAD")]


def _cleanup_local_branch_names(branch_names):
    return [b.strip() for b in branch_names if not b.endswith("HEAD")]


def _cleanup_tag_names(tag_names):
    return [t.strip() for t in tag_names]
