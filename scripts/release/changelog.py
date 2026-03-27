#!/usr/bin/env python3

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

import argparse
import os
import sys
import traceback

import github3

ORG = "elastic"
REPO = "rally"

# Path to the GitHub API token file.
CHANGELOG_TOKEN_PATH = os.path.expanduser(os.environ.get("RALLY_CHANGELOG_TOKEN_FILE", "~/.github/rally_release_changelog.token"))


def find_milestone(repo, title, state):
    for m in repo.milestones(state=state, sort="due_date"):
        if m.title == title:
            return m
    return None


def ensure_open_milestone(repo, title, dry_run=False):
    """Return an open milestone for *title*, creating one if missing (unless *dry_run*)."""
    m = find_milestone(repo, title, state="open")
    if m:
        return m

    closed = find_milestone(repo, title, state="closed")
    if closed:
        print("Milestone already closed: [%s]." % title, file=sys.stderr)
        sys.exit(2)

    if dry_run:
        print("dry-run: no open milestone [%s]; would create on GitHub in non-dry mode." % title, file=sys.stderr)
        sys.exit(2)

    m = repo.create_milestone(title, state="open")
    if not m:
        print("Failed to create a new milestone [%s]." % title, file=sys.stderr)
        sys.exit(2)

    print("Created a new milestone [%s]." % title, file=sys.stderr)
    return m


def print_category(heading, issue_list):
    if issue_list:
        print("#### %s\n" % heading, file=sys.stdout)
        for issue in issue_list:
            print_issue(issue)
        print("", file=sys.stdout)


def print_issue(issue):
    breaking_hint = " (Breaking)" if labelled(issue, label_name="breaking") else ""
    print("* [#%s](%s)%s: %s" % (str(issue.number), issue.html_url, breaking_hint, issue.title), file=sys.stdout)


def labelled(issue, label_name):
    for label in issue.labels():
        if label.name == label_name:
            return True
    return False


def is_pr(issue):
    """

    :param issue: an issue. May also be a PR.
    :return: True iff the issue is actually a PR.
    """
    return issue.html_url and "pull" in issue.html_url and issue.pull_request().merged


def pr_list(i):
    return [item for item in i if is_pr(item)]


def list_subtract(a, b):
    return [item for item in a if item not in b]


def prs(gh, milestone, with_labels, without_labels=None):
    prs_labelled = pr_list(gh.issues_on(ORG, REPO, milestone=milestone.number, state="closed", labels=with_labels))
    filtered_prs = prs_labelled
    if without_labels:
        for label in without_labels:
            prs_not_labelled = pr_list(gh.issues_on(ORG, REPO, milestone=milestone.number, state="closed", labels=label))
            filtered_prs = list_subtract(filtered_prs, prs_not_labelled)
    return filtered_prs


def main():
    parser = argparse.ArgumentParser(description="Generate Rally release changelog text from GitHub milestones.")
    parser.add_argument(
        "--dry",
        action="store_true",
        help="Do not create milestones on GitHub; fail if the milestone is missing.",
    )
    parser.add_argument("milestone", help="Milestone title (release version), e.g. 2.13.0")
    args = parser.parse_args()
    milestone_name = args.milestone
    dry_run = args.dry

    # requires a personal GitHub access token with permission `public_repo` (see https://github.com/settings/tokens)
    try:
        with open(CHANGELOG_TOKEN_PATH, encoding="utf-8") as token_file:
            token = token_file.readline().strip()
    except OSError as e:
        print("changelog.py: cannot read token file [%s]: %s" % (CHANGELOG_TOKEN_PATH, e), file=sys.stderr)
        sys.exit(1)

    gh = github3.login(token=token)
    if gh is None:
        print("changelog.py: GitHub login failed (invalid token or API error).", file=sys.stderr)
        sys.exit(1)

    rally_repo = gh.repository(ORG, REPO)

    milestone = ensure_open_milestone(rally_repo, milestone_name, dry_run=dry_run)
    if milestone.open_issues > 0:
        print("There are [%d] open issues on milestone [%s]. Aborting..." % (milestone.open_issues, milestone_name), file=sys.stderr)
        sys.exit(2)

    print("changelog.py: generating changelog for milestone [%s] (markdown on stdout)" % milestone_name, file=sys.stderr)
    print("### %s\n" % milestone_name, file=sys.stdout)

    print_category("Highlights", prs(gh, milestone, with_labels="highlight"))
    print_category(
        "Enhancements", prs(gh, milestone, with_labels="enhancement", without_labels=[":Docs", "highlight", ":internal", ":misc"])
    )
    print_category("Bug Fixes", prs(gh, milestone, with_labels="bug", without_labels=[":Docs", "highlight", ":internal", ":misc"]))
    print_category("Doc Changes", prs(gh, milestone, with_labels=":Docs", without_labels=["highlight", ":misc"]))
    print_category("Miscellaneous Changes", prs(gh, milestone, with_labels=":misc", without_labels=[":internal"]))


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        print("changelog.py: error: %s" % e, file=sys.stderr)
        sys.exit(1)
