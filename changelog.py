#!/usr/bin/env python3

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

# requires pip3 install --pre github3.py

import os
import sys

import github3

ORG = "elastic"
REPO = "rally"


def find_milestone(repo, title):
    for m in repo.milestones(state="open", sort="due_date"):
        if m.title == title:
            return m
    return None


def print_category(heading, issue_list):
    if issue_list:
        print("#### %s\n" % heading)
        for issue in issue_list:
            print_issue(issue)
        print("")


def print_issue(issue):
    breaking_hint = " (Breaking)" if labelled(issue, label_name="breaking") else ""
    print("* [#%s](%s)%s: %s" % (str(issue.number), issue.html_url, breaking_hint, issue.title))


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
    return issue.html_url and "pull" in issue.html_url


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
    if len(sys.argv) != 2:
        print("usage: %s milestone" % sys.argv[0], file=sys.stderr)
        exit(1)

    milestone_name = sys.argv[1]

    # requires a personal Github access token with permission `public_repo` (see https://github.com/settings/tokens)
    gh = github3.login(token=open("%s/.github/rally_release_changelog.token" % os.getenv("HOME"), "r").readline().strip())

    rally_repo = gh.repository(ORG, REPO)

    milestone = find_milestone(rally_repo, title=milestone_name)
    if not milestone:
        print("No open milestone named [%s] found." % milestone_name, file=sys.stderr)
        exit(2)
    if milestone.open_issues > 0:
        print("There are [%d] open issues on milestone [%s]. Aborting..." % (milestone.open_issues, milestone_name), file=sys.stderr)
        exit(2)

    print("### %s\n" % milestone_name)

    print_category("Highlights", prs(gh, milestone, with_labels="highlight"))
    print_category("Enhancements", prs(gh, milestone, with_labels="enhancement", without_labels=[":Docs", "highlight", ":internal", ":misc"]))
    print_category("Bug Fixes", prs(gh, milestone, with_labels="bug", without_labels=[":Docs", "highlight", ":internal", ":misc"]))
    print_category("Doc Changes", prs(gh, milestone, with_labels=":Docs", without_labels=["highlight", ":misc"]))
    print_category("Miscellaneous Changes", prs(gh, milestone, with_labels=":misc", without_labels=[":internal"]))


if __name__ == '__main__':
    main()
