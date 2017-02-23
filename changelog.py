#!/usr/bin/env python3

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


def print_category(heading, issues_list):
    if issues_list:
        print("#### %s\n" % heading)
        for issue in issues_list:
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


def is_issue(issue):
    """

    :param issue: an issue. May also be a PR.
    :return: True iff the issue is a "real" issue.
    """
    return issue.html_url and "issue" in issue.html_url


def issue_list(i):
    return [item for item in i if is_issue(item)]


def list_subtract(a, b):
    return [item for item in a if item not in b]


def issues(gh, milestone, with_labels, without_labels=None):
    issues_labelled = issue_list(gh.issues_on(ORG, REPO, milestone=milestone.number, state="closed", labels=with_labels))
    filtered_issues = issues_labelled
    if without_labels:
        for label in without_labels:
            issues_not_labelled = issue_list(gh.issues_on(ORG, REPO, milestone=milestone.number, state="closed", labels=label))
            filtered_issues = list_subtract(filtered_issues, issues_not_labelled)
    return filtered_issues


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

    print_category("Highlights", issues(gh, milestone, with_labels="highlight"))
    print_category("Enhancements", issues(gh, milestone, with_labels="enhancement", without_labels=[":Docs", "highlight"]))
    print_category("Bug Fixes", issues(gh, milestone, with_labels="bug", without_labels=[":Docs", "highlight"]))
    print_category("Doc Changes", issues(gh, milestone, with_labels=":Docs", without_labels=["highlight"]))


if __name__ == '__main__':
    main()
