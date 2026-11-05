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

"""
Manage Rally's changelog fragments (see changelog.d/README.md).

  python3 changelog.py check --base origin/master   # CI: the branch adds valid fragments
  python3 changelog.py collect X.Y.Z --dry-run      # release checks: every fragment is valid and maps to a PR
  python3 changelog.py collect X.Y.Z                # release: link fragments to their PRs, then `scriv collect`
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path

try:
    import tomllib
except ImportError:  # Python 3.10
    import tomli as tomllib  # type: ignore[no-redef]

ROOT = Path(__file__).resolve().parent
FRAGMENT_DIR = "changelog.d"
PR_URL = "https://github.com/elastic/rally/pull/{number}"
# Must match the bullet in changelog.d/new_fragment.md.j2
PLACEHOLDER = "Describe the change for Rally users."

HTML_COMMENT = re.compile(r"<!--.*?-->", re.DOTALL)
HEADING = re.compile(r"^###\s+(.+?)\s*$")
BULLET = re.compile(r"^[*-]\s+(.*)$")
ALREADY_LINKED = re.compile(r"^\[#\d+\]\(")
# Squash merges end with "(#1234)", merge commits start with "Merge pull request #1234".
SQUASH_PR = re.compile(r"\(#(\d+)\)\s*$")
MERGE_PR = re.compile(r"^Merge pull request #(\d+)")


def git(*args):
    return subprocess.run(["git", *args], cwd=ROOT, check=True, capture_output=True, text=True).stdout


def categories():
    with open(ROOT / "pyproject.toml", "rb") as f:
        return tomllib.load(f)["tool"]["scriv"]["categories"]


def is_fragment(path):
    path = Path(path)
    return path.parent.name == FRAGMENT_DIR and path.suffix == ".md" and path.name != "README.md"


def all_fragments():
    return sorted(p.relative_to(ROOT) for p in (ROOT / FRAGMENT_DIR).iterdir() if is_fragment(p))


def validate(path, known_categories):
    """Return a list of problems with the fragment at ``path``; empty if it is valid."""
    text = HTML_COMMENT.sub("", (ROOT / path).read_text(encoding="utf-8"))
    problems = []
    category = None
    bullets = 0
    for line in text.splitlines():
        if not line.strip():
            continue
        if m := HEADING.match(line):
            category = m.group(1)
            if category not in known_categories:
                problems.append(f"unknown category [{category}], expected one of: {', '.join(known_categories)}")
        elif category is None:
            problems.append(f"text outside of a '### <category>' section: [{line.strip()}]")
        elif m := BULLET.match(line):
            bullets += 1
            if m.group(1).strip() == PLACEHOLDER:
                problems.append("replace the placeholder bullet with a description of your change")
    if bullets == 0:
        problems.append("no entry found: uncomment a category and add a bullet below it")
    return problems


def validate_all(paths):
    known = categories()
    ok = True
    for path in paths:
        for problem in validate(path, known):
            print(f"{path}: {problem}", file=sys.stderr)
            ok = False
    return ok


def pr_number(path):
    """Find the pull request that added ``path`` from the subject of the commit that added it to the main branch."""
    # --first-parent -m attributes the file to the merge commit rather than to a commit on the merged branch.
    subjects = git("log", "--first-parent", "-m", "--diff-filter=A", "--format=%s", "--", str(path)).splitlines()
    if not subjects:
        return None
    subject = subjects[-1]  # oldest commit adding the file
    m = SQUASH_PR.search(subject) or MERGE_PR.match(subject)
    return int(m.group(1)) if m else None


def link_bullets(text, number):
    """Prefix every top-level bullet in ``text`` with a link to PR ``number``, unless it already has one."""
    link = f"[#{number}]({PR_URL.format(number=number)})"
    lines = []
    for line in text.splitlines(keepends=True):
        m = BULLET.match(line)
        if m and not ALREADY_LINKED.match(m.group(1)):
            line = f"* {link}: {m.group(1)}\n"
        lines.append(line)
    return "".join(lines)


def cmd_check(args):
    added = git("diff", "--name-only", "--diff-filter=A", f"{args.base}...HEAD", "--", FRAGMENT_DIR).splitlines()
    fragments = [p for p in added if is_fragment(p)]
    if not fragments:
        print(
            f"No changelog fragment added in {FRAGMENT_DIR}/. Run `uv run scriv create` and fill it in "
            "(see changelog.d/README.md), or ask a maintainer to add the `skip-changelog` label.",
            file=sys.stderr,
        )
        return 1
    if not validate_all(fragments):
        return 1
    print(f"Found valid changelog fragment(s): {', '.join(fragments)}")
    return 0


def cmd_collect(args):
    fragments = all_fragments()
    if not fragments:
        print(f"No changelog fragments in {FRAGMENT_DIR}/, nothing to release.", file=sys.stderr)
        return 1
    if not validate_all(fragments):
        return 1

    numbers = {}
    for path in fragments:
        number = pr_number(path)
        if number is None:
            # Allow a manual fix: a fragment whose bullets are all linked already doesn't need a PR.
            text = HTML_COMMENT.sub("", (ROOT / path).read_text(encoding="utf-8"))
            bullets = [m.group(1) for line in text.splitlines() if (m := BULLET.match(line))]
            if all(ALREADY_LINKED.match(b) for b in bullets):
                continue
            print(f"{path}: cannot find the pull request that added this fragment; link its bullets by hand", file=sys.stderr)
            return 1
        numbers[path] = number

    if args.dry_run:
        for path in fragments:
            print(f"{path}: #{numbers[path]}" if path in numbers else f"{path}: linked by hand")
        return 0

    for path, number in numbers.items():
        fragment = ROOT / path
        fragment.write_text(link_bullets(fragment.read_text(encoding="utf-8"), number), encoding="utf-8")
    subprocess.run(["scriv", "collect", "--version", args.version], cwd=ROOT, check=True)
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    subparsers = parser.add_subparsers(dest="command", required=True)

    check = subparsers.add_parser("check", help="check that the current branch adds valid changelog fragments")
    check.add_argument("--base", default="origin/master", help="the branch to compare against (default: %(default)s)")
    check.set_defaults(func=cmd_check)

    collect = subparsers.add_parser("collect", help="link fragments to their pull requests and collect them into CHANGELOG.md")
    collect.add_argument("version", help="the version being released, e.g. 2.14.0")
    collect.add_argument("--dry-run", action="store_true", help="only validate fragments and resolve their pull requests")
    collect.set_defaults(func=cmd_collect)

    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
