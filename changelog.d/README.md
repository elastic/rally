# Changelog fragments

Every pull request that changes something a Rally user could notice adds a changelog fragment
to this directory. At release time, fragments are assembled into [CHANGELOG.md](../CHANGELOG.md)
and deleted.

## Adding a fragment

```
uv run scriv create
```

This creates a file such as `changelog.d/20261105_101500_jdoe_my_branch.md`. Edit it: uncomment
one category and write one bullet describing the change for Rally users, for example:

```markdown
### Enhancements

* Add `prepare-track` command to download and decompress a track's corpora ahead of a race.
```

You can also create the file by hand: any unique `.md` name in this directory works.

- Write for users, not reviewers: say what changed for them, not how it was implemented.
- Don't add the PR number or link. It is added at release time from the merge commit.
- Use one fragment per pull request, usually with one bullet. Several categories in one fragment
  are fine when a PR really does several things.

### Categories

| Category | Use for |
| --- | --- |
| Breaking Changes | Anything that requires users to change how they run Rally or write tracks |
| Highlights | Notable new features; maintainers may also promote entries here at release time |
| Enhancements | New features and improvements |
| Bug Fixes | Fixes for behavior users could hit |
| Doc Changes | User-facing documentation changes |
| Miscellaneous Changes | Dependency upgrades and other changes worth mentioning |

## No fragment needed

Purely internal changes (tests, CI, refactoring, dev tooling) don't need a fragment. A maintainer
adds the `skip-changelog` label to the pull request instead. Dependabot PRs are exempt
automatically.

## Release time

`python3 changelog.py collect X.Y.Z` (run by `prepare-release.sh`) finds the pull request that
added each fragment, prefixes its bullets with a link to it, and runs `scriv collect`.
