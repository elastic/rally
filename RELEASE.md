### How to release Rally?

The release process in Rally is automated as much as possible. Suppose you want to release Rally 0.9.3. Then just run `make release release_version=0.9.2 next_version=0.9.3`. The parameter `release_version` refers to the release that is being released and the parameter `next_version` refers to the next development version. 

This will automatically run all tests (including integration tests), build and upload all artifacts and update the changelog and the list of contributors.

It is recommended to run `make release-checks release_version=0.9.2 next_version=0.9.3` before the release. The script will check for the requirements mentioned in [Preconditions](#preconditions) and [the Initial setup](#initial_setup).

### Manual Tasks

* Close milestone on Github: https://github.com/elastic/rally/milestones.
* Prepare offline installation package `source scripts/offline-install.sh "${RELEASE_VERSION}"`.
* Upload offline install package to Github: https://github.com/elastic/rally/releases/edit/$RELEASE_VERSION.
* Announce on Discuss: https://discuss.elastic.co/c/annoucements.


### Preconditions

* Ensure that the master branch is checked out and your working copy is clean (run `git status`).
* Ensure that the associated milestone on Github contains no open tickets (otherwise the release will fail).

### Initial Setup

```
pip3 install twine sphinx sphinx_rtd_theme
# to automatically generate the changelog
pip3 install --pre github3.py
```

In order to automatically generate the changelog, setup a personal Github access token with permission `public_repo` (see https://github.com/settings/tokens). Store the token in `~/.github/rally_release_changelog.token`.

The release process requires a valid gpg key configured both locally and on GitHub. Please follow the [instructions](https://git-scm.com/book/id/v2/Git-Tools-Signing-Your-Work) to set your gpg for git.

It also requires:

* A [PyPI account](https://pypi.org/account/register/) with admin access for the esrally project in order to upload the release.
* A [Readthedocs account](https://readthedocs.org/accounts/signup/) with admin access for the esrally project in order to maintain the documentation.
* GPG is properly setup for signing the release tag for your git user (see `git config user.name` and `git config user.email`).
