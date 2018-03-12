### How to release Rally?

The release process in Rally is automated as much as possible. Suppose you want to release Rally 0.9.3. Then just run `make release current=0.9.3 next=0.9.4`. The parameter `current` refers to the release that is being released and the parameter `next` refers to the next development version.

This will automatically run all tests (including integration tests), build and upload all artifacts and update the changelog and the list of contributors.

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

It also requires:

* A [PyPI account](https://pypi.org/account/register/) with admin access for the esrally project in order to upload the release.
* A [Readthedocs account](https://readthedocs.org/accounts/signup/) with admin access for the esrally project in order to maintain the documentation.
* GPG is properly setup for signing the release tag for your git user (see `git config user.name` and `git config user.email`).
