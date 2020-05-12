### How to release Rally?

The release process in Rally is automated as much as possible.
It will automatically run all tests (including integration tests), build and upload all artifacts and update the changelog and the list of contributors.

### Example: suppose you want to release Rally 2.0.0

1. Run release checks to validate requirements mentioned in [Preconditions](#preconditions) and [the Initial setup](#initial_setup) using:

    `make release-checks release_version=2.0.0 next_version=2.0.1` before the release.

2. If 1. succeeds, run the actual release:

    `make release release_version=2.0.0 next_version=2.0.1`.

    The parameter `release_version` is the version to release and the parameter `next_version` refers to the next development version.

**NOTE**:
If you hit the error `github3.exceptions.ForbiddenError: 403 Resource protected by organization SAML enforcement. You must grant your personal token access to this organization.` visit [https://github.com/settings/tokens](https://github.com/settings/tokens) and click `Enable SSO` on your token.

### Manual Tasks

Please execute the following tasks after `make release` has finished:

* Close milestone on Github: https://github.com/elastic/rally/milestones.
* Prepare offline installation package `./scripts/offline-install.sh "${RELEASE_VERSION}"`.
* Upload offline install package to Github: https://github.com/elastic/rally/releases/edit/$RELEASE_VERSION.
* Build and publish Docker image using the CI job: Build and publish Docker image using: https://elasticsearch-ci.elastic.co/view/All/job/elastic+rally-release-docker+master specifying the $RELEASE_VERSION
* Announce on Discuss: https://discuss.elastic.co/c/annoucements.

### Preconditions

* Ensure that the master branch is checked out and your working copy is clean (run `git status`).
* Ensure that integration tests can run to completion successfully on your machine (run `make it`).
* Ensure that the associated milestone on Github contains no open tickets (otherwise the release will fail).

### Initial Setup

In order to automatically generate the changelog, setup a personal Github access token with permission `public_repo` (see https://github.com/settings/tokens). Store the token in `~/.github/rally_release_changelog.token`.

The release process requires a valid gpg key configured both locally and on GitHub. Please follow the [instructions](https://git-scm.com/book/id/v2/Git-Tools-Signing-Your-Work) to set your gpg for git.

It also requires:

* A [PyPI account](https://pypi.org/account/register/) with admin access for the esrally project in order to upload the release.
* A [Readthedocs account](https://readthedocs.org/accounts/signup/) with admin access for the esrally project in order to maintain the documentation.
* GPG is properly setup for signing the release tag for your git user (see `git config user.name` and `git config user.email`).
