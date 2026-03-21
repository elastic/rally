Developing Rally
================

Prerequisites
-------------

Install the following software packages:

* `uv <https://docs.astral.sh/uv/getting-started/installation/>`_ 
* JDK version required to build Elasticsearch. Please refer to the `build setup requirements <https://github.com/elastic/elasticsearch/blob/main/CONTRIBUTING.md#contributing-to-the-elasticsearch-codebase>`_.
  For running Rally's integration tests (e.g. ``make it`` or ``make it_tracks_compat``), ensure your environment uses **Java 17 or 21** (recent Rally versions use Java 21 in CI). Set ``JAVA_HOME`` or ``JAVA21_HOME`` accordingly.
* `Docker <https://docs.docker.com/install/>`_ and on Linux additionally `docker-compose <https://docs.docker.com/compose/install/>`_.
* `jq <https://stedolan.github.io/jq/download/>`_
* git

Check the :doc:`installation guide </install>` for detailed installation instructions for these packages.

Rally does not support Windows and is only actively tested on macOS and Linux.

.. _dev_installation_instructions:

Installation Instructions for Development
-----------------------------------------

::

    git clone https://github.com/elastic/rally.git
    cd rally
    make install
    source .venv/bin/activate
    ./rally --help

IDE Setup
---------

Rally uses automatic code formatters. You can apply them by running ``make format``.

However, consider using editor integrations to do it automatically: you'll need to configure `black <https://black.readthedocs.io/en/stable/integrations/editors.html>`_ and `isort <https://github.com/PyCQA/isort/wiki/isort-Plugins>`_.

Also consider running `pre-commit install` to run lint as part of your git commits.

Automatic Updates
~~~~~~~~~~~~~~~~~

Rally has a built-in auto-update feature when you install it from sources. By default, it will update from the remote named ``origin``. If you want to auto-update from a different remote, provide ``--update-from-remote=YOUR_REMOTE_NAME`` as first parameter.

To work conveniently with Rally, we suggest that you add the Rally project directory to your ``PATH``. In case you use a different remote, you should also define aliases in your shell's config file, e.g.::

    alias rally='rally --update-from-remote=elastic '
    alias rallyd='rallyd --update-from-remote=elastic '

Then you can invoke Rally or the :doc:`Rally daemon </rally_daemon>` as usual and have auto-update still work.

Also note that automatic updates are disabled in the following cases:

* There are local (uncommitted) changes in the Rally project directory
* A different branch than ``master`` is checked out
* You have specified ``--skip-update`` as the first command line parameter
* You have specified ``--offline`` as a command line parameter for Rally

Configuring Rally
~~~~~~~~~~~~~~~~~

Rally creates a default configuration automatically on first run. For further configuration, see the :doc:`configuration help page </configuration>`.

Key Components of Rally
-----------------------

To get a rough understanding of Rally, it makes sense to get to know its key components:

* `Race Control`: is responsible for proper execution of the race. It sets up all components and acts as a high-level controller.
* `Mechanic`: can build and prepare a benchmark candidate for the race. It checks out the source, builds Elasticsearch, provisions and starts the cluster.
* `Track`: is a concrete benchmarking scenario, e.g. the http_logs benchmark. It defines the data set to use.
* `Challenge`: is the specification on what benchmarks should be run and its configuration (e.g. index, then run a search benchmark with 1000 iterations)
* `Car`: is a concrete system configuration for a benchmark, e.g. an Elasticsearch single-node cluster with default settings.
* `Driver`: drives the race, i.e. it is executing the benchmark according to the track specification.
* `Reporter`: A reporter tells us how the race went (currently only after the fact).

There is a dedicated :doc:`tutorial on how to add new tracks to Rally</adding_tracks>`.

.. _dev_preparing_a_release:

Preparing a release
-------------------

The script ``scripts/release/prepare.sh`` automates steps before opening a release pull request: it rebuilds ``NOTICE.txt``, refreshes ``AUTHORS``, prepends ``CHANGELOG.md`` from a GitHub milestone (via ``scripts/release/changelog.py``), writes ``esrally/_version.py``, creates a git commit, and runs ``pip install --editable .`` to assert the reported ``esrally`` version.

You need:

* A milestone on ``elastic/rally`` titled exactly like the version argument (e.g. ``2.13.0``). ``scripts/release/changelog.py`` uses an open milestone with that title if one exists; otherwise it reopens a closed milestone with the same title or creates a new open milestone. Assign merged PRs to the milestone so the generated changelog sections are populated.
* A token file at ``~/.github/rally_release_changelog.token`` for the GitHub API (see ``scripts/release/changelog.py``). The token must allow creating or updating milestones when none is open.

Creating a fine-grained personal access token
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``scripts/release/changelog.py`` uses the GitHub API to list closed issues and pull requests on a milestone and to **create or reopen** milestones when needed (milestones are part of the Issues API).

Follow GitHub's `fine-grained personal access token documentation <https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens#creating-a-fine-grained-personal-access-token>`_ for the latest UI; the outline below matches the typical flow:

#. In GitHub: **Settings → Developer settings → Personal access tokens → Fine-grained tokens → Generate new token**.
#. Set a name and expiration (recommend a short lifetime such as 7 days for release-only tokens).
#. **Resource owner:** your GitHub user (the usual case).
#. **Repository access:** **Only select repositories** → choose **elastic/rally**. Your account must have a role on that repository that can manage milestones.
#. **Repository permissions:** under **Issues**, set **Read and write**. GitHub maps milestone create/update and milestone-scoped issue queries to the Issues permission on fine-grained tokens.
#. Generate the token and copy it when shown; it is displayed only once.

If the **elastic** organization enforces SAML SSO, open the token on GitHub after creation and use **Configure SSO** / **Authorize** for **elastic**. Without that step, the API can return 403 even when permissions are correct.

Store the token as a single line in ``~/.github/rally_release_changelog.token``, or point ``RALLY_CHANGELOG_TOKEN`` at a file containing the token (see ``scripts/release/prepare-docker.sh``).

A **classic** personal access token with the **public_repo** scope (public repositories only) or the **repo** scope can also work, as noted in ``scripts/release/changelog.py``; fine-grained tokens with Issues **Read and write** are preferred for least privilege.

From a clean working tree (after staging intended changes), run::

    ./scripts/release/prepare.sh X.Y.Z

**Makefile:** ``make release RELEASE_VERSION=X.Y.Z`` runs ``clean``, ``install``, ``docs``, ``lint``, ``test``, and ``release-checks``, then on Linux runs ``scripts/release/prepare.sh``; on non-Linux it runs ``scripts/release/prepare-docker.sh`` instead (same prerequisite steps).

**Docker (maintainers):** ``scripts/release/prepare-docker.sh`` builds the image defined in ``scripts/release/Dockerfile`` (Python 3.13, uv, jq, git, compilers) and runs ``make release`` inside a container with the repository bind-mounted. By default ``DOCKER_USER`` is the host UID and GID (from ``id -u`` and ``id -g``). It runs ``make pre-commit`` on the **host** first; the bump commit inside the container uses ``git commit --no-verify`` (via ``PREPARE_RELEASE_NO_VERIFY``) so hooks are not run twice. Optional environment variables and behavior are documented in the script header (e.g. ``RALLY_CHANGELOG_TOKEN``, ``RALLY_GITCONFIG``, ``DOCKER_IMAGE``, ``DOCKER_USER``, ``RALLY_PREPARE_RELEASE_SKIP_HOST_PRE_COMMIT``). This is separate from the published ``elastic/rally`` benchmark image; see :doc:`docker` for the latter.

How to contribute code
----------------------

See the `contributors guide <https://github.com/elastic/rally/blob/master/CONTRIBUTING.md>`_. We strive to be PEP-8 compliant but don't follow it to the letter.
