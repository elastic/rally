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

Also consider running ``make install-git-hooks`` to point git at ``scripts/githooks`` so lint runs on each commit (same as ``make pre-commit`` via the hook there).

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

Elastic maintainers should follow the **Rally Release Process** runbook on Codex:
`Rally Release Process <https://codex.elastic.dev/r/elasticsearch-team/teams/performance/runbooks/rally-release-process>`_.
It covers GitHub tokens, milestones, ``make release`` / ``scripts/release/prepare.sh``, tagging, PyPI, and post-release tasks.

The ``make release`` target does **not** run ``clean``, ``install``, ``docs``, ``lint``, or ``test``. Before opening the release pull request, run the validation your team expects (for example ``make check-all`` and ``make release-checks RELEASE_VERSION=X.Y.Z``). Platform differences for ``release-checks`` (GPG, ``origin``, skipped checks on macOS / in Docker) live in ``scripts/release/checks.sh``. Run ``make pre-commit`` on the host before releasing if you want hooks to run before the version bump; the bump commit inside the container skips hooks by design (see the header comment in ``scripts/release/prepare-docker.sh``).

Docker bind mounts, optional environment variables, the Python base image for the release prep container, and ``.dockerignore`` are documented in the header comment of ``scripts/release/prepare-docker.sh``, in ``scripts/release/Dockerfile``, and under **Release preparation image (maintainers)** in :doc:`docker`. Changelog and milestone logic live in ``scripts/release/changelog.py``.

How to contribute code
----------------------

See the `contributors guide <https://github.com/elastic/rally/blob/master/CONTRIBUTING.md>`_. We strive to be PEP-8 compliant but don't follow it to the letter.
