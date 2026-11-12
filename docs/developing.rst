Developing Rally
================

Prerequisites
-------------

Install the following software packages:

* `uv <https://docs.astral.sh/uv/getting-started/installation/>`_ 
* JDK version required to build Elasticsearch. Please refer to the `build setup requirements <https://github.com/elastic/elasticsearch/blob/main/CONTRIBUTING.md#contributing-to-the-elasticsearch-codebase>`_.
* `Docker <https://docs.docker.com/install/>`_ and on Linux additionally `docker compose <https://docs.docker.com/compose/install/>`_.
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
    esrally --help

``make install`` creates a virtual environment in ``.venv`` with an editable install of Rally, so ``esrally`` and ``esrallyd`` (or ``uv run esrally``) run the code from your working tree directly. To pick up upstream changes, pull them with git as usual.

IDE Setup
---------

Rally uses automatic code formatters. You can apply them by running ``make format``.

However, consider using editor integrations to do it automatically: you'll need to configure `black <https://black.readthedocs.io/en/stable/integrations/editors.html>`_ and `isort <https://github.com/PyCQA/isort/wiki/isort-Plugins>`_.

Type hints are checked with mypy as part of ``make lint``; modules listed under ``[tool.mypy.overrides]`` in ``pyproject.toml`` must be fully annotated. ``make lint`` also fails if type annotation coverage of the ``esrally`` package drops below the threshold set in ``.pre-commit-config.yaml``; run ``pre-commit run type-coverage --all-files`` to see the current coverage.

Also consider running `pre-commit install` to run lint as part of your git commits.

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

How to contribute code
----------------------

See the `contributors guide <https://github.com/elastic/rally/blob/master/CONTRIBUTING.md>`_. We strive to be PEP-8 compliant but don't follow it to the letter.
