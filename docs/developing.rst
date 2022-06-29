Developing Rally
================

Prerequisites
-------------

Install the following software packages:

* pyenv installed, Follow the instructions in the output of ``pyenv init`` to setup your shell and then restart it before proceeding. For more details please refer to the pyenv `installation instructions <https://github.com/pyenv/pyenv#installation>`_.
* JDK version required to build Elasticsearch. Please refer to the `build setup requirements <https://github.com/elastic/elasticsearch/blob/main/CONTRIBUTING.md#contributing-to-the-elasticsearch-codebase>`_.
* `Docker <https://docs.docker.com/install/>`_ and on Linux additionally `docker-compose <https://docs.docker.com/compose/install/>`_.
* git

Check the :doc:`installation guide </install>` for detailed installation instructions for these packages.

Rally does not support Windows and is only actively tested on macOS and Linux.

Installation Instructions for Development
-----------------------------------------

::

    git clone https://github.com/elastic/rally.git
    cd rally
    make prereq
    make install
    source .venv/bin/activate
    ./rally --help

IDE Setup
---------

Rally uses automatic code formatters. They're enforced by ``make lint`` and you can apply then by running ``make format``.

However, consider using editor integrations to do it automatically: you'll need to configure `black <https://black.readthedocs.io/en/stable/integrations/editors.html>`_ and `isort <https://github.com/PyCQA/isort/wiki/isort-Plugins>`_.

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

How to contribute code
----------------------

See the `contributors guide <https://github.com/elastic/rally/blob/master/CONTRIBUTING.md>`_. We strive to be PEP-8 compliant but don't follow it to the letter.
