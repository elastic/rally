Installation
============

This is the detailed installation guide for Rally. If you are in a hurry you can check the :doc:`quickstart guide </quickstart>`.

Installing Rally
----------------

Rally does not support Windows and is only actively tested on macOS and Linux. Check the :ref:`requirements <install_requirements>` below first.

The recommended way to install Rally is as a standalone tool with `uv <https://docs.astral.sh/uv/>`_ (see their `installation instructions <https://docs.astral.sh/uv/getting-started/installation/>`_)::

    uv tool install --python 3.13 esrally

uv installs Rally into its own isolated environment, downloading Python 3.13 if needed. If ``esrally`` is not found afterwards, run ``uv tool update-shell`` and restart your shell. `pipx <https://pipx.pypa.io/>`_ works the same way: ``pipx install esrally`` (and ``pipx ensurepath``).

Alternatively, install Rally into a virtual environment::

    python3 -m venv .venv
    source .venv/bin/activate
    python3 -m pip install esrally

Run ``source .venv/bin/activate`` whenever you want to use Rally, and ``deactivate`` when you are done.

.. note::

   Avoid ``pip install --user`` or ``sudo pip install``. Many recent Linux distributions and Homebrew prevent installing packages into the system Python (`PEP 668 <https://peps.python.org/pep-0668/>`_).

Verify the installation with ``esrally --version``.

.. _install_uv:

Installing with pinned dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The commands above install the newest dependency versions that Rally allows. To install exactly the dependency versions that the Rally release was tested with instead (those from Rally's ``uv.lock``, shipped inside every release as ``rally-constraints.txt``), run the installation script as a regular user. It needs neither uv nor Python on your machine::

    curl -sSf https://raw.githubusercontent.com/elastic/rally/master/scripts/install.sh | bash

It installs the latest release on PyPI. To install a specific release, pass ``--version`` after ``bash -s --``, e.g. ``bash -s -- --version 2.14.0``. If you prefer to read the script before running it, download it with ``curl -sSf -o install.sh <url>`` and run ``bash install.sh``.

The script:

1. Downloads a fixed uv version with `uv's standalone installer <https://docs.astral.sh/uv/getting-started/installation/>`__ into a temporary directory, which it deletes when it is done. It does this even if uv is installed, so that neither an existing uv installation nor your shell configuration is changed.
2. Downloads the Rally release from PyPI and reads the constraints that ship in it, so the Rally version and the constraints always come from the same release. It refuses releases that do not ship them instead of installing unpinned dependencies.
3. Runs ``uv tool install`` with Python 3.13 and these constraints, which puts ``esrally`` into ``~/.local/bin``. If that directory is not on your ``PATH``, the script prints the command that adds it to your shell configuration.

The script refuses to run as root, because Elasticsearch cannot be started as root, and does not install the optional extras below. To uninstall Rally, run it with ``--uninstall``: this keeps Rally's configuration and data in ``~/.rally``.

Optional features
~~~~~~~~~~~~~~~~~

Some features need extra dependencies, which you can install with `extras <https://packaging.python.org/en/latest/specifications/dependency-specifiers/#extras>`_, e.g. ``uv tool install --python 3.13 'esrally[s3]'``:

* ``s3``: download track data from a private AWS S3 bucket.
* ``otlp``: convert :doc:`OTLP JSON corpora </otlp_ingest>` to binary protobuf.

Upgrading
~~~~~~~~~

Run ``uv tool upgrade esrally``, ``pipx upgrade esrally`` or, in a virtual environment, ``python3 -m pip install --upgrade esrally``. If you used the :ref:`installation script <install_uv>`, run it again instead: ``uv tool upgrade esrally`` would keep Rally at its pinned version.

.. _install_requirements:

Requirements
------------

.. _install_python:

Python
~~~~~~

Rally requires Python 3.10 or above. Verify with ``python3 --version``, or pass ``--python`` to ``uv tool install`` as shown above.

git
~~~

Rally requires git 1.9 or later (verify with ``git --version``) to fetch tracks and Elasticsearch configurations. You don't need git if you only use your own tracks (``--track-path``) and either benchmark an existing cluster (``--pipeline=benchmark-only``) or refer to Elasticsearch configurations with ``--team-path``.

Faster decompression (optional)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Install ``pbzip2``, ``pigz`` and ``zstd`` with your package manager to speed up decompressing track corpora. Rally uses them for ``.bz2``, ``.gz`` and ``.zst`` files respectively when they are available, and falls back to Python's built-in decompression otherwise. The corpora of the `standard tracks <https://github.com/elastic/rally-tracks>`_ are compressed with ``bzip2``, so ``pbzip2`` makes the biggest difference.

Hardware
~~~~~~~~

Use an SSD on each load generator machine. In bulk-indexing benchmarks, multiple clients read different parts of the data files concurrently, and spinning disks handle this random access pattern poorly.

Running Elasticsearch with Rally
--------------------------------

These requirements only apply to machines where Rally provisions Elasticsearch. If you use Rally only as a load generator to :doc:`benchmark an existing cluster </recipes>`, you can skip them.

* A JDK that is `supported by the Elasticsearch version <https://www.elastic.co/support/matrix#matrix_jvm>`_ you benchmark. Rally finds it via the ``JAVA_HOME`` environment variable. To benchmark a wide range of Elasticsearch releases, you can also set ``JAVAx_HOME``, where ``x`` is the major version of a JDK (e.g. ``JAVA17_HOME``). Rally then chooses the highest JDK version that the benchmarked Elasticsearch version supports.
* The `recommended system settings <https://www.elastic.co/guide/en/elasticsearch/reference/current/system-config.html>`_ for Elasticsearch.

Docker
------

Docker images of Rally are available on `Docker Hub <https://hub.docker.com/r/elastic/rally>`_. See :doc:`Running Rally with Docker </docker>` for details.

Offline Install
---------------

For machines without Internet access, Rally provides an offline installation package. See :ref:`offline installation <offline_install>`.

Next Steps
----------

On the first invocation Rally creates a default configuration file which you can customize. Follow the :doc:`configuration help page </configuration>` for more guidance.
