Installation
============

This is the detailed installation guide for Rally. If you are in a hurry you can check the :doc:`quickstart guide </quickstart>`.

Installing Rally
----------------

Rally does not support Windows and is only actively tested on macOS and Linux. Check the :ref:`requirements <install_requirements>` below first.

Install Rally by running the installation script as a regular user::

    curl -sSf https://raw.githubusercontent.com/elastic/rally/master/scripts/install.sh | bash

The script:

1. Downloads a fixed uv version with `uv's standalone installer <https://docs.astral.sh/uv/getting-started/installation/>`__ into a temporary directory, which it deletes when it is done. It does this even if uv is installed, so that neither an existing uv installation nor your shell configuration is changed.
2. Downloads the `esrally package <https://pypi.org/project/esrally/>`_ from PyPI and reads the constraints that ship in it, so the Rally version and the constraints always come from the same release. It refuses releases that do not ship them instead of installing unpinned dependencies.
3. Runs ``uv tool install`` with Python 3.13 and these constraints, which puts ``esrally`` into ``~/.local/bin``. If that directory is not on your ``PATH``, the script prints the command that adds it to your shell configuration. If Python 3.13 is not installed on your system, it will download it.

The script refuses to run as root, because Elasticsearch cannot be started as root.

Verify the installation with ``esrally --version``.

.. _install_optional_features:

Optional features
~~~~~~~~~~~~~~~~~

Some features need optional extras of the ``esrally`` package, which the installation script installs with ``--extra``, for example ``bash -s -- --extra s3``. Repeat ``--extra`` for several extras. Their dependencies are pinned like Rally's. The extras are:

* ``s3``: download track data from a private AWS S3 bucket.
* ``otlp``: convert :doc:`OTLP JSON corpora </otlp_ingest>` to binary protobuf.

Upgrading and uninstalling
~~~~~~~~~~~~~~~~~~~~~~~~~~

To upgrade Rally, run the installation script again, with the same ``--extra`` options: it installs the latest release together with the dependency versions of that release. Without them, it removes the extras. If you have uv yourself, don't use ``uv tool upgrade esrally``, which keeps Rally at its pinned version.

To uninstall Rally, run the script with ``--uninstall``::

    curl -sSf https://raw.githubusercontent.com/elastic/rally/master/scripts/install.sh | bash -s -- --uninstall

This keeps Rally's configuration and data in ``~/.rally``.

.. _install_requirements:

Requirements
------------

.. _install_python:

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

* No JDK is needed by default: Rally runs Elasticsearch with the JDK that is bundled with it. You only need to install a JDK to run Elasticsearch on a different one with :ref:`--runtime-jdk <clr_runtime_jdk>`, or to build Elasticsearch from sources. In both cases it must be `supported by the Elasticsearch version <https://www.elastic.co/support/matrix#matrix_jvm>`_ you benchmark, and Rally finds it via ``JAVAx_HOME``, where ``x`` is the major version of the JDK (e.g. ``JAVA21_HOME``), or ``JAVA_HOME``.
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
