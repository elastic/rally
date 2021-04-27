Installation
============

This is the detailed installation guide for Rally. If you are in a hurry you can check the :doc:`quickstart guide </quickstart>`.

Hardware Requirements
---------------------

Use an SSD on the load generator machine. If you run bulk-indexing benchmarks, Rally will read one or more data files from disk. Usually, you will configure multiple clients and each client reads a portion of the data file. To the disk this appears as a random access pattern where spinning disks perform poorly. To avoid an accidental bottleneck on client-side you should therefore use an SSD on each load generator machine.

Prerequisites
-------------

Rally does not support Windows and is only actively tested on MacOS and Linux. Install the following packages first.

.. _install_python:

Python
~~~~~~

* Python 3.8 or better available as ``python3`` on the path. Verify with: ``python3 --version``.
* Python3 header files (included in the Python3 development package).
* ``pip3`` available on the path. Verify with ``pip3 --version``.

We recommend to use `pyenv <https://github.com/pyenv/pyenv>`_ to manage installation of Python. For details refer to their `installation instructions <https://github.com/pyenv/pyenv#installation>`_ and **ensure that all of** `pyenv's prerequisites <https://github.com/pyenv/pyenv/wiki/common-build-problems#prerequisites>`_ are installed.

Once ``pyenv`` is installed, install a compatible Python version::

    # Install Python
    pyenv install {MIN_PY_VER}

    # select that version for the current user
    # see https://github.com/pyenv/pyenv/blob/master/COMMANDS.md#pyenv-global for details
    pyenv global {MIN_PY_VER}

    # Upgrade pip
    python3 -m pip install --user --upgrade pip

git
~~~

Git is not required if **all** of the following conditions are met:

* You are using Rally only as a load generator (``--pipeline=benchmark-only``) or you are referring to Elasticsearch configurations with ``--team-path``.
* You create your own tracks and refer to them with ``--track-path``.

In all other cases, Rally requires ``git 1.9`` or better. Verify with ``git --version``.

**Debian / Ubuntu**

::

    sudo apt-get install git


**Red Hat / CentOS / Amazon Linux**

::

    sudo yum install git


.. note::

   If you use RHEL, install a recent version of git via the `Red Hat Software Collections <https://www.softwarecollections.org/en/scls/rhscl/git19/>`_.

**MacOS**

``git`` is already installed on MacOS.

pbzip2
~~~~~~

It is strongly recommended to install ``pbzip2`` to speed up decompressing the corpora of Rally `standard tracks <https://github.com/elastic/rally-tracks>`_.
If you have created :doc:`custom tracks </adding_tracks>` using corpora compressed with ``gzip`` instead of ``bzip2``, it's also advisable to install ``pigz`` to speed up the process.

**Debian / Ubuntu**

::

    sudo apt-get install pbzip2

**Red Hat / CentOS / Amazon Linux**

``pbzip`` is available via the `EPEL repository <https://fedoraproject.org/wiki/EPEL#Quickstart>`_.

::

    sudo yum install pbzip2

**MacOS**

Install via `Homebrew <https://brew.sh/>`_:

::

    brew install pbzip2


JDK
~~~

A JDK is required on all machines where you want to launch Elasticsearch. If you use Rally just as a load generator to :doc:`benchmark remote clusters </recipes>`, no JDK is required. For details on how to install a JDK check your operating system's documentation pages.

To find the JDK, Rally expects the environment variable ``JAVA_HOME`` to be set on all targeted machines. To have more specific control, for example when you want to benchmark across a wide range of Elasticsearch releases, you can also set ``JAVAx_HOME`` where ``x``  is the major version of a JDK (e.g. ``JAVA8_HOME`` would point to a JDK 8 installation). Rally will then choose the highest supported JDK per version of Elasticsearch that is available.


.. note::

   If you have Rally download, install and benchmark a local copy of Elasticsearch (i.e., the `default Rally behavior <http://esrally.readthedocs.io/en/stable/quickstart.html#run-your-first-race>`_) be sure to configure the Operating System (OS) of your Rally server with the `recommended kernel settings <https://www.elastic.co/guide/en/elasticsearch/reference/master/system-config.html>`_

Optional dependencies
---------------------

S3 support is optional and can be installed using the ``s3`` extra. If you need S3 support, install ``esrally[s3]`` instead of just ``esrally``, but other than that follow the instructions below.

Installing Rally
----------------

1. Ensure ``~/.local/bin`` is in your ``$PATH``.
2. Ensure pip is the latest version: ``python3 -m pip install --user --upgrade pip``
3. Install Rally: ``python3 -m pip install --user esrally``.

VirtualEnv Install
------------------

You can also use Virtualenv to install Rally into an isolated Python environment without sudo.

1. Set up a new virtualenv environment in a directory with ``python3 -m venv .venv``
2. Activate the environment with ``source /path/to/virtualenv/.venv/bin/activate``
3. Ensure pip is the latest version: ``python3 -m pip install --upgrade pip``
    .. important::
       Omitting this step might cause the next step (Rally installation) to fail due to broken dependencies. The pip version must be at minimum ``20.3``.
4. Install Rally with ``python3 -m pip install esrally``

Whenever you want to use Rally, run the activation script (step 2 above) first.  When you are done, simply execute ``deactivate`` in the shell to exit the virtual environment.

Docker
------

Docker images of Rally can be found in `DockerHub <https://hub.docker.com/r/elastic/rally>`_.

Please refer to :doc:`Running Rally with Docker <docker/>` for detailed instructions.

.. _install_offline-install:

Offline Install
---------------

.. ifconfig:: release.endswith('.dev0')

    .. warning::

        This documentation is for the version of Rally currently under development. We do not provide offline installation packages for development versions.
        Were you looking for the `documentation of the latest stable version <//esrally.readthedocs.io/en/stable/>`_?

If you are in a corporate environment using Linux servers that do not have any access to the Internet, you can use Rally's offline installation package. Follow these steps to install Rally:

1. Install all prerequisites as documented above.
2. Download the offline installation package for the `latest release <https://github.com/elastic/rally/releases/latest>`_ and copy it to the target machine(s).
3. Decompress the installation package with ``tar -xzf esrally-dist-linux-*.tar.gz``.
4. Run the install script with ``sudo ./esrally-dist-linux-*/install.sh``.

Next Steps
----------

On the first invocation Rally creates a default configuration file which you can customize. Follow the :doc:`configuration help page </configuration>` for more guidance.
