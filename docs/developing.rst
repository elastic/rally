Developing Rally
================

Prerequisites
-------------

Please ensure that the following packages are installed before installing Rally in development mode:

* Python 3.4 or better available as `python3` on the path (verify with: ``python3 --version`` which should print ``Python 3.4.0`` (or higher))
* ``pip3`` available on the path (verify with ``pip3 --version``)
* JDK 8
* git 1.9 or better
* Gradle 3.3 or better

Rally does not support Windows and is only actively tested on Mac OS X and Linux.

Installation Instructions for Development
-----------------------------------------

::

    git clone https://github.com/elastic/rally.git
    cd rally
    ./rally


If you get errors during installation, it is probably due to the installation of ``psutil`` which we use to gather system metrics like CPU utilization. Please check the `installation instructions of psutil <https://github.com/giampaolo/psutil/blob/master/INSTALL.rst>`_ in this case. Keep in mind that Rally is based on Python 3 and you need to install the Python 3 header files instead of the Python 2 header files on Linux.

Configuring Rally
~~~~~~~~~~~~~~~~~

Before we can run our first benchmark, we have to configure Rally. Just invoke ``./rally configure`` and Rally will automatically detect that its configuration file is missing and prompt you for some values and write them to ``~/.rally/rally.ini``. After you've configured Rally, it will exit.

For more information see :doc:`configuration help page </configuration>`.

Key Components of Rally
-----------------------

To get a rough understanding of Rally, it makes sense to get to know its key components:

* `Race Control`: is responsible for proper execution of the race. It sets up all components and acts as a high-level controller.
* `Mechanic`: can build and prepare a benchmark candidate for the race. It checks out the source, builds Elasticsearch, provisions and starts the cluster.
* `Track`: is a concrete benchmarking scenario, e.g. the logging benchmark. It defines the data set to use.
* `Challenge`: is the specification on what benchmarks should be run and its configuration (e.g. index, then run a search benchmark with 1000 iterations)
* `Car`: is a concrete system configuration for a benchmark, e.g. an Elasticsearch single-node cluster with default settings.
* `Driver`: drives the race, i.e. it is executing the benchmark according to the track specification.
* `Reporter`: A reporter tells us how the race went (currently only after the fact).

There is a dedicated :doc:`tutorial on how to add new tracks to Rally</adding_tracks>`.

How to contribute code
----------------------

First of all, please read the `contributors guide <https://github.com/elastic/rally/blob/master/CONTRIBUTING.md>`_.

We strive to be PEP-8 compliant but don't follow it to the letter.