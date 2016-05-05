Developing Rally
================

Prerequisites
-------------

Please ensure that the following packages are installed before installing Rally in development mode:

* Python 3.4+ available as `python3` on the path (verify with: ``python3 --version`` which should print ``Python 3.4.0`` (or higher))
* ``pip3`` available on the path (verify with ``pip3 --version``)
* JDK 8+
* Gradle 2.8+
* git
* unzip (install via ``apt-get install unzip`` on  Debian based distributions or check your distribution's documentation)
* Elasticsearch: Rally stores its metrics in a dedicated Elasticsearch instance. If you don't want to set it up yourself you can also use `Elastic Cloud <https://www.elastic.co/cloud>`_.
* Optional: Kibana (also included in `Elastic Cloud <https://www.elastic.co/cloud>`_).

Rally does not support Windows and is only actively tested on Mac OS X and Linux.

Installation Instructions for Development
-----------------------------------------

::

    # Clone the repo
    git clone https://github.com/elastic/rally.git
    cd rally
    # Install Rally in development mode, may require sudo. For detailed instructions, please see below
    python3 setup.py develop


If you get errors during installation, it is probably due to the installation of ``psutil`` which we use to gather system metrics like CPU utilization. Please check the `installation instructions of psutil <https://github.com/giampaolo/psutil/blob/master/INSTALL.rst>` in this case. Keep in mind that Rally is based on Python 3 and you need to install the Python 3 header files instead of the Python 2 header files on Linux.

Non-sudo Install
~~~~~~~~~~~~~~~~

If you don't want to use ``sudo`` when installing Rally, installation is still possible but a little more involved:

1. Specify the ``--user`` option when installing Rally (step 2 above), so the command to be issued is: ``python3 setup.py develop --user``.
2. Check the output of the install script or lookup the `Python documentation on the variable site.USER_BASE <https://docs.python.org/3.5/library/site.html#site.USER_BASE>`_ to find out where the script is located. On Linux, this is typically ``~/.local/bin``.

You can now either add ``~/.local/bin`` to your path or invoke Rally via ``~/.local/bin/esrally`` instead of just ``esrally``.


Key Components of Rally
-----------------------

To get a rough understanding of Rally, it makes sense to get to know its key components:

* `Race Control`: is responsible for proper execution of the race. It sets up all components and acts as a high-level controller.
* `Mechanic`: can build and prepare a benchmark candidate for the race. It checks out the source, builds Elasticsearch, provisions and starts the cluster.
* `Track`: is a concrete benchmarking scenario, e.g. the logging benchmark. It defines the data set to use.
* `TrackSetup`: is a concrete system configuration for a benchmark, e.g. Elasticsearch default settings. Note: There are some lose ends in the code due to the porting efforts. The implementation is very likely to change significantly.
* `Driver`: drives the race, i.e. it is executing the benchmark according to the track specification.
* `Reporter`: A reporter tells us how the race went (currently only after the fact).

When implementing a new benchmark, create a new file in `track` and create a new `Track` and one or more `TrackSetup` instances. 
See `track/geonames_track.py` for an example. The new track will be picked up automatically. You can run Rally with your track 
by issuing `esrally --track=your-track-name`. All available tracks can be listed with `esrally list tracks`.

How to contribute code
----------------------

First of all, please read the `contributors guide <https://github.com/elastic/rally/blob/master/CONTRIBUTING.md>`_.

We strive to be PEP-8 compliant but don't follow it to the letter.