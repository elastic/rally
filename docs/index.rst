Getting Started
===============

What is Rally?
--------------

So you want to benchmark Elasticsearch? Then Rally is for you. Rally started as an effort to help developers in the Elasticsearch development team to run benchmarks on their machines. As our users are very creative and use Elasticsearch for all kinds of things, we have to cover a broad range of different performance characteristics and to find out how Elasticsearch performs under various conditions we run different benchmarks.

Rally is build around a few assumptions:

* Everything is run on the same machine (but `we are about to change that <https://github.com/elastic/rally/issues/71>`_)
* You want to add a specific data set to an Elasticsearch index and then run benchmarking queries on it

We are continuously working to remove these restrictions. In contrast to other home-grown benchmarking scripts, we have put considerable effort in Rally to ensure the benchmarking data are reproducible.

First Time Setup
----------------

Prerequisites
~~~~~~~~~~~~~

Please ensure that the following packages are installed before installing Rally:

* Python 3.4 or better available as `python3` on the path (verify with: ``python3 --version`` which should print ``Python 3.4.0`` or higher)
* ``pip3`` available on the path (verify with ``pip3 --version``)
* JDK 8
* git 1.9 or better

Rally does not support Windows and is only actively tested on Mac OS X and Linux.

.. note::

   If you use RHEL, please ensure to install a recent version of git via the `Red Hat Software Collections <https://www.softwarecollections.org/en/scls/rhscl/git19/>`_.


Installing Rally
~~~~~~~~~~~~~~~~

Simply install Rally with pip: ``pip3 install esrally``

.. note::

   Depending on your system setup you may need to prepend this command with ``sudo``.

If you get errors during installation, it is probably due to the installation of ``psutil`` which we use to gather system metrics like CPU utilization. Please check the `installation instructions of psutil <https://github.com/giampaolo/psutil/blob/master/INSTALL.rst>`_ in this case. Keep in mind that Rally is based on Python 3 and you need to install the Python 3 header files instead of the Python 2 header files on Linux.

Non-sudo Install
~~~~~~~~~~~~~~~~

If you don't want to use ``sudo`` when installing Rally, installation is still possible but a little more involved:

1. Specify the ``--user`` option when installing Rally (step 2 above), so the command to be issued is: ``python3 setup.py develop --user``.
2. Check the output of the install script or lookup the `Python documentation on the variable site.USER_BASE <https://docs.python.org/3.5/library/site.html#site.USER_BASE>`_ to find out where the script is located. On Linux, this is typically ``~/.local/bin``.

You can now either add ``~/.local/bin`` to your path or invoke Rally via ``~/.local/bin/esrally`` instead of just ``esrally``.

VirtualEnv Install
~~~~~~~~~~~~~~~~~~

You can also use Virtualenv to install Rally into an isolated Python environment without sudo.

1. Set up a new virtualenv environment in a directory with ``virtualenv --python=python3``.
2. Activate the environment with ``/path/to/virtualenv/dir/bin/activate``.
3. Install Rally with ``pip install esrally``

Whenever you want to use Rally, run the activation script (step 2 above) first.  When you are done, simply execute ``deactivate`` in the shell to exit the virtual environment.


Configuring Rally
-----------------

Before we can run our first benchmark, we have to configure Rally. Just invoke ``esrally configure`` and Rally will automatically detect that its configuration file is missing and prompt you for some values and write them to `~/.rally/rally.ini`. After you've configured Rally, it will exit.

.. note::

   If you get the error ``UnicodeEncodeError: 'ascii' codec can't encode character``, please configure your shell so it supports UTF-8 encoding. You can check the output of ``locale`` which should show UTF-8 as sole encoding. If in doubt, add ``export LC_ALL=en_US.UTF-8`` to your shell init file (e.g. ``~.bashrc`` if you use Bash) and relogin.


For more information see :doc:`configuration help page </configuration>`.

Running the first benchmark
---------------------------

Now we are ready to run the first benchmark with Rally: Just invoke ``esrally``. This will start Rally with sensible defaults. It will download the necessary benchmark data, checkout the latest version of Elasticsearch, build it and finally run the benchmark.

.. note::
   If you want to benchmark a binary distribution instead of a source distribution then run ``esrally --pipeline=from-distribution --distribution-version=VERSION_NUMBER`` (``VERSION_NUMBER`` is for example ``5.0.0-alpha1`` and is identical to the version number used in the download URL)

When the benchmark is done, a summary report is written to the command line:::

   ------------------------------------------------------
       _______             __   _____
      / ____(_)___  ____ _/ /  / ___/_________  ________
     / /_  / / __ \/ __ `/ /   \__ \/ ___/ __ \/ ___/ _ \
    / __/ / / / / / /_/ / /   ___/ / /__/ /_/ / /  /  __/
   /_/   /_/_/ /_/\__,_/_/   /____/\___/\____/_/   \___/
   ------------------------------------------------------
                                                     Metric      Value
   --------------------------------------------------------  ---------
                           Min Indexing Throughput [docs/s]      19501
                        Median Indexing Throughput [docs/s]      20232
                           Max Indexing Throughput [docs/s]      21172
                                        Indexing time [min]    55.7989
                                           Merge time [min]    12.9766
                                         Refresh time [min]    5.20067
                                           Flush time [min]  0.0648667
                                  Merge throttle time [min]   0.796417
               Query latency default (50.0 percentile) [ms]    5.89058
               Query latency default (90.0 percentile) [ms]    6.71282
              Query latency default (100.0 percentile) [ms]    7.65307
    Query latency country_agg_cached (50.0 percentile) [ms]    1.70223
    Query latency country_agg_cached (90.0 percentile) [ms]    2.34819
   Query latency country_agg_cached (100.0 percentile) [ms]    3.42547
                Query latency scroll (50.0 percentile) [ms]    16.1226
                Query latency scroll (90.0 percentile) [ms]    17.2383
               Query latency scroll (100.0 percentile) [ms]    18.8419
                  Query latency term (50.0 percentile) [ms]    2.10049
                  Query latency term (90.0 percentile) [ms]    2.77537
                 Query latency term (100.0 percentile) [ms]    4.52081
                Query latency phrase (50.0 percentile) [ms]    1.82687
                Query latency phrase (90.0 percentile) [ms]    2.63714
               Query latency phrase (100.0 percentile) [ms]    5.39892
           Query latency country_agg (50.0 percentile) [ms]    112.049
           Query latency country_agg (90.0 percentile) [ms]    128.426
          Query latency country_agg (100.0 percentile) [ms]    155.989
                               Median CPU usage (index) [%]    668.025
                               Median CPU usage (stats) [%]     143.75
                              Median CPU usage (search) [%]      223.1
                                     Total Young Gen GC [s]     39.447
                                       Total Old Gen GC [s]      7.108
                                            Index size [GB]    3.25475
                                       Totally written [GB]    17.8434
                                Heap used for segments [MB]    21.7504
                              Heap used for doc values [MB]    0.16436
                                   Heap used for terms [MB]    20.0293
                                   Heap used for norms [MB]   0.105469
                                  Heap used for points [MB]   0.773487
                                  Heap used for points [MB]   0.677795
                                              Segment count        136
                        Indices Stats(90.0 percentile) [ms]    3.16053
                        Indices Stats(99.0 percentile) [ms]    5.29526
                       Indices Stats(100.0 percentile) [ms]    5.64971
                          Nodes Stats(90.0 percentile) [ms]    3.19611
                          Nodes Stats(99.0 percentile) [ms]    4.44111
                         Nodes Stats(100.0 percentile) [ms]    5.22527

.. note::
You can save this report also to a file by using ``--report-file=/path/to/your/report.md`` and write it also as CSV with ``--report-format=csv``.


Before relying too much on the numbers, please double-check that you did not introduce any bottlenecks and that your hardware is sufficient (e.g. spinning disks are not a good idea, better use SSDs). For additional insights and metrics you can activate different :doc:`telemetry devices </telemetry>` in Rally.

Also be very careful and get a deep understanding of the measurement approaches when comparing performance numbers on different OS. Sometimes certain measurements are supported only on one OS but not on another (e.g. disk I/O statistics) and different OS handle I/O differently, so often it makes no sense to directly compare the results of benchmarks run on different OSes.

Other command line flags
------------------------

Rally supports more command line flags, just run ``esrally --help`` to see what's possible or check the :doc:`command line reference </command_line_reference>`.

Contents
--------

.. toctree::
   :maxdepth: 1

   configuration
   command_line_reference
   tournament
   telemetry
   pipelines
   metrics
   adding_benchmarks
   developing

License
-------

This software is licensed under the Apache License, version 2 ("ALv2"), quoted below.

Copyright 2015-2016 Elasticsearch <https://www.elastic.co>

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
