Getting Started
===============

What is Rally?
--------------

So you want to benchmark Elasticsearch? Then Rally is for you. Rally started as an effort to help developers in the Elasticsearch development team to run benchmarks on their machines. As our users are very creative and use Elasticsearch for all kinds of things, we have to cover a broad range of different performance characteristics. Rally is build around a few assumptions:

* Everything is run on the same machine (but `we are about to change that <https://github.com/elastic/rally/issues/71>`_)
* You want to add a specific data set to an Elasticsearch index and then run benchmarking queries on it

These are some of the core assumptions and we are continuously working to remove those restrictions. In contrast to other home-grown benchmarking scripts, we have put considerable effort in Rally to ensure the benchmarking data are reproducible.

First Time Setup
----------------

Prerequisites
~~~~~~~~~~~~~

Rally can build Elasticsearch either from sources or use an `official binary distribution <https://www.elastic.co/downloads/elasticsearch>`_. If you have Rally build Elasticsearch from sources, it can only be used to benchmark Elasticsearch 5.0 and above. The reason is that with Elasticsearch 5.0 the build tool was switched from Maven to Gradle. As Rally only supports Gradle, it is limited to Elasticsearch 5.0 and above.

Please ensure that the following packages are installed before installing Rally:

* Python 3.4+ available as `python3` on the path (verify with: ``python3 --version`` which should print ``Python 3.4.0`` (or higher))
* ``pip3`` available on the path (verify with ``pip3 --version``)
* JDK 8+
* unzip (install via ``apt-get install unzip`` on  Debian based distributions or check your distribution's documentation)
* Elasticsearch: Rally stores its metrics in a dedicated Elasticsearch instance. If you don't want to set it up yourself you can also use `Elastic Cloud <https://www.elastic.co/cloud>`_.
* Optional: Kibana (also included in `Elastic Cloud <https://www.elastic.co/cloud>`_).

If you want to build Elasticsearch from sources you will also need:

* Gradle 2.8+
* git

Rally does not support Windows. It may or may not work but we actively test it only on Mac OS X and Linux.

Preparation
~~~~~~~~~~~

First `install Elasticsearch <https://www.elastic.co/downloads/elasticsearch>`_ 2.3 or higher. A simple out-of-the-box installation with a single node will suffice. Rally uses this instance to store metrics data. It will setup the necessary indices by itself. The configuration procedure of Rally will you ask for host and port of this cluster.

.. note::

   Rally will choose the port range 39200-39300 (HTTP) and 39300-39400 (transport) for the benchmark cluster, so please ensure that this port range is not used by the metrics store.

Optional but recommended is to install also `Kibana <https://www.elastic.co/downloads/kibana>`_. Kibana will not be auto-configured but a sample
dashboard is delivered with Rally in ``$PACKAGE_ROOT/esrally/resources/kibana.json`` which can be imported to Kibana:

1. Create a new Kibana instance pointing to Rally's Elasticsearch data store
2. Create an index pattern "rally-*" and use "trial-timestamp" as time-field name (you might need to import some data first)
3. Go to Settings > Objects and import ``$PACKAGE_ROOT/esrally/resources/kibana.json``. Note that it assumes that the environment name is "nightly". Otherwise you won't see any data in graphs. You can either provide "nightly" as environment name during the initial configuration of Rally or search and replace it with your environment name before uploading.

Installing Rally
~~~~~~~~~~~~~~~~

Simply install Rally with pip: ``pip3 install esrally``

.. note::

   Depending on your system setup you may need to prepend this command with ``sudo``.

Configuring Rally
-----------------

Before we can run our first benchmark, we have to configure Rally. Just invoke `esrally configure` and Rally will automatically detect that its configuration file is missing and prompt you for some values and write them to `~/.rally/rally.ini`. After you've configured Rally, it will exit.

Running the first benchmark
---------------------------

Now we are ready to run the first benchmark with Rally. First, be sure to start the Elasticsearch metrics store instance and then just invoke `esrally`. This will start Rally with sensible defaults. It will download the necessary benchmark data, checkout the latest version of Elasticsearch, build it and finally run the benchmark.

.. note::
   If you want to benchmark a binary distribution instead of a source distribution then run ``esrally --pipeline=from-distribution --distribution-version=VERSION_NUMBER`` (``VERSION_NUMBER`` is for example ``5.0.0-alpha1``)

When the benchmark is done, a summary report is written to the command line:::

   ------------------------------------------------------
       _______             __   _____
      / ____(_)___  ____ _/ /  / ___/_________  ________
      / /_  / / __ \/ __ `/ /   \__ \/ ___/ __ \/ ___/ _ \
    / __/ / / / / / /_/ / /   ___/ / /__/ /_/ / /  /  __/
   /_/   /_/_/ /_/\__,_/_/   /____/\___/\____/_/   \___/
   ------------------------------------------------------
   Indexing Results (Throughput):
     median 12481 docs/s (min: 3952, max: 13399)

   Query Latency:
     Query latency [default]:
       99.0 Percentile: 7.23 ms
       99.9 Percentile: 11.91 ms
       100.0 Percentile: 13.02 ms
     Query latency [term]:
       99.0 Percentile: 4.25 ms
       99.9 Percentile: 5.18 ms
       100.0 Percentile: 5.94 ms
     Query latency [phrase]:
       99.0 Percentile: 3.89 ms
       99.9 Percentile: 5.06 ms
       100.0 Percentile: 5.13 ms
     Query latency [country_agg]:
       99.0 Percentile: 149.51 ms
       99.9 Percentile: 174.36 ms
       100.0 Percentile: 177.42 ms
     Query latency [country_agg_cached]:
       99.0 Percentile: 2.19 ms
       99.9 Percentile: 2.92 ms
       100.0 Percentile: 3.19 ms
     Query latency [scroll]:
       99.0 Percentile: 19.37 ms
       99.9 Percentile: 21.03 ms
       100.0 Percentile: 21.06 ms
   Total times:
     Indexing time      : 88.5 min
     Merge time         : 61.9 min
     Refresh time       : 8.5 min
     Flush time         : 0.1 min
     Merge throttle time: 1.6 min

   Merge times:
     Postings      : No metric data
     Stored Fields : No metric data
     Doc Values    : No metric data
     Norms         : No metric data
     Vectors       : No metric data

   System Metrics
     Median indexing CPU utilization (index): 658.1%
     Median indexing CPU utilization (search): 217.8%
     Total time spent in young gen GC: 81.60500s
     Total time spent in old gen GC: 10.89100s

   Index Metrics
   Could not determine disk usage metrics
     Total heap used for segments     : 35.17MB
     Total heap used for doc values   : 0.18MB
     Total heap used for terms        : 33.69MB
     Total heap used for norms        : 0.11MB
     Total heap used for stored fields: 0.68MB
     Index segment count: 148

   Stats request latency:
     Indices stats: 24.10ms
     Nodes stats: 23.15ms

Before relying too much on the numbers, please double-check that you did not introduce any bottlenecks and that your hardware is sufficient (e.g. spinning disks are not a good idea, better use SSDs). For additional insights and metrics you can activate different :doc:`telemetry devices </telemetry>` in Rally.

Also be very careful and get a deep understanding of the measurement approaches when comparing performance numbers on different OS. Sometimes certain measurements are supported only on one OS but not on another (e.g. disk I/O statistics) and different OS handle I/O differently.

Other command line flags
------------------------

Rally supports more command line flags, just run `esrally --help` to see what's possible.

Contents
--------

.. toctree::
   :maxdepth: 2

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
