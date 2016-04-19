Rally
=====

Rally is the macrobenchmarking framework for Elasticsearch

Prerequisites
-------------

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

Getting Started
---------------

Preparation
~~~~~~~~~~~

First `install Elasticsearch <https://www.elastic.co/downloads/elasticsearch>`_ 2.3 or higher. A simple out-of-the-box installation with a single node will suffice. Rally uses this instance to store metrics data. It will setup the necessary indices by itself. The configuration procedure of Rally will you ask for host and port of this cluster.

**Note**: Rally will choose the port range 39200-39300 (HTTP) and 39300-39400 (transport) for the benchmark cluster, so please ensure 
that this port range is not used by the metrics store.

Optional but recommended is to install also `Kibana <https://www.elastic.co/downloads/kibana>`_. Kibana will not be auto-configured but a sample
dashboard is delivered with Rally in ``esrally/resources/kibana.json`` which can be imported to Kibana:

1. Create a new Kibana instance pointing to Rally's Elasticsearch data store
2. Create an index pattern "rally-*" and use "trial-timestamp" as time-field name (you might need to import some data first)
3. Go to Settings > Objects and import ``esrally/resources/kibana.json``. Note that it assumes that the environment name is "nightly". Otherwise you won't see any data in graphs. You can either provide "nightly" as environment name during the initial configuration of Rally or search and replace it with your environment name before uploading.

Installing Rally
~~~~~~~~~~~~~~~~

1. Install Rally with pip: ``pip3 install esrally`` (note: depending on your system setup you need prepend this command with ``sudo``).
2. Configure Rally: ``esrally configure``. It will prompt you for some values and write them to the config file ``~/.rally/rally.ini``.
3. Run Rally: ``esrally``. It is now properly set up and will run the benchmarks.

Command Line Options
--------------------

Rally has a list of supported command line options. Just run `esrally --help`.

Here are some examples:

* ``esrally``: Runs the benchmarks and reports the results on the command line. This is what you typically want to do in development. It assumes lots of defaults; its canonical form is ``esrally race --pipeline=from-sources-complete --revision=current --track=geonames --track-setup=defaults``.
* ``esrally --pipeline from-sources-skip-build``: Assumes that an Elasticsearch ZIP file has already been build and just runs the benchmark.
* ``esrally --revision ebe3fd2``: Checks out the revision ``ebe3fd2`` from git, builds it and runs benchmarks against it. Note that will only work if the build is based on Gradle (i.e. Elasticsearch 5.0+)

For more details, please refer to `Rally's user guide <https://esrally.readthedocs.org/>`_.

How to Contribute
-----------------
 
See all details in the `contributor guidelines <CONTRIBUTING.md>`_.
 
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
