Getting Started
===============

What is Rally?
--------------

So you want to benchmark Elasticsearch? Then Rally is for you. Rally started as an effort to help developers in the Elasticsearch development team to run benchmarks on their machines. As our users are very creative and use Elasticsearch for all kinds of things, we have to cover a broad range of different performance characteristics. Rally is build around a few assumptions:

* Everything is run on the same machine (but `we are about to change that <https://github.com/elastic/rally/issues/71`_)
* You want to add a specific data set to an Elasticsearch index and then run benchmarking queries on it

These are some of the core assumptions and we are continuously working to remove those restrictions. In contrast to other home-grown benchmarking scripts, we have put considerable effort in Rally to ensure the benchmarking data are reproducible.

Compatibility
-------------

Rally can build Elasticsearch either from sources or use an official binary release. If you have Rally build Elasticsearch from sources, it can only be used to benchmark Elasticsearch 5.0 and above. The reason is that with Elasticsearch 5.0 the build tool was switched from Maven to Gradle. As Rally only supports Gradle, it is limited to Elasticsearch 5.0 and above.

Also note that Rally does not support Windows. It may or may not work but we actively test it only on Mac OS X and Linux.

Installing Rally
----------------

Please refer to the `installation instructions in the README <https://github.com/elastic/rally/blob/master/README.rst#getting-started>`_.

Configuring Rally
-----------------

Before we can run our first benchmark, we have to configure Rally. Just invoke `esrally` and Rally will automatically detect that its configuration file is missing and prompt you for some values and write them to `~/.rally/rally.ini`. After you've configured Rally, it will exit.

Running the first benchmark
---------------------------

Now we are ready to run the first benchmark with Rally. Just invoke `esrally` again. This will start Rally with sensible defaults. It will download the necessary benchmark data, checkout the latest version of Elasticsearch, build it and finally run the benchmark.

When the benchmark is done, a summary report is written to the command line.

Before relying too much on the numbers, please double-check that you did not introduce any bottlenecks and that your hardware is sufficient (e.g. spinning disks are not a good idea, better use SSDs). For additional insights and metrics you can activate different :doc:`telemetry devices </telemetry>` in Rally.

Other command line flags
------------------------

Rally supports more command line flags, just run `esrally --help` to see what's possible.

Contents
--------

.. toctree::
   :maxdepth: 2

   telemetry
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
