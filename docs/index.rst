Rally |release|
===============


.. ifconfig:: release.endswith('.dev0')

    .. warning::

        This documentation is for the version of Rally currently under development.
        Where you looking for the `documentation of the latest stable version <//esrally.readthedocs.io/en/stable/>`_?

You want to benchmark Elasticsearch? Then Rally is for you. It can help you with the following tasks:

* Setup and teardown of an Elasticsearch cluster for benchmarking
* Management of benchmark data and specifications even across Elasticsearch versions
* Running benchmarks and recording results
* Finding performance problems by attaching so-called telemetry devices
* Comparing performance results

We have also put considerable effort in Rally to ensure that benchmarking data are reproducible.

Getting Help or Contributing to Rally
-------------------------------------

Use our `Discuss forum <https://discuss.elastic.co/c/elasticsearch/rally>`_ to provide feedback or ask questions about Rally. Please see our `contribution guide <https://github.com/elastic/rally/blob/master/CONTRIBUTING.md>`_ on guidelines for contributors.

Source Code
-----------

Rally's source code is available on `Github <https://github.com/elastic/rally>`_.

Contents
--------

.. toctree::
   :maxdepth: 1

   quickstart
   install
   configuration
   race
   adding_tracks
   car
   faq
   telemetry
   pipelines
   metrics
   tournament
   recipes
   command_line_reference
   developing

License
-------

This software is licensed under the Apache License, version 2 ("ALv2"), quoted below.

Copyright 2015-|year| Elasticsearch <https://www.elastic.co>

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
