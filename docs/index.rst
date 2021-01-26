Rally |release|
===============


.. ifconfig:: release.endswith('.dev0')

    .. warning::

        This documentation is for the version of Rally currently under development.
        Were you looking for the `documentation of the latest stable version <//esrally.readthedocs.io/en/stable/>`_?

You want to benchmark Elasticsearch? Then Rally is for you. It can help you with the following tasks:

* Setup and teardown of an Elasticsearch cluster for benchmarking
* Management of benchmark data and specifications even across Elasticsearch versions
* Running benchmarks and recording results
* Finding performance problems by attaching so-called telemetry devices
* Comparing performance results

We have also put considerable effort in Rally to ensure that benchmarking data are reproducible.

Getting Help or Contributing to Rally
-------------------------------------

* Use our `Discuss forum <https://discuss.elastic.co/tags/c/elastic-stack/elasticsearch/rally>`_ to provide feedback or ask questions about Rally.
* See our `contribution guide <https://github.com/elastic/rally/blob/master/CONTRIBUTING.md>`_ on guidelines for contributors.

Source Code
-----------

Rally's source code is available on `Github <https://github.com/elastic/rally>`_. You can also check the `changelog <https://github.com/elastic/rally/releases>`_ and the `roadmap <https://github.com/elastic/rally/milestones>`_ there.

.. toctree::
   :caption: Getting Started with Rally
   :maxdepth: 2

   quickstart
   install
   docker
   race
   tournament
   cluster_management
   recipes

.. toctree::
   :caption: Extending Rally
   :maxdepth: 2

   adding_tracks
   developing

.. toctree::
   :caption: Reference Documentation
   :maxdepth: 2

   versions
   command_line_reference
   configuration
   offline
   track
   car
   elasticsearch_plugins
   telemetry
   rally_daemon
   pipelines
   metrics
   summary_report

.. toctree::
   :caption: Additional Information
   :maxdepth: 2

   migrate
   faq
   glossary
   community



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
