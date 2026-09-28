Elasticsearch Version Support
-----------------------------

Minimum supported version
=========================

Rally |release| can benchmark Elasticsearch |min_es_version| and above.

End-of-life Policy
==================

Elasticsearch follows `end-of-life (EOL) policy <https://www.elastic.co/support/eol>`_. Prior to version 2.14.0, Rally supported all Elasticsearch versions marked as supported in Elasticsearch EOL policy at the time of Rally release. Starting from version 2.14.0, Rally supports current and previous major Elasticsearch version at the time of Rally release. The oldest supported Elasticsearch version as per Elasticsearch EOL is not guaranteed to be supported by the most recent Rally release.

The following table summarizes the minimum and maximum supported Elasticsearch version per Rally release:

.. list-table:: Rally-Elasticsearch compatibility matrix
   :header-rows: 1
   :widths: 33 33 33

   * - Rally release
     - Minimum supported server version
     - Maximum supported server version
   * - 2.7.1
     - 6.8
     - 8.x
   * - 2.8.0 - 2.13.0
     - 6.8
     - 9.x
   * - 2.14.0 - |release|
     - 8.0
     - 9.x
   * - some future release
     - 9.0
     - 10.x

Metrics store
=============

Rally only supports Elasticsearch 8.x and above when using Elasticsearch as a :doc:`metrics store </metrics>`.
