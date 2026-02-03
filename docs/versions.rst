Elasticsearch Version Support
-----------------------------

Minimum supported version
=========================

Rally |release| can benchmark Elasticsearch |min_es_version| and above.
However, Rally does not support Elasticsearch clusters using the OSS license.

End-of-life Policy
==================

The following table summarizes the minimum supported Elasticsearch version per Rally release:

.. list-table:: Minimum supported Elasticsearch version
   :header-rows: 1
   :widths: 40 60

   * - Rally release
     - Minimum supported Elasticsearch
   * - < 2.14.0
     - 6.8.0 and above
   * - >= 2.14.0
     - |min_es_version| and above

As a rule of thumbs for the future as `Elasticsearch version reaches end-of-life <https://www.elastic.co/support/eol>`_, Rally may drop support in a future release with the upgrade of Elasticsearch Python client on which rally depends on.

Metrics store
=============

Rally only supports Elasticsearch 8.x and above when using Elasticsearch as a :doc:`metrics store </metrics>`.
