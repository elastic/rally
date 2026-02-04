Elasticsearch Version Support
-----------------------------

Minimum supported version
=========================

Rally |release| can benchmark Elasticsearch |min_es_version| and above.
However, Rally does not support Elasticsearch clusters using the OSS license.

End-of-life Policy
==================

Before version Rally 2.14.0 policy for supporting old Elasticsearch server versions "all currently supported ES versions" (which leads to Y = X-2 with X the version of the Elasticsearch Python client and Y the version of the supported Elasticsearch server). Now we want to change it to Y = X-1.

The following table summarizes the minimum supported Elasticsearch version per Rally release:

.. list-table:: Minimum supported Elasticsearch version
   :header-rows: 1
   :widths: 25 25 25 25

   * - Rally version
     - Python client version
     - Minimum supported server version
     - Maximum supported server version
   * - 2.7.1
     - 7.14.0
     - 6.8
     - 8.x
   * - 2.8.0 to 2.13.0
     - 8.6.1
     - 6.8
     - 9.x
   * - >= 2.14.0
     - 9.2.1
     - 8.0
     - 8.0 to 10.x

As a rule of thumbs for the future as `Elasticsearch version reaches end-of-life <https://www.elastic.co/support/eol>`_, Rally will drop support for that version in the next Rally release.

Metrics store
=============

Rally only supports Elasticsearch 8.x and above when using Elasticsearch as a :doc:`metrics store </metrics>`.
