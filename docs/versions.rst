Elasticsearch Version Support
-----------------------------

Minimum supported version
=========================

Rally |release| can benchmark Elasticsearch |min_es_version| and above.
However, Rally does not support Elasticsearch clusters using the OSS license.

End-of-life Policy
==================

The latest version of Rally allows to benchmark all currently supported versions of Elasticsearch. Once an `Elasticsearch version reaches end-of-life <https://www.elastic.co/support/eol>`_, Rally will support benchmarking its last minor version until a new major version comes out. For example, after the release of Elasticsearch 8.0.0, Rally dropped support for Elasticsearch <= 6.7.x.

Metrics store
=============

Rally only supports Elasticsearch 7.x and above when using Elasticsearch as a :doc:`metrics store </metrics>`.
