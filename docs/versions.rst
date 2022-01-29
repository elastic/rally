Elasticsearch Version Support
-----------------------------

Minimum supported version
=========================

Rally |release| can benchmark Elasticsearch |min_es_version| and above.
However, Rally does not support Elasticsearch clusters using the OSS license.

End-of-life Policy
==================

The latest version of Rally allows to benchmark all currently supported versions of Elasticsearch. Once an `Elasticsearch version reaches end-of-life <https://www.elastic.co/support/eol>`_, Rally will support benchmarking the corresponding version for two more years. For example, Elasticsearch 1.7.x has been supported until 2017-01-16. Rally drops support for Elasticsearch 1.7.x two years after that date on 2019-01-16. Version support is dropped in the next Rally maintenance release after that date.

Metrics store
=============

Rally only supports Elasticsearch 7.x and above when using Elasticsearch as a :doc:`metrics store </metrics>`.
