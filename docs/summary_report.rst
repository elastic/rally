Summary Report
==============

At the end of each :doc:`race </race>`, Rally shows a summary report. Below we'll explain the meaning of each line including a reference to its corresponding :doc:`metrics key </metrics>` which can be helpful if you want to build your own reports in Kibana. Note that not every summary report will show all lines.

Cumulative indexing time of primary shards
------------------------------------------

* **Definition**: Cumulative time used for indexing as reported by the indices stats API. Note that this is not Wall clock time (i.e. if M indexing threads ran for N minutes, we will report M * N minutes, not N minutes).
* **Corresponding metrics key**: ``indexing_total_time``

Cumulative indexing time across primary shards
----------------------------------------------

* **Definition**: Minimum, median and maximum cumulative time used for indexing across primary shards as reported by the indices stats API.
* **Corresponding metrics key**: ``indexing_total_time`` (property: ``per-shard``)

Cumulative indexing throttle time of primary shards
---------------------------------------------------

* **Definition**: Cumulative time that indexing has been throttled as reported by the indices stats API. Note that this is not Wall clock time (i.e. if M indexing threads ran for N minutes, we will report M * N minutes, not N minutes).
* **Corresponding metrics key**: ``indexing_throttle_time``

Cumulative indexing throttle time across primary shards
-------------------------------------------------------

* **Definition**: Minimum, median and maximum cumulative time used that indexing has been throttled across primary shards as reported by the indices stats API.
* **Corresponding metrics key**: ``indexing_throttle_time`` (property: ``per-shard``)

Cumulative merge time of primary shards
---------------------------------------

* **Definition**: Cumulative runtime of merges of primary shards, as reported by the indices stats API. Note that this is not Wall clock time.
* **Corresponding metrics key**: ``merges_total_time``

Cumulative merge count of primary shards
----------------------------------------

* **Definition**: Cumulative number of merges of primary shards, as reported by indices stats API under ``_all/primaries``.
* **Corresponding metrics key**: ``merges_total_count``

Cumulative merge time across primary shards
-------------------------------------------

* **Definition**: Minimum, median and maximum cumulative time of merges across primary shards, as reported by the indices stats API.
* **Corresponding metrics key**: ``merges_total_time`` (property: ``per-shard``)

Cumulative refresh time of primary shards
-----------------------------------------

* **Definition**: Cumulative time used for index refresh of primary shards, as reported by the indices stats API. Note that this is not Wall clock time.
* **Corresponding metrics key**: ``refresh_total_time``

Cumulative refresh count of primary shards
------------------------------------------

* **Definition**: Cumulative number of refreshes of primary shards, as reported by indices stats API under ``_all/primaries``.
* **Corresponding metrics key**: ``refresh_total_count``

Cumulative refresh time across primary shards
---------------------------------------------

* **Definition**: Minimum, median and maximum cumulative time for index refresh across primary shards, as reported by the indices stats API.
* **Corresponding metrics key**: ``refresh_total_time`` (property: ``per-shard``)

Cumulative flush time of primary shards
---------------------------------------

* **Definition**: Cumulative time used for index flush of primary shards, as reported by the indices stats API. Note that this is not Wall clock time.
* **Corresponding metrics key**: ``flush_total_time``

Cumulative flush count of primary shards
----------------------------------------

* **Definition**: Cumulative number of flushes of primary shards, as reported by indices stats API under ``_all/primaries``.
* **Corresponding metrics key**: ``flush_total_count``

Cumulative flush time across primary shards
-------------------------------------------

* **Definition**: Minimum, median and maximum time for index flush across primary shards as reported by the indices stats API.
* **Corresponding metrics key**: ``flush_total_time`` (property: ``per-shard``)

Cumulative merge throttle time of primary shards
------------------------------------------------

* **Definition**: Cumulative time within merges that have been throttled, as reported by the indices stats API. Note that this is not Wall clock time.
* **Corresponding metrics key**: ``merges_total_throttled_time``

Cumulative merge throttle time across primary shards
----------------------------------------------------

* **Definition**: Minimum, median and maximum cumulative time that merges have been throttled across primary shards as reported by the indices stats API.
* **Corresponding metrics key**: ``merges_total_throttled_time`` (property: ``per-shard``)


ML processing time
------------------

* **Definition**: Minimum, mean, median and maximum time in milliseconds that a machine learning job has spent processing a single bucket.
* **Corresponding metrics key**: ``ml_processing_time``


Total Young Gen GC time
-----------------------

* **Definition**: The total runtime of the young generation garbage collector across the whole cluster as reported by the node stats API.
* **Corresponding metrics key**: ``node_total_young_gen_gc_time``


Total Young Gen GC count
------------------------

* **Definition**: The total number of young generation garbage collections across the whole cluster as reported by the node stats API.
* **Corresponding metrics key**: ``node_total_young_gen_gc_count``


Total Old Gen GC time
---------------------

* **Definition**: The total runtime of the old generation garbage collector across the whole cluster as reported by the node stats API.
* **Corresponding metrics key**: ``node_total_old_gen_gc_time``

Total Old Gen GC count
----------------------

* **Definition**: The total number of old generation garbage collections across the whole cluster as reported by the node stats API.
* **Corresponding metrics key**: ``node_total_old_gen_gc_count``

Store size
----------

* **Definition**: The size in bytes of the index (excluding the translog) as reported by the indices stats API.
* **Corresponding metrics key**: ``store_size_in_bytes``

Translog size
-------------

* **Definition**: The size in bytes of the translog as reported by the indices stats API.
* **Corresponding metrics key**: ``translog_size_in_bytes``

Heap used for ``X``
-------------------

Where ``X`` is one of:


* doc values
* terms
* norms
* points
* stored fields

..

* **Definition**: Number of bytes used for the corresponding item as reported by the indices stats API.
* **Corresponding metrics keys**: ``segments_*_in_bytes``

Segment count
-------------

* **Definition**: Total number of segments as reported by the indices stats API.
* **Corresponding metrics key**: ``segments_count``


Throughput
----------

Rally reports the minimum, mean, median and maximum throughput for each task.

* **Definition**: Number of operations that Elasticsearch can perform within a certain time period, usually per second.
* **Corresponding metrics key**: ``throughput``

Latency
-------

Rally reports several percentile numbers for each task. Which percentiles are shown depends on how many requests Rally could capture (i.e. Rally will not show a 99.99th percentile if it could only capture five samples because that would be a vanity metric).

* **Definition**: Time period between submission of a request and receiving the complete response. It also includes wait time, i.e. the time the request spends waiting until it is ready to be serviced by Elasticsearch.
* **Corresponding metrics key**: ``latency``

Service time
------------

Rally reports several percentile numbers for each task. Which percentiles are shown depends on how many requests Rally could capture (i.e. Rally will not show a 99.99th percentile if it could only capture five samples because that would be a vanity metric).

* **Definition**: Time period between start of request processing and receiving the complete response. This metric can easily be mixed up with ``latency`` but does not include waiting time. This is what most load testing tools refer to as "latency" (although it is incorrect).
* **Corresponding metrics key**: ``service_time``

.. _summary_report_error_rate:

Error rate
----------

* **Definition**: The ratio of erroneous responses relative to the total number of responses. Any exception thrown by the Python Elasticsearch client is considered erroneous (e.g. HTTP response codes 4xx, 5xx or network errors (network unreachable)). For specific details, check the `reference documentation of the Elasticsearch client <https://elasticsearch-py.readthedocs.io>`_. Usually any error rate greater than zero is alerting. You should investigate the root cause by inspecting Rally and Elasticsearch logs and rerun the benchmark.
* **Corresponding metrics key**: ``service_time``. Each ``service_time`` record has a ``meta.success`` flag. Rally simply counts how often this flag is ``true`` and ``false`` respectively.
