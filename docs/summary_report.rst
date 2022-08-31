Summary Report
==============

At the end of each :doc:`race </race>`, Rally shows a summary report. Below we'll explain the meaning of each line including a reference to its corresponding :doc:`metrics key </metrics>` which can be helpful if you want to build your own reports in Kibana. Note that not every summary report will show all lines.

Cumulative indexing time of primary shards
------------------------------------------

* **Definition**: Cumulative time used for indexing as reported by the index stats API. Note that this is not Wall clock time (i.e. if M indexing threads ran for N minutes, we will report M * N minutes, not N minutes).
* **Corresponding metrics key**: ``indexing_total_time``

Cumulative indexing time across primary shards
----------------------------------------------

* **Definition**: Minimum, median and maximum cumulative time used for indexing across primary shards as reported by the index stats API.
* **Corresponding metrics key**: ``indexing_total_time`` (property: ``per-shard``)

Cumulative indexing throttle time of primary shards
---------------------------------------------------

* **Definition**: Cumulative time that indexing has been throttled as reported by the index stats API. Note that this is not Wall clock time (i.e. if M indexing threads ran for N minutes, we will report M * N minutes, not N minutes).
* **Corresponding metrics key**: ``indexing_throttle_time``

Cumulative indexing throttle time across primary shards
-------------------------------------------------------

* **Definition**: Minimum, median and maximum cumulative time used that indexing has been throttled across primary shards as reported by the index stats API.
* **Corresponding metrics key**: ``indexing_throttle_time`` (property: ``per-shard``)

Cumulative merge time of primary shards
---------------------------------------

* **Definition**: Cumulative runtime of merges of primary shards, as reported by the index stats API. Note that this is not Wall clock time.
* **Corresponding metrics key**: ``merges_total_time``

Cumulative merge count of primary shards
----------------------------------------

* **Definition**: Cumulative number of merges of primary shards, as reported by index stats API under ``_all/primaries``.
* **Corresponding metrics key**: ``merges_total_count``

Cumulative merge time across primary shards
-------------------------------------------

* **Definition**: Minimum, median and maximum cumulative time of merges across primary shards, as reported by the index stats API.
* **Corresponding metrics key**: ``merges_total_time`` (property: ``per-shard``)

Cumulative refresh time of primary shards
-----------------------------------------

* **Definition**: Cumulative time used for index refresh of primary shards, as reported by the index stats API. Note that this is not Wall clock time.
* **Corresponding metrics key**: ``refresh_total_time``

Cumulative refresh count of primary shards
------------------------------------------

* **Definition**: Cumulative number of refreshes of primary shards, as reported by index stats API under ``_all/primaries``.
* **Corresponding metrics key**: ``refresh_total_count``

Cumulative refresh time across primary shards
---------------------------------------------

* **Definition**: Minimum, median and maximum cumulative time for index refresh across primary shards, as reported by the index stats API.
* **Corresponding metrics key**: ``refresh_total_time`` (property: ``per-shard``)

Cumulative flush time of primary shards
---------------------------------------

* **Definition**: Cumulative time used for index flush of primary shards, as reported by the index stats API. Note that this is not Wall clock time.
* **Corresponding metrics key**: ``flush_total_time``

Cumulative flush count of primary shards
----------------------------------------

* **Definition**: Cumulative number of flushes of primary shards, as reported by index stats API under ``_all/primaries``.
* **Corresponding metrics key**: ``flush_total_count``

Cumulative flush time across primary shards
-------------------------------------------

* **Definition**: Minimum, median and maximum time for index flush across primary shards as reported by the index stats API.
* **Corresponding metrics key**: ``flush_total_time`` (property: ``per-shard``)

Cumulative merge throttle time of primary shards
------------------------------------------------

* **Definition**: Cumulative time within merges that have been throttled, as reported by the index stats API. Note that this is not Wall clock time.
* **Corresponding metrics key**: ``merges_total_throttled_time``

Cumulative merge throttle time across primary shards
----------------------------------------------------

* **Definition**: Minimum, median and maximum cumulative time that merges have been throttled across primary shards as reported by the index stats API.
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

Total ZGC Cycles GC time
------------------------

* **Definition**: The total time spent doing GC by the ZGC garbage collector across the whole cluster as reported by the node stats API.
* **Corresponding metrics key**: ``node_total_zgc_cycles_gc_time``

Total ZGC Cycles GC count
-------------------------

* **Definition**: The total number of garbage collections performed by ZGC across the whole cluster as reported by the node stats API.
* **Corresponding metrics key**: ``node_total_zgc_cycles_gc_count``

Total ZGC Pauses GC time
------------------------

* **Definition**: The total time spent in Stop-The-World pauses by the ZGC garbage collector across the whole cluster as reported by the node stats API.
* **Corresponding metrics key**: ``node_total_zgc_pauses_gc_time``

Total ZGC Pauses GC count
-------------------------

* **Definition**: The total number of Stop-The-World pauses performed by ZGC across the whole cluster as reported by the node stats API.
* **Corresponding metrics key**: ``node_total_zgc_pauses_gc_count``

Store size
----------

* **Definition**: The size in bytes of the index (excluding the translog) as reported by the index stats API.
* **Corresponding metrics key**: ``store_size_in_bytes``

Translog size
-------------

* **Definition**: The size in bytes of the translog as reported by the index stats API.
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

* **Definition**: Number of bytes used for the corresponding item as reported by the index stats API.
* **Corresponding metrics keys**: ``segments_*_in_bytes``

Segment count
-------------

* **Definition**: Total number of segments as reported by the index stats API.
* **Corresponding metrics key**: ``segments_count``

Total Ingest Pipeline count
---------------------------

* **Definition**: Total number of documents ingested by all nodes within the cluster, over the duration of the race.
* **Corresponding metrics key**: ``ingest_pipeline_cluster_count``


Total Ingest Pipeline time
---------------------------

* **Definition**: Total time, in milliseconds, spent preprocessing ingest documents by all nodes within the cluster, over the duration of the race.
* **Corresponding metrics key**: ``ingest_pipeline_cluster_time``

Total Ingest Pipeline failed
----------------------------

* **Definition**: Total number of failed ingest operations by all nodes within the cluster, over the duration of the race.
* **Corresponding metrics key**: ``ingest_pipeline_cluster_failed``

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

* **Definition**: Time period between sending a request and receiving the corresponding response. This metric can easily be mixed up with ``latency`` but does not include waiting time. This is what most load testing tools refer to as "latency" (although it is incorrect).
* **Corresponding metrics key**: ``service_time``

.. _summary_report_processing_time:

Processing time
---------------

.. note::

    Processing time is only reported if the setting ``output.processingtime`` is set to ``true`` in the :ref:`configuration file <configuration_reporting>`.

Rally reports several percentile numbers for each task. Which percentiles are shown depends on how many requests Rally could capture (i.e. Rally will not show a 99.99th percentile if it could only capture five samples because that would be a vanity metric).

* **Definition**: Time period between start of request processing and receiving the complete response. Contrary to service time, this metric also includes Rally's client side processing overhead. Large differences between service time and processing time indicate a high overhead in the client and can thus point to a potential client-side bottleneck which requires investigation.
* **Corresponding metrics key**: ``processing_time``

.. _summary_report_error_rate:

Error rate
----------

* **Definition**: The ratio of erroneous responses relative to the total number of responses. Any exception thrown by the Python Elasticsearch client is considered erroneous (e.g. HTTP response codes 4xx, 5xx or network errors (network unreachable)). For specific details, check the `reference documentation of the Elasticsearch client <https://elasticsearch-py.readthedocs.io>`_. Usually any error rate greater than zero is alerting. You should investigate the root cause by inspecting Rally and Elasticsearch logs and rerun the benchmark.
* **Corresponding metrics key**: ``service_time``. Each ``service_time`` record has a ``meta.success`` flag. Rally simply counts how often this flag is ``true`` and ``false`` respectively.

Disk usage
----------

.. note::

    The following disk usage summaries are only reported if the  :ref:`disk-usage-stats <disk-usage-stats>` telemetry device is enabled.

Per field total disk usage
..........................

* **Definition**: The total number of bytes that a single field uses on disk. Recorded for each field returned by the disk usage API even if the total is 0.
* **Corresponding metrics keys**: ``disk_usage_total``
* **Metric metadata**: ``index`` and ``field``

Per field inverted index disk usage
...................................

* **Definition**: The number of bytes that a single field uses for its inverted index on disk. Recorded for each field with a non-0 byte inverted index. Expect this on `text` and `keyword` fields but not on `long` or `date` fields.
* **Corresponding metrics keys**: ``disk_usage_inverted_index``
* **Metric metadata**: ``index`` and ``field``

Per field stored fields disk usage
..................................

* **Definition**: The number of bytes that a single field uses for stored fields on disk. Recorded for each field with non-0 byte stored fields. Expect this for `_id` and `_source`.
* **Corresponding metrics keys**: ``disk_usage_stored_fields``
* **Metric metadata**: ``index`` and ``field``

Per field doc values disk usage
...............................

* **Definition**: The number of bytes that a single field uses for doc values on disk. Recorded for each field with non-0 byte doc values. Expect this on every most fields.
* **Corresponding metrics keys**: ``disk_usage_doc_values``
* **Metric metadata**: ``index`` and ``field``

Per field points disk usage
...........................

* **Definition**: The number of bytes that a single field uses for points on disk. Recorded for each field with a non-0 byte BKD tree. Expect this on `long` and `date` fields but not on `text` and `keyword` fields.
* **Corresponding metrics keys**: ``disk_usage_points``
* **Metric metadata**: ``index`` and ``field``

Per field norms disk usage
..........................

* **Definition**: The number of bytes that a single field uses for norms on disk. Recorded for each field with a non-0 byte norms. Expect this for `text` fields.
* **Corresponding metrics keys**: ``disk_usage_norms``
* **Metric metadata**: ``index`` and ``field``

Per field term vectors disk usage
.................................

* **Definition**: The number of bytes that a single field uses for term vectors on disk. Recorded for each field with a non-0 byte term vectors. Expect this for `text` fields configured to store term vectors. This is rare.
* **Corresponding metrics keys**: ``disk_usage_term_vectors``
* **Metric metadata**: ``index`` and ``field``
