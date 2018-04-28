Summary Report
==============

At the end of each :doc:`race </race>`, Rally shows a summary report. Below we'll explain the meaning of each line including a reference to its corresponding :doc:`metrics key </metrics>` which can be helpful if you want to build your own reports in Kibana. Note that not every summary report will show all lines.

Indexing time
-------------

* **Definition**: Total time used for indexing as reported by the indices stats API. Note that this is not Wall clock time (i.e. if M indexing threads ran for N minutes, we will report M * N minutes, not N minutes).
* **Corresponding metrics key**: ``indexing_total_time``

Indexing throttle time
----------------------

* **Definition**: Total time that indexing has been throttled as reported by the indices stats API. Note that this is not Wall clock time (i.e. if M indexing threads ran for N minutes, we will report M * N minutes, not N minutes).
* **Corresponding metrics key**: ``indexing_throttle_time``

Merge time
----------

* **Definition**: Total runtime of merges as reported by the indices stats API. Note that this is not Wall clock time.
* **Corresponding metrics key**: ``merges_total_time``

Refresh time
------------

* **Definition**: Total time used for index refresh as reported by the indices stats API. Note that this is not Wall clock time.
* **Corresponding metrics key**: ``refresh_total_time``

Flush time
----------

* **Definition**: Total time used for index flush as reported by the indices stats API. Note that this is not Wall clock time.
* **Corresponding metrics key**: ``flush_total_time``

Merge throttle time
-------------------

* **Definition**: Total time within merges have been throttled as reported by the indices stats API. Note that this is not Wall clock time.
* **Corresponding metrics key**: ``merges_total_throttled_time``


Merge time (``X``)
------------------

Where ``X`` is one of:

* postings
* stored fields
* doc values
* norms
* vectors
* points

..

* **Definition**: Different merge times as reported by Lucene. Only available if Lucene index writer trace logging is enabled (use `--car-params="verbose_iw_logging_enabled:true"` for that).
* **Corresponding metrics keys**: ``merge_parts_total_time_*``


Median CPU usage
----------------

* **Definition**: Median CPU usage in percent of the Elasticsearch process during the whole race based on a one second sample period. The maximum value is N * 100% where N is the number of CPU cores available
* **Corresponding metrics key**: ``cpu_utilization_1s``


Total Young Gen GC
------------------

* **Definition**: The total runtime of the young generation garbage collector across the whole cluster as reported by the node stats API.
* **Corresponding metrics key**: ``node_total_young_gen_gc_time``


Total Old Gen GC
----------------

* **Definition**: The total runtime of the old generation garbage collector across the whole cluster as reported by the node stats API.
* **Corresponding metrics key**: ``node_total_old_gen_gc_time``

Index size
----------

* **Definition**: Final resulting index size on the file system after all nodes have been shutdown at the end of the benchmark. It includes all files in the nodes' data directories (actual index files and translog).
* **Corresponding metrics key**: ``final_index_size_bytes``

Store size
----------

* **Definition**: The size in bytes of the index (excluding the translog) as reported by the indices stats API.
* **Corresponding metrics key**: ``store_size_in_bytes``

Translog size
-------------

* **Definition**: The size in bytes of the translog as reported by the indices stats API.
* **Corresponding metrics key**: ``translog_size_in_bytes``

Totally written
---------------

* **Definition**: number of bytes that have been written to disk during the benchmark. On Linux this metric reports only the bytes that have been written by Elasticsearch, on Mac OS X it reports the number of bytes written by all processes.
* **Corresponding metrics key**: ``disk_io_write_bytes``

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

Rally reports the minimum, median and maximum throughput for each task.

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

Error rate
----------

* **Definition**: The ratio of erroneous responses relative to the total number of responses. Any exception thrown by the Python Elasticsearch client is considered erroneous (e.g. HTTP response codes 4xx, 5xx or network errors (network unreachable)). For specific details, check the `reference documentation of the Elasticsearch client <https://elasticsearch-py.readthedocs.io>`_. Usually any error rate greater than zero is alerting. You should investigate the root cause by inspecting Rally and Elasticsearch logs and rerun the benchmark.
* **Corresponding metrics key**: ``service_time``. Each ``service_time`` record has a ``meta.success`` flag. Rally simply counts how often this flag is ``true`` and ``false`` respectively.
