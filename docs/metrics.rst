Metrics
=======

Metrics Records
---------------

At the end of a race, Rally stores all metrics records in its metrics store, which is a dedicated Elasticsearch cluster. Rally stores the metrics in the indices ``rally-metrics-*``. It will create a new index for each month.

Here is a typical metrics record::


    {
          "environment": "nightly",
          "race-timestamp": "20160421T042749Z",
          "race-id": "6ebc6e53-ee20-4b0c-99b4-09697987e9f4",
          "@timestamp": 1461213093093,
          "relative-time-ms": 10507.328,
          "relative-time": 10507.328,
          "track": "geonames",
          "track-params": {
            "shard-count": 3
          },
          "challenge": "append-no-conflicts",
          "car": "defaults",
          "sample-type": "normal",
          "name": "throughput",
          "value": 27385,
          "unit": "docs/s",
          "task": "index-append-no-conflicts",
          "operation": "index-append-no-conflicts",
          "operation-type": "Index",
          "meta": {
            "cpu_physical_cores": 36,
            "cpu_logical_cores": 72,
            "cpu_model": "Intel(R) Xeon(R) CPU E5-2699 v3 @ 2.30GHz",
            "os_name": "Linux",
            "os_version": "3.19.0-21-generic",
            "host_name": "beast2",
            "node_name": "rally-node0",
            "source_revision": "a6c0a81",
            "distribution_version": "8.0.0-SNAPSHOT",
            "tag_reference": "Github ticket 1234"
          }
        }

As you can see, we do not only store the metrics name and its value but lots of meta-information. This allows you to create different visualizations and reports in Kibana.

Below we describe each field in more detail.

environment
~~~~~~~~~~~

The environment describes the origin of a metric record. You define this value in the initial configuration of Rally. The intention is to clearly separate different benchmarking environments but still allow to store them in the same index.

track, track-params, challenge, car
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is the track, challenge and car for which the metrics record has been produced. If the user has provided track parameters with the command line parameter, ``--track-params``, each of them is listed here too.

If you specify a car with mixins, it will be stored as one string separated with "+", e.g. ``--car="4gheap,ea"`` will be stored as ``4gheap+ea`` in the metrics store in order to simplify querying in Kibana. Check the :doc:`cars </car>` documentation for more details.

sample-type
~~~~~~~~~~~

Rally can be configured to run for a certain period in warmup mode. In this mode samples will be collected with the ``sample-type`` "warmup" but only "normal" samples are considered for the results that reported.

race-timestamp
~~~~~~~~~~~~~~

A constant timestamp (always in UTC) that is determined when Rally is invoked.

race-id
~~~~~~~

A UUID that changes on every invocation of Rally. It is intended to group all samples of a benchmarking run.

@timestamp
~~~~~~~~~~

The timestamp in milliseconds since epoch determined when the sample was taken. For request-related metrics, such as ``latency`` or ``service_time`` this is the timestamp when Rally has issued the request.

relative-time-ms
~~~~~~~~~~~~~~~~

.. warning::

    This property has been introduced for a transition period between Rally 2.1.0 and Rally 2.3.0. It is deprecated with Rally 2.2.1 and will be removed in Rally 2.3.0. Use ``relative-time`` instead.


The relative time in milliseconds since the start of the benchmark. This is useful for comparing time-series graphs over multiple races, e.g. you might want to compare the indexing throughput over time across multiple races. As they should always start at the same (relative) point in time, absolute timestamps are not helpful.

relative-time
~~~~~~~~~~~~~

The relative time in milliseconds since the start of the benchmark. This is useful for comparing time-series graphs over multiple races, e.g. you might want to compare the indexing throughput over time across multiple races. As they should always start at the same (relative) point in time, absolute timestamps are not helpful.

name, value, unit
~~~~~~~~~~~~~~~~~

This is the actual metric name and value with an optional unit (counter metrics don't have a unit). Depending on the nature of a metric, it is either sampled periodically by Rally, e.g. the CPU utilization or query latency or just measured once like the final size of the index.

task, operation, operation-type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``task`` is the name of the task (as specified in the track file) that ran when this metric has been gathered. Most of the time, this value will be identical to the operation's name but if the same operation is ran multiple times, the task name will be unique whereas the operation may occur multiple times. It will only be set for metrics with name ``latency`` and ``throughput``.

``operation`` is the name of the operation (as specified in the track file) that ran when this metric has been gathered. It will only be set for metrics with name ``latency`` and ``throughput``.

``operation-type`` is the more abstract type of an operation. During a race, multiple queries may be issued which are different ``operation``s but they all have the same ``operation-type`` (Search). For some metrics, only the operation type matters, e.g. it does not make any sense to attribute the CPU usage to an individual query but instead attribute it just to the operation type.

meta
~~~~

Rally captures also some meta information for each metric record:

* CPU info: number of physical and logical cores and also the model name
* OS info: OS name and version
* Host name
* Node name: If Rally provisions the cluster, it will choose a unique name for each node.
* Source revision: We always record the git hash of the version of Elasticsearch that is benchmarked. This is even done if you benchmark an official binary release.
* Distribution version: We always record the distribution version of Elasticsearch that is benchmarked. This is even done if you benchmark a source release.
* Custom tag: You can define one custom tag with the command line flag ``--user-tag``. The tag is prefixed by ``tag_`` in order to avoid accidental clashes with Rally internal tags.
* Operation-specific: The optional substructure ``operation`` contains additional information depending on the type of operation. For bulk requests, this may be the number of documents or for searches the number of hits.

Note that depending on the "level" of a metric record, certain meta information might be missing. It makes no sense to record host level meta info for a cluster wide metric record, like a query latency (as it cannot be attributed to a single node).

Metric Keys
-----------

Rally stores the following metrics:

* ``latency``: Time period between submission of a request and receiving the complete response. It also includes wait time, i.e. the time the request spends waiting until it is ready to be serviced by Elasticsearch.
* ``service_time`` Time period between start of request processing and receiving the complete response. This metric can easily be mixed up with ``latency`` but does not include waiting time. This is what most load testing tools refer to as "latency" (although it is incorrect).
* ``throughput``: Number of operations that Elasticsearch can perform within a certain time period, usually per second. See the :doc:`track reference </track>` for a definition of what is meant by one "operation" for each operation type.
* ``disk_io_write_bytes``: number of bytes that have been written to disk during the benchmark. On Linux this metric reports only the bytes that have been written by Elasticsearch, on Mac OS X it reports the number of bytes written by all processes.
* ``disk_io_read_bytes``: number of bytes that have been read from disk during the benchmark. The same caveats apply on Mac OS X as for ``disk_io_write_bytes``.
* ``node_startup_time``: The time in seconds it took from process start until the node is up.
* ``node_total_young_gen_gc_time``: The total runtime of the young generation garbage collector across the whole cluster as reported by the node stats API.
* ``node_total_young_gen_gc_count``: The total number of young generation garbage collections across the whole cluster as reported by the node stats API.
* ``node_total_old_gen_gc_time``: The total runtime of the old generation garbage collector across the whole cluster as reported by the node stats API.
* ``node_total_old_gen_gc_count``: The total number of old generation garbage collections across the whole cluster as reported by the node stats API.
* ``segments_count``: Total number of segments as reported by the indices stats API.
* ``segments_memory_in_bytes``: Number of bytes used for segments as reported by the indices stats API.
* ``segments_doc_values_memory_in_bytes``: Number of bytes used for doc values as reported by the indices stats API.
* ``segments_stored_fields_memory_in_bytes``: Number of bytes used for stored fields as reported by the indices stats API.
* ``segments_terms_memory_in_bytes``: Number of bytes used for terms as reported by the indices stats API.
* ``segments_norms_memory_in_bytes``: Number of bytes used for norms as reported by the indices stats API.
* ``segments_points_memory_in_bytes``: Number of bytes used for points as reported by the indices stats API.
* ``merges_total_time``: Cumulative runtime of merges of primary shards, as reported by the indices stats API. Note that this is not Wall clock time (i.e. if M merge threads ran for N minutes, we will report M * N minutes, not N minutes). These metrics records also have a ``per-shard`` property that contains the times across primary shards in an array.
* ``merges_total_count``: Cumulative number of merges of primary shards, as reported by indices stats API under ``_all/primaries``.
* ``merges_total_throttled_time``: Cumulative time within merges have been throttled as reported by the indices stats API. Note that this is not Wall clock time.  These metrics records also have a ``per-shard`` property that contains the times across primary shards in an array.
* ``indexing_total_time``: Cumulative time used for indexing of primary shards, as reported by the indices stats API. Note that this is not Wall clock time.  These metrics records also have a ``per-shard`` property that contains the times across primary shards in an array.
* ``indexing_throttle_time``: Cumulative time that indexing has been throttled, as reported by the indices stats API. Note that this is not Wall clock time.  These metrics records also have a ``per-shard`` property that contains the times across primary shards in an array.
* ``refresh_total_time``: Cumulative time used for index refresh of primary shards, as reported by the indices stats API. Note that this is not Wall clock time.  These metrics records also have a ``per-shard`` property that contains the times across primary shards in an array.
* ``refresh_total_count``: Cumulative number of refreshes of primary shards, as reported by indices stats API under ``_all/primaries``.
* ``flush_total_time``: Cumulative time used for index flush of primary shards, as reported by the indices stats API. Note that this is not Wall clock time.  These metrics records also have a ``per-shard`` property that contains the times across primary shards in an array.
* ``flush_total_count``: Cumulative number of flushes of primary shards, as reported by indices stats API under ``_all/primaries``.
* ``final_index_size_bytes``: Final resulting index size on the file system after all nodes have been shutdown at the end of the benchmark. It includes all files in the nodes' data directories (actual index files and translog).
* ``store_size_in_bytes``: The size in bytes of the index (excluding the translog), as reported by the indices stats API.
* ``translog_size_in_bytes``: The size in bytes of the translog, as reported by the indices stats API.
* ``ml_processing_time``: A structure containing the minimum, mean, median and maximum bucket processing time in milliseconds per machine learning job. These metrics are only available if a machine learning job has been created in the respective benchmark.
