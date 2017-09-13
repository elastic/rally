Metrics
=======

Metrics Records
---------------

At the end of a race, Rally stores all metrics records in its metrics store, which is a dedicated Elasticsearch cluster. Rally stores the metrics in the indices ``rally-metrics-*``. It will create a new index for each month.

Here is a typical metrics record::


    {
          "environment": "nightly",
          "track": "geonames",
          "challenge": "append-no-conflicts",
          "car": "defaults",
          "sample-type": "normal",
          "trial-timestamp": "20160421T042749Z",
          "@timestamp": 1461213093093,
          "relative-time": 10507328,
          "name": "throughput",
          "value": 27385,
          "unit": "docs/s",
          "operation": "index-append-no-conflicts",
          "operation-type": "Index",
          "lap": 1,
          "meta": {
            "cpu_physical_cores": 36,
            "cpu_logical_cores": 72,
            "cpu_model": "Intel(R) Xeon(R) CPU E5-2699 v3 @ 2.30GHz",
            "os_name": "Linux",
            "os_version": "3.19.0-21-generic",
            "host_name": "beast2",
            "node_name": "rally-node0",
            "source_revision": "a6c0a81",
            "distribution_version": "5.0.0-SNAPSHOT",
            "tag_reference": "Github ticket 1234",
          }
        }

As you can see, we do not only store the metrics name and its value but lots of meta-information. This allows you to create different visualizations and reports in Kibana.

Below we describe each field in more detail.

environment
~~~~~~~~~~~

The environment describes the origin of a metric record. You define this value in the initial configuration of Rally. The intention is to clearly separate different benchmarking environments but still allow to store them in the same index.

track, challenge, car
~~~~~~~~~~~~~~~~~~~~~

This is the track, challenge and car for which the metrics record has been produced. Note that if you specify a car with mixins, it will be stored as one string separated with "+", e.g. ``--car="4gheap,ea"`` will be stored as ``4gheap+ea`` in the metrics store in order to simplify querying in Kibana. For more details, please see the :doc:`cars </car>` documentation.

sample-type
~~~~~~~~~~~

Rally runs warmup trials but records all samples. Normally, we are just interested in "normal" samples but for a full picture we might want to look also at "warmup" samples.

trial-timestamp
~~~~~~~~~~~~~~~

A constant timestamp (always in UTC) that is determined when Rally is invoked. It is intended to group all samples of a benchmark trial.

@timestamp
~~~~~~~~~~

The timestamp in milliseconds since epoch determined when the sample was taken.

relative-time
~~~~~~~~~~~~~

The relative time in microseconds since the start of the benchmark. This is useful for comparing time-series graphs over multiple trials, e.g. you might want to compare the indexing throughput over time across multiple benchmark trials. Obviously, they should always start at the same (relative) point in time and absolute timestamps are useless for that.

name, value, unit
~~~~~~~~~~~~~~~~~

This is the actual metric name and value with an optional unit (counter metrics don't have a unit). Depending on the nature of a metric, it is either sampled periodically by Rally, e.g. the CPU utilization or query latency or just measured once like the final size of the index.

operation, operation-type
~~~~~~~~~~~~~~~~~~~~~~~~~

``operation`` is the name of the operation (as specified in the track file) that ran when this metric has been gathered. It will only be set for metrics with name ``latency`` and ``throughput``.

``operation-type`` is the more abstract type of an operation. During a race, multiple queries may be issued which are different ``operation``s but they all have the same ``operation-type`` (Search). For some metrics, only the operation type matters, e.g. it does not make any sense to attribute the CPU usage to an individual query but instead attribute it just to the operation type.

lap
~~~

The lap number in which this metric was gathered. Laps start at 1. See the :doc:`command line reference </command_line_reference>` for more info on laps.


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
* ``merge_parts_total_time_*``: Different merge times as reported by Lucene. Only available if Lucene index writer trace logging is enabled.
* ``merge_parts_total_docs_*``: See ``merge_parts_total_time_*``
* ``disk_io_write_bytes``: number of bytes that have been written to disk during the benchmark. On Linux this metric reports only the bytes that have been written by Elasticsearch, on Mac OS X it reports the number of bytes written by all processes.
* ``disk_io_read_bytes``: number of bytes that have been read from disk during the benchmark. The same caveats apply on Mac OS X as for ``disk_io_write_bytes``.
* ``cpu_utilization_1s``: CPU usage in percent of the Elasticsearch process based on a one second sample period. The maximum value is N * 100% where N is the number of CPU cores available.
* ``node_total_old_gen_gc_time``: The total runtime of the old generation garbage collector across the whole cluster as reported by the node stats API.
* ``node_total_young_gen_gc_time``: The total runtime of the young generation garbage collector across the whole cluster as reported by the node stats API.
* ``segments_count``: Total number of segments as reported by the indices stats API.
* ``segments_memory_in_bytes``: Number of bytes used for segments as reported by the indices stats API.
* ``segments_doc_values_memory_in_bytes``: Number of bytes used for doc values as reported by the indices stats API.
* ``segments_stored_fields_memory_in_bytes``: Number of bytes used for stored fields as reported by the indices stats API.
* ``segments_terms_memory_in_bytes``: Number of bytes used for terms as reported by the indices stats API.
* ``segments_norms_memory_in_bytes``: Number of bytes used for norms as reported by the indices stats API.
* ``segments_points_memory_in_bytes``: Number of bytes used for points as reported by the indices stats API.
* ``merges_total_time``: Total runtime of merges as reported by the indices stats API. Note that this is not Wall clock time (i.e. if M merge threads ran for N minutes, we will report M * N minutes, not N minutes).
* ``merges_total_throttled_time``: Total time within merges have been throttled as reported by the indices stats API. Note that this is not Wall clock time.
* ``indexing_total_time``: Total time used for indexing as reported by the indices stats API. Note that this is not Wall clock time.
* ``refresh_total_time``: Total time used for index refresh as reported by the indices stats API. Note that this is not Wall clock time.
* ``flush_total_time``: Total time used for index flush as reported by the indices stats API. Note that this is not Wall clock time.
* ``final_index_size_bytes``: Final resulting index size after the benchmark.
