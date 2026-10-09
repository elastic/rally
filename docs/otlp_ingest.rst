OTLP Metrics Ingest
===================

Rally supports benchmarking Elasticsearch's native `OTLP metrics ingest endpoint <https://www.elastic.co/docs/manage-data/ingest/otlp-endpoint>`_ (``/_otlp/v1/metrics``).
This lets you measure how quickly Elasticsearch can accept a realistic stream of OpenTelemetry metrics delivered in binary protobuf format, with optional per-record gzip compression.

Overview
--------

The OTLP ingest feature introduces a new corpus format (``otlp-metrics``) and a new operation type (``otlp-ingest``).
Instead of bulk-indexing newline-delimited JSON, Rally sends pre-serialized ``ExportMetricsServiceRequest`` protobuf messages directly to the OTLP endpoint — matching exactly what a real OpenTelemetry Collector would send.

The data flow at a glance:

1. **Generate** — Use ``metricsgenreceiver`` to produce an OTLP JSON corpus file (``metrics.otlp.json``).
2. **Prepare** — During ``prepare-track``, Rally converts the JSON corpus to a binary protobuf file (``metrics.otlp.json.pb`` or ``metrics.otlp.json.pbgz``). This is a one-time conversion per machine.
3. **Race** — During ``race``, Rally streams records from the ``.pb`` file directly to Elasticsearch's ``/_otlp/v1/metrics`` endpoint.

Generating Corpus Data
----------------------

The corpus data can be generated with `metricsgenreceiver <https://github.com/elastic/metricsgenreceiver>`_. It is an OpenTelemetry Collector receiver that generates realistic metric streams from configurable scenarios (e.g., ``builtin/hostmetrics``, ``builtin/kubeletstats-pod``).
It has two output modes:

* **File export** — writes OTLP JSON to disk for later use as a Rally corpus.
* **Direct ingest** — sends binary protobuf requests straight to Elasticsearch's ``/_otlp`` endpoint, bypassing Rally entirely. Useful for smoke-testing the stack or one-off data seeding.

.. _otlp_install_metricsgen:

Installing metricsgenreceiver
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Download a pre-built binary from the `releases page <https://github.com/elastic/metricsgenreceiver/releases>`_ or `build it from source <https://github.com/elastic/metricsgenreceiver#building>`_. The examples below assume that the ``otelcol`` binary is on your ``PATH``. The following examples were tested with `v1.0.12 <https://github.com/elastic/metricsgenreceiver/releases/tag/v1.0.12>`_ release.

.. _otlp_generate_corpus:

Generating a corpus file
~~~~~~~~~~~~~~~~~~~~~~~~

The following ``otelcol.yaml`` config generates one hour of host metrics at 10-second intervals from 10 simulated hosts, writing the output to ``./corpus/metrics.otlp.json``::

    receivers:
      metricsgen:
        start_time: "2025-01-01T00:00:00Z"
        end_time: "2025-01-01T01:00:00Z"
        interval: 10s
        exit_after_end: true
        seed: 123
        scenarios:
          - path: builtin/hostmetrics
            scale: 10

    processors:
      batch:
        send_batch_size: 1700

    exporters:
      file:
        path: ./corpus/metrics.otlp.json

    service:
      pipelines:
        metrics:
          receivers: [metricsgen]
          processors: [batch]
          exporters: [file]

Run it::

    mkdir -p corpus
    otelcol --config otelcol.yaml

When ``exit_after_end: true`` is set, the collector exits automatically once the configured time range is exhausted. The resulting ``metrics.otlp.json`` file is a newline-delimited sequence of OTLP JSON records, each representing one ``ExportMetricsServiceRequest`` batch.

The ``receivers`` section determines the volume of data produced, see :ref:`otlp_tuning_corpus_size`. In the example, an hourly interval dictated by ``start_time`` and ``end_time`` is split into 360 ticks as per ``interval`` setting. In each tick, 10 hosts (``scale``) produce a number of datapoints. The number of datapoints depends on the data shape captured in a scenario. In case of ``builtin/hostmetrics`` scenario there are 170 datapoints per host per tick. Overall, there are 1700 datapoints in each 10s interval.

.. note::
  To determine the number of datapoints per host find ``datapoints`` in ``metricsgenreceiver`` output. In the example it reports 612000 datapoints. With 360 ticks and 10 hosts this gives us ``612000 / 360 / 10 = 170`` datapoints per host, per tick.

In non-realtime mode (no ``real_time: true`` setting), ``metricsgenreceiver`` produces data as quickly as possible. In this mode, the number of batches on the output depends on the ``batch`` processor ``send_batch_size`` setting which defaults to 8192 datapoints. To align batches with time intervals, ``send_batch_size`` was reduced to 1700 datapoints because that is the total number of datapoints produced every 10s by each of 10 hosts. This results in 360 batches.

::

  % wc -l corpus/metrics.otlp.json
    360 corpus/metrics.otlp.json

.. _otlp_direct_ingest:

Sending directly to Elasticsearch
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To send metrics directly to Elasticsearch instead of writing to a file, add the ``otlphttp/elasticsearch`` exporter to the pipeline::

  receivers:
    metricsgen:
      start_time: "2025-01-01T00:00:00Z"
      end_time: "2025-01-01T01:00:00Z"
      interval: 10s
      exit_after_end: true
      seed: 123
      scenarios:
        - path: builtin/hostmetrics
          scale: 10

  processors:
    batch:
      send_batch_size: 1700

  extensions:
    basicauth/client:
      client_auth:
        username: elastic
        password: changeme

  exporters:
    otlphttp/elasticsearch:
      compression: gzip # send gzip-compressed protobuf, matching Rally's gzip: true mode
      encoding: proto
      endpoint: "https://localhost:9200/_otlp"
      auth:
        authenticator: basicauth/client
      sending_queue:
        enabled: true
        block_on_overflow: true
        queue_size: 4
        num_consumers: 4
      tls:
        insecure_skip_verify: true

  service:
    extensions:
      - basicauth/client
    pipelines:
      metrics:
        receivers: [metricsgen]
        processors: [batch]
        exporters: [otlphttp/elasticsearch]

Key exporter settings:

* **encoding: proto** — sends binary protobuf (``application/x-protobuf``) rather than OTLP JSON. This is the wire format Elasticsearch's ``/_otlp`` endpoint expects.
* **compression: gzip** — gzip-compresses each request body, equivalent to what Rally sends when ``gzip: true`` is set on an ``otlp-ingest`` operation. This is the standard OTel Collector behaviour and generally improves throughput.
* **block_on_overflow: true** — prevents the collector from dropping records if the send queue fills up. Useful when generating data faster than Elasticsearch can ingest it.
* **num_consumers** — number of parallel senders from the queue to Elasticsearch. Increase this to saturate high-throughput clusters.

.. note::

   The ``endpoint`` must include the ``/_otlp`` path prefix. Elasticsearch routes ``/_otlp/v1/metrics`` to the OTLP metrics ingest handler.

This mode is useful for quick manual testing, but for reproducible benchmarking use the file exporter to capture the corpus first, then replay it through Rally.

.. _otlp_tuning_corpus_size:

Tuning corpus size
~~~~~~~~~~~~~~~~~~

Adjust the following parameters to produce different corpus sizes:

* **scale** — number of simulated instances (e.g., hosts, pods). Higher scale = more time series = larger file.
* **interval** — scrape interval. Smaller interval = more data points per host per hour.
* **start_time / end_time** — time range. Longer range = more records.
* **scenarios** — swap in ``builtin/kubeletstats-pod``, ``builtin/tsbs-devops``, etc. for different metric shapes.

Typical corpus sizes for ``builtin/hostmetrics``:

+--------+-----------+-------------+------------+-----------+-----------+-----------+
| Scale  | Interval  | Duration    | Datapoints | JSON size | PB size   | PBGZ size |
+========+===========+=============+============+===========+===========+===========+
| 10     | 10s       | 1 hour      | 612k       | ~182 MiB  | ~72 MiB   | ~6.9 MiB  |
+--------+-----------+-------------+------------+-----------+-----------+-----------+
| 100    | 10s       | 1 hour      | 6.12M      | ~1.8 GiB  | ~719 MiB  | ~69 MiB   |
+--------+-----------+-------------+------------+-----------+-----------+-----------+
| 1000   | 10s       | 24 hours    | 1469M      | ~427 GiB  | ~168 GiB  | ~16 GiB   |
+--------+-----------+-------------+------------+-----------+-----------+-----------+

.. _otlp_track_definition:

Track Definition
----------------

OTLP corpora use ``"source-format": "otlp-metrics"`` in the track definition. A minimal track has the following ``track.json`` file.

.. code-block:: json

  {
    "version": 2,
    "description": "OTLP metrics ingest benchmark",
    "data-streams": [
      {
        "name": "metrics-hostmetricsreceiver.otel-default"
      }
    ],
    "component-templates": [
      {
        "name": "metrics-otel@custom",
        "template": "metrics-otel@custom.template.json",
        "template-path": "component_template"
      }
    ],
    "corpora": [
      {
        "name": "otlp-metrics",
        "documents": [
          {
            "source-format": "otlp-metrics",
            "source-file": "metrics.otlp.json",
            "uncompressed-bytes": 190950696,
            "document-count": 360
          }
        ]
      }
    ],
    "operations": [
      {
        "name": "ingest-otlp-metrics",
        "operation-type": "otlp-ingest",
        "corpora": "otlp-metrics",
        "gzip": true
      }
    ],
    "challenges": [
      {
        "name": "default",
        "schedule": [
          {
            "name": "delete-data-streams",
            "operation": {
              "operation-type": "delete-data-stream"
            }
          },
          {
            "name": "delete-component-templates",
            "operation": {
              "operation-type": "delete-component-template"
            }
          },
          {
            "name": "create-all-templates",
            "operation": {
              "operation-type": "create-component-template",
              "request-params": {
                "create": "true"
              }
            }
          },
          {
            "name": "create-data-stream",
            "operation": {
              "operation-type": "create-data-stream",
              "include-in-reporting": false
            }
          },
          {
            "operation": "ingest-otlp-metrics",
            "clients": 4
          }
        ]
      }
    ]
  }

The ``track.json`` file references ``metrics-otel@custom.template.json`` file with the following content. Note how ``index.time_series`` settings are used to accept timestamps from statically defined time range which matches ``metricsgenreceiver`` configuration.

.. code-block:: json

  {
    "name": "metrics-otel@custom",
    "component_template": {
      "template": {
        "lifecycle": {},
        "settings": {
          "index": {
            "time_series": {
              "start_time": "2025-01-01T00:00:00Z",
              "end_time": "2025-01-07T01:00:00Z"
            }
          }
        }
      }
    }
  }  

Corpus document fields
~~~~~~~~~~~~~~~~~~~~~~

The following fields are relevant for ``otlp-metrics`` corpora. See :ref:`track_corpora` for the full corpus syntax.

.. list-table::
   :widths: 20 10 70
   :header-rows: 1

   * - Field
     - Required
     - Description
   * - ``source-format``
     - Yes
     - Must be ``otlp-metrics`` to enable OTLP metrics handling.
   * - ``source-file``
     - Yes
     - Name of the OTLP JSON file produced by ``metricsgenreceiver`` (one ``ExportMetricsServiceRequest`` per line), relative to the corpus data directory. It may be an archive (e.g. ``metrics.otlp.json.zst``) containing exactly one file named like the archive without its extension; Rally decompresses it before conversion.
   * - ``document-count``
     - Yes
     - Number of records (lines) in the source file. Used to verify a local or downloaded file.
   * - ``base-url``
     - No
     - Location to download corpus files from. Rally first tries to download pre-built protobuf files (``<source-file>.pb`` or ``<source-file>.pbgz``, compressed with the same archive extension as ``source-file`` if any, plus the ``.offset`` index) and downloads ``source-file`` only if they are not available. Can also be specified at ``corpus`` level.
   * - ``compressed-bytes``
     - No
     - Size in bytes of the archive given in ``source-file``. Used to verify a local or downloaded archive.
   * - ``uncompressed-bytes``
     - No
     - Size in bytes of the uncompressed JSON file. Used to verify the file after download or decompression. A local file of a different size is downloaded again or, for data bundled with the track, rejected.

The ``otlp-ingest`` operation
------------------------------

See :ref:`operation_otlp_ingest` for the full operation syntax.

.. list-table::
   :widths: 25 10 15 50
   :header-rows: 1

   * - Parameter
     - Required
     - Default
     - Description
   * - ``corpora``
     - No
     - all corpora
     - Name of the corpus to read from. Must match a corpus name in the track definition. The selected corpora must contain exactly one ``otlp-metrics`` document set, otherwise Rally reports an error.
   * - ``gzip``
     - No
     - ``false``
     - When ``true``, Rally pre-compresses each record during ``prepare-track`` and stores them in a ``.pbgz`` file. At race time the compressed bytes are sent verbatim with ``Content-Encoding: gzip``, so no runtime compression overhead occurs on the hot path. This matches what a real OTel Collector sends when ``compression: gzip`` is set (see :ref:`otlp_direct_ingest`). Recommended for realistic benchmarks and for clusters that support gzip ingest.
   * - ``retries-on-error``
     - No
     - ``5``
     - Number of retries on transient errors (HTTP 429, 502, 503, 504, connection errors).
   * - ``retry-wait-period``
     - No
     - ``0.5``
     - Base backoff in seconds for exponential backoff with full jitter (capped at 30s). Attempts wait up to ``0.5s``, ``1s``, ``2s``, ``4s`` … between retries.
   * - ``request-timeout``
     - No
     - (none)
     - Client-side timeout in seconds per request.
   * - ``looped``
     - No
     - ``false``
     - When ``true``, cycles through the corpus indefinitely instead of stopping after one pass. Useful for sustained-throughput benchmarks.

Retry behaviour
~~~~~~~~~~~~~~~

The runner distinguishes three error types, which appear in the ``error-type`` field of failed operation results:

* **backpressure** — HTTP 429 (Too Many Requests). Elasticsearch is overloaded; retried with exponential backoff.
* **transport** — HTTP 502/503/504 or a connection-level error. Likely a transient network or gateway issue; retried with exponential backoff.
* **rejected** — Any other HTTP 4xx error (e.g., 400, 401, 403). The request was rejected as invalid; not retried.

.. note::

   The runner disables the elastic-transport client's built-in retry logic (``max_retries=0`` on the transport) so that Rally's own backoff loop has full control. Without this, the transport would fire four rapid back-to-back retries on a 429 before Rally's backoff could react, which would hammer an already-overloaded cluster.

Corpus Preparation
------------------

When you run ``esrally prepare-track`` (or the first time ``esrally race`` is called with a new corpus), Rally converts the OTLP JSON file to a binary protobuf file. To convert locally, first install the optional dependencies with ``python -m pip install 'esrally[otlp]'``. Conversion is a one-time cost per machine.

The preparation strategy is:

1. **Already valid** — If ``metrics.otlp.json.pb`` exists and is newer than the source JSON, skip conversion entirely.
2. **Download pre-built** — If the track specifies a remote corpus URL, Rally tries to download ``metrics.otlp.json.pb`` (or ``metrics.otlp.json.pb.zst``) directly, avoiding the need to download the larger JSON source.
3. **Convert locally** — If the JSON is present locally, Rally converts it to ``.pb`` using parallel worker processes.

Binary protobuf format (``.pb``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``.pb`` file is a sequence of length-prefixed records:

* 4-byte big-endian ``uint32`` — the byte length of the following record
* N bytes — a serialized ``ExportMetricsServiceRequest`` protobuf message

A companion ``.pb.offset`` index file maps record numbers to byte offsets for efficient multi-client partitioning without scanning the whole file. One offset entry is written every 1000 records.

Gzip protobuf format (``.pbgz``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When ``gzip: true`` is set on an ``otlp-ingest`` operation, Rally produces a ``.pbgz`` file during ``prepare-track``. The on-disk layout is identical to ``.pb`` — length-prefixed records — but each record payload is an independent gzip stream rather than a raw protobuf message:

* 4-byte big-endian ``uint32`` — the byte length of the **compressed** payload
* N bytes — a gzip stream containing the serialized ``ExportMetricsServiceRequest``

Each record is compressed individually (not the whole file), so the runner can stream records from the file and POST them directly without any decompression or recompression. The request is sent with ``Content-Type: application/x-protobuf`` and ``Content-Encoding: gzip``, which Elasticsearch decompresses before parsing.

This is byte-for-byte equivalent to what an OTel Collector sends when configured with ``compression: gzip, encoding: proto``.

Compression is applied at ``compresslevel=6`` with a fixed ``mtime=0`` in the gzip header, making the output deterministic across runs (the same JSON input always produces the same ``.pbgz``).

The same ``.pbgz.offset`` companion index is generated alongside the ``.pbgz``, so multi-client partitioning works identically to the uncompressed case.

**When to use gzip:**

* Use ``gzip: true`` (i.e., ``.pbgz``) for realistic benchmarks — a real OTel Collector always sends gzip-compressed protobuf. This also reduces corpus file sizes on disk by 50–80% compared to ``.pb``.
* Use ``gzip: false`` (i.e., ``.pb``) when you specifically want to measure Elasticsearch's raw ingest throughput without the decompression overhead, or when the cluster does not support ``Content-Encoding: gzip`` on the OTLP endpoint.

If the same corpus is used by two operations — one with ``gzip: true`` and one with ``gzip: false`` — Rally produces both ``.pb`` and ``.pbgz`` during a single ``prepare-track`` run.

Tuning parallel conversion
~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, conversion uses all available CPU cores. Memory usage grows with the number of workers and the size of the largest records in the corpus. To cap it (e.g., in memory-constrained environments), set the ``RALLY_OTLP_CONVERSION_WORKERS`` environment variable::

    RALLY_OTLP_CONVERSION_WORKERS=4 esrally prepare-track ...

Multi-client Partitioning
--------------------------

When a challenge runs ``otlp-ingest`` with multiple clients (``"clients": N``), Rally splits the corpus across clients so each client reads a distinct, non-overlapping slice of the records. Partitioning uses the ``.pb.offset`` index for O(1) seek to each client's starting record.

Each client's slice size is ``floor(total_records / N)``; the final client gets any remainder. This guarantees each record is sent exactly once per pass across all clients.

Metrics
-------

Each ``otlp-ingest`` operation records the following metrics in Rally's results:

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Metric
     - Description
   * - ``throughput``
     - Requests per second delivered to Elasticsearch.
   * - ``latency``
     - Request latency, including retry waits.
   * - ``service_time``
     - Request service time, including retry waits.
   * - ``request-size-bytes``
     - Payload size of each request in bytes.
   * - ``error-type``
     - Error classification on failure: ``backpressure``, ``transport``, or ``rejected``.

End-to-end Example
------------------

The following walks through generating a small corpus and running a benchmark against a local Elasticsearch with OTLP ingest enabled.

**Step 1 — Generate corpus data**

Follow :ref:`otlp_install_metricsgen` and :ref:`otlp_generate_corpus` to produce ``metrics.otlp.json`` corpus file.

**Step 2 — Create a track**

Place ``metrics.otlp.json`` in Rally's data directory for the corpus. The directory name must match the ``name`` of the corpus in ``track.json`` (``otlp-metrics`` here).

::

    mkdir -p ~/.rally/benchmarks/data/otlp-metrics
    cp corpus/metrics.otlp.json ~/.rally/benchmarks/data/otlp-metrics/

Then create ``~/rally-tracks/otlp-test/track.json`` and ``~/rally-tracks/otlp-test/metrics-otel@custom.template.json`` from :ref:`otlp_track_definition`.

**Step 3 — Prepare the track**

::

    esrally prepare-track --track-path=~/rally-tracks/otlp-test

This converts ``metrics.otlp.json`` to ``metrics.otlp.json.pbgz`` (one-time cost).

**Step 4 — Run the benchmark**

::

    esrally race --track-path=~/rally-tracks/otlp-test \
      --distribution-version=9.5.4 \
      --target-hosts=127.0.0.1:9200 \
      --car="defaults,x-pack-security" \
      --client-options="basic_auth_user:'rally',basic_auth_password:'rally-password',use_ssl:true,verify_certs:false"

Rally creates Elasticsearch 9.5.4 installation, and streams the protobuf corpus from four parallel clients to ``/_otlp/v1/metrics``, reports throughput and latency, and retries automatically on backpressure.
