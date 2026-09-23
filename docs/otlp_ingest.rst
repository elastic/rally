OTLP Metrics Ingest
===================

Rally supports benchmarking `Elasticsearch's native OTLP ingest endpoint <https://www.elastic.co/docs/solutions/observability/apm/open-telemetry>`_ (``/_otlp/v1/metrics``).
This lets you measure how quickly Elasticsearch can accept a realistic stream of OpenTelemetry metrics delivered in binary protobuf format, with optional per-record gzip compression.

Overview
--------

The OTLP ingest feature introduces a new corpus format (``otlp-proto``) and a new operation type (``otlp-ingest``).
Instead of bulk-indexing newline-delimited JSON, Rally sends pre-serialized ``ExportMetricsServiceRequest`` protobuf messages directly to the OTLP endpoint — matching exactly what a real OpenTelemetry Collector would send.

The data flow at a glance:

1. **Generate** — Use ``metricsgenreceiver`` to produce an OTLP JSON corpus file (``metrics.otlp.json``).
2. **Prepare** — During ``prepare-track``, Rally converts the JSON corpus to a binary protobuf file (``metrics.otlp.pb`` or ``metrics.otlp.pbgz``). This is a one-time conversion per machine.
3. **Race** — During ``race``, Rally streams records from the ``.pb`` file directly to Elasticsearch's ``/_otlp/v1/metrics`` endpoint.

Generating Corpus Data with ``metricsgenreceiver``
--------------------------------------------------

`metricsgenreceiver <https://github.com/elastic/metricsgenreceiver>`_ is an OpenTelemetry Collector receiver that generates realistic metric streams from configurable scenarios (e.g., ``builtin/hostmetrics``, ``builtin/kubeletstats-pod``).
It has two output modes:

* **File export** — writes OTLP JSON to disk for later use as a Rally corpus.
* **Direct ingest** — sends binary protobuf requests straight to Elasticsearch's ``/_otlp`` endpoint, bypassing Rally entirely. Useful for smoke-testing the stack or one-off data seeding.

.. _otlp_install_metricsgen:

Installing metricsgenreceiver
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Download a pre-built binary from the `releases page <https://github.com/elastic/metricsgenreceiver/releases>`_, or build from source using ``ocb``::

    curl --proto '=https' --tlsv1.2 -fL -o ocb \
      https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/cmd%2Fbuilder%2Fv0.139.0/ocb_0.139.0_darwin_arm64
    chmod +x ocb
    ./ocb --config builder-config.yaml

This produces ``./otelcol-dev/otelcol``.

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
    ./otelcol-dev/otelcol --config otelcol.yaml

When ``exit_after_end: true`` is set, the collector exits automatically once the configured time range is exhausted. The resulting ``metrics.otlp.json`` file is a newline-delimited sequence of OTLP JSON records, each representing one ``ExportMetricsServiceRequest`` batch.

.. _otlp_direct_ingest:

Sending directly to Elasticsearch
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To send metrics directly to Elasticsearch instead of (or in addition to) writing a file, add the ``otlphttp/elasticsearch`` exporter to the pipeline::

    extensions:
      basicauth/client:
        client_auth:
          username: elastic
          password: changeme

    exporters:
      otlphttp/elasticsearch:
        compression: gzip
        encoding: proto
        endpoint: "https://localhost:9200/_otlp"
        auth:
          authenticator: basicauth/client
        sending_queue:
          enabled: true
          block_on_overflow: true
          queue_size: 5
          num_consumers: 5
        tls:
          insecure_skip_verify: true

    service:
      extensions:
        - basicauth/client
      pipelines:
        metrics:
          receivers: [metricsgen]
          processors: [batch]
          exporters: [otlphttp/elasticsearch]   # or [file, otlphttp/elasticsearch] for both

Key exporter settings:

* **``encoding: proto``** — sends binary protobuf (``application/x-protobuf``) rather than OTLP JSON. This is the wire format Elasticsearch's ``/_otlp`` endpoint expects.
* **``compression: gzip``** — gzip-compresses each request body, equivalent to what Rally sends when ``gzip: true`` is set on an ``otlp-ingest`` operation. This is the standard OTel Collector behaviour and generally improves throughput.
* **``block_on_overflow: true``** — prevents the collector from dropping records if the send queue fills up. Useful when generating data faster than Elasticsearch can ingest it.
* **``num_consumers``** — number of parallel senders from the queue to Elasticsearch. Increase this to saturate high-throughput clusters.

.. note::

   The ``endpoint`` must include the ``/_otlp`` path prefix. Elasticsearch routes ``/_otlp/v1/metrics`` to the OTLP metrics ingest handler.

This mode is useful for quick manual testing, but for reproducible benchmarking use the file exporter to capture the corpus first, then replay it through Rally.

Tuning corpus size
~~~~~~~~~~~~~~~~~~

Adjust the following parameters to produce different corpus sizes:

* **``scale``** — number of simulated instances (e.g., hosts, pods). Higher scale = more time series = larger file.
* **``interval``** — scrape interval. Smaller interval = more data points per host per hour.
* **``start_time`` / ``end_time``** — time range. Longer range = more records.
* **``scenarios``** — swap in ``builtin/kubeletstats-pod``, ``builtin/tsbs-devops``, etc. for different metric shapes.

Typical corpus sizes for ``builtin/hostmetrics``:

+--------+-----------+-------------------+------------------+
| Scale  | Interval  | Duration          | Approx file size |
+========+===========+===================+==================+
| 10     | 10s       | 1 hour            | ~15 MB           |
+--------+-----------+-------------------+------------------+
| 100    | 10s       | 1 hour            | ~150 MB          |
+--------+-----------+-------------------+------------------+
| 1000   | 10s       | 24 hours          | ~3.6 GB          |
+--------+-----------+-------------------+------------------+

Track Definition
----------------

OTLP corpora use ``"source-format": "otlp-proto"`` in the track definition. A minimal track looks like this:

.. code-block:: json

    {
      "version": 2,
      "description": "OTLP metrics ingest benchmark",
      "corpora": [
        {
          "name": "otlp-metrics",
          "documents": [
            {
              "source-format": "otlp-proto",
              "source-file": "metrics.otlp.json",
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
              "operation": "ingest-otlp-metrics",
              "clients": 4,
              "target-throughput": 100
            }
          ]
        }
      ]
    }

Corpus document fields
~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 20 10 70
   :header-rows: 1

   * - Field
     - Required
     - Description
   * - ``source-format``
     - Yes
     - Must be ``"otlp-proto"`` to enable OTLP handling.
   * - ``source-file``
     - Yes
     - Path to the OTLP JSON file produced by ``metricsgenreceiver``.
   * - ``document-count``
     - Yes
     - Number of records (lines) in the source file. Used for progress reporting and partitioning.
   * - ``compressed-bytes``
     - No
     - Size of the compressed source archive, if hosted remotely.
   * - ``uncompressed-bytes``
     - No
     - Uncompressed size of the source file. Used for download progress reporting.

The ``otlp-ingest`` operation
------------------------------

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
     - Name of the corpus to read from. Must match a corpus name in the track definition.
   * - ``gzip``
     - No
     - ``false``
     - When ``true``, Rally pre-compresses each record during ``prepare-track`` and stores them in a ``.pbgz`` file. At race time the compressed bytes are sent verbatim with ``Content-Encoding: gzip``, so no runtime compression overhead occurs on the hot path. This matches what a real OTel Collector sends when ``compression: gzip`` is set (see :ref:`otlp_direct_ingest`). Recommended for realistic benchmarks and for clusters that support gzip ingest.
   * - ``endpoint``
     - No
     - ``/_otlp/v1/metrics``
     - OTLP endpoint path on Elasticsearch.
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

* **``backpressure``** — HTTP 429 (Too Many Requests). Elasticsearch is overloaded; retried with exponential backoff.
* **``transport``** — HTTP 502/503/504 or a connection-level error. Likely a transient network or gateway issue; retried with exponential backoff.
* **``rejected``** — Any other HTTP 4xx error (e.g., 400, 401, 403). The request was rejected as invalid; not retried.

.. note::

   The runner disables the elastic-transport client's built-in retry logic (``max_retries=0`` on the transport) so that Rally's own backoff loop has full control. Without this, the transport would fire four rapid back-to-back retries on a 429 before Rally's backoff could react, which would hammer an already-overloaded cluster.

Corpus Preparation (``prepare-track``)
---------------------------------------

When you run ``esrally prepare-track`` (or the first time ``esrally race`` is called with a new corpus), Rally converts the OTLP JSON file to a binary protobuf file. This is a one-time cost per machine.

The preparation strategy is:

1. **Already valid** — If ``metrics.otlp.pb`` exists and is newer than the source JSON, skip conversion entirely.
2. **Download pre-built** — If the track specifies a remote corpus URL, Rally tries to download ``metrics.otlp.pb`` (or ``metrics.otlp.pb.zst``) directly, avoiding the need to download the larger JSON source.
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

By default, conversion uses all available CPU cores. To cap this (e.g., in memory-constrained environments), set the ``RALLY_OTLP_CONVERSION_WORKERS`` environment variable::

    RALLY_OTLP_CONVERSION_WORKERS=4 esrally prepare-track ...

Each worker uses approximately 200 MB of memory, with up to four in-flight batches at once (~500 MB additional). Total RAM budget: roughly ``(workers + 4) × 200 MB``.

Multi-client Partitioning
--------------------------

When a challenge runs ``otlp-ingest`` with multiple clients (``"clients": N``), Rally splits the corpus across clients so each client reads a distinct, non-overlapping slice of the records. Partitioning uses the ``.pb.offset`` index for O(1) seek to each client's starting record; if the index is absent it is generated on first use (one-time cost).

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
     - End-to-end latency per request, including retries.
   * - ``service_time``
     - Time for a single attempt (excluding retry waits).
   * - ``request-size-bytes``
     - Payload size of each request in bytes.
   * - ``error-type``
     - Error classification on failure: ``backpressure``, ``transport``, or ``rejected``.

End-to-end Example
------------------

The following walks through generating a small corpus and running a benchmark against a local Elasticsearch with OTLP ingest enabled.

**Step 1 — Generate corpus data**

The ``minimised.yaml`` config from the ``metricsgenreceiver`` repository generates a small corpus suitable for quick tests (10 hosts, 10s interval, 1 hour = 360 records)::

    ./otelcol-dev/otelcol --config minimised.yaml

``minimised.yaml`` defines both a ``file`` exporter (for Rally corpus generation) and an ``otlphttp/elasticsearch`` exporter (for direct ingest). The active pipeline uses the file exporter by default — swap the ``exporters`` line in the ``service.pipelines.metrics`` section to send directly to Elasticsearch instead::

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

    extensions:
      basicauth/client:
        client_auth:
          username: elastic
          password: changeme

    exporters:
      file:
        path: ./corpus/metrics.otlp.json
      otlphttp/elasticsearch:
        compression: gzip      # send gzip-compressed protobuf, matching Rally's gzip: true mode
        encoding: proto
        endpoint: "https://localhost:9200/_otlp"
        auth:
          authenticator: basicauth/client
        sending_queue:
          enabled: true
          block_on_overflow: true
          queue_size: 5
          num_consumers: 5
        tls:
          insecure_skip_verify: true

    service:
      extensions:
        - basicauth/client
      pipelines:
        metrics:
          receivers: [metricsgen]
          processors: [batch]
          exporters: [file]                    # change to [otlphttp/elasticsearch] to ingest directly

This writes ``./corpus/metrics.otlp.json`` (360 lines).

**Step 2 — Create a track**

Place ``metrics.otlp.json`` in Rally's data directory for the corpus::

    mkdir -p ~/.rally/benchmarks/data/otlp-metrics
    cp corpus/metrics.otlp.json ~/.rally/benchmarks/data/otlp-metrics/

The directory name must match the ``"name"`` of the corpus in ``track.json`` (``"otlp-metrics"`` here). Then create ``~/rally-tracks/otlp-test/track.json``::

    {
      "version": 2,
      "description": "OTLP metrics ingest benchmark",
      "corpora": [
        {
          "name": "otlp-metrics",
          "documents": [
            {
              "source-format": "otlp-proto",
              "source-file": "metrics.otlp.json",
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
              "operation": "ingest-otlp-metrics",
              "clients": 4,
              "target-throughput": 100
            }
          ]
        }
      ]
    }

**Step 3 — Prepare the track**

::

    esrally prepare-track --track-path=~/rally-tracks/otlp-test \
      --target-hosts=localhost:9200

This converts ``metrics.otlp.json`` → ``metrics.otlp.pbgz`` (one-time cost).

**Step 4 — Run the benchmark**

::

    esrally race --track-path=~/rally-tracks/otlp-test \
      --target-hosts=localhost:9200 \
      --client-options="basic_auth_user:'elastic',basic_auth_password:'changeme',use_ssl:true,verify_certs:false" \
      --challenge=default

Rally streams the protobuf corpus from four parallel clients to ``/_otlp/v1/metrics``, reports throughput and latency, and retries automatically on backpressure.
