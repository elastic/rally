Rally and Elastic Serverless
============================

Overview
--------

`Elastic Serverless <https://docs.elastic.co/serverless>`_ (later called "Serverless") is a fully managed solution hiding the complexity of Elasticsearch clusters including details such as nodes, shards, data tiers, and scaling. The Elasticsearch API used in Serverless is a subset of the API available in traditional Elasticsearch installations, requiring Rally modifications to support it.

This section explains what was changed in Rally, and what track modifications are required to move existing workloads to Serverless.

.. note::

    Elastic Serverless is in technical preview and is subject to change. Released Rally versions might be lagging behind those changes. If you run into Rally errors when used with Serverless, please try the latest :ref:`development version<dev_installation_instructions>` before reaching out for `help <https://github.com/elastic/rally#getting-help>`_.

Rally
-----

Rally determines if it is benchmarking a Serverless cluster by looking at the ``version.build_flavor`` attribute returned by Elasticsearch root endpoint (``/``). If Serverless is confirmed, Rally *automatically* skips all tasks based on :ref:`operations<track_operations>` status in Serverless. For instance, a task with :ref:`node-stats<operation_node_stats>` operation is skipped because ``/_nodes/stats`` Elasticsearch API is unavailable in Serverless. The status determination is hardcoded into Rally, so if the API status changes in Elasticsearch, Rally needs to be updated too. The same skip logic applies to :doc:`/telemetry`.

.. note::

    Elasticsearch Serverless API is not publicly documented yet. Today, the ultimate documentation is Elasticsearch `source code <https://github.com/elastic/elasticsearch>`_. Please look for ``@ServerlessScope(Scope.PUBLIC)`` annotation to determine if a given REST action is available in Serverless.

The automatic task skip can be overridden through :ref:`run-on-serverless<track_schedule>` property::

  {
    [..]
    "schedule": [
      {
        "operation": "node-stats",
        "clients": 1,
        "run-on-serverless": True
      },
      [..]
    ]
  }

The Serverless detection and the skip manifests through the following messages on the console::

  % esrally race --track="geonames" --target-hosts=${ES_HOST}:443 --pipeline=benchmark-only --client-options="use_ssl:true,api_key:${ES_API_KEY}" --on-error=abort --test-mode
  [..]
  [INFO] Race id is [d9d82d3f-dd74-4af0-90ea-8bdd449c824c]
  [INFO] Detected Elasticsearch Serverless mode with operator=[False].
  [INFO] Excluding [check-cluster-health], [force-merge], [wait-until-merges-finish], [index-stats], [node-stats] as challenge [append-no-conflicts] is run on serverless.
  [INFO] Racing on track [geonames], challenge [append-no-conflicts] and car ['external'] with version [8.11.0].
  [..]

The automatic skip does not apply to the following tasks:

- tasks specified in ``parallel`` elements,
- tasks based on ``raw-request`` and ``composite`` operations,
- tasks based on :ref:`custom runners<adding_tracks_custom_runners>`.

If there is a need to skip or modify behaviour of any of the above when benchmarking Serverless cluster, this can be achieved through modification of a track definition (see next section).

Furthermore, some operations behave differently when used in the Serverless context. Specifically:

- ``delete-composable-templates`` ignores ``delete-matching-indices`` property,
- ``cache`` property in all search operations (``search``, ``paginated-search``, ``scroll-search``, ``composite-agg``) defaults to ``false``.

Tracks
------
Track files use Jinja2 as a template language with a few :ref:`extensions<advanced_extensions>`. To accommodate for Serverless, ``build_flavor`` global variable was added. It resolves to ``serverless`` when benchmarking a Serverless cluster. The primary use case for this variable is conditional exclusion of index settings which are unavailable or hidden from users in Serverless.

.. note::

    The index setting status in Serverless is not publicly documented yet. Today, the ultimate documentation is Elasticsearch `source code <https://github.com/elastic/elasticsearch>`_. Please look for ``Property.ServerlessPublic`` property to determine if a given index setting is publicly available in Serverless.

In the following example index JSON file, index settings are only applied when non-Serverless cluster is benchmarked::

  {
    "settings": {
      {%- if build_flavor != "serverless" -%}
      "index.number_of_shards": {{number_of_shards | default(5)}},
      "index.number_of_replicas": {{number_of_replicas | default(0)}},
      "index.requests.cache.enable": false
      {%- endif -%}
    },
    [..]
  }

A similar approach can be used to express more nuanced benchmark variations. In the following example ``operation-1`` is used when benchmarking a non-Serverless cluster, whereas ``operation-2`` when benchmarking a Serverless cluster::

  {
    "schedule": [
      {
        "parallel": {
          "tasks": [
            {%- if build_flavor != "serverless" -%}
            {
              "name": "operation-1",
              [..]
            },
            {%- else -%}
            {
              "name": "operation-2",
              [..]
            },
            {%- endif -%}
            {
              "name": "operation-3",
              [..]
            },
          ]
        }
      }
    ]
  }

.. note::

    In addition to ``build_flavor``, Rally provides the ``serverless_operator`` Jinja2 global variable (`example <https://github.com/elastic/rally-tracks/blob/324996e627f11bb6af286970dedcd82d25c1d0b5/geonames/index.json#L3-L8>`_). It is for internal Elastic use only. There is no point in using it in custom track definitions.

Hints
-----

- Elasticsearch Serverless should provide a clear message indicating unavailable API endpoint or index setting which can happen with custom tracks or some of the `public tracks <https://github.com/elastic/rally-tracks>`_. To surface this error fail fast by using ``--on-error="abort"`` command line option when running Rally. 
- Consider using ``--track-params="post_ingest_sleep:true"`` track parameter when benchmarking with `public tracks <https://github.com/elastic/rally-tracks>`_. Consult track README files to confirm availability of this parameter. The intention of the parameter is to introduce an extra delay between ingesting the data and running the search for better result stability. In traditional non-Serverless clusters this role is fulfilled by force merge operation, but explicit force merge action is not available in Serverless. When the post-ingest sleep is enabled, its duration is controlled by ``post_ingest_sleep_duration`` which defaults to 30s.



