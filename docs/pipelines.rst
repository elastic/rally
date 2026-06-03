Pipelines
=========

A pipeline is a series of steps that are performed to get benchmark results. This is *not* intended to customize the actual benchmark but rather what happens before and after a benchmark.

An example will clarify the concept: If you want to benchmark a binary distribution of Elasticsearch, Rally has to download a distribution archive, decompress it, start Elasticsearch and then run the benchmark. However, if you want to benchmark a source build of Elasticsearch, it first has to build a distribution using the Gradle Wrapper. So, in both cases, different steps are involved and that's what pipelines are for.

You can get a list of all pipelines with ``esrally list pipelines``::

    Available pipelines:

    Name                     Description
    -----------------------  ---------------------------------------------------------------------------------------------
    from-sources             Builds and provisions Elasticsearch, runs a benchmark and reports results.
    from-distribution        Downloads an Elasticsearch distribution, provisions it, runs a benchmark and reports results.
    benchmark-only           Assumes an already running Elasticsearch instance, runs a benchmark and reports results

benchmark-only
~~~~~~~~~~~~~~

This is intended if you want to provision a cluster by yourself. Do not use this pipeline unless you are absolutely sure you need to. As Rally has not provisioned the cluster, results are not easily reproducable and it also cannot gather a lot of metrics (like CPU usage).

To benchmark a cluster, you also have to specify the hosts to connect to. An example invocation::

    esrally race --track=geonames --pipeline=benchmark-only --target-hosts=search-node-a.intranet.acme.com:9200,search-node-b.intranet.acme.com:9200

To benchmark multiple clusters simultaneously, add ``--multi-cluster``. See :ref:`multi-cluster mode <multi_cluster_mode>`.

from-distribution
~~~~~~~~~~~~~~~~~

This pipeline allows to benchmark an official Elasticsearch distribution which will be automatically downloaded by Rally. An example invocation::

    esrally race --track=geonames --pipeline=from-distribution --distribution-version={ES_CLIENT_VER}

The version numbers have to match the name in the download URL path.

You can also benchmark Elasticsearch snapshot versions by specifying the snapshot repository::

    esrally race --track=geonames --pipeline=from-distribution --distribution-version=8.0.0-SNAPSHOT --distribution-repository=snapshot

However, this feature is mainly intended for continuous integration environments and by default you should just benchmark official distributions.

.. note::

   This pipeline is just mentioned for completeness but Rally will autoselect it for you. All you need to do is to define the ``--distribution-version`` flag.

.. _pipelines_from-sources:

from-sources
~~~~~~~~~~~~

You should use this pipeline when you want to build and benchmark Elasticsearch from sources.

Remember that you also need git installed. If that's not the case you'll get an error. An example invocation::

    esrally race --track=geonames --pipeline=from-sources --revision=latest

You have to specify a :ref:`revision <clr_revision>`.

.. note::

   This pipeline is just mentioned for completeness but Rally will automatically select it for you. All you need to do is to define the ``--revision`` flag.

Artifacts are cached for seven days by default in ``~/.rally/benchmarks/distributions/src``. Artifact caching can be configured with the following sections in the section ``source`` in the configuration file in ``~/.rally/rally.ini``:

* ``cache`` (default: ``True``): Set to ``False`` to disable artifact caching.
* ``cache.days`` (default: ``7``): The maximum age in days of an artifact before it gets evicted from the artifact cache.

.. _multi_cluster_mode:

Multi-cluster mode
------------------

Multi-cluster mode lets you run the same benchmark against multiple Elasticsearch clusters simultaneously. It is enabled with the ``--multi-cluster`` flag and is independent of which pipeline you choose — you will typically combine it with ``benchmark-only``.

For each task in the schedule, Rally runs that task against **all clusters in parallel** before advancing to the next task. Results are stored in a single race with a ``cluster`` field on each result document, and the terminal summary shows a side-by-side table with one column per cluster.

You must specify two or more named clusters in ``--target-hosts`` using JSON format, with matching keys in ``--client-options``. See :ref:`target-hosts advanced topics <command_line_reference_advanced_topics>` for the full format reference.

**Example**

 ::

   esrally race --track=geonames --pipeline=benchmark-only --multi-cluster \
     --target-hosts='{"cluster-a":["host1:9200"],"cluster-b":["host2:9200"]}' \
     --client-options='{"cluster-a":{"timeout":60},"cluster-b":{"timeout":60}}'

.. note::

   Custom runners work in multi-cluster mode without any changes. For each cluster Rally calls your runner once with a single ``default`` client scoped to that cluster — the same interface as single-cluster mode.
