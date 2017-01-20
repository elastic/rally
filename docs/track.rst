Track Reference
---------------

Definition
==========

A track is the description of one ore more benchmarking scenarios with a specific document corpus. It defines for example the involved indices, data files and which operations are invoked. Its most important attributes are:

* One or more indices, each with one or more types
* The queries to issue
* Source URL of the benchmark data
* A list of steps to run, which we'll call "challenge", for example indexing data with a specific number of documents per bulk request or running searches for a defined number of iterations.

Tracks are written as JSON files and are kept in a separate track repository, which is located at https://github.com/elastic/rally-tracks. This repository has separate branches for different Elasticsearch versions and Rally will check out the appropriate branch based on the command line parameter ``--distribution-version``. If the parameter is missing, Rally will assume by default that you are benchmarking the latest version of Elasticsearch and will checkout the ``master`` branch of the track repository.

Anatomy of a track
==================

A track JSON file consists of the following sections:

* indices
* operations
* challenges

In the ``indices`` section you describe the relevant indices. Rally can auto-manage them for you: it can download the associated data files, create and destroy the index and apply the relevant mappings. Sometimes, you may want to have full control over the index. Then you can specify ``"auto-managed": false`` on an index. Rally will then assume the index is already present. However, there are some disadvantages with this approach. First of all, this can only work if you set up the cluster by yourself and use the pipeline ``benchmark-only``. Second, the index is out of control of Rally, which means that you need to keep track for yourself of the index configuration. Third, it does not play nice with the ``laps`` feature (which you can use to run multiple iterations). Usually, Rally will destroy and recreate all specified indices for each lap but if you use ``"auto-managed": false``, it cannot do that. As a consequence it will produce bogus metrics if your track specifies that Rally should run bulk-index operations (as you'll just overwrite existing documents from lap 2 on). So please use extra care if you don't let Rally manage the track's indices.

In the ``operations`` section you describe which operations are available for this track and how they are parametrized.

In the ``challenges`` section you describe one or more execution schedules for the operations defined in the ``operations`` block. Think of it as different scenarios that you want to test for your data set. An example challenge is to index with 2 clients at maximum throughput while searching with another two clients with 10 operations per second.

Track elements
==============

The track elements that are described here are defined in `Rally's JSON schema for tracks <https://github.com/elastic/rally/blob/master/esrally/resources/track-schema.json>`_. Rally uses this track schema to validate your tracks when it is loading them.

Each track defines a three info attributes:

* ``description`` (mandatory): A human-readable description of the track.
* ``short-description`` (mandatory): A shorter description of the track.
* ``data-url`` (optional): A http or https URL that points to the root path where Rally can obtain the corresponding data for this track. This element is not needed if data are only generated on the fly by a custom runner.

Example::

    {
        "short-description": "Standard benchmark in Rally (8.6M POIs from Geonames)",
        "description": "This test indexes 8.6M documents (POIs from Geonames, total 2.8 GB json) using 8 client threads and 5000 docs per bulk request against Elasticsearch",
        "data-url": "http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/geonames"
    }

meta
....

For each track, an optional structure, called ``meta`` can be defined. You are free which properties this element should contain.

This element can also be defined on the following elements:

* ``challenge``
* ``operation``
* ``task``

If the ``meta`` structure contains the same key on different elements, more specific ones will override the same key of more generic elements. The order from generic to most specific is:

1. track
2. challenge
3. operation
4. task

E.g. a key defined on a task, will override the same key defined on a challenge. All properties defined within the merged ``meta`` structure, will get copied into each metrics record.

indices
.......

The ``indices`` section contains a list of all indices that are used by this track. By default Rally will assume that it can destroy and create these indices at will.

Each index in this list consists of the following properties:

* ``name`` (mandatory): The name of the index.
* ``auto-managed`` (optional, defaults to ``true``): Controls whether Rally or the user takes care of creating / destroying the index. If this setting is ``false``, Rally will neither create nor delete this index but just assume its presence.
* types (optional): A list of types in this index.

Each type consists of the following properties:

* ``name`` (mandatory): Name of the type.
* ``mapping`` (mandatory): File name of the corresponding mapping file.
* ``documents`` (optional): File name of the corresponding documents that should be indexed. This file has to be compressed either as bz2, zip or tar.gz and must contain exactly one JSON file with the same name
* ``document-count`` (optional): Number of documents in the documents file. This number will be used to verify that all documents have been indexed successfully.
* ``compressed-bytes`` (optional): The size in bytes of the compressed document file. This number is used to show users how much data will be downloaded by Rally and also to check whether the download is complete.
* ``uncompressed-bytes`` (optional): The size in bytes of the documents file after decompression.

Example::

    "indices": [
        {
          "name": "geonames",
          "types": [
            {
              "name": "type",
              "mapping": "mappings.json",
              "documents": "documents.json.bz2",
              "document-count": 8647880,
              "compressed-bytes": 197857614,
              "uncompressed-bytes": 2790927196
            }
          ]
        }
    ]

templates
.........

The ``indices`` section contains a list of all index templates that Rally should create.

* ``name`` (mandatory): Index template name
* ``index-pattern`` (mandatory): Index pattern that matches the index template. This must match the definition in the index template file.
* ``delete-matching-indices`` (optional, defaults to ``true``): Delete all indices that match the provided index pattern before start of the benchmark.
* ``template`` (mandatory): Index template file name

Example::

    "templates": [
        {
            "name": "my-default-index-template",
            "index-pattern": "my-index-*",
            "delete-matching-indices": true,
            "template": "default-template.json"
        }
    ]

operations
..........

The ``operations`` section contains a list of all operations that are available later when specifying challenges. Operations define the static properties of a request against Elasticsearch whereas the ``schedule`` element defines the dynamic properties (such as the target throughput).

Each operation consists of the following properties:

* ``name`` (mandatory): The name of this operation. You can choose this name freely. It is only needed to reference the operation when defining schedules.
* ``operation-type`` (mandatory): Type of this operation. Out of the box, Rally supports the following operation types: ``index``, ``force-merge``, ``index-stats``, ``node-stats`` and ``search``. You can run arbitrary operations however by defining :doc:`custom runners </adding_tracks>`.

Depending on the operation type a couple of further parameters can be specified.

index
~~~~~

The operation type ``index`` supports the following properties:

* ``bulk-size`` (mandatory): Defines the bulk size in number of documents.
* ``batch-size`` (optional): Defines how many documents Rally will read at once. This is an expert setting and only meant to avoid accidental bottlenecks for very small bulk sizes (e.g. if you want to benchmark with a bulk-size of 1, you should set batch-size higher).
* ``pipeline`` (optional): Defines the name of an (existing) ingest pipeline that should be used (only supported from Elasticsearch 5.0).
* ``conflicts`` (optional): Type of index conflicts to simulate. If not specified, no conflicts will be simulated. Valid values are: 'sequential' (A document id is replaced with a document id with a sequentially increasing id), 'random' (A document id is replaced with a document id with a random other id).
* ``action-and-meta-data`` (optional): Defines how Rally should handle the action and meta-data line for bulk indexing. Valid values are 'generate' (Rally will automatically generate an action and meta-data line), 'none' (Rally will not send an action and meta-data line) or 'sourcefile' (Rally will assume that the source file contains a valid action and meta-data line).

Example::

    {
      "name": "index-append",
      "operation-type": "index",
      "bulk-size": 5000
    }

search
~~~~~~

The operation type ``search`` supports the following properties:

* ``index`` (optional): An `index pattern <https://www.elastic.co/guide/en/elasticsearch/reference/current/multi-index.html>`_ that defines which indices should be targeted by this query. Only needed if the ``index`` section contains more than one index. Otherwise, Rally will automatically derive the index to use. If you have defined multiple indices and want to query all of them, just specify ``"index": "_all"``.
* ``type`` (optional): Defines the type within the specified index for this query.
* ``cache`` (optional): Whether to use the query request cache. By default, Rally will define no value thus the default depends on the benchmark candidate settings and Elasticsearch version.
* ``body`` (mandatory): The query body.
* ``pages`` (optional): Number of pages to retrieve. If this parameter is present, a scroll query will be executed.
* ``results-per-page`` (optional):  Number of documents to retrieve per page for scroll queries.

Example::

    {
      "name": "default",
      "operation-type": "search",
      "body": {
        "query": {
          "match_all": {}
        }
      }
    }

challenges
..........

The ``challenges`` section contains a list of challenges which describe the benchmark scenarios for this data set. It can reference all operations that are defined in the ``operations`` section.

Each challenge consists of the following properties:

* ``name`` (mandatory): A descriptive name of the challenge. Should not contain spaces in order to simplify handling on the command line for users.
* ``description`` (mandatory): A human readable description of the challenge.
* ``default`` (optional): If true, Rally selects this challenge by default if the user did not specify a challenge on the command line. If your track only defines one challenge, it is implicitly selected as default, otherwise you need define ``"default": true`` on exactly one challenge.
* ``index-settings`` (optional): Defines the index settings of the benchmark candidate when an index is created. Note that these settings are only applied if the index is auto-managed.
* ``schedule`` (mandatory): Defines the concrete execution order of operations. It is described in more detail below.

schedule
~~~~~~~~

The ``schedule`` element contains a list of tasks that are executed by Rally. Each task consists of the following properties:

* ``clients`` (optional, defaults to 1): The number of clients that should execute a task concurrently.
* ``warmup-iterations`` (optional, defaults to 0): Number of iterations that Rally should execute to warmup the benchmark candidate. Warmup iterations will not show up in the measurement results.
* ``iterations`` (optional, defaults to 1): Number of measurement iterations that Rally executes. The command line report will automatically adjust the percentile numbers based on this number (i.e. if you just run 5 iterations you will not get a 99.9th percentile because we need at least 1000 iterations to determine this value precisely).
* ``warmup-time-period`` (optional, defaults to 0): A time period in seconds that Rally considers for warmup of the benchmark candidate. All response data captured during warmup will not show up in the measurement results.
* ``time-period`` (optional): A time period in seconds that Rally considers for measurement. Note that for bulk indexing you should usually not define this time period. Rally will just bulk index all documents and consider every sample after the warmup time period as measurement sample.
* ``target-throughput`` (optional): Defines the benchmark mode. If it is not defined, Rally assumes this is a throughput benchmark and will run the task as fast as it can. This is mostly needed for batch-style operations where it is more important to achieve the best throughput instead of an acceptable latency. If it is defined, it specifies the number of requests per second over all clients. E.g. if you specify ``target-throughput: 1000`` with 8 clients, it means that each client will issue 125 (= 1000 / 8) requests per second. In total, all clients will issue 1000 requests each second. If Rally reports less than the specified throughput then Elasticsearch simply cannot reach it.
* ``target-interval`` (optional): This is just ``1 / target-throughput`` (in seconds) and may be more convenient for cases where the throughput is less than one operation per second. Define either ``target-throughput`` or ``target-interval`` but not both (otherwise Rally will raise an error).

You should usually use time periods for batch style operations and iterations for the rest. However, you can also choose to run a query for a certain time period.

All tasks in the ``schedule`` list are executed sequentially in the order in which they have been defined. However, it is also possible to execute multiple tasks concurrently, by wrapping them in a ``parallel`` element. The ``parallel`` element defines of the following properties:

* ``clients`` (optional): The number of clients that should execute all tasks concurrently. It is usually not necessary to specify it because the number of clients can also be defined per task.
* ``warmup-iterations`` (optional, defaults to 0): Allows to define a different default value for all tasks of the ``parallel`` element.
* ``iterations`` (optional, defaults to 1): Allows to define a different default value for all tasks of the ``parallel`` element.
* ``tasks`` (mandatory): Defines a list of tasks that should be executed concurrently. Each task in the list can define the same properties as defined above.

.. note::

    ``parallel`` elements cannot be nested.


Examples
~~~~~~~~

Note that we do not show the operation definition in the examples below but you should be able to infer from the operation name what it is doing.

In this example Rally will run a bulk index operation unthrottled for one hour::


      "schedule": [
        {
          "operation": "bulk",
          "warmup-time-period": 120,
          "time-period": 3600,
          "clients": 8
        }
    ]

If we want to run a few queries concurrently, we can use the ``parallel`` element::


      "schedule": [
        {
          "parallel": {
            "tasks": [
              {
                "operation": "match-all",
                "clients": 4,
                "warmup-iterations": 50,
                "iterations": 100,
                "target-throughput": 50
              },
              {
                "operation": "term",
                "clients": 2,
                "warmup-iterations": 50,
                "iterations": 100,
                "target-throughput": 200
              },
              {
                "operation": "phrase",
                "clients": 2,
                "warmup-iterations": 50,
                "iterations": 100,
                "target-throughput": 200
              }
            ]
          }
        }
      ]

This schedule will run a match all query, a term query and a phrase query concurrently. It will run with eight clients in total (four for the match all query and two each for the term and phrase query). You can also see that each task can have different settings.

In this scenario, we run indexing and a few queries concurrently::

      "schedule": [
        {
          "parallel": {
            "tasks": [
              {
                "operation": "bulk",
                "warmup-time-period": 120,
                "time-period": 3600,
                "clients": 8,
                "target-throughput": 50
              },
              {
                "operation": "default",
                "clients": 2,
                "warmup-iterations": 50,
                "iterations": 100,
                "target-throughput": 50
              },
              {
                "operation": "term",
                "clients": 2,
                "warmup-iterations": 50,
                "iterations": 100,
                "target-throughput": 200
              },
              {
                "operation": "phrase",
                "clients": 2,
                "warmup-iterations": 50,
                "iterations": 100,
                "target-throughput": 200
              }
            ]
          }
        }
      ]
