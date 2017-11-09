Track Reference
---------------

Definition
==========

A track is the description of one ore more benchmarking scenarios with a specific document corpus. It defines for example the involved indices, data files and which operations are invoked. Its most important attributes are:

* One or more indices, each with one or more types
* The queries to issue
* Source URL of the benchmark data
* A list of steps to run, which we'll call "challenge", for example indexing data with a specific number of documents per bulk request or running searches for a defined number of iterations.

Track File Format and Storage
=============================

A track is specified in a JSON file.

Ad-hoc use
..........

For ad-hoc use you can store a track definition anywhere on the file system and reference it with ``--track-path``, e.g::

   # provide a directory - Rally searches for a track.json file in this directory
   # Track name is "app-logs"
   esrally --track-path=~/Projects/tracks/app-logs
   # provide a file name - Rally uses this file directly
   # Track name is "syslog"
   esrally --track-path=~/Projects/tracks/syslog.json

Rally will also search for additional files like mappings or data files in the provided directory. If you use advanced features like :ref:`custom runners <adding_tracks_custom_runners>` or :ref:`parameter sources <adding_tracks_custom_param_sources>` we recommend that you create a separate directory per track.

Custom Track Repositories
.........................

Alternatively, you can store Rally tracks also in a dedicated git repository which we call a "track repository". Rally provides a default track repository that is hosted on `Github <https://github.com/elastic/rally-tracks>`_. You can also add your own track repositories although this requires a bit of additional work. First of all, track repositories need to be managed by git. The reason is that Rally can benchmark multiple versions of Elasticsearch and we use git branches in the track repository to determine the best match for each track (based on the command line parameter ``--distribution-version``). The versioning scheme is as follows:

* The `master` branch needs to work with the latest `master` branch of Elasticsearch.
* All other branches need to match the version scheme of Elasticsearch, i.e. ``MAJOR.MINOR.PATCH-SUFFIX`` where all parts except ``MAJOR`` are optional.

Rally implements a fallback logic so you don't need to define a branch for each patch release of Elasticsearch. For example:

* The branch `6.0.0-alpha1` will be chosen for the version ``6.0.0-alpha1`` of Elasticsearch.
* The branch `5` will be chosen for all versions for Elasticsearch with the major version 5, e.g. ``5.0.0``, ``5.1.3`` (provided there is no specific branch).

Rally tries to use the branch with the best match to the benchmarked version of Elasticsearch.

Rally will also search for related files like mappings or custom runners or parameter sources in the track repository. However, Rally will use a separate directory to look for data files (``~/.rally/benchmarks/data/$TRACK_NAME/``). The reason is simply that we do not want to check multi-GB data files into git.

Creating a new track repository
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

All track repositories are located in ``~/.rally/benchmarks/tracks``. If you want to add a dedicated track repository, called ``private`` follow these steps::

    cd ~/.rally/benchmarks/tracks
    mkdir private
    cd private
    git init
    # add your track now
    git add .
    git commit -m "Initial commit"


If you want to share your tracks with others you need to add a remote and push it::

    git remote add origin git@git-repos.acme.com:acme/rally-tracks.git
    git push -u origin master

If you have added a remote you should also add it in ``~/.rally/rally.ini``, otherwise you can skip this step. Open the file in your editor of choice and add the following line in the section ``tracks``::

    private.url = <<URL_TO_YOUR_ORIGIN>>

Rally will then automatically update the local tracking branches before the benchmark starts.

You can now verify that everything works by listing all tracks in this track repository::

    esrally list tracks --track-repository=private

This shows all tracks that are available on the ``master`` branch of this repository. Suppose you only created tracks on the branch ``2`` because you're interested in the performance of Elasticsearch 2.x, then you can specify also the distribution version::

    esrally list tracks --track-repository=private --distribution-version=2.0.0


Rally will follow the same branch fallback logic as described above.

Adding an already existing track repository
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you want to add a track repository that already exists, just open ``~/.rally/rally.ini`` in your editor of choice and add the following line in the section ``tracks``::

    your_repo_name.url = <<URL_TO_YOUR_ORIGIN>>

After you have added this line, have Rally list the tracks in this repository::

    esrally list tracks --track-repository=your_repo_name

When to use what?
.................

We recommend the following path:

* Start with a simple json file. The file name can be arbitrary.
* If you need :ref:`custom runners <adding_tracks_custom_runners>` or :ref:`parameter sources <adding_tracks_custom_param_sources>`, create one directory per track. Then you can keep everything that is related to one track in one place. Remember that the track JSON file needs to be named ``track.json``.
* If you want to version your tracks so they can work with multiple versions of Elasticsearch (e.g. you are running benchmarks before an upgrade), use a track repository.

Anatomy of a track
==================

A track JSON file consists of the following sections:

* indices
* operations
* challenges

In the ``indices`` section you describe the relevant indices. Rally can auto-manage them for you: it can download the associated data files, create and destroy the index and apply the relevant mappings. Sometimes, you may want to have full control over the index. Then you can specify ``"auto-managed": false`` on an index. Rally will then assume the index is already present. However, there are some disadvantages with this approach. First of all, this can only work if you set up the cluster by yourself and use the pipeline ``benchmark-only``. Second, the index is out of control of Rally, which means that you need to keep track for yourself of the index configuration. Third, it does not play nice with the ``laps`` feature (which you can use to run multiple iterations). Usually, Rally will destroy and recreate all specified indices for each lap but if you use ``"auto-managed": false``, it cannot do that. As a consequence it will produce bogus metrics if your track specifies that Rally should run bulk-index operations (as you'll just overwrite existing documents from lap 2 on). So please use extra care if you don't let Rally manage the track's indices.

In the ``operations`` section you describe which operations are available for this track and how they are parametrized. This section is optional and you can also define any operations directly per challenge. You can use it, if you want to share operation definitions between challenges.

In the ``challenge`` or ``challenges`` section you describe one or more execution schedules respectively. Each schedule either uses the operations defined in the ``operations`` block or defines the operations to execute inline. Think of a challenge as a scenario that you want to test for your data set. An example challenge is to index with 2 clients at maximum throughput while searching with another two clients with 10 operations per second.

Track elements
==============

The track elements that are described here are defined in `Rally's JSON schema for tracks <https://github.com/elastic/rally/blob/master/esrally/resources/track-schema.json>`_. Rally uses this track schema to validate your tracks when it is loading them.

Each track defines the following info attributes:

* ``version`` (optional): An integer describing the track specification version in use. Rally uses it to detect incompatible future track specification versions and raise an error. See the table below for a reference of valid versions.
* ``description`` (mandatory): A human-readable description of the track.
* ``short-description`` (mandatory): A shorter description of the track.
* ``data-url`` (optional): A http or https URL that points to the root path where Rally can obtain the corresponding data for this track. This element is not needed if data are only generated on the fly by a custom runner.

=========================== =============
Track Specification Version Rally version
=========================== =============
                          1       >=0.7.3
=========================== =============

The ``version`` property has been introduced with Rally 0.7.3. Rally versions before 0.7.3 do not recognize this property and thus cannot detect incompatible track specification versions.

Example::

    {
        "version": 1,
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
* ``documents`` (optional): File name of the corresponding documents that should be indexed. For local use, this file can be a ``.json`` file. If you provide a ``data-url`` we recommend that you provide a compressed file here. The following extensions are supported: ``.zip``, ``.bz2``, ``.gz``, ``.tar``, ``.tar.gz``, ``.tgz`` or ``.tar.bz2``. It must contain exactly one JSON file with the same name. The preferred file extension for our official tracks is ``.bz2``.
* ``includes-action-and-meta-data`` (optional, defaults to ``false``): Defines whether the documents file contains already an action and meta-data line (``true``) or only documents (``false``).
* ``document-count`` (mandatory if ``documents`` is set): Number of documents in the documents file. This number is used by Rally to determine which client indexes which part of the document corpus (each of the N clients gets one N-th of the document corpus). If you are using parent-child, specify the number of parent documents.
* ``compressed-bytes`` (optional but recommended if ``documents`` is set): The size in bytes of the compressed document file. This number is used to show users how much data will be downloaded by Rally and also to check whether the download is complete.
* ``uncompressed-bytes`` (optional but recommended if ``documents`` is set): The size in bytes of the documents file after decompression. This number is used by Rally to show users how much disk space the decompressed file will need and to check that the whole file could be decompressed successfully.

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

With the operation type ``index`` you can execute `bulk requests <http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html>`_. It supports the following properties:

* ``index`` (optional): An index name that defines which indices should be targeted by this indexing operation. Only needed if the ``index`` section contains more than one index and you don't want to index all of them with this operation.
* ``bulk-size`` (mandatory): Defines the bulk size in number of documents.
* ``batch-size`` (optional): Defines how many documents Rally will read at once. This is an expert setting and only meant to avoid accidental bottlenecks for very small bulk sizes (e.g. if you want to benchmark with a bulk-size of 1, you should set batch-size higher).
* ``pipeline`` (optional): Defines the name of an (existing) ingest pipeline that should be used (only supported from Elasticsearch 5.0).
* ``conflicts`` (optional): Type of index conflicts to simulate. If not specified, no conflicts will be simulated. Valid values are: 'sequential' (A document id is replaced with a document id with a sequentially increasing id), 'random' (A document id is replaced with a document id with a random other id).

Example::

    {
      "name": "index-append",
      "operation-type": "index",
      "bulk-size": 5000
    }


Throughput will be reported as number of indexed documents per second.

force-merge
~~~~~~~~~~~

With the operation type ``force-merge`` you can call the `force merge API <http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-forcemerge.html>`_. On older versions of Elasticsearch (prior to 2.1), Rally will use the ``optimize API`` instead. It supports the following parameter:

* ``max_num_segments`` (optional)  The number of segments the index should be merged into. Defaults to simply checking if a merge needs to execute, and if so, executes it.

Throughput metrics are not necessarily very useful but will be reported in the number of completed force-merge operations per second.

index-stats
~~~~~~~~~~~

With the operation type ``index-stats`` you can call the `indices stats API <http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-stats.html>`_. It does not support any parameters.

Throughput will be reported as number of completed `index-stats` operations per second.

node-stats
~~~~~~~~~~

With the operation type ``nodes-stats`` you can execute `nodes stats API <http://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-nodes-stats.html>`_. It does not support any parameters.

Throughput will be reported as number of completed `node-stats` operations per second.

search
~~~~~~

With the operation type ``search`` you can execute `request body searches <http://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html>`_. It supports the following properties:

* ``index`` (optional): An `index pattern <https://www.elastic.co/guide/en/elasticsearch/reference/current/multi-index.html>`_ that defines which indices should be targeted by this query. Only needed if the ``index`` section contains more than one index. Otherwise, Rally will automatically derive the index to use. If you have defined multiple indices and want to query all of them, just specify ``"index": "_all"``.
* ``type`` (optional): Defines the type within the specified index for this query.
* ``cache`` (optional): Whether to use the query request cache. By default, Rally will define no value thus the default depends on the benchmark candidate settings and Elasticsearch version.
* ``request-params`` (optional): A structure containing arbitrary request parameters. The supported parameters names are documented in the `Python ES client API docs <http://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch.search>`_. Parameters that are implicitly set by Rally (e.g. `body` or `request_cache`) are not supported (i.e. you should not try to set them and if so expect unspecified behavior).
* ``body`` (mandatory): The query body.
* ``pages`` (optional): Number of pages to retrieve. If this parameter is present, a scroll query will be executed. If you want to retrieve all result pages, use the value "all".
* ``results-per-page`` (optional):  Number of documents to retrieve per page for scroll queries.

Example::

    {
      "name": "default",
      "operation-type": "search",
      "body": {
        "query": {
          "match_all": {}
        }
      },
      "request-params": {
        "_source_include": "some_field",
        "analyze_wildcard": false
      }
    }

For scroll queries, throughput will be reported as number of retrieved scroll pages per second. The unit is ops/s, where one op(eration) is one page that has been retrieved. The rationale is that each HTTP request corresponds to one operation and we need to issue one HTTP request per result page. Note that if you use a dedicated Elasticsearch metrics store, you can also use other request-level meta-data such as the number of hits for your own analyses.

For other queries, throughput will be reported as number of search requests per second, also measured as ops/s.

challenge
.........

If you track has only one challenge, you can use the ``challenge`` element. If you have multiple challenges, you can define an array of ``challenges``.

This section contains one or more challenges which describe the benchmark scenarios for this data set. A challenge can reference all operations that are defined in the ``operations`` section.

Each challenge consists of the following properties:

* ``name`` (mandatory): A descriptive name of the challenge. Should not contain spaces in order to simplify handling on the command line for users.
* ``description`` (mandatory): A human readable description of the challenge.
* ``default`` (optional): If true, Rally selects this challenge by default if the user did not specify a challenge on the command line. If your track only defines one challenge, it is implicitly selected as default, otherwise you need define ``"default": true`` on exactly one challenge.
* ``index-settings`` (optional): Defines the index settings of the benchmark candidate when an index is created. Note that these settings are only applied if the index is auto-managed.
* ``schedule`` (mandatory): Defines the concrete execution order of operations. It is described in more detail below.

.. note::
   You should strive to minimize the number of challenges. If you just want to run a subset of the tasks in a challenge, use :ref:`task filtering <clr_include_tasks>`.

schedule
~~~~~~~~

The ``schedule`` element contains a list of tasks that are executed by Rally. Each task consists of the following properties:

* ``operation`` (mandatory): This property refers either to the name of an operation that has been defined in the ``operations`` section or directly defines an operation inline.
* ``clients`` (optional, defaults to 1): The number of clients that should execute a task concurrently.
* ``warmup-iterations`` (optional, defaults to 0): Number of iterations that each client should execute to warmup the benchmark candidate. Warmup iterations will not show up in the measurement results.
* ``iterations`` (optional, defaults to 1): Number of measurement iterations that each client executes. The command line report will automatically adjust the percentile numbers based on this number (i.e. if you just run 5 iterations you will not get a 99.9th percentile because we need at least 1000 iterations to determine this value precisely).
* ``warmup-time-period`` (optional, defaults to 0): A time period in seconds that Rally considers for warmup of the benchmark candidate. All response data captured during warmup will not show up in the measurement results.
* ``time-period`` (optional): A time period in seconds that Rally considers for measurement. Note that for bulk indexing you should usually not define this time period. Rally will just bulk index all documents and consider every sample after the warmup time period as measurement sample.
* ``schedule`` (optional, defaults to ``deterministic``): Defines the schedule for this task, i.e. it defines at which point in time during the benchmark an operation should be executed. For example, if you specify a ``deterministic`` schedule and a target-interval of 5 (seconds), Rally will attempt to execute the corresponding operation at second 0, 5, 10, 15 ... . Out of the box, Rally supports ``deterministic`` and ``poisson`` but you can define your own :doc:`custom schedules </adding_tracks>`.
* ``target-throughput`` (optional): Defines the benchmark mode. If it is not defined, Rally assumes this is a throughput benchmark and will run the task as fast as it can. This is mostly needed for batch-style operations where it is more important to achieve the best throughput instead of an acceptable latency. If it is defined, it specifies the number of requests per second over all clients. E.g. if you specify ``target-throughput: 1000`` with 8 clients, it means that each client will issue 125 (= 1000 / 8) requests per second. In total, all clients will issue 1000 requests each second. If Rally reports less than the specified throughput then Elasticsearch simply cannot reach it.
* ``target-interval`` (optional): This is just ``1 / target-throughput`` (in seconds) and may be more convenient for cases where the throughput is less than one operation per second. Define either ``target-throughput`` or ``target-interval`` but not both (otherwise Rally will raise an error).

Defining operations
...................

In the following snippet we define two operations ``force-merge`` and a ``match-all`` query separately in an operations block::

   {
     "operations": [
       {
         "name": "force-merge",
         "operation-type": "force-merge"
       },
       {
         "name": "match-all-query",
         "operation-type": "search",
         "body": {
           "query": {
             "match_all": {}
           }
         }
       }
     ],
     "challenge": {
       "name": "just-query",
       "description": "",
       "schedule": [
         {
           "operation": "force-merge",
           "clients": 1
         },
         {
           "operation": "match-all-query",
           "clients": 4,
           "warmup-iterations": 1000,
           "iterations": 1000,
           "target-throughput": 100
         }
       ]
     }
   }

If we do not want to reuse these operations, we can also define them inline. Note that the ``operations`` section is gone::

   {
     "challenge": {
       "name": "just-query",
       "description": "",
       "schedule": [
         {
           "operation": {
             "name": "force-merge",
             "operation-type": "force-merge"
           },
           "clients": 1
         },
         {
           "operation": {
             "name": "match-all-query",
             "operation-type": "search",
             "body": {
               "query": {
                 "match_all": {}
               }
             }
           },
           "clients": 4,
           "warmup-iterations": 1000,
           "iterations": 1000,
           "target-throughput": 100
         }
       ]
     }
   }

Contrary to the ``query``, the ``force-merge`` operation does not take any parameters, so Rally allows us to just specify the ``operation-type`` for this operation. It's name will be the same as the operation's type::

   {
     "challenge": {
       "name": "just-query",
       "description": "",
       "schedule": [
         {
           "operation": "force-merge",
           "clients": 1
         },
         {
           "operation": {
             "name": "match-all-query",
             "operation-type": "search",
             "body": {
               "query": {
                 "match_all": {}
               }
             }
           },
           "clients": 4,
           "warmup-iterations": 1000,
           "iterations": 1000,
           "target-throughput": 100
         }
       ]
     }
   }

Choosing a schedule
...................

Rally allows you to choose between the following schedules to simulate traffic:

* `deterministically distributed <https://en.wikipedia.org/wiki/Degenerate_distribution>`_
* `Poisson distributed <https://en.wikipedia.org/wiki/Poisson_distribution>`_

The diagram below shows how different schedules in Rally behave during the first ten seconds of a benchmark. Each schedule is configured for a (mean) target throughput of one operation per second.

.. image:: schedulers_10s.png
   :alt: Comparison of Scheduling Strategies in Rally

If you want as much reproducibility as possible you can choose the `deterministic` schedule. A Poisson distribution models random independent arrivals of clients which on average match the expected arrival rate which makes it suitable for modelling the behaviour of multiple clients that decide independently when to issue a request. For this reason, Poisson processes play an important role in `queueing theory <https://en.wikipedia.org/wiki/Queueing_theory>`_.

If you have more complex needs on how to model traffic, you can also implement a :doc:`custom schedule </adding_tracks>`.

Time-based vs. iteration-based
..............................

You should usually use time periods for batch style operations and iterations for the rest. However, you can also choose to run a query for a certain time period.

All tasks in the ``schedule`` list are executed sequentially in the order in which they have been defined. However, it is also possible to execute multiple tasks concurrently, by wrapping them in a ``parallel`` element. The ``parallel`` element defines of the following properties:

* ``clients`` (optional): The number of clients that should execute the provided tasks. If you specify this property, Rally will only use as many clients as you have defined on the ``parallel`` element (see examples)!
* ``warmup-time-period`` (optional, defaults to 0): Allows to define a default value for all tasks of the ``parallel`` element.
* ``time-period`` (optional, no default value if not specified): Allows to define a default value for all tasks of the ``parallel`` element.
* ``warmup-iterations`` (optional, defaults to 0): Allows to define a default value for all tasks of the ``parallel`` element.
* ``iterations`` (optional, defaults to 1): Allows to define a default value for all tasks of the ``parallel`` element.
* ``completed-by`` (optional): Allows to define the name of one task in the ``tasks`` list. As soon as this task has completed, the whole ``parallel`` task structure is considered completed. If this property is not explicitly defined, the ``parallel`` task structure is considered completed as soon as all its subtasks have completed. A task is completed if and only if all associated clients have completed execution.
* ``tasks`` (mandatory): Defines a list of tasks that should be executed concurrently. Each task in the list can define the following properties that have been defined above: ``clients``, ``warmup-time-period``, ``time-period``, ``warmup-iterations`` and ``iterations``.

.. note::

    ``parallel`` elements cannot be nested.

.. warning::

    Specify the number of clients on each task separately. If you specify this number on the ``parallel`` element instead, Rally will only use that many clients in total and you will only want to use this behavior in very rare cases (see examples)!


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

If we want to run a few queries concurrently, we can use the ``parallel`` element (note how we can define default values on the ``parallel`` element)::


      "schedule": [
        {
          "parallel": {
            "warmup-iterations": 50,
            "iterations": 100,
            "tasks": [
              {
                "operation": "match-all",
                "clients": 4,
                "target-throughput": 50
              },
              {
                "operation": "term",
                "clients": 2,
                "target-throughput": 200
              },
              {
                "operation": "phrase",
                "clients": 2,
                "target-throughput": 200
              }
            ]
          }
        }
      ]

This schedule will run a match all query, a term query and a phrase query concurrently. It will run with eight clients in total (four for the match all query and two each for the term and phrase query). You can also see that each task can have different settings.

In this scenario, we run indexing and a few queries concurrently with a total of 14 clients::

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

We can use ``completed-by`` to stop querying as soon as bulk-indexing has completed::

      "schedule": [
        {
          "parallel": {
            "completed-by": "bulk",
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
                "warmup-time-period": 480,
                "time-period": 7200,
                "target-throughput": 50
              }
            ]
          }
        }
      ]

We can also mix sequential tasks with the ``parallel`` element. In this scenario we are indexing with 8 clients and continue querying with 6 clients after indexing has finished::

    "schedule": [
      {
        "operation": "bulk",
        "warmup-time-period": 120,
        "time-period": 3600,
        "clients": 8,
        "target-throughput": 50
      },
      {
        "parallel": {
          "warmup-iterations": 50,
          "iterations": 100,
          "tasks": [
            {
              "operation": "default",
              "clients": 2,
              "target-throughput": 50
            },
            {
              "operation": "term",
              "clients": 2,
              "target-throughput": 200
            },
            {
              "operation": "phrase",
              "clients": 2,
              "target-throughput": 200
            }
          ]
        }
      }
    ]

Be aware of the following case where we explicitly define that we want to run only with two clients *in total*::

      "schedule": [
        {
          "parallel": {
            "warmup-iterations": 50,
            "iterations": 100,
            "clients": 2,
            "tasks": [
              {
                "operation": "match-all",
                "target-throughput": 50
              },
              {
                "operation": "term",
                "target-throughput": 200
              },
              {
                "operation": "phrase",
                "target-throughput": 200
              }
            ]
          }
        }
      ]

Rally will *not* run all three tasks concurrently because you specified that you want only two clients in total. Hence, Rally will first run "match-all" and "term" concurrently (with one client each). After they have finished, Rally will run "phrase" with one client.
