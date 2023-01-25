Advanced Topics
===============

.. _template_language:

Templating Track Files with Jinja2
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Rally uses `Jinja2 <http://jinja.pocoo.org/docs/dev/>`_ as a template language so you can use Jinja2 expressions in track files.

Elasticsearch utilizes Mustache formatting in a few places, notably in `search templates <https://www.elastic.co/guide/en/elasticsearch/reference/7.4/search-template.html>`_ and `Watcher templates <https://www.elastic.co/guide/en/elasticsearch/reference/7.4/actions-email.html>`_. If you are using Mustache in your Rally tracks you must `escape them properly <https://jinja.palletsprojects.com/en/2.10.x/templates/#escaping>`_. See :ref:`put_pipeline` for an example.

Extensions
""""""""""

Rally also provides a few extensions to Jinja2:

* ``now``: a global variable that represents the current date and time when the template is evaluated by Rally.
* ``days_ago()``: a `filter <http://jinja.pocoo.org/docs/dev/templates/#filters>`_ that you can use for date calculations.

You can find an example in the ``http_logs`` track::

    {
      "name": "range",
        "index": "logs-*",
        "type": "type",
        "body": {
          "query": {
            "range": {
              "@timestamp": {
                "gte": "now-{{'15-05-1998' | days_ago(now)}}d/d",
                "lt": "now/d"
              }
            }
          }
        }
      }
    }

The data set that is used in the ``http_logs`` track starts on 26-04-1998 but we want to ignore the first few days for this query, so we start on 15-05-1998. The expression ``{{'15-05-1998' | days_ago(now)}}`` yields the difference in days between now and the fixed start date and allows us to benchmark time range queries relative to now with a predetermined data set.

* ``rally.collect(parts)``: a `macro <https://jinja.pocoo.org/docs/dev/templates/#macros>`_ that you can use to join track fragments. See the :ref:`example above<track_collect_helper>`.
* ``rally.exists_set_param(setting_name, value, default_value=None, comma=True)``: a `macro <https://jinja.pocoo.org/docs/dev/templates/#macros>`_ that you can use to set the value of a track parameter without having to check if it exists.

.. important::
    To use macros you must declare ``{% import "rally.helpers" as rally with context %}`` at the top of your track; see :ref:`the docs <track_collect_helper>` for more details and the `geonames track <https://github.com/elastic/rally-tracks/blob/b2f86df5f0c18461fdb64dd9ee1fe16bd3653b9d/geonames/track.json#L1>`_ for an example.

Example:

Suppose you need an operation that specifies the Elasticsearch transient setting ``indices.recovery.max_bytes_per_sec`` if and only if it has been provided as a track parameter.

Your operation could look like::

    {
      "operation": {
        "operation-type": "raw-request",
        "method": "PUT",
        "path": "/_cluster/settings",
        "body": {
          "transient": {
            "cluster.routing.allocation.node_initial_primaries_recoveries": 8
            {{ rally.exists_set_param("indices.recovery.max_bytes_per_sec", es_snapshot_restore_recovery_max_bytes_per_sec) }}
          }
        }
      }
    }

Note the lack of a comma after the first setting ``cluster.routing.allocation.node_initial_primaries_recoveries``. This is intentional since the helper will insert it if the parameter exists (this behavior can be changed using ``comma=False``).

Assuming we pass ``--track-params="es_snapshot_restore_recovery_max_bytes_per_sec:-1"`` the helper will end up rendering the operation as::

    {
      "operation": {
        "operation-type": "raw-request",
        "method": "PUT",
        "path": "/_cluster/settings",
        "body": {
          "transient": {
            "cluster.routing.allocation.node_initial_primaries_recoveries": 8,"indices.recovery.max_bytes_per_sec": -1
          }
        }
      }
    }


The parameter ``default_value`` controls the value to use for the setting if it is undefined. If the setting is undefined and there is no default value, nothing will be added.

.. _adding_tracks_custom_param_sources:

Controlling Operation Parameters Using Custom Parameter Sources
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. warning::

    Your parameter source is on a performance-critical code-path. Double-check with :ref:`Rally's profiling support <clr_enable_driver_profiling>` that you did not introduce any bottlenecks.


Consider the following operation definition::

    {
      "name": "term",
      "operation-type": "search",
      "body": {
        "query": {
          "term": {
            "body": "physician"
          }
        }
      }
    }

This query is defined statically but if you want to vary parameters, for example to search also for "mechanic" or "nurse, you can write your own "parameter source" in Python.

First, define the name of your parameter source in the operation definition::

    {
      "name": "term",
      "operation-type": "search",
      "param-source": "my-custom-term-param-source"
      "professions": ["mechanic", "physician", "nurse"]
    }

Rally recognizes the parameter source and looks for a file ``track.py`` next to ``track.json``. This file contains the implementation of the parameter source::

    import random


    def random_profession(track, params, **kwargs):
        # choose a suitable index: if there is only one defined for this track
        # choose that one, but let the user always override index and type.
        if len(track.indices) == 1:
            default_index = track.indices[0].name
            if len(track.indices[0].types) == 1:
                default_type = track.indices[0].types[0].name
            else:
                default_type = None
        else:
            default_index = "_all"
            default_type = None

        index_name = params.get("index", default_index)
        type_name = params.get("type", default_type)

        # you must provide all parameters that the runner expects
        return {
            "body": {
                "query": {
                    "term": {
                        "body": "%s" % random.choice(params["professions"])
                    }
                }
            },
            "index": index_name,
            "type": type_name,
            "cache": params.get("cache", False)
        }

    def register(registry):
        registry.register_param_source("my-custom-term-param-source", random_profession)

The example above shows a simple case that is sufficient if the operation to which your parameter source is applied is idempotent and it does not matter whether two clients execute the same operation.

The function ``random_profession`` is the actual parameter source. Rally will bind the name "my-custom-term-param-source" to this function by calling ``register``. ``register`` is called by Rally before the track is executed.

The parameter source function needs to declare the parameters ``track``, ``params`` and ``**kwargs``. ``track`` contains a structured representation of the current track and ``params`` contains all parameters that have been defined in the operation definition in ``track.json``. We use it in the example to read the professions to choose. The third parameter is there to ensure a more stable API as Rally evolves.

We also derive an appropriate index and document type from the track's index definitions but allow the user to override this choice with the ``index`` or ``type`` parameters::

    {
      "name": "term",
      "operation-type": "search",
      "param-source": "my-custom-term-param-source"
      "professions": ["mechanic", "physician", "nurse"],
      "index": "employee*",
      "type": "docs"
    }

.. note::

    Please remember about index and mapping types usage in ``index.json`` and ``track.json`` in Elasticsearch prior to 7.0.0 as specified in notes above.


If you need more control, you need to implement a class. Below is the implementation of the same parameter source as a class::

    import random


    class TermParamSource:
        def __init__(self, track, params, **kwargs):
            # choose a suitable index: if there is only one defined for this track
            # choose that one, but let the user always override index and type.
            if len(track.indices) == 1:
                default_index = track.indices[0].name
                if len(track.indices[0].types) == 1:
                    default_type = track.indices[0].types[0].name
                else:
                    default_type = None
            else:
                default_index = "_all"
                default_type = None

            # we can eagerly resolve these parameters already in the constructor...
            self._index_name = params.get("index", default_index)
            self._type_name = params.get("type", default_type)
            self._cache = params.get("cache", False)
            # ... but we need to resolve "profession" lazily on each invocation later
            self._params = params
            # Determines whether this parameter source will be "exhausted" at some point or
            # Rally can draw values infinitely from it.
            self.infinite = True

        def partition(self, partition_index, total_partitions):
            return self

        def params(self):
            # you must provide all parameters that the runner expects
            return {
                "body": {
                    "query": {
                        "term": {
                            "body": "%s" % random.choice(self._params["professions"])
                        }
                    }
                },
                "index": self._index_name,
                "type": self._type_name,
                "cache": self._cache
            }


    def register(registry):
        registry.register_param_source("my-custom-term-param-source", TermParamSource)


In ``register`` you bind the name in the track specification to your parameter source implementation class similar to the previous example. ``TermParamSource`` is the actual parameter source and needs to fulfill a few requirements:

* The constructor needs to have the signature ``__init__(self, track, params, **kwargs)``.
* ``partition(self, partition_index, total_partitions)`` is called by Rally to "assign" the parameter source across multiple clients. Typically you can just return ``self``. If each client needs to act differently then you can provide different parameter source instances here as well.
* ``params(self)``: This method returns a dictionary with all parameters that the corresponding "runner" expects. This method will be invoked once for every iteration during the race. In the example, we parameterize the query by randomly selecting a profession from a list.
* ``infinite``: This property helps Rally to determine whether to let the parameter source determine when a task should be finished (when ``infinite`` is ``False``) or whether the task properties (e.g. ``iterations`` or ``time-period``) determine when a task should be finished. In the former case, the parameter source needs to raise ``StopIteration`` to indicate when it is finished.

For cases, where you want to provide a progress indication (this is typically the case when ``infinite`` is ``False``), you can implement a property ``percent_completed`` which returns a floating point value between ``0.0`` and ``1.0``. Rally will query this value before each call to ``params()`` and uses it to indicate progress. However:

* Rally will not check ``percent_completed`` if it can derive progress in any other way.
* The value of ``percent_completed`` is purely informational and does not influence when Rally considers an operation to be completed.

.. note::

    The method ``params(self)`` as well as the property ``percent_completed`` are called on a performance-critical path. Don't do anything that takes a lot of time (avoid any I/O). For searches, you should usually throttle throughput anyway and there it does not matter that much but if the corresponding operation is run without throughput throttling, double-check that your custom parameter source does not introduce a bottleneck.

Custom parameter sources can use the Python standard API but using any additional libraries is not supported.

You can also implement your parameter sources and runners in multiple Python files but the main entry point is always ``track.py``. The root package name of your plugin is the name of your track.

.. _adding_tracks_custom_runners:

Creating Your Own Operations With Custom Runners
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. warning::

    Your runner is on a performance-critical code-path. Double-check with :ref:`Rally's profiling support <clr_enable_driver_profiling>` that you did not introduce any bottlenecks.

Runners execute an operation against Elasticsearch. Rally supports many operations out of the box already, see the :doc:`track reference </track>` for a complete list. If you want to call any other Elasticsearch API, define a custom runner.

Consider we want to use the percolate API with an older version of Elasticsearch which is not supported by Rally. To achieve this, we implement a custom runner in the following steps.

In ``track.json`` set the ``operation-type`` to "percolate" (you can choose this name freely)::


    {
      "name": "percolator_with_content_google",
      "operation-type": "percolate",
      "body": {
        "doc": {
          "body": "google"
        },
        "track_scores": true
      }
    }


Then create a file ``track.py`` next to ``track.json`` and implement the following two functions::

    async def percolate(es, params):
        await es.percolate(
                index="queries",
                doc_type="content",
                body=params["body"]
              )

    def register(registry):
        registry.register_runner("percolate", percolate, async_runner=True)

The function ``percolate`` is the actual runner and takes the following parameters:

* ``es``, is an instance of the Elasticsearch Python client
* ``params`` is a ``dict`` of parameters provided by its corresponding parameter source. Treat this parameter as read-only.

This function can return:

* Nothing at all. Then Rally will assume by default ``1`` and ``"ops"`` (see below).
* A tuple of ``weight`` and a ``unit``, which is usually ``1`` and ``"ops"``. If you run a bulk operation you might return the bulk size here, for example in number of documents or in MB. Then you'd return for example ``(5000, "docs")`` Rally will use these values to store throughput metrics.
* A ``dict`` with arbitrary keys. If the ``dict`` contains the key ``weight`` it is assumed to be numeric and chosen as weight as defined above. The key ``unit`` is treated similarly. All other keys are added to the ``meta`` section of the corresponding service time and latency metrics records.

Similar to a parameter source you also need to bind the name of your operation type to the function within ``register``.

To illustrate how to use custom return values, suppose we want to implement a custom runner that calls the `pending tasks API <https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-pending.html>`_ and returns the number of pending tasks as additional meta-data::

    async def pending_tasks(es, params):
        response = await es.cluster.pending_tasks()
        return {
            "weight": 1,
            "unit": "ops",
            "pending-tasks-count": len(response["tasks"])
        }

    def register(registry):
        registry.register_runner("pending-tasks", pending_tasks, async_runner=True)


If you need more control, you can also implement a runner class. The example above, implemented as a class looks as follows::

    class PercolateRunner:
        async def __call__(self, es, params):
            await es.percolate(
                index="queries",
                doc_type="content",
                body=params["body"]
            )

        def __repr__(self, *args, **kwargs):
            return "percolate"

    def register(registry):
        registry.register_runner("percolate", PercolateRunner(), async_runner=True)


The actual runner is implemented in the method ``__call__`` and the same return value conventions apply as for functions. For debugging purposes you should also implement ``__repr__`` and provide a human-readable name for your runner. Finally, you need to register your runner in the ``register`` function.

Runners also support Python's `asynchronous context manager <https://docs.python.org/3/reference/datamodel.html#async-context-managers>`_ interface. Rally uses a new context for each request. Implementing the asynchronous context manager interface can be handy for cleanup of resources after executing an operation. Rally uses it, for example, to clear open scrolls.

If you have specified multiple Elasticsearch clusters using :ref:`target-hosts <command_line_reference_advanced_topics>` you can make Rally pass a dictionary of client connections instead of one for the ``default`` cluster in the ``es`` parameter.

To achieve this you need to:

* Use a runner class
* Specify ``multi_cluster = True`` as a class attribute
* Use any of the cluster names specified in :ref:`target-hosts <command_line_reference_advanced_topics>` as a key for the ``es`` dict

Example (assuming Rally has been invoked specifying ``default`` and ``remote`` in `target-hosts`)::

    class CreateIndexInRemoteCluster:
        multi_cluster = True

        async def __call__(self, es, params):
            await es["remote"].indices.create(index="remote-index")

        def __repr__(self, *args, **kwargs):
            return "create-index-in-remote-cluster"

    def register(registry):
        registry.register_runner("create-index-in-remote-cluster", CreateIndexInRemoteCluster(), async_runner=True)


.. note::

    You need to implement ``register`` just once and register all parameter sources and runners there.

For cases, where you want to provide a progress indication, you can implement the two properties ``percent_completed`` which returns a floating point value between ``0.0`` and ``1.0`` and the property ``completed`` which needs to return ``True`` if the runner has completed. This can be useful in cases when it is only possible to determine progress by calling an API, for example when waiting for a recovery to finish.

.. warning::

    Rally will still treat such a runner like any other. If you want to poll status at certain intervals then limit the number of calls by specifying the ``target-throughput`` property on the corresponding task.

.. _adding_tracks_custom_schedulers:

Controlling Task Execution Behavior With Custom Schedulers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. warning::

    Your scheduler is on a performance-critical code-path. Double-check with :ref:`Rally's profiling support <clr_enable_driver_profiling>` that you did not introduce any bottlenecks.

If you want to rate-limit execution of tasks, you can specify a ``target-throughput`` either as a number to specify the operations per second or, if supported by the operation, as string denoting the target throughput with a different unit. For example, Rally attempts to run this term query 20 times per second::

  {
    "operation": "term",
    "target-throughput": 20
  }

This is identical to::

  {
    "operation": "term",
    "target-throughput": "20 ops/s"
  }

By default, Rally uses a `deterministic distribution <https://en.wikipedia.org/wiki/Degenerate_distribution>`_ to determine when to schedule the next operation. Hence it executes the term query at 0, 50ms, 100ms, 150ms and so on. The scheduler is also aware of the number of clients. Consider this example::

  {
    "operation": "term",
    "target-throughput": 20,
    "clients": 4
  }

If Rally would not take the number of clients into account and would still issue requests (from each of the four clients) at the same points in time (i.e. 0, 50ms, 100ms, 150ms, ...), it would run at a target throughput of 4 * 20 = 80 operations per second. Hence, Rally will automatically reduce the rate at which each client will execute requests. Each client will issue requests at 0, 200ms, 400ms, 600ms, 800ms, 1000ms and so on. Each client issues five requests per second but as there are four of them, we still have a target throughput of 20 operations per second. You should keep this in mind, when writing your own custom schedules.

To create a custom scheduler, create a file ``track.py`` next to ``track.json`` and implement the scheduler class::

    import random

    class RandomScheduler:
        def __init__(self, task, target_throughput):
            self.rate = 1 / target_throughput
            # scale accordingly with the number of clients
            self.variation = task.clients * params.get("variation-millis", 10)

        def next(self, current):
            # roughly matches the target throughput with some random variation
            return current + self.rate + random.randint(-self.variation // 2, self.variation // 2) / 1000.0

The scheduler class also needs to be registered::

    def register(registry):
        registry.register_scheduler("my-random", RandomScheduler)

You can then use your custom scheduler as follows::

  {
    "operation": "term",
    "schedule": "my-random",
    "clients": 10,
    "target-throughput": 100,
    "variation-millis": 1
  }

Manipulating Track Objects And Data With Track Processors
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In order to perform pre-flight activity such as data generation, you can make use of the ``TrackProcessor`` interface::

    class TrackProcessor:
        def on_after_load_track(self, track: track.Track) -> None:
            """
            :param track: The current track.
            """

        def on_prepare_track(self, track: track.Track, data_root_dir: str) -> Generator[Tuple[Callable, dict], None, None]:
            """

            :param track: The current track. This parameter should be treated as effectively immutable. Any modifications
                          will not be reflected in subsequent phases of the benchmark.
            :param data_root_dir: The data root directory on the current machine as configured by the user.
            :return: an Generator[Tuple[Callable, dict], None, None] of function/parameter pairs to be executed by the prepare track's executor
            actors.
            """


To "duck type" this class, you must implement the following methods:

* ``on_after_load_track``
    This method is called by Rally after a track has been loaded by Rally but before work is distributed to worker actors. Implementations are expected to modify the
    provided track object in place.
* ``on_prepare_track``
    This method is called by Rally after the ``on_after_load_track`` phase. Here, any data that is necessary for
    benchmark execution should be prepared, e.g. by downloading data or generating it. Implementations should
    be aware that this method might be called on a different machine than ``on_after_load_track`` and they cannot
    share any state in between phases. The method should `yield` a tuple of a Callable and its parameters for each
    Track Processor thread to be able to work in parallel.

Consider the DefaultTrackPreparator below, which is invoked by default unless overridden by custom registered track processors::

    class DefaultTrackPreparator(TrackProcessor):
        def __init__(self):
            super().__init__()
            # just declare here, will be injected later
            self.cfg = None
            self.downloader = None
            self.decompressor = None
            self.track = None

        def on_after_load_track(self, track):
            ...

        @staticmethod
        def prepare_docs(cfg, track, corpus, preparator):
            for document_set in corpus.documents:
                if document_set.is_bulk:
                    data_root = data_dir(cfg, track.name, corpus.name)
                    logging.getLogger(__name__).info(
                        "Resolved data root directory for document corpus [%s] in track [%s] to [%s].", corpus.name, track.name, data_root
                    )
                    if len(data_root) == 1:
                        preparator.prepare_document_set(document_set, data_root[0])
                    # attempt to prepare everything in the current directory and fallback to the corpus directory
                    elif not preparator.prepare_bundled_document_set(document_set, data_root[0]):
                        preparator.prepare_document_set(document_set, data_root[1])

        def on_prepare_track(self, track, data_root_dir):
            prep = DocumentSetPreparator(track.name, self.downloader, self.decompressor)
            for corpus in used_corpora(track):
                params = {"cfg": self.cfg, "track": track, "corpus": corpus, "preparator": prep}
                yield DefaultTrackPreparator.prepare_docs, params

In this case, you can see by default we do nothing here for ``on_after_load_track`` to mutate the track, but yield a tuple of the ``prepare_docs``
function and its parameters for each corpus in the track ``corpora``. After this is called, these tuples are given to each TrackProcessor worker actor
to be executed in parallel.

.. note::
    By default, Rally creates 1 TrackProcessor worker process for each CPU on the machine where Rally is invoked. To override this behavior, you can use the
    :ref:`system` ``available.cores`` property.

Once your TrackProcessor is created, it needs to be registered in ``track.py``::

    def register(registry):
        registry.register_track_processor(MyTrackProcessor())

Multiple TrackProcessors can be registered this way, and will be invoked sequentially (all ``on_after_load_track`` calls, and then all ``on_prepare_track`` calls).

.. warning::
    Registering custom TrackProcessors prevents the DefaultTrackProcessor from executing. This is expert functionality, as all steps for resolving
    data for your track should be performed in your TrackProcessors.
