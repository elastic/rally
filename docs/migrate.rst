Migration Guide
===============

Migrating to Rally 2.5.1
------------------------

Elasticsearch development branch has moved
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Elasticsearch is `migrating its development branch from master to main <https://github.com/elastic/elasticsearch/issues/76950>`_. To account for this, Rally now checkouts the ``main`` branch when :ref:`building from sources<pipelines_from-sources>`. This will happen automatically unless you have made changes in ``~/.rally/benchmarks/src/elasticsearch``.

Migrating to Rally 2.4.0
------------------------

Support for Elasticsearch < 6.8.0 has been dropped
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To benchmark Elasticsearch 6.x nodes, you need to upgrade to 6.8.x first.

JSON for cli arguments that accept comma-separated values
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Rally now accepts JSON files (ending in .json) or inline JSON strings for cli arguments that accept comma separated values like `--car`, `--telemetry` etc. The only allowed JSON is a plain array e.g. ``esrally race ... --car='["4g", "trial-license"]'``.

Migrating to Rally 2.3.0
------------------------

Support for Elasticsearch 6.x as a metrics store has been dropped
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can still benchmark Elasticsearch 6.x nodes, but can no longer use an Elasticsearch 6.x :doc:`metrics store </metrics>`.

``relative-time-ms`` is removed
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

    This removal is only relevant if you have configured an Elasticsearch metrics store for Rally.

The deprecated property ``relative-time-ms`` has been removed in Rally 2.3.0. Use the property ``relative-time`` instead to retrieve the same metric.

Primary and replica shard counts are now configurable for persistent Elasticsearch metrics stores
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

     Primary and replica shard counts are only configurable for persistent metrics stores (``datastore.type = elasticsearch``).

With this release, the number of primary and replica shards are now configurable for ``rally-*`` indices. These can be set via the ``datastore.number_of_shards`` and ``datastore.number_of_replicas`` options in ``rally.ini``.

Migrating to Rally 2.2.1
------------------------

``relative-time-ms`` is deprecated
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

    This deprecation is only relevant if you have configured an Elasticsearch metrics store for Rally.

The property ``relative-time-ms`` has been deprecated in Rally 2.2.1. Use the re-introduced property ``relative-time`` instead to retrieve the same metric. ``relative-time-ms`` will be dropped in Rally 2.3.0.

Migrating to Rally 2.2.0
------------------------

Support for Elasticsearch 5.x has been dropped
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

According to our :doc:`version policy </versions>` we have dropped support for Elasticsearch 5.x with Rally 2.2.0. Attempting to benchmark an unsupported Elasticsearch version leads to an error at startup.

Semantics of on-error cli argument have changed
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Starting with Rally 2.2.0 the behavior of ``on-error`` cli argument has changed.

The existing default value ``continue-on-non-fatal`` is now renamed to ``continue`` and will keep the same behavior as the older default, i.e. cause the benchmark to fail only on network connection errors.
The ``abort`` value also continues to behave the same (abort as soon as any error happens), however, it can be made to behave like ``continue`` on the **task level** using the new task property ``ignore-response-error-level=non-fatal``.

For more details see: :ref:`cli option on-error <command_line_reference_on_error>` and the task parameter :ref:`ignore-response-error-level <track_schedule>`.

``relative-time`` is removed
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

    This removal is only relevant if you have configured an Elasticsearch metrics store for Rally.

The deprecated metric ``relative-time`` has been dropped in Rally 2.2.0. Use ``relative-time-ms`` instead to retrieve the same metric but denoted in milliseconds instead of microseconds. We will reintroduce this property with new semantics in Rally 2.3.0. See below for an overview of the migration plan for this metric:

* Rally 2.1.0: ``relative-time`` has been deprecated and Rally added a new field ``relative-time-ms`` which contains the relative time in milliseconds.
* Rally 2.2.0: ``relative-time`` is dropped. Rally only populates the field ``relative-time-ms`` which contains the relative time in milliseconds.
* Rally 2.3.0: ``relative-time`` will be reintroduced and contain the relative time in milliseconds. The field ``relative-time-ms`` will be deprecated.
* Rally 2.4.0: ``relative-time-ms`` will be dropped.

``race`` and ``info`` now require ``--track``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Previously we deprecated the default ``--track`` value of ``geonames``.  As of Rally 2.2.0, this parameter is mandatory where applicable.

``cluster-settings`` specified in the track are not honored
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The deprecated challenge property ``cluster_settings`` is not honored anymore. Custom cluster settings should be set as follows:

* Static cluster settings should be defined via rally-teams and the property ``additional_cluster_settings``
* Dynamic cluster settings should be defined via the put-settings operation

Migrating to Rally 2.1.0
------------------------

``relative-time`` is deprecated
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note::

    This deprecation is only relevant if you have configured an Elasticsearch metrics store for Rally.

The metric ``relative-time`` contains the relative time since Rally has started executing a task denoted in microseconds. All other time-based metrics like service time or latency are denoted in milliseconds and we will align this property over the coming minor releases as follows:

* Rally 2.1.0: ``relative-time`` is deprecated and Rally adds a new field ``relative-time-ms`` which contains the relative time in milliseconds.
* Rally 2.2.0: ``relative-time`` will be dropped. Rally only populates the field ``relative-time-ms`` which contains the relative time in milliseconds.
* Rally 2.3.0: ``relative-time`` will be reintroduced and contain the relative time in milliseconds. The field ``relative-time-ms`` will be deprecated.
* Rally 2.4.0: ``relative-time-ms`` will be dropped.

Semantics of request-related timestamps have changed
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When Rally stores metrics in a dedicated metrics store, it records additional meta-data such as the absolute and relative timestamp when a sample has been collected. Previously, these timestamps have represented the point in time when a sample has been collected. For request-related metrics such as ``latency`` and ``service_time`` these timestamps represent now the point in time when a request has been sent by Rally.

Throttling is active from the beginning
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Previously Rally has issued the first request immediately regardless of the target throughput. With this release, Rally will defer the first request according to the target throughput and the scheduling policy. Together with a poisson schedule, this measure avoids coordination among clients that hit Elasticsearch at exactly the same time causing a large initial spike.

Custom bulk parameter sources need to provide a unit
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Previously, Rally has implicitly used the unit ``docs`` for bulk operations. With this release, custom parameter sources for bulk operations need to provide also a ``unit`` property or benchmarks will fail with::

    esrally.exceptions.DataError: Parameter source for operation 'bulk-index' did not provide the mandatory parameter 'unit'. Add it to your parameter source and try again.

Pipelines from-sources-complete and from-sources-skip-build are removed
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The previously deprecated pipelines ``from-sources-complete`` and ``from-sources-skip-build`` have been removed. Specify ``--pipeline=from-sources`` instead.

Rally requires a subcommand
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Previously a subcommand was optional when running a benchmark. With Rally 2.1.0 a subcommand is always required. So instead of invoking::

    esrally --distribution-version=7.10.0

Invoke Rally with the ``race`` subcommand instead::

    esrally race --distribution-version=7.10.0


Running without a track is deprecated
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Previously Rally has implicitly chosen the geonames track as default when ``--track`` was not provided. We want users to make a conscious choice of the workload and not specifying the track explicitly is deprecated (to be removed in Rally 2.2.0). So instead of invoking::

    esrally race --distribution-version=7.10.0

Invoke Rally with ``--track=geonames`` instead::

    esrally race --distribution-version=7.10.0 --track=geonames


Migrating to Rally 2.0.4
------------------------

Running without a subcommand is deprecated
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Rally 2.0.4 will warn when invoked without subcommand. So instead of invoking::

    esrally --distribution-version=7.10.0

Invoke Rally with the ``race`` subcommand instead::

    esrally race --distribution-version=7.10.0

When Rally is invoked without a subcommand it will issue the following warning on the command line and in the log file::

    [WARNING] Invoking Rally without a subcommand is deprecated and will be required with Rally 2.1.0. Specify the 'race' subcommand explicitly.

Migrating to Rally 2.0.3
------------------------

Scheduler API has changed
^^^^^^^^^^^^^^^^^^^^^^^^^

With Rally 2.0.3, the scheduler API has changed. The existing API still works but is deprecated and will be removed in Rally 2.1.0:

* Scheduler functions should be replaced by scheduler classes
* The constructor for scheduler classes changes. Instead of receiving ``params``, it receives the entire ``task`` and the precalculated target throughput.

Consider the following scheduler implemented as a function using the deprecated API. The target throughput is hardcoded to one operation per second::

    def scheduler_function(current):
        return current + 1

This needs to be reimplemented as follows. We assume that the property ``target-throughput`` is now specified on the respective task instead of hard-coding it in the scheduler. Rally will calculate the correct target throughput and inject it into the scheduler class::

    class SchedulerClass:
        def __init__(self, task, target_throughput):
            self.rate = 1 / target_throughput

        def next(self, current):
            return current + self.rate


Also schedulers that are implemented as a class using the deprecated API, need to be changed::

    class MyScheduler:
        def __init__(self, params):
            # assume one client by default
            self.clients = params.get("clients", 1)
            target_throughput = params["target-throughput"] / self.clients
            self.rate = 1 / target_throughput

        def next(self, current):
            return current + self.rate

To use the new API introduced with Rally 2.0.3, this class needs to be changed as follows::

    class MyScheduler:
        # target throughput is already calculated by Rally and is injected here
        # Additional parameters can be retrieved from the task if needed (task.params).
        def __init__(self, task, target_throughput):
            self.rate = 1 / target_throughput

        def next(self, current):
            return current + self.rate


For more details, please see the :ref:`updated scheduler documentation <adding_tracks_custom_schedulers>`.

bulk-size metrics property is dropped
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Metrics records for bulk request don't contain the ``bulk-size`` property anymore. Please use the ``weight`` property instead and consider the ``unit`` property to interpret the value.

--include-tasks and --exclude-tasks affect all operations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Prior to 2.0.3, administrative tasks (see :ref:`operations documentation<track_operations>`) were exempt from filtering and would run regardless of filtering. ``--include-tasks`` and ``--exclude-tasks`` flags now can affect all operations in a track. If you make use of include filters, it is advised to check that all desired operations are listed.

configure subcommand is dropped
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Prior to Rally 2.0.3, Rally had to be configured initially using ``esrally configure``. With Rally 2.0.3, Rally creates a default configuration automatically and users are encouraged to edit Rally's configuration file themselves. Refer to the new :doc:`configuration reference </configuration>` for the configurable properties.

Migrating to Rally 2.0.1
------------------------

Pipelines from-sources-complete and from-sources-skip-build are deprecated
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Rally 2.0.1 caches source artifacts automatically in ``~/.rally/benchmarks/distributions/src``. Therefore, it is not necessary anymore to explicitly skip the build with ``--pipeline=from-sources-skip-build``. Specify ``--pipeline=from-sources`` instead. See the :doc:`pipeline reference documentation </pipelines>` for more details.

wait-for-recovery requires an ``index`` parameter
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Previously, the ``wait-for-recovery`` operation checked all indices but with Rally 2.0.1 an ``index`` parameter is required and only that index (or index pattern) is checked.

Migrating to Rally 2.0.0
------------------------

Minimum Python version is 3.8.0
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Rally 2.0.0 requires Python 3.8.0. Check the :ref:`updated installation instructions <install_python>` for more details.

JAVA_HOME and the bundled runtime JDK
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Rally can optionally use the bundled runtime JDK by setting ``--runtime-jdk="bundled"``. This setting will use the JDK that is bundled with
Elasticsearch and not honor any ``JAVA_HOME`` settings you may have set.

Meta-Data for queries are omitted
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Rally 2.0.0 does not determine query meta-data anymore by default to reduce the risk of client-side bottlenecks. The following meta-data fields are affected:

* ``hits``
* ``hits_relation``
* ``timed_out``
* ``took``

If you still want to retrieve them (risking skewed results due to additional overhead), set the new property ``detailed-results`` to ``true`` for any operation of type ``search``.

Runner API uses asyncio
^^^^^^^^^^^^^^^^^^^^^^^

In order to support more concurrent clients in the future, Rally is moving from a synchronous model to an asynchronous model internally. With Rally 2.0.0 all custom runners need to be implemented using async APIs and a new bool argument ``async_runner=True`` needs to be provided upon registration. Below is an example how to migrate a custom runner function.

A custom runner prior to Rally 2.0.0::

    def percolate(es, params):
        es.percolate(
           index="queries",
           doc_type="content",
           body=params["body"]
        )

    def register(registry):
        registry.register_runner("percolate", percolate)

With Rally 2.0.0, the implementation changes as follows::

    async def percolate(es, params):
        await es.percolate(
                index="queries",
                doc_type="content",
                body=params["body"]
              )

    def register(registry):
        registry.register_runner("percolate", percolate, async_runner=True)

Apply to the following changes for each custom runner:

* Prefix the function signature with ``async``.
* Add an ``await`` keyword before each Elasticsearch API call.
* Add ``async_runner=True`` as the last argument to the ``register_runner`` function.

For more details please refer to the updated documentation on :ref:`custom runners <adding_tracks_custom_runners>`.

``trial-id`` and ``trial-timestamp`` are removed
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Since Rally 1.4.0, Rally uses the properties ``race-id`` and ``race-timestamp`` when writing data to the Elasticsearch metrics store. The properties ``trial-id`` and ``trial-timestamp`` were populated but are removed in this release. Any visualizations that still rely on these properties need to be changed to the new ones.

Migrating to Rally 1.4.1
------------------------

Document IDs are now padded with 0 instead of spaces
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When Rally 1.4.1 generates document IDs, it will pad them with '0' instead of ' ' - 0000000000 instead of '         0', etc.
Elasticsearch has optimizations for numeric IDs, so observed performance in Elasticsearch should improve slightly.


Migrating to Rally 1.4.0
------------------------

cluster-settings is deprecated in favor of the put-settings operation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Before Rally 1.4.0, cluster settings could be specified on the track with the ``cluster-settings`` property. This functionality is deprecated and you should set dynamic cluster settings via the new ``put-settings`` runner. Static settings should instead be set via ``--car-params``.

Build logs are stored in Rally's log directory
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you benchmark source builds of Elasticsearch, Rally has previously stored the build output log in a race-specific directory. With this release, Rally will store the most recent build log in ``/home/user/.rally/logs/build.log``.

Index size and Total Written are not included in the command line report
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Elasticsearch nodes are now managed independently of benchmark execution and thus all system metrics ("index size" and "total written") may be determined after the command line report has been written. The corresponding metrics (``final_index_size_bytes`` and ``disk_io_write_bytes``) are still written to the Elasticsearch metrics store if one is configured.

Node details are omitted from race metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Before Rally 1.4.0, the file ``race.json`` contained node details (such as the number of cluster nodes or details about the nodes' operating system version) if Rally provisioned the cluster. With this release, this information is now omitted. This change also applies to the indices ``rally-races*`` in case you have setup an Elasticsearch metrics store. We recommend to use user tags in case such information is important, e.g. for visualising results.

``trial-id`` and ``trial-timestamp`` are deprecated
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With Rally 1.4.0, Rally will use the properties ``race-id`` and ``race-timestamp`` when writing data to the Elasticsearch metrics store. The properties ``trial-id`` and ``trial-timestamp`` are still populated but will be removed in a future release. Any visualizations that rely on these properties should be changed to the new ones.

Custom Parameter Sources
^^^^^^^^^^^^^^^^^^^^^^^^

With Rally 1.4.0, we have changed the API for custom parameter sources. The ``size()`` method is now deprecated and is instead replaced with a new property called ``infinite``. If you have previously returned ``None`` in ``size()``, ``infinite`` should be set to ``True``, otherwise ``False``. Also, we recommend to implement the property ``percent_completed`` as Rally might not be able to determine progress in some cases. See below for some examples.

Old::

    class CustomFiniteParamSource:
        # ...
        def size():
            return calculate_size()

        def params():
            return next_parameters()

    class CustomInfiniteParamSource:
        # ...
        def size():
            return None

        # ...


New::

    class CustomFiniteParamSource:
        def __init__(self, track, params, **kwargs):
            self.infinite = False
            # to track progress
            self.current_invocation = 0

        # ...
        # Note that we have removed the size() method

        def params():
            self.current_invocation += 1
            return next_parameters()

        # Implementing this is optional but recommended for proper progress reports
        @property
        def percent_completed(self):
            # for demonstration purposes we use calculate_size() here
            # to determine the expected number of invocations. However, if
            # it is possible to determine this value upfront, it is best
            # to cache it in a field and just reuse the value
            return self.current_invocation / calculate_size()


    class CustomInfiniteParamSource:
        def __init__(self, track, params, **kwargs):
            self.infinite = True
            # ...

        # ...
        # Note that we have removed the size() method
        # ...


Migrating to Rally 1.3.0
------------------------
Races now stored by ID instead of timestamp
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
With Rally 1.3.0, Races will be stored by their Trial ID instead of their timestamp.
This means that on disk, a given race will be found at ``benchmarks/races/62d1e928-48b0-4d07-9899-07b45d031566/`` instead of ``benchmarks/races/2019-07-03-17-52-07``

Laps feature removed
^^^^^^^^^^^^^^^^^^^^
The ``--laps`` parameter and corresponding multi-run trial functionality has been removed from execution and reporting.
If you need lap functionality, the following shell script can be used instead::

    RALLY_LAPS=3

    for lap in $(seq 1 ${RALLY_LAPS})
    do
      esrally --pipeline=benchmark-only --user-tag lap:$lap
    done


Migrating to Rally 1.2.1
------------------------

CPU usage is not measured anymore
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With Rally 1.2.1, CPU usage will neither be measured nor reported. We suggest to use system monitoring tools like ``mpstat``, ``sar`` or `Metricbeat <https://www.elastic.co/downloads/beats/metricbeat>`_ to measure CPU usage instead.


Migrating to Rally 1.1.0
------------------------

``request-params`` in operations are passed as is and not serialized
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With Rally 1.1.0 any operations supporting the optional ``request-params`` property will pass the structure as is without attempting to serialize values.
Until now, ``request-params`` relied on parameters being supported by the Elasticsearch Python client API calls. This means that for example boolean type parameters
should be specified as strings i.e. ``"true"`` or ``"false"`` rather than ``true/false``.

**Example**

Using ``create-index`` before ``1.1.0``::

    {
      "name": "create-all-indices",
      "operation-type": "create-index",
      "settings": {
        "index.number_of_shards": 1
      },
      "request-params": {
        "wait_for_active_shards": true
      }
    }

Using ``create-index`` starting with ``1.1.0``::

    {
      "name": "create-all-indices",
      "operation-type": "create-index",
      "settings": {
        "index.number_of_shards": 1
      },
      "request-params": {
        "wait_for_active_shards": "true"
      }
    }


Migrating to Rally 1.0.1
------------------------

Logs are not rotated
^^^^^^^^^^^^^^^^^^^^

With Rally 1.0.1 we have disabled automatic rotation of logs by default because it can lead to race conditions due to Rally's multi-process architecture. If you did not change the default out-of-the-box logging configuration, Rally will automatically fix your configuration. Otherwise, you need to replace all instances of ``logging.handlers.TimedRotatingFileHandler`` with ``logging.handlers.WatchedFileHandler`` to disable log rotation.

To rotate logs we recommend to use external tools like `logrotate <https://linux.die.net/man/8/logrotate>`_. See the following example as a starting point for your own ``logrotate`` configuration and ensure to replace the path ``/home/user/.rally/logs/rally.log`` with the proper one::

    /home/user/.rally/logs/rally.log {
            # rotate daily
            daily
            # keep the last seven log files
            rotate 7
            # remove logs older than 14 days
            maxage 14
            # compress old logs ...
            compress
            # ... after moving them
            delaycompress
            # ignore missing log files
            missingok
            # don't attempt to rotate empty ones
            notifempty
    }

Migrating to Rally 1.0.0
------------------------

Handling of JDK versions
^^^^^^^^^^^^^^^^^^^^^^^^

Previously the path to the JDK needed to be configured in Rally's configuration file (``~/.rally/rally.ini``) but this is too inflexible given the increased JDK release cadence. In order to keep up, we define now the allowed runtime JDKs in `rally-teams <https://github.com/elastic/rally-teams/blob/master/cars/v1/vanilla/config.ini>`_ per Elasticsearch version.

To resolve the path to the appropriate JDK you need to define the environment variable ``JAVA_HOME`` on each targeted machine.

You can also set version-specific environment variables, e.g. ``JAVA7_HOME``, ``JAVA8_HOME`` or ``JAVA10_HOME`` which will take precedence over ``JAVA_HOME``.

.. note::

    Rally will choose the highest appropriate JDK per Elasticsearch version. You can use ``--runtime-jdk`` to force a specific JDK version but the path will still be resolved according to the logic above.

Custom Parameter Sources
^^^^^^^^^^^^^^^^^^^^^^^^

In Rally 0.10.0 we have deprecated some parameter names in custom parameter sources. In Rally 1.0.0, these deprecated names have been removed. Therefore you need to replace the following parameter names if you use them in custom parameter sources:

============== ======================= =======================
Operation type Old name                New name
============== ======================= =======================
search         use_request_cache       cache
search         request_params          request-params
search         items_per_page          results-per-page
bulk           action_metadata_present action-metadata-present
force-merge    max_num_segments        max-num-segments
============== ======================= =======================

In Rally 0.9.0 the signature of custom parameter sources has also changed. In Rally 1.0.0 we have removed the backwards compatibility layer so you need to change the signatures.

Old::

    # for parameter sources implemented as functions
    def custom_param_source(indices, params):

    # for parameter sources implemented as classes
    class CustomParamSource:
        def __init__(self, indices, params):


New::

    # for parameter sources implemented as functions
    def custom_param_source(track, params, **kwargs):

    # for parameter sources implemented as classes
    class CustomParamSource:
        def __init__(self, track, params, **kwargs):

You can use the property ``track.indices`` to access indices.

Migrating to Rally 0.11.0
-------------------------

Versioned teams
^^^^^^^^^^^^^^^

.. note::

    You can skip this section if you do not create custom Rally teams.

We have introduced versioned team specifications and consequently the directory structure changes. All cars and plugins need to reside in a version-specific subdirectory now. Up to now the structure of a team repository was as follows::

    .
    ├── cars
    │   ├── 1gheap.ini
    │   ├── 2gheap.ini
    │   ├── defaults.ini
    │   ├── ea
    │   │   └── config
    │   │       └── jvm.options
    │   ├── ea.ini
    │   └── vanilla
    │       └── config
    │           ├── elasticsearch.yml
    │           ├── jvm.options
    │           └── log4j2.properties
    └── plugins
        ├── core-plugins.txt
        └── transport_nio
            ├── default
            │   └── config
            │       └── elasticsearch.yml
            └── transport.ini

Starting with Rally 0.11.0, Rally will look for a directory "v1" within ``cars`` and ``plugins``. The files that should be copied to the Elasticsearch directory, need to be contained in a ``templates`` subdirectory. Therefore, the new structure is as follows::

    .
    ├── cars
    │   └── v1
    │       ├── 1gheap.ini
    │       ├── 2gheap.ini
    │       ├── defaults.ini
    │       ├── ea
    │       │   └── templates
    │       │       └── config
    │       │           └── jvm.options
    │       ├── ea.ini
    │       └── vanilla
    │           └── templates
    │               └── config
    │                   ├── elasticsearch.yml
    │                   ├── jvm.options
    │                   └── log4j2.properties
    └── plugins
        └── v1
            ├── core-plugins.txt
            └── transport_nio
                ├── default
                │   └── templates
                │       └── config
                │           └── elasticsearch.yml
                └── transport.ini

It is also required that you create a file ``variables.ini`` for all your car config bases (optional for mixins). Therefore, the full directory structure is::

    .
    ├── cars
    │   └── v1
    │       ├── 1gheap.ini
    │       ├── 2gheap.ini
    │       ├── defaults.ini
    │       ├── ea
    │       │   └── templates
    │       │       └── config
    │       │           └── jvm.options
    │       ├── ea.ini
    │       └── vanilla
    │           ├── config.ini
    │           └── templates
    │               └── config
    │                   ├── elasticsearch.yml
    │                   ├── jvm.options
    │                   └── log4j2.properties
    └── plugins
        └── v1
            ├── core-plugins.txt
            └── transport_nio
                ├── default
                │   └── templates
                │       └── config
                │           └── elasticsearch.yml
                └── transport.ini

For distribution-based builds, ``config.ini`` file needs to contain a section ``variables`` and a ``release_url`` property::

    [variables]
    release_url=https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-oss-{{VERSION}}.tar.gz


Migrating to Rally 0.10.0
-------------------------

Removal of auto-detection and dependency on Gradle
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We have removed the auto-detection and dependency on Gradle, required until now to build from source, in favor of the `Gradle Wrapper <https://docs.gradle.org/current/userguide/gradle_wrapper.html>`_ which is present in the `Elasticsearch repository <https://github.com/elastic/elasticsearch>`_ for all branches >= 5.0.0.

Use full build command in plugin configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With Rally 0.10.0 we have removed the property :code:`build.task` for plugin definitions, in the :code:`source` section of the Rally configuration file.
Instead, a new property :code:`build.command` has been introduced where the **full build command** needs to be supplied.

The earlier syntax, to build a hypothetical plugin called :code:`my-plugin` `alongside Elasticsearch <elasticsearch_plugins.html#plugins-built-alongside-elasticsearch>`_, required::

    plugin.my-plugin.build.task = :my-plugin:plugin:assemble

This needs to be changed to the full command::

    plugin.my-plugin.build.command = ./gradlew :my-plugin:plugin:assemble

Note that if you are configuring `Plugins based on a released Elasticsearch version <elasticsearch_plugins.html#plugins-based-on-a-released-elasticsearch-version>`_ the command specified in :code:`build.command` will be executed from the plugins root directory. It's likely this directory won't have the Gradle Wrapper so you'll need to specify the full path to a Gradle command e.g.::

    plugin.my-plugin.build.command = /usr/local/bin/gradle :my-plugin:plugin:assemble

Check `Building plugins from sources <elasticsearch_plugins.html#building-plugins-from-sources>`_ for more information.

Removal of operation type ``index``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We have removed the operation type ``index`` which has been deprecated with Rally 0.8.0. Use ``bulk`` instead as operation type.

Removal of the command line parameter ``--cluster-health``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We have removed the command line parameter ``--cluster-health`` which has been deprecated with Rally 0.8.0. When using Rally's standard tracks, specify the expected cluster health as a track parameter instead, e.g.: ``--track-params="cluster_health:'yellow'"``.

Removal of index-automanagement
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We have removed the possibility that Rally automatically deletes and creates indices. Therefore, you need to add the following definitions explicitly at the beginning of a schedule if you want Rally to create declared indices::

        "schedule": [
          {
            "operation": "delete-index"
          },
          {
            "operation": {
              "operation-type": "create-index",
              "settings": {
                "index.number_of_replicas": 0
              }
            }
          },
          {
            "operation": {
              "operation-type": "cluster-health",
              "request-params": {
                "wait_for_status": "green"
              }
            }
          }

The example above also shows how to provide per-challenge index settings. If per-challenge index settings are not required, you can just specify them in the index definition file.

This behavior applies similarly to index templates as well.

Custom Parameter Sources
^^^^^^^^^^^^^^^^^^^^^^^^

We have aligned the internal names between parameter sources and runners with the ones that are specified by the user in the track file. If you have implemented custom parameter sources or runners, adjust the parameter names as follows:

============== ======================= =======================
Operation type Old name                New name
============== ======================= =======================
search         use_request_cache       cache
search         request_params          request-params
search         items_per_page          results-per-page
bulk           action_metadata_present action-metadata-present
force-merge    max_num_segments        max-num-segments
============== ======================= =======================

Migrating to Rally 0.9.0
------------------------

Track Syntax
^^^^^^^^^^^^

With Rally 0.9.0, we have changed the track file format. While the previous format is still supported with deprecation warnings, we recommend that you adapt your tracks as we will remove the deprecated syntax with the next minor release.

Below is an example of a track with the previous syntax::

    {
      "description": "Tutorial benchmark for Rally",
      "data-url": "http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/geonames",
      "indices": [
        {
          "name": "geonames",
          "types": [
            {
              "name": "type",
              "mapping": "mappings.json",
              "documents": "documents.json",
              "document-count": 8647880,
              "uncompressed-bytes": 2790927196
            }
          ]
        }
      ],
      "challenge": {
        "name": "index-only",
        "index-settings": {
          "index.number_of_replicas": 0
        },
        "schedule": [
          {
            "operation": {
              "operation-type": "bulk",
              "bulk-size": 5000
            },
            "warmup-time-period": 120,
            "clients": 8
          }
        ]
      }
    }

Before Rally 0.9.0, indices have been created implicitly. We will remove this ability and thus you need to tell Rally explicitly that you want to create indices. With Rally 0.9.0 your track should look as follows::

    {
      "description": "Tutorial benchmark for Rally",
      "indices": [
        {
          "name": "geonames",
          "body": "index.json",
          "auto-managed": false,
          "types": [ "type" ]
        }
      ],
      "corpora": [
        {
          "name": "geonames",
          "documents": [
            {
              "base-url": "http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/geonames",
              "source-file": "documents.json",
              "document-count": 8647880,
              "uncompressed-bytes": 2790927196
            }
          ]
        }
      ],
      "challenge": {
        "name": "index-only",
        "schedule": [
          {
            "operation": "delete-index"
          },
          {
            "operation": {
              "operation-type": "create-index",
              "settings": {
                "index.number_of_replicas": 0
              }
            }
          },
          {
            "operation": {
              "operation-type": "cluster-health",
              "request-params": {
                "wait_for_status": "green"
              }
            }
          },
          {
            "operation": {
              "operation-type": "bulk",
              "bulk-size": 5000
            },
            "warmup-time-period": 120,
            "clients": 8
          }
        ]
      }
    }

Let's go through the necessary changes one by one.

Define the document corpus separately
"""""""""""""""""""""""""""""""""""""

Previously you had to define the document corpus together with the document type. In order to allow you to reuse existing document corpora across tracks, you now need to specify any document corpora separately::

    "corpora": [
      {
        "name": "geonames",
        "documents": [
          {
            "base-url": "http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/geonames",
            "source-file": "documents.json",
            "document-count": 8647880,
            "uncompressed-bytes": 2790927196
          }
        ]
      }
    ]

Note that this is just a simple example that should cover the most basic case. Be sure to check the :doc:`track reference </track>` for all details.

Change the index definition
"""""""""""""""""""""""""""

The new index definition now looks as follows::

        {
          "name": "geonames",
          "body": "index.json",
          "auto-managed": false,
          "types": [ "type" ]
        }

We have added a ``body`` property to the index and removed the ``mapping`` property from the type. In fact, the only information that we need about the document type is its name, hence it is now a simple list of strings. Just put all type mappings now into the ``mappings`` property of the index definition; see also the `create index API documentation <https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html>`_.

Secondly, we have disabled index auto-management by setting ``auto-managed`` to ``false``. This allows us to define explicit tasks below to manage our index. Note that index auto-management is still working in Rally 0.9.0 but it will be removed with the next minor release Rally 0.10.0.

Explicitly delete and recreate the index
""""""""""""""""""""""""""""""""""""""""

We have also added three tasks at the beginning of the schedule::

          {
            "operation": "delete-index"
          },
          {
            "operation": {
              "operation-type": "create-index",
              "settings": {
                "index.number_of_replicas": 0
              }
            }
          },
          {
            "operation": {
              "operation-type": "cluster-health",
              "request-params": {
                "wait_for_status": "green"
              }
            }
          }

These tasks represent what Rally previously did implicitly.

The first task will delete all indices that have been declared in the ``indices`` section if they existed previously. This ensures that we don't have any leftovers from previous benchmarks.

After that we will create all indices that have been declared in the ``indices`` section. Note that we have also removed the special property ``index-settings`` and moved it to the ``settings`` parameter of ``create-index``. Rally will merge any settings from the index body definition with these settings. This means you should define settings that are always the same in the index body and settings that change from challenge to challenge in the ``settings`` property.

Finally, Rally will check that the cluster health is green. If you want to be able to override the cluster health check parameters from the command line, you can leverage Rally's track parameter feature::

          {
            "operation": {
              "operation-type": "cluster-health",
              "request-params": {
                "wait_for_status": "{{ cluster_health|default('green') }}"
              }
            }
          }

If you don't specify anything on the command line, Rally will use the default value but you can e.g. specify ``--track-params="cluster_health:'yellow'"`` so Rally will check for (at least) a yellow cluster health status.

Note that you can :doc:`customize these operations </track>`.

Custom Parameter Sources
^^^^^^^^^^^^^^^^^^^^^^^^

With Rally 0.9.0, the API for custom parameter sources has changed. Previously, the following syntax was valid::

    # for parameter sources implemented as functions
    def custom_param_source(indices, params):

    # for parameter sources implemented as classes
    class CustomParamSource:
        def __init__(self, indices, params):


With Rally 0.9.0, the signatures need to be changed to::

    # for parameter sources implemented as functions
    def custom_param_source(track, params, **kwargs):

    # for parameter sources implemented as classes
    class CustomParamSource:
        def __init__(self, track, params, **kwargs):

Rally will issue a warning along the lines of ``Parameter source 'custom_param_source' is using deprecated method signature`` if your track is affected. If you need access to the ``indices`` list, you can call ``track.indices`` to retrieve it from the track.
