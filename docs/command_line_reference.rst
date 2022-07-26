Command Line Reference
======================

You can control Rally with subcommands and command line flags:

* Subcommands determine which task Rally performs.
* Command line flags are used to change Rally's behavior but not all command line flags can be used for each subcommand. To find out which command line flags are supported by a specific subcommand, just run ``esrally <<subcommand>> --help``.

Subcommands
-----------

``race``
~~~~~~~~

The ``race`` subcommand is used to execute a benchmark.


``list``
~~~~~~~~

The ``list`` subcommand is used to list different configuration options:


* telemetry: Will show all :doc:`telemetry devices </telemetry>` that are supported by Rally.
* tracks: Will show all tracks that are supported by Rally. As this *may* depend on the Elasticsearch version that you want to benchmark, you can specify ``--distribution-version`` and also ``--distribution-repository`` as additional options.
* pipelines: Will show all :doc:`pipelines </pipelines>` that are supported by Rally.
* races: Will show a list of the most recent races. This is needed for the :doc:`tournament mode </tournament>`.
* cars: Will show all cars that are supported by Rally (i.e. Elasticsearch configurations).
* elasticsearch-plugins: Will show all Elasticsearch plugins and their configurations that are supported by Rally.

To list a specific configuration option, place it after the ``list`` subcommand. For example, ``esrally list pipelines`` will list all pipelines known to Rally.

``info``
~~~~~~~~

The ``info`` subcommand prints details about a track. Example::

    esrally info --track=noaa --challenge=append-no-conflicts

This will print::

    Showing details for track [noaa]:

    * Description: Global daily weather measurements from NOAA
    * Documents: 33,659,481
    * Compressed Size: 947.3 MB
    * Uncompressed Size: 9.0 GB

    ================================================
    Challenge [append-no-conflicts] (run by default)
    ================================================

    Indexes the whole document corpus using Elasticsearch default settings. We only adjust the number of replicas as we benchmark a single node cluster and Rally will only start the benchmark if the cluster turns green and we want to ensure that we don't use the query cache. Document ids are unique so all index operations are append only. After that a couple of queries are run.

    Schedule:
    ----------

    1. delete-index
    2. create-index
    3. check-cluster-health
    4. index (8 clients)
    5. refresh-after-index
    6. force-merge
    7. refresh-after-force-merge
    8. range_field_big_range
    9. range_field_small_range
    10. range_field_conjunction_big_range_small_term_query
    11. range_field_conjunction_small_range_small_term_query
    12. range_field_conjunction_small_range_big_term_query
    13. range_field_conjunction_big_range_big_term_query
    14. range_field_disjunction_small_range_small_term_query
    15. range_field_disjunction_big_range_small_term_query

It is also possible to use task filters (e.g. ``--include-tasks``) or to refer to a track via its path (``--track-path``) or use a different track repository (``--track-repository``).

``compare``
~~~~~~~~~~~

This subcommand is needed for :doc:`tournament mode </tournament>` and its usage is described there.

``create-track``
~~~~~~~~~~~~~~~~

This subcommand creates a basic track from data in an existing cluster. See the :ref:`track tutorial <add_track_create_track>` for a complete walkthrough.

``download``
~~~~~~~~~~~~~

This subcommand can be used to download Elasticsearch distributions. Example::

    esrally download --distribution-version=6.8.0 --quiet

This will download the OSS distribution of Elasticsearch 6.8.0. Because ``--quiet`` is specified, Rally will suppress all non-essential output (banners, progress messages etc.) and only return the location of the binary on the local machine after it has downloaded it::

    {
      "elasticsearch": "/Users/dm/.rally/benchmarks/distributions/elasticsearch-oss-6.8.0.tar.gz"
    }

To download the default distribution you need to specify a license (via ``--car``)::

    esrally download --distribution-version=6.8.0 --car=basic-license --quiet

This will show the path to the default distribution::

    {
      "elasticsearch": "/Users/dm/.rally/benchmarks/distributions/elasticsearch-6.8.0.tar.gz"
    }

``install``
~~~~~~~~~~~

.. warning::

    This subcommand is experimental. Expect the functionality and the command line interface to change significantly even in patch releases.

This subcommand can be used to install a single Elasticsearch node. Example::

    esrally install --quiet --distribution-version=7.4.2 --node-name="rally-node-0" --network-host="127.0.0.1" --http-port=39200 --master-nodes="rally-node-0" --seed-hosts="127.0.0.1:39300"

This will output the id of this installation::

    {
      "installation-id": "69ffcfee-6378-4090-9e93-87c9f8ee59a7"
    }

Please see the :doc:`cluster management tutorial </cluster_management>` for the intended use and a complete walkthrough.

``start``
~~~~~~~~~

.. warning::

    This subcommand is experimental. Expect the functionality and the command line interface to change significantly even in patch releases.

This subcommand can be used to start a single Elasticsearch node that has been previously installed with the ``install`` subcommand. Example::

    esrally start --installation-id=INSTALLATION_ID --race-id=RACE_ID

``INSTALLATION_ID`` is the installation id value as shown in the output of the ``install`` command. The ``RACE_ID`` can be chosen freely but is required to be the same for all nodes in a cluster.

Please see the :doc:`cluster management tutorial </cluster_management>` for the intended use and a complete walkthrough.

``stop``
~~~~~~~~

.. warning::

    This subcommand is experimental. Expect the functionality and the command line interface to change significantly even in patch releases.

This subcommand can be used to stop a single Elasticsearch node that has been previously started with the ``start`` subcommand. Example::

    esrally stop --installation-id=INSTALLATION_ID

``INSTALLATION_ID`` is the installation id value as shown in the output of the ``install`` command.

Please see the :doc:`cluster management tutorial </cluster_management>` for the intended use and a complete walkthrough.

Command Line Flags
------------------

``track-path``
~~~~~~~~~~~~~~

Can be either a directory that contains a ``track.json`` file or a ``.json`` file with an arbitrary name that contains a track specification. ``--track-path`` and ``--track-repository`` as well as ``--track`` are mutually exclusive. See the :doc:`track reference </track>` to decide whether you should use ``--track-path`` or ``--track-repository`` / ``--track``.

Examples::

   # provide a directory - Rally searches for a track.json file in this directory
   # Track name is "app-logs"
   esrally race --track-path=~/Projects/tracks/app-logs
   # provide a file name - Rally uses this file directly
   # Track name is "syslog"
   esrally race --track-path=~/Projects/tracks/syslog.json


``track-repository``
~~~~~~~~~~~~~~~~~~~~

Selects the track repository that Rally should use to resolve tracks. By default the ``default`` track repository is used, which is available in the Github project `rally-tracks <https://github.com/elastic/rally-tracks>`_. See the :doc:`track reference </track>` on how to add your own track repositories. ``--track-path`` and ``--track-repository`` as well as ``--track`` are mutually exclusive.

``track-revision``
~~~~~~~~~~~~~~~~~~

Selects a specific revision in the track repository. By default, Rally will choose the most appropriate branch on its own but in some cases it is necessary to specify a certain commit. This is mostly needed when testing whether a change in performance has occurred due to a change in the workload.

``track``
~~~~~~~~~

Selects the track that Rally should run. For more details on how tracks work, see :doc:`adding tracks </adding_tracks>` or the :doc:`track reference </track>`. ``--track-path`` and ``--track-repository`` as well as ``--track`` are mutually exclusive.

.. _clr_track_params:

``track-params``
~~~~~~~~~~~~~~~~

With this parameter you can inject variables into tracks. The supported variables depend on the track and you should check the track JSON file to see which variables can be provided.

It accepts a list of comma-separated key-value pairs or a JSON file name. The key-value pairs have to be delimited by a colon.

**Examples**:

Consider the following track snippet showing a single challenge::

    {
      "name": "index-only",
      "schedule": [
         {
           "operation": {
             "operation-type": "bulk",
             "bulk-size": {{ bulk_size|default(5000) }}
           },
           "warmup-time-period": 120,
           "clients": {{ clients|default(8) }}
         }
      ]
    }

Rally tracks can use the Jinja templating language and the construct ``{{ some_variable|default(0) }}`` that you can see above is a `feature of Jinja <http://jinja.pocoo.org/docs/2.10/templates/#default>`_ to define default values for variables.

We can see that it defines two variables:

* ``bulk_size`` with a default value of 5000
* ``clients`` with a default value of 8

When we run this track, we can override these defaults:

* ``--track-params="bulk_size:2000,clients:16"`` will set the bulk size to 2000 and the number of clients for bulk indexing to 16.
* ``--track-params="bulk_size:8000"`` will just set the bulk size to 8000 and keep the default value of 8 clients.
* ``--track-params="params.json"`` will read the track parameters from a JSON file (defined below)

Example JSON file::

   {
      "bulk_size": 2000,
      "clients": 16
   }

All track parameters are recorded for each metrics record in the metrics store. Also, when you run ``esrally list races``, it will show all track parameters::

    Race Timestamp    Track    Track Parameters          Challenge            Car       User Tag
    ----------------  -------  ------------------------- -------------------  --------  ---------
    20160518T122341Z  pmc      bulk_size=8000            append-no-conflicts  defaults
    20160518T112341Z  pmc      bulk_size=2000,clients=16 append-no-conflicts  defaults

Note that the default values are not recorded or shown (Rally does not know about them).

``challenge``
~~~~~~~~~~~~~

A track consists of one or more challenges. With this flag you can specify which challenge should be run. If you don't specify a challenge, Rally derives the default challenge itself. To see the default challenge of a track, run ``esrally list tracks``.

``race-id``
~~~~~~~~~~~

A unique identifier for this race. By default a random UUID is automatically chosen by Rally.

``installation-id``
~~~~~~~~~~~~~~~~~~~

.. warning::

    This command line parameter is experimental. Expect the functionality and the command line interface to change significantly even in patch releases.

A unique identifier for an installation. This id is automatically generated by Rally when the ``install`` subcommand is invoked and needs to be provided in the ``start`` and ``stop`` subcommands in order to refer to that installation.

``node-name``
~~~~~~~~~~~~~

.. warning::

    This command line parameter is experimental. Expect the functionality and the command line interface to change significantly even in patch releases.

Used to specify the current node's name in the cluster when it is setup via the ``install`` subcommand.

``cluster-name``
~~~~~~~~~~~~~~~~

.. warning::

    This command line parameter is experimental. Expect the functionality and the command line interface to change significantly even in patch releases.

This parameter is useful in benchmarks involved multiple Elasticsearch clusters. It's used to configure the cluster name of the current Elasticsearch node when it is setup via the ``install`` or ``race`` subcommand. The following example sets up two Elasticsearch clusters: ``cluster-1`` and ``cluster-2``, and each has two nodes::

    # install node-1 in cluster-1
    esrally install --quiet --distribution-version=8.2.2 --node-name="node-1" --cluster-name=cluster-1 --network-host="192.168.1.1" --http-port=39200 --master-nodes="node-1" --seed-hosts="192.168.1.1:39300,192.168.1.2:39300"
    # install node-2 in cluster-1
    esrally install --quiet --distribution-version=8.2.2 --node-name="node-2" --cluster-name=cluster-1 --network-host="192.168.1.2" --http-port=39200 --master-nodes="node-1" --seed-hosts="192.168.1.1:39300,192.168.1.2:39300"
    # install node-3 in cluster-2
    esrally install --quiet --distribution-version=8.2.2 --node-name="node-3" --cluster-name=cluster-2 --network-host="192.168.1.3" --http-port=39200 --master-nodes="node-3" --seed-hosts="192.168.1.3:39300,192.168.1.4:39300"
    # install node-4 in cluster-2
    esrally install --quiet --distribution-version=8.2.2 --node-name="node-4" --cluster-name=cluster-2 --network-host="192.168.1.4" --http-port=39200 --master-nodes="node-3" --seed-hosts="192.168.1.3:39300,192.168.1.4:39300"

If the ``cluster-name`` parameter is not specified, Rally will use ``rally-benchmark`` as the default cluster name.

``network-host``
~~~~~~~~~~~~~~~~

.. warning::

    This command line parameter is experimental. Expect the functionality and the command line interface to change significantly even in patch releases.

Used to specify the IP address to which the current cluster node should bind to when it is setup via the ``install`` subcommand.

``http-port``
~~~~~~~~~~~~~

.. warning::

    This command line parameter is experimental. Expect the functionality and the command line interface to change significantly even in patch releases.

Used to specify the port on which the current cluster node should listen for HTTP traffic when it is setup via the ``install`` subcommand. The corresponding transport port will automatically be chosen by Rally and is always the specified HTTP port + 100.

``master-nodes``
~~~~~~~~~~~~~~~~

.. warning::

    This command line parameter is experimental. Expect the functionality and the command line interface to change significantly even in patch releases.

A comma-separated list that specifies the node names of the master nodes in the cluster when a node is setup via the ``install`` subcommand. Example::

    --master-nodes="rally-node-0,rally-node-1,rally-node-2"

This will treat the nodes named ``rally-node-0``, ``rally-node-1`` and ``rally-node-2`` as `initial master nodes <https://www.elastic.co/guide/en/elasticsearch/reference/current/discovery-settings.html#initial_master_nodes>`_.

``seed-hosts``
~~~~~~~~~~~~~~

.. warning::

    This command line parameter is experimental. Expect the functionality and the command line interface to change significantly even in patch releases.

A comma-separated list if IP:transport port pairs used to specify the seed hosts in the cluster when a node is setup via the ``install`` subcommand. See the `Elasticsearch documentation <https://www.elastic.co/guide/en/elasticsearch/reference/current/discovery-settings.html#unicast.hosts>`_ for a detailed explanation of the seed hosts parameter. Example::

    --seed-hosts="192.168.14.77:39300,192.168.14.78:39300,192.168.14.79:39300"

.. _clr_include_tasks:

``include-tasks``
~~~~~~~~~~~~~~~~~

Each challenge consists of one or more tasks but sometimes you are only interested to run a subset of all tasks. For example, you might have prepared an index already and want only to repeatedly run search benchmarks. Or you want to run only the indexing task but nothing else.

You can use ``--include-tasks`` to specify a comma-separated list of tasks that you want to run. Each item in the list defines either the name of a task or the operation type of a task. Only the tasks that match will be executed. You can use the ``info`` subcommand to list the tasks of a challenge, or look at the track source.

.. note::

    Tasks will be executed in the order that are defined in the challenge, not in the order they are defined in the command.

.. note::

    Task filters are case-sensitive.

**Examples**:

* Execute only the tasks with the name ``index`` and ``term``: ``--include-tasks="index,term"``
* Execute only tasks of type ``search``: ``--include-tasks="type:search"``
* Execute only tasks that contain the tag ``read-op``: ``--include-tasks="tag:read-op"``
* You can also mix and match: ``--include-tasks="index,type:search"``

``exclude-tasks``
~~~~~~~~~~~~~~~~~

Similarly to :ref:`include-tasks <clr_include_tasks>` when a challenge consists of one or more tasks you might be interested in excluding a single operations but include the rest.

You can use ``--exclude-tasks`` to specify a comma-separated list of tasks that you want to skip. Each item in the list defines either the name of a task or the operation type of a task. Only the tasks that did not match will be executed.

**Examples**:

* Skip any tasks with the name ``index`` and ``term``: ``--exclude-tasks="index,term"``
* Skip any tasks of type ``search``: ``--exclude-tasks="type:search"``
* Skip any tasks that contain the tag ``setup``: ``--exclude-tasks="tag:setup"``
* You can also mix and match: ``--exclude-tasks="index,type:search,tag:setup"``

``team-repository``
~~~~~~~~~~~~~~~~~~~

Selects the team repository that Rally should use to resolve cars. By default the ``default`` team repository is used, which is available in the Github project `rally-teams <https://github.com/elastic/rally-teams>`__. See the documentation about :doc:`cars </car>` on how to add your own team repositories.

``team-revision``
~~~~~~~~~~~~~~~~~

Selects a specific revision in the team repository. By default, Rally will choose the most appropriate branch on its own (see the :doc:`car reference </car>` for more details) but in some cases it is necessary to specify a certain commit. This is mostly needed when benchmarking specific historic commits of Elasticsearch which are incompatible with the current master branch of the team repository.


``team-path``
~~~~~~~~~~~~~

A directory that contains a team configuration. ``--team-path`` and ``--team-repository`` are mutually exclusive. See the :doc:`car reference </car>` for the required directory structure.

Example::

   esrally race --track=geonames --team-path=~/Projects/es-teams

``target-os``
~~~~~~~~~~~~~

Specifies the name of the target operating system for which an artifact should be downloaded. By default this value is automatically derived based on the operating system Rally is run. This command line flag is only applicable to the ``download`` subcommand and allows to download an artifact for a different operating system. Example::

    esrally download --distribution-version=7.5.1 --target-os=linux

``target-arch``
~~~~~~~~~~~~~~~

Specifies the name of the target CPU architecture for which an artifact should be downloaded. By default this value is automatically derived based on the CPU architecture Rally is run. This command line flag is only applicable to the ``download`` subcommand and allows to download an artifact for a different CPU architecture. Example::

    esrally download --distribution-version=7.5.1 --target-arch=x86_64


``car``
~~~~~~~

A :doc:`car </car>` defines the Elasticsearch configuration that will be used for the benchmark. To see a list of possible cars, issue ``esrally list cars``. You can specify one or multiple comma-separated values.

**Example**

 ::

   esrally race --track=geonames --car="4gheap,ea"


Rally will configure Elasticsearch with 4GB of heap (``4gheap``) and enable Java assertions (``ea``).

``car-params``
~~~~~~~~~~~~~~

Allows to override config variables of Elasticsearch. It accepts a list of comma-separated key-value pairs or a JSON file name. The key-value pairs have to be delimited by a colon.

**Example**

 ::

   esrally race --track=geonames --car="4gheap" --car-params="data_paths:'/opt/elasticsearch'"

The variables that are exposed depend on the `car's configuration <https://github.com/elastic/rally-teams/tree/master/cars>`__. In addition, Rally implements special handling for the variable ``data_paths`` (by default the value for this variable is determined by Rally).


``elasticsearch-plugins``
~~~~~~~~~~~~~~~~~~~~~~~~~

A comma-separated list of Elasticsearch plugins to install for the benchmark. If a plugin supports multiple configurations you need to specify the configuration after the plugin name. To see a list of possible plugins and configurations, issue ``esrally list elasticsearch-plugins``.

Example::

   esrally race --track=geonames --elasticsearch-plugins="analysis-icu,xpack:security"

In this example, Rally will install the ``analysis-icu`` plugin and the ``x-pack`` plugin with the ``security`` configuration. See the reference documentation about :doc:`Elasticsearch plugins </elasticsearch_plugins>` for more details.

``plugin-params``
~~~~~~~~~~~~~~~~~

Allows to override variables of Elasticsearch plugins. It accepts a list of comma-separated key-value pairs or a JSON file name. The key-value pairs have to be delimited by a colon.

Example::

    esrally race --track=geonames --distribution-version=6.1.1. --elasticsearch-plugins="x-pack:monitoring-http" --plugin-params="monitoring_type:'http',monitoring_host:'some_remote_host',monitoring_port:10200,monitoring_user:'rally',monitoring_password:'m0n1t0r1ng'"

This enables the HTTP exporter of `X-Pack Monitoring <https://www.elastic.co/products/x-pack/monitoring>`_ and exports the data to the configured monitoring host.

``pipeline``
~~~~~~~~~~~~

Selects the :doc:`pipeline </pipelines>` that Rally should run.

Rally can autodetect the pipeline in most cases. If you specify ``--distribution-version`` it will auto-select the pipeline ``from-distribution`` otherwise it will use ``from-sources``.

``enable-assertions``
~~~~~~~~~~~~~~~~~~~~~

This option enables assertions on tasks. If an assertion fails, the race is aborted with a message indicating which assertion has failed.

.. _clr_enable_driver_profiling:

``enable-driver-profiling``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

This option enables a profiler on all tasks that the load test driver performs. It is intended to help track authors spot accidental bottlenecks, especially if they implement their own runners or parameter sources. When this mode is enabled, Rally will enable a profiler in the load driver module. After each task and for each client, Rally will add the profile information to a dedicated profile log file. For example::

   2017-02-09 08:23:24,35 rally.profile INFO
   === Profile START for client [0] and task [index-append-1000] ===
      16052402 function calls (15794402 primitive calls) in 180.221 seconds

      Ordered by: cumulative time

      ncalls  tottime  percall  cumtime  percall filename:lineno(function)
         130    0.001    0.000  168.089    1.293 /Users/dm/Projects/rally/esrally/driver/driver.py:908(time_period_based)
         129    0.260    0.002  168.088    1.303 /Users/dm/.rally/benchmarks/tracks/develop/bottleneck/parameter_sources/bulk_source.py:79(params)
      129000    0.750    0.000  167.791    0.001 /Users/dm/.rally/benchmarks/tracks/develop/bottleneck/parameter_sources/randomevent.py:142(generate_event)
      516000    0.387    0.000  160.485    0.000 /Users/dm/.rally/benchmarks/tracks/develop/bottleneck/parameter_sources/weightedarray.py:20(get_random)
      516000    6.199    0.000  160.098    0.000 /Users/dm/.rally/benchmarks/tracks/develop/bottleneck/parameter_sources/weightedarray.py:23(__random_index)
      516000    1.292    0.000  152.289    0.000 /usr/local/Cellar/python3/3.6.0/Frameworks/Python.framework/Versions/3.6/lib/python3.6/random.py:96(seed)
      516000  150.783    0.000  150.783    0.000 {function Random.seed at 0x10b7fa2f0}
      129000    0.363    0.000   45.686    0.000 /Users/dm/.rally/benchmarks/tracks/develop/bottleneck/parameter_sources/randomevent.py:48(add_fields)
      129000    0.181    0.000   41.742    0.000 /Users/dm/.rally/benchmarks/tracks/develop/bottleneck/parameter_sources/randomevent.py:79(add_fields)
      ....

   === Profile END for client [0] and task [index-append-1000] ===

In this example we can spot quickly that ``Random.seed`` is called excessively, causing an accidental bottleneck in the load test driver.

.. _clr_test_mode:

``test-mode``
~~~~~~~~~~~~~

Allows you to test a track without running it for the whole duration. This mode is only intended for quick sanity checks when creating a track. Don't rely on these numbers at all (they are meaningless).

If you write your own track you need to :ref:`prepare your track to support this mode <add_track_test_mode>`.

``telemetry``
~~~~~~~~~~~~~

Activates the provided :doc:`telemetry devices </telemetry>` for this race.

**Example**

 ::

   esrally race --track=geonames --telemetry=jfr,jit


This activates Java flight recorder and the JIT compiler telemetry devices.

.. _clr_telemetry_params:

``telemetry-params``
~~~~~~~~~~~~~~~~~~~~

Allows to set parameters for telemetry devices. It accepts a list of comma-separated key-value pairs or a JSON file name. The key-value pairs have to be delimited by a colon. See the :doc:`telemetry devices </telemetry>` documentation for a list of supported parameters.

Example::

    esrally race --track=geonames --telemetry=jfr --telemetry-params="recording-template:'profile'"

This enables the Java flight recorder telemetry device and sets the ``recording-template`` parameter to "profile".

For more complex cases specify a JSON file. Store the following as ``telemetry-params.json``::

   {
     "node-stats-sample-interval": 10,
     "node-stats-include-indices-metrics": "completion,docs,fielddata"
   }

and reference it when running Rally::

   esrally race --track=geonames --telemetry="node-stats" --telemetry-params="telemetry-params.json"


``runtime-jdk``
~~~~~~~~~~~~~~~

By default, Rally will derive the appropriate runtime JDK versions automatically per version of Elasticsearch. For example, it will choose JDK 12, 11 or 8 for Elasticsearch 7.0.0. It will choose the highest available version.

This command line parameter sets the major version of the JDK that Rally should use to run Elasticsearch. It is required that either ``JAVA_HOME`` or ``JAVAx_HOME`` (where ``x`` is the major version, e.g. ``JAVA11_HOME`` for a JDK 11) points to the appropriate JDK.

Example::

   # Run a benchmark with defaults
   esrally race --track=geonames --distribution-version=7.0.0
   # Force to run with JDK 11
   esrally race --track=geonames --distribution-version=7.0.0 --runtime-jdk=11

It is also possible to specify the JDK that is bundled with Elasticsearch with the special value ``bundled``. The `JDK is bundled from Elasticsearch 7.0.0 onwards <https://www.elastic.co/guide/en/elasticsearch/reference/current/release-highlights-7.0.0.html#_bundle_jdk_in_elasticsearch_distribution>`_.

.. _clr_revision:

``revision``
~~~~~~~~~~~~

If you actively develop Elasticsearch and want to benchmark a source build of Elasticsearch (which Rally will create for you), you can specify the git revision of Elasticsearch that you want to benchmark. The default value is ``current``.

You can specify the revision in different formats:

* ``--revision=latest``: Use the HEAD revision from origin/main.
* ``--revision=current``: Use the current revision (i.e. don't alter the local source tree).
* ``--revision=abc123``: Where ``abc123`` is some git revision hash.
* ``--revision=@2013-07-27T10:37:00Z``: Determines the revision that is closest to the provided date. Rally logs to which git revision hash the date has been resolved and if you use Elasticsearch as metrics store (instead of the default in-memory one), :doc:`each metric record will contain the git revision hash also in the meta-data section </metrics>`.

Supported date format: If you specify a date, it has to be ISO-8601 conformant and must start with an ``@`` sign to make it easier for Rally to determine that you actually mean a date.

If you want to create source builds of Elasticsearch plugins, you need to specify the revision for Elasticsearch and all relevant plugins separately. Revisions for Elasticsearch and each plugin need to be comma-separated (``,``). Each revision is prefixed either by ``elasticsearch`` or by the plugin name and separated by a colon (``:``). As core plugins are contained in the Elasticsearch repo, there is no need to specify a revision for them (the revision would even be ignored in fact).

Examples:

* Build latest Elasticsearch and plugin "my-plugin": ``--revision="elasticsearch:latest,my-plugin:latest"``
* Build Elasticsearch tag ``v7.12.0`` and revision ``abc123`` of plugin "my-plugin": ``--revision="elasticsearch:v7.12.0,my-plugin:abc123"``

Note that it is still required to provide the parameter ``--elasticsearch-plugins``. Specifying a plugin with ``--revision`` just tells Rally which revision to use for building the artifact. See the documentation on :doc:`Elasticsearch plugins </elasticsearch_plugins>` for more details.

``distribution-version``
~~~~~~~~~~~~~~~~~~~~~~~~

If you want Rally to launch and benchmark a cluster using a binary distribution, you can specify the version here.

.. note::

    Do not use ``distribution-version`` when benchmarking a cluster that hasn't been setup by Rally (i.e. together with ``pipeline=benchmark-only``); Rally automatically derives and stores the version of the cluster in the metrics store regardless of the ``pipeline`` used.

**Example**

 ::

   esrally race --track=geonames --distribution-version=7.0.0


Rally will then benchmark the official Elasticsearch 7.0.0 distribution. Please check our :doc:`version support page </versions>` to see which Elasticsearch versions are currently supported by Rally.

``distribution-repository``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Rally does not only support benchmarking official distributions but can also benchmark snapshot builds. This is option is really just intended for `our benchmarks that are run in continuous integration <https://elasticsearch-benchmarks.elastic.co/>`_ but if you want to, you can use it too. The only supported value out of the box is ``release`` (default) but you can define arbitrary repositories in ``~/.rally/rally.ini``.

**Example**

Say, you have an in-house repository where Elasticsearch snapshot builds get published. Then you can add the following in the ``distributions`` section of your Rally config file:

::

   in_house_snapshot.url = https://www.example.org/snapshots/elasticsearch/elasticsearch-{{VERSION}}.tar.gz
   in_house_snapshot.cache = false

The ``url`` property defines the URL pattern for this repository. The ``cache`` property defines whether Rally should always download a new archive (``cache=false``) or just reuse a previously downloaded version (``cache=true``). Rally will replace the ``{{VERSION}}`` placeholder of in the ``url`` property with the value of ``distribution-version`` provided by the user on the command line.

You can use this distribution repository with the name "in_house_snapshot" as follows::

   esrally race --track=geonames --distribution-repository=in_house_snapshot --distribution-version=7.0.0-SNAPSHOT

This will benchmark the latest 7.0.0 snapshot build of Elasticsearch.

``report-format``
~~~~~~~~~~~~~~~~~

The command line reporter in Rally displays a table with key metrics after a race. With this option you can specify whether this table should be in ``markdown`` format (default) or ``csv``.

``report-numbers-align``
~~~~~~~~~~~~~~~~~~~~~~~~

By default, the number columns are aligned with the decimal place in the same spot for the command line report. You can override the default alignment. Possible values are ``right``, ``center``, ``left``, and ``decimal`` (the default).

``show-in-report``
~~~~~~~~~~~~~~~~~~

By default, the command line reporter will only show values that are available (``available``). With ``all`` you can force it to show a line for every value, even undefined ones, and with ``all-percentiles`` it will show only available values but force output of all possible percentile values.

This command line parameter is not available for comparing races.


``report-file``
~~~~~~~~~~~~~~~

By default, the command line reporter will print the results only on standard output, but can also write it to a file.

**Example**

 ::

   esrally race --track=geonames --report-format=csv --report-file=~/benchmarks/result.csv

.. _clr_client_options:

``client-options``
~~~~~~~~~~~~~~~~~~

With this option you can customize Rally's internal Elasticsearch client.

It accepts a list of comma-separated key-value pairs. The key-value pairs have to be delimited by a colon. These options are passed directly to the Elasticsearch Python client API. See `their documentation on a list of supported options <http://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch>`_.

We support the following data types:

* Strings: Have to be enclosed in single quotes. Example: ``ca_certs:'/path/to/CA_certs'``
* Numbers: There is nothing special about numbers. Example: ``sniffer_timeout:60``
* Booleans: Specify either ``true`` or ``false``. Example: ``use_ssl:true``
* None: Specify ``None`` without quotes.``'None'`` will be treated as string. Example: ``timeout:None``

Default value: ``timeout:60`` (applies any time ``timeout`` is not specified. Set ``timeout:None`` to omit any timeouts)

Rally recognizes the following client options in addition:

* ``max_connections``: By default, Rally will choose the maximum allowed number of connections automatically (equal to the number of simulated clients but at least 256 connections). With this property it is possible to override that logic but a minimum of 256 is enforced internally.
* ``enable_cleanup_closed`` (default: ``true``): In some cases, `Elasticsearch does not properly close SSL connections <https://github.com/elastic/elasticsearch/issues/76642>`_ and the number of open connections increases as a result. When this client option is set to ``true``, the Elasticsearch client will check and forcefully close these connections.
* ``static_responses``: The path to a JSON file containing path patterns and the corresponding responses. When this value is set to ``true``, Rally will not send requests to Elasticsearch but return static responses as specified by the file. This is useful to diagnose performance issues in Rally itself. See below for a specific example.
* ``create_api_key_per_client`` (default: ``false``): If set to ``true``, Rally will create a unique `Elasticsearch API key <https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-create-api-key.html>`_ for each simulated client that issues requests against Elasticsearch during the benchmark. This is useful for simulating workloads where data is indexed by many distinct agents, each configured with its own API key, as is typical with Elastic Agent. Note that ``basic_auth_user`` and ``basic_auth_password`` must also be provided, and the ``basic_auth_user`` must have `sufficient privileges <https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-create-api-key.html#security-api-create-api-key-prereqs>`_ to create API keys. These basic auth credentials are used to create the API keys at the start of the benchmark and delete them at the end, but only the generated API keys will be used during benchmark execution.

**Examples**

Here are a few common examples:

* Enable HTTP compression: ``--client-options="http_compress:true"``
* Enable basic authentication: ``--client-options="basic_auth_user:'user',basic_auth_password:'password'"``. Avoid the characters ``'``, ``,`` and ``:`` in user name and password as Rally's parsing of these options is currently really simple and there is no possibility to escape characters.

**TLS/SSL**

This is applicable e.g. if you have X-Pack Security installed.
Enable it with ``use_ssl:true``.

**TLS/SSL Certificate Verification**

Server certificate verification is controlled with the ``verify_certs`` boolean. The default value is `true`. To disable use ``verify_certs:false``.
If ``verify_certs:true``, Rally will attempt to verify the certificate provided by Elasticsearch. If this certificate is signed by a private certificate authority (CA), you will also need to supply that CA in PEM format using ``ca_certs:'/path/to/cacert.pem'``.

You can also optionally present client certificates, e.g. if Elasticsearch has been configured with ``xpack.security.http.ssl.client_authentication: required`` (see also `Elasticsearch HTTP TLS/SSL settings <https://www.elastic.co/guide/en/elasticsearch/reference/current/security-settings.html#http-tls-ssl-settings>`_).
Client certificates can be presented regardless of the ``verify_certs`` setting, but it's strongly recommended to always verify the server certificates.

**TLS/SSL Examples**

* Enable SSL, verify server certificates using public CA: ``--client-options="use_ssl:true,verify_certs:true"``. Note that you don't need to set ``ca_cert`` (which defines the path to the root certificates). Rally does this automatically for you.
* Enable SSL, verify server certificates using private CA: ``--client-options="use_ssl:true,verify_certs:true,ca_certs:'/path/to/cacert.pem'"``
* Enable SSL, verify server certificates using private CA, present client certificates: ``--client-options="use_ssl:true,verify_certs:true,ca_certs:'/path/to/cacert.pem',client_cert:'/path/to/client_cert.pem',client_key:'/path/to/client_key.pem'"``

**Converting PKCS#12 Files to PEM Format**

Rally supports PEM format for CA and client certificates. Certificates in PKCS#12 formatted keystores will need to be exported to PEM format in order for Rally to use them. The ``openssl`` command can be used to export certificates from a PKCS#12 keystore. In the following example, PKCS#12 file ``elastic-stack-ca.p12`` is generated by the Elasticsearch ``elasticsearch-certutil ca`` command and contains only the CA certificate and private key.

* Export CA certificate:

 ::

   openssl pkcs12 -in elastic-stack-ca.p12 -nokeys -out cacert.pem

* Export a node private key for client authentication:

 ::

   openssl pkcs12 -in elastic-certificates.p12 -nocerts -nodes -out client_key.pem

* Export the node certificate for client authentication:

 ::

   openssl pkcs12 -in elastic-certificates.p12 -nokeys -clcerts -out client_cert.pem

**Static Responses**

Define a JSON file containing a list of objects with the following properties:

* ``path``: A path or path pattern that should be matched. Only leading and trailing wildcards (``*``) are supported. A path containing only a wildcard acts matches any path.
* ``body``: The respective response body.
* ``body-encoding``: Either ``raw`` or ``json``. Use ``json`` by default and ``raw`` for the operation-type ``bulk`` and ``search``.

Here we define the necessary responses for a track that bulk-indexes data::

    [
      {
        "path": "*/_bulk",
        "body": {
          "errors": false,
          "took": 1
        },
        "body-encoding": "raw"
      },
      {
        "path": "/_cluster/health*",
        "body": {
          "status": "green",
          "relocating_shards": 0
        },
        "body-encoding": "json"
      },
      {
        "path": "/_cluster/settings",
        "body": {
          "persistent": {},
          "transient": {}
        },
        "body-encoding": "json"
      },
      {
        "path": "/_all/_stats/_all",
        "body": {
          "_all": {
            "total": {
              "merges": {
                "current": 0
              }
            }
          }
        },
        "body-encoding": "json"
      },
      {
        "path": "*",
        "body": {},
        "body-encoding": "json"
      }
    ]

.. note::
   Paths are evaluated from top to bottom. Therefore, place more restrictive paths at the top of the file.

Save the above responses as ``responses.json`` and execute a benchmark as follows::

    esrally race --track=geonames --challenge=append-no-conflicts-index-only --pipeline=benchmark-only --distribution-version=8.0.0 --client-options="static_responses:'responses.json'"

.. note::
   Use ``--pipeline=benchmark-only`` as Rally should not start any cluster when static responses are used.

**Create an API key per client**

* Enable API-key generation per client: ``--client-options="use_ssl:true,basic_auth_user:'user',basic_auth_password:'password',create_api_key_per_client:true"``

.. _command_line_reference_on_error:

``on-error``
~~~~~~~~~~~~

This option controls how Rally behaves when a response error occurs. The following values are possible:

* ``continue``: (default) only records that an error has happened and will continue with the benchmark unless there is a fatal error. At the end of a race, errors show up in the "error rate" metric.
* ``abort``: aborts the benchmark on the first request error with a detailed error message. It is possible to permit *individual* tasks to ignore non-fatal errors using :ref:`ignore-response-error-level <track_schedule>`.

.. attention::

    The only error that is considered fatal is "Connection Refused" (`ECONNREFUSED <http://man7.org/linux/man-pages/man2/connect.2.html>`_).

``load-driver-hosts``
~~~~~~~~~~~~~~~~~~~~~

By default, Rally will run its load driver on the same machine where you start the benchmark. However, if you benchmark larger clusters, one machine may not be enough to generate sufficient load. Hence, you can specify a comma-separated list of hosts which should be used to generate load with ``--load-driver-hosts``.

**Example**

 ::

   esrally race --track=geonames --load-driver-hosts=10.17.20.5,10.17.20.6

In the example, above Rally will generate load from the hosts ``10.17.20.5`` and ``10.17.20.6``. For this to work, you need to start a Rally daemon on these machines, see :ref:`distributing the load test driver <recipe_distributed_load_driver>` for a complete example.

``target-hosts``
~~~~~~~~~~~~~~~~

If you run the ``benchmark-only`` :doc:`pipeline </pipelines>` or you want Rally to :doc:`benchmark a remote cluster </recipes>`, then you can specify a comma-delimited list of hosts:port pairs to which Rally should connect. The default value is ``127.0.0.1:9200``.

**Example**

 ::

   esrally race --track=geonames --pipeline=benchmark-only --target-hosts=10.17.0.5:9200,10.17.0.6:9200

This will run the benchmark against the hosts 10.17.0.5 and 10.17.0.6 on port 9200. See ``client-options`` if you use X-Pack Security and need to authenticate or Rally should use https.

You can also target multiple clusters with ``--target-hosts`` for specific use cases. This is described in the :ref:`Advanced topics section <command_line_reference_advanced_topics>`.

``limit``
~~~~~~~~~

Allows to control the number of races returned by ``esrally list races`` The default value is 10.

**Example**

The following invocation will list the 50 most recent races::

   esrally list races --limit=50


``quiet``
~~~~~~~~~

Suppresses some output on the command line.

``kill-running-processes``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Rally attempts to generate benchmark results that are not skewed unintentionally. Consequently, if some benchmark is running, Rally will not allow you to start another one. Instead, you should stop the current benchmark and start another one manually. This flag can be added to handle automatically this process for you.

Only one Rally benchmark is allowed to run at the same time. If any processes is running, it is going to kill them and allow Rally to continue to run a new benchmark.

The default value is ``false``.

``offline``
~~~~~~~~~~~

Tells Rally that it should assume it has no connection to the Internet when checking for track data. The default value is ``false``. Note that Rally will only assume this for tracks but not for anything else, e.g. it will still try to download Elasticsearch distributions that are not locally cached or fetch the Elasticsearch source tree.

``preserve-install``
~~~~~~~~~~~~~~~~~~~~

Rally usually installs and launches an Elasticsearch cluster internally and wipes the entire directory after the benchmark is done. Sometimes you want to keep this cluster including all data after the benchmark has finished and that's what you can do with this flag. Note that depending on the track that has been run, the cluster can eat up a very significant amount of disk space (at least dozens of GB). The default value is configurable in the advanced configuration but usually ``false``.

.. note::
   This option does only affect clusters that are provisioned by Rally. More specifically, if you use the pipeline ``benchmark-only``, this option is ineffective as Rally does not provision a cluster in this case.

``user-tag``
~~~~~~~~~~~~

This is only relevant when you want to run :doc:`tournaments </tournament>`. You can use this flag to attach an arbitrary text to the meta-data of each metric record and also the corresponding race. This will help you to recognize a race when you run ``esrally list races`` as you don't need to remember the concrete timestamp on which a race has been run but can instead use your own descriptive names.

The required format is ``key`` ":" ``value``. You can choose ``key`` and  ``value`` freely.

**Example**

 ::

   esrally race --track=pmc --user-tag="intention:github-issue-1234-baseline,gc:cms"

You can also specify multiple tags. They need to be separated by a comma.

**Example**

 ::

   esrally race --track=pmc --user-tag="disk:SSD,data_node_count:4"



When you run ``esrally list races``, this will show up again::

    Race Timestamp    Track    Track Parameters   Challenge            Car       User Tag
    ----------------  -------  ------------------ -------------------  --------  ------------------------------------
    20160518T122341Z  pmc                         append-no-conflicts  defaults  intention:github-issue-1234-baseline
    20160518T112341Z  pmc                         append-no-conflicts  defaults  disk:SSD,data_node_count:4

This will help you recognize a specific race when running ``esrally compare``.

``indices``
~~~~~~~~~~~

A comma-separated list of index patterns to target when generating a track with the ``create-track`` subcommand.

**Examples**

Target a single index::

    esrally create-track --track=acme --indices="products" --target-hosts=127.0.0.1:9200 --output-path=~/tracks

Target multiple indices::

    esrally create-track --track=acme --indices="products,companies" --target-hosts=127.0.0.1:9200 --output-path=~/tracks

Use index patterns::

    esrally create-track --track=my-logs --indices="logs-*" --target-hosts=127.0.0.1:9200 --output-path=~/tracks


.. note::
   If the cluster requires authentication specify credentials via ``--client-options`` as described in the :ref:`command line reference <clr_client_options>`.

.. _command_line_reference_advanced_topics:

Advanced topics
---------------

``target-hosts``
~~~~~~~~~~~~~~~~

Rally can also create client connections for multiple Elasticsearch clusters.
This is only useful if you want to create :ref:`custom runners <adding_tracks_custom_runners>` that execute operations against multiple clusters, for example for `cross cluster search <https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-cross-cluster-search.html>`_ or cross cluster replication.

To define the host:port pairs for additional clusters with ``target-hosts`` you can specify either a JSON filename (ending in ``.json``) or an inline JSON string. The JSON object should be a collection of name:value pairs. ``name`` is string for the cluster name and there **must be** one ``default``.

Examples:

* json file: ``--target-hosts="target_hosts1.json"``::

    { "default": ["127.0.0.1:9200","10.127.0.3:19200"] }

* json file defining two clusters: ``--target-hosts="target_hosts2.json"``::

    {
      "default": [
        {"host": "127.0.0.1", "port": 9200},
        {"host": "127.0.0.1", "port": 19200}
      ],
      "remote":[
        {"host": "10.127.0.3", "port": 9200},
        {"host": "10.127.0.8", "port": 9201}
      ]
    }

* json inline string defining two clusters::

    --target-hosts="{\"default\":[\"127.0.0.1:9200\"],\"remote\":[\"127.0.0.1:19200\",\"127.0.0.1:19201\"]}"

.. NOTE::
   **All** :ref:`built-in operations <track_operations>` will use the connection to the ``default`` cluster. However, you can utilize the client connections to the additional clusters in your :ref:`custom runners <adding_tracks_custom_runners>`.

``client-options``
~~~~~~~~~~~~~~~~~~

``client-options`` can optionally specify options for the Elasticsearch clients when multiple clusters have been defined with ``target-hosts``. The default is ``timeout:60`` for all cluster connections.

The format is similar to ``target-hosts``, supporting both filenames ending in ``.json`` or inline JSON, however, the parameters are a collection of name:value pairs, as opposed to arrays.

Examples, assuming that two clusters have been specified with ``--target-hosts``:

* json file: ``--client-options="client_options1.json"``::

    {
      "default": {
        "timeout": 120
    },
      "remote": {
        "use_ssl": true,
        "verify_certs": false,
        "ca_certs": "/path/to/cacert.pem"
      }
    }

* json inline string defining two clusters::

    --client-options="{\"default\":{\"timeout\": 120}, \"remote\": {\"use_ssl\":true,\"verify_certs\":false,\"ca_certs\":\"/path/to/cacert.pem\"}}"

.. WARNING::
   If you use ``client-options`` you must specify options for **every** cluster name defined with ``target-hosts``. Rally will raise an error if there is a mismatch.

Command line parameters accepting comma-separated values
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Parameters that can accept comma-separated values such as ``--car``, ``--telemetry``, ``--include-tasks`` etc. can also accept a JSON array.
This can either be defined in a file ending in ``.json`` or passed as an inline JSON string.

Examples:

* comma-separated values::

    --car="4gheap,trial-license"

* json file: ``--car="car.json"``::

    $ cat car.json
    ["4gheap", "trial-license"]

* json inline string::

    esrally race ... --telemetry='["node-stats", "recovery-stats"]'
