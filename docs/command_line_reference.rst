Command Line Reference
======================

You can control Rally with subcommands and command line flags:

* Subcommands determine which task Rally performs.
* Command line flags are used to change Rally's behavior but not all command line flags can be used for each subcommand. To find out which command line flags are supported by a specific subcommand, just run ``esrally <<subcommand>> --help``.

Subcommands
-----------

``race``
~~~~~~~~

The ``race`` subcommand is used to actually run a benchmark. It is the default one and chosen implicitly if none is given.


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


``compare``
~~~~~~~~~~~

This subcommand is needed for :doc:`tournament mode </tournament>` and its usage is described there.

``configure``
~~~~~~~~~~~~~

This subcommand is needed to :doc:`configure </configuration>` Rally. It is implicitly chosen if you start Rally for the first time but you can rerun this command at any time.

Command Line Flags
------------------

``track-path``
~~~~~~~~~~~~~~

Can be either a directory that contains a ``track.json`` file or a ``.json`` file with an arbitrary name that contains a track specification. ``--track-path`` and ``--track-repository`` as well as ``--track`` are mutually exclusive. See the :doc:`track reference </track>` to decide whether you should use ``--track-path`` or ``--track-repository`` / ``--track``.

Examples::

   # provide a directory - Rally searches for a track.json file in this directory
   # Track name is "app-logs"
   esrally --track-path=~/Projects/tracks/app-logs
   # provide a file name - Rally uses this file directly
   # Track name is "syslog"
   esrally --track-path=~/Projects/tracks/syslog.json


``track-repository``
~~~~~~~~~~~~~~~~~~~~

Selects the track repository that Rally should use to resolve tracks. By default the ``default`` track repository is used, which is available in the Github project `rally-tracks <https://github.com/elastic/rally-tracks>`_. See the :doc:`track reference </track>` on how to add your own track repositories. ``--track-path`` and ``--track-repository`` as well as ``--track`` are mutually exclusive.

``track``
~~~~~~~~~

Selects the track that Rally should run. By default the ``geonames`` track is run. For more details on how tracks work, see :doc:`adding tracks </adding_tracks>` or the :doc:`track reference </track>`. ``--track-path`` and ``--track-repository`` as well as ``--track`` are mutually exclusive.

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

.. _clr_include_tasks:

``include-tasks``
~~~~~~~~~~~~~~~~~

Each challenge consists of one or more tasks but sometimes you are only interested to run a subset of all tasks. For example, you might have prepared an index already and want only to repeatedly run search benchmarks. Or you want to run only the indexing task but nothing else.

You can use ``--include-tasks`` to specify a comma-separated list of tasks that you want to run. Each item in the list defines either the name of a task or the operation type of a task. Only the tasks that match will be executed. Currently there is also no command that lists the tasks of a challenge so you need to look at the track source.

**Examples**:

* Execute only the tasks with the name ``index`` and ``term``: ``--include-tasks="index,term"``
* Execute only tasks of type ``search``: ``--include-tasks="type:search"``
* You can also mix and match: ``--include-tasks="index,type:search"``

``team-repository``
~~~~~~~~~~~~~~~~~~~

Selects the team repository that Rally should use to resolve cars. By default the ``default`` team repository is used, which is available in the Github project `rally-teams <https://github.com/elastic/rally-teams>`__. See the documentation about :doc:`cars </car>` on how to add your own team repositories.

``team-path``
~~~~~~~~~~~~~

A directory that contains a team configuration. ``--team-path`` and ``--team-repository`` are mutually exclusive. See the :doc:`car reference </car>` for the required directory structure.

Example::

   esrally --team-path=~/Projects/es-teams

``car``
~~~~~~~

A :doc:`car </car>` defines the Elasticsearch configuration that will be used for the benchmark. To see a list of possible cars, issue ``esrally list cars``. You can specify one or multiple comma-separated values.

**Example**

 ::

   esrally --car="4gheap,ea"


Rally will configure Elasticsearch with 4GB of heap (``4gheap``) and enable Java assertions (``ea``).

``car-params``
~~~~~~~~~~~~~~

Allows to override config variables of Elasticsearch. It accepts a list of comma-separated key-value pairs or a JSON file name. The key-value pairs have to be delimited by a colon.

**Example**

 ::

   esrally --car="4gheap" --car-params="data_paths:'/opt/elasticsearch'"

The variables that are exposed depend on the `car's configuration <https://github.com/elastic/rally-teams/tree/master/cars>`__. In addition, Rally implements special handling for the variable ``data_paths`` (by default the value for this variable is determined by Rally).


``elasticsearch-plugins``
~~~~~~~~~~~~~~~~~~~~~~~~~

A comma-separated list of Elasticsearch plugins to install for the benchmark. If a plugin supports multiple configurations you need to specify the configuration after the plugin name. To see a list of possible plugins and configurations, issue ``esrally list elasticsearch-plugins``.

Example::

   esrally --elasticsearch-plugins="analysis-icu,xpack:security"

In this example, Rally will install the ``analysis-icu`` plugin and the ``x-pack`` plugin with the ``security`` configuration. See the reference documentation about :doc:`Elasticsearch plugins </elasticsearch_plugins>` for more details.

``plugin-params``
~~~~~~~~~~~~~~~~~

Allows to override variables of Elasticsearch plugins. It accepts a list of comma-separated key-value pairs or a JSON file name. The key-value pairs have to be delimited by a colon.

Example::

    esrally --distribution-version=6.1.1. --elasticsearch-plugins="x-pack:monitoring-http" --plugin-params="monitoring_type:'https',monitoring_host:'some_remote_host',monitoring_port:10200,monitoring_user:'rally',monitoring_password:'m0n1t0r1ng'"

This enables the HTTP exporter of `X-Pack Monitoring <https://www.elastic.co/products/x-pack/monitoring>`_ and exports the data to the configured monitoring host.

``pipeline``
~~~~~~~~~~~~

Selects the :doc:`pipeline </pipelines>` that Rally should run.

Rally can autodetect the pipeline in most cases. If you specify ``--distribution-version`` it will auto-select the pipeline ``from-distribution`` otherwise it will use ``from-sources-complete``.

``laps``
~~~~~~~~

Allows to run the benchmark for multiple laps (defaults to 1 lap). Each lap corresponds to one full execution of a track but note that the benchmark candidate is not restarted in between.

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

   esrally --telemetry=jfr,jit


This activates Java flight recorder and the JIT compiler telemetry devices.

``telemetry-params``
~~~~~~~~~~~~~~~~~~~~

Allows to set parameters for telemetry devices. It accepts a list of comma-separated key-value pairs or a JSON file name. The key-value pairs have to be delimited by a colon. See the :doc:`telemetry devices </telemetry>` documentation for a list of supported parameters.

Example::

    esrally --telemetry=jfr --telemetry-params="recording-template:'profile'"

This enables the Java flight recorder telemetry device and sets the ``recording-template`` parameter to "profile".

.. _clr_revision:

``revision``
~~~~~~~~~~~~

If you actively develop Elasticsearch and want to benchmark a source build of Elasticsearch (which Rally will create for you), you can specify the git revision of Elasticsearch that you want to benchmark. But note that Rally uses and expects the Gradle Wrapper in the Elasticsearch repository (``./gradlew``) which effectively means that it will only support this for Elasticsearch 5.0 or better. The default value is ``current``.

You can specify the revision in different formats:

* ``--revision=latest``: Use the HEAD revision from origin/master.
* ``--revision=current``: Use the current revision (i.e. don't alter the local source tree).
* ``--revision=abc123``: Where ``abc123`` is some git revision hash.
* ``--revision=@2013-07-27T10:37:00Z``: Determines the revision that is closest to the provided date. Rally logs to which git revision hash the date has been resolved and if you use Elasticsearch as metrics store (instead of the default in-memory one), :doc:`each metric record will contain the git revision hash also in the meta-data section </metrics>`.

Supported date format: If you specify a date, it has to be ISO-8601 conformant and must start with an ``@`` sign to make it easier for Rally to determine that you actually mean a date.

If you want to create source builds of Elasticsearch plugins, you need to specify the revision for Elasticsearch and all relevant plugins separately. Revisions for Elasticsearch and each plugin need to be comma-separated (``,``). Each revision is prefixed either by ``elasticsearch`` or by the plugin name and separated by a colon (``:``). As core plugins are contained in the Elasticsearch repo, there is no need to specify a revision for them (the revision would even be ignored in fact).

Examples:

* Build latest Elasticsearch and plugin "my-plugin": ``--revision="elasticsearch:latest,my-plugin:latest"``
* Build Elasticsearch tag ``v5.6.1`` and revision ``abc123`` of plugin "my-plugin": ``--revision="elasticsearch:v5.6.1,my-plugin:abc123"``

Note that it is still required to provide the parameter ``--elasticsearch-plugins``. Specifying a plugin with ``--revision`` just tells Rally which revision to use for building the artifact. See the documentation on :doc:`Elasticsearch plugins </elasticsearch_plugins>` for more details.

``distribution-version``
~~~~~~~~~~~~~~~~~~~~~~~~

If you want to benchmark a binary distribution, you can specify the version here.

**Example**

 ::

   esrally --distribution-version=2.3.3


Rally will then benchmark the official Elasticsearch 2.3.3 distribution.

Rally works with all releases of Elasticsearch that are `supported by Elastic <https://www.elastic.co/support/matrix#show_compatibility>`_.

The following versions are already end-of-life:

* ``0.x``: Rally is not tested, and not expected to work for this version; we will make no effort to make Rally work.
* ``1.x``: Rally works on a best-effort basis with this version but support may be removed at any time.

Additionally, Rally will always work with the current development version of Elasticsearch (by using either a snapshot repository or by building Elasticsearch from sources).

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

   esrally --distribution-repository=in_house_snapshot --distribution-version=7.0.0-SNAPSHOT

This will benchmark the latest 7.0.0 snapshot build of Elasticsearch.

``report-format``
~~~~~~~~~~~~~~~~~

The command line reporter in Rally displays a table with key metrics after a race. With this option you can specify whether this table should be in ``markdown`` format (default) or ``csv``.

``show-in-report``
~~~~~~~~~~~~~~~~~~

By default, the command line reporter will only show values that are available (``available``). With ``all`` you can force it to show a line for every value, even undefined ones, and with ``all-percentiles`` it will show only available values but force output of all possible percentile values.

This command line parameter is not available for comparing races.


``report-file``
~~~~~~~~~~~~~~~

By default, the command line reporter will print the results only on standard output, but can also write it to a file.

**Example**

 ::

   esrally --report-format=csv --report-file=~/benchmarks/result.csv

``client-options``
~~~~~~~~~~~~~~~~~~

With this option you can customize Rally's internal Elasticsearch client.

It accepts a list of comma-separated key-value pairs. The key-value pairs have to be delimited by a colon. These options are passed directly to the Elasticsearch Python client API. See `their documentation on a list of supported options <http://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch>`_.

We support the following data types:

* Strings: Have to be enclosed in single quotes. Example: ``ca_certs:'/path/to/CA_certs'``
* Numbers: There is nothing special about numbers. Example: ``sniffer_timeout:60``
* Booleans: Specify either ``true`` or ``false``. Example: ``use_ssl:true``

In addition to the options, supported by the Elasticsearch client, it is also possible to enable HTTP compression by specifying ``compressed:true``

Default value: ``timeout:60``

.. warning::
   If you provide your own client options, the default value will not be magically merged. You have to specify all client options explicitly. The only exceptions to this rule is ``ca_cert`` (see below).

**Examples**

Here are a few common examples:

* Enable HTTP compression: ``--client-options="compressed:true"``
* Enable SSL (e.g. if you have X-Pack Security installed): ``--client-options="use_ssl:true,verify_certs:true"``. Note that you don't need to set ``ca_cert`` (which defines the path to the root certificates). Rally does this automatically for you.
* Enable SSL with a client key and certificate: ``--client-options="use_ssl:true,verify_certs:true,ca_certs:'/path/to/cacert.pem',client_cert:'/path/to/client_cert.pem',client_key='/path/to/client_key.pem"`` (see also the `Elasticsearch Python client docs <http://elasticsearch-py.readthedocs.io/en/master/index.html#ssl-and-authentication>`_)
* Enable basic authentication: ``--client-options="basic_auth_user:'user',basic_auth_password:'password'"``. Avoid the characters ``'``, ``,`` and ``:`` in user name and password as Rally's parsing of these options is currently really simple and there is no possibility to escape characters.

``on-error``
~~~~~~~~~~~~

This option controls whether Rally will ``continue`` or ``abort`` when a request error occurs. By default, Rally will just record errors and report the error rate at the end of a race. With ``--on-error=abort``, Rally will immediately abort the race on the first error and print a detailed error message.

``load-driver-hosts``
~~~~~~~~~~~~~~~~~~~~~

By default, Rally will run its load driver on the same machine where you start the benchmark. However, if you benchmark larger clusters, one machine may not be enough to generate sufficient load. Hence, you can specify a comma-separated list of hosts which should be used to generate load with ``--load-driver-hosts``.

**Example**

 ::

   esrally --load-driver-hosts=10.17.20.5,10.17.20.6

In the example, above Rally will generate load from the hosts ``10.17.20.5`` and ``10.17.20.6``. For this to work, you need to start a Rally daemon on these machines, see :ref:`distributing the load test driver <recipe_distributed_load_driver>` for a complete example.

``target-hosts``
~~~~~~~~~~~~~~~~

If you run the ``benchmark-only`` :doc:`pipeline </pipelines>` or you want Rally to :doc:`benchmark a remote cluster </recipes>`, then you can specify a comma-delimited list of hosts:port pairs to which Rally should connect. The default value is ``127.0.0.1:9200``.

**Example**

 ::

   esrally --pipeline=benchmark-only --target-hosts=10.17.0.5:9200,10.17.0.6:9200

This will run the benchmark against the hosts 10.17.0.5 and 10.17.0.6 on port 9200. See ``client-options`` if you use X-Pack Security and need to authenticate or Rally should use https.

You can also target multiple clusters with ``--target-hosts`` for specific use cases. This is described in the :ref:`Advanced topics section <command_line_reference_advanced_topics>`.

``quiet``
~~~~~~~~~

Suppresses some output on the command line.

``offline``
~~~~~~~~~~~

Tells Rally that it should assume it has no connection to the Internet when checking for track data. The default value is ``false``. Note that Rally will only assume this for tracks but not for anything else, e.g. it will still try to download Elasticsearch distributions that are not locally cached or fetch the Elasticsearch source tree.

``preserve-install``
~~~~~~~~~~~~~~~~~~~~

Rally usually installs and launches an Elasticsearch cluster internally and wipes the entire directory after the benchmark is done. Sometimes you want to keep this cluster including all data after the benchmark has finished and that's what you can do with this flag. Note that depending on the track that has been run, the cluster can eat up a very significant amount of disk space (at least dozens of GB). The default value is configurable in the advanced configuration but usually ``false``.

.. note::
   This option does only affect clusters that are provisioned by Rally. More specifically, if you use the pipeline ``benchmark-only``, this option is ineffective as Rally does not provision a cluster in this case.

``advanced-config``
~~~~~~~~~~~~~~~~~~~

This flag determines whether Rally should present additional (advanced) configuration options. The default value is ``false``.

**Example**

 ::

   esrally configure --advanced-config


``assume-defaults``
~~~~~~~~~~~~~~~~~~~

This flag determines whether Rally should automatically accept all values for configuration options that provide a default. This is mainly intended to configure Rally automatically in CI runs. The default value is ``false``.

**Example**

 ::

   esrally configure --assume-defaults=true

``user-tag``
~~~~~~~~~~~~

This is only relevant when you want to run :doc:`tournaments </tournament>`. You can use this flag to attach an arbitrary text to the meta-data of each metric record and also the corresponding race. This will help you to recognize a race when you run ``esrally list races`` as you don't need to remember the concrete timestamp on which a race has been run but can instead use your own descriptive names.

The required format is ``key`` ":" ``value``. You can choose ``key`` and  ``value`` freely.

**Example**

 ::

   esrally --user-tag="intention:github-issue-1234-baseline,gc:cms"

You can also specify multiple tags. They need to be separated by a comma.

**Example**

 ::

   esrally --user-tag="disk:SSD,data_node_count:4"



When you run ``esrally list races``, this will show up again::

    Race Timestamp    Track    Track Parameters   Challenge            Car       User Tag
    ----------------  -------  ------------------ -------------------  --------  ------------------------------------
    20160518T122341Z  pmc                         append-no-conflicts  defaults  intention:github-issue-1234-baseline
    20160518T112341Z  pmc                         append-no-conflicts  defaults  disk:SSD,data_node_count:4

This will help you recognize a specific race when running ``esrally compare``.

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

``client-options`` can optionally specify options for the Elasticsearch clients when multiple clusters have been defined with ``target-hosts``. If omitted, the default is ``timeout:60`` for all cluster connections.

The format is similar to ``target-hosts``, supporting both filenames ending in ``.json`` or inline JSON, however, the parameters are a collection of name:value pairs, as opposed to arrays.

Examples, assuming that two clusters have been specified with ``--target-hosts``:

* json file: ``--client-options="client_options1.json"``::

    {
      "default": {
        "timeout": 60
    },
      "remote": {
        "use_ssl": true,
        "verify_certs": false,
        "ca_certs": "/path/to/cacert.pem"
      }
    }

* json inline string defining two clusters::

    --client-options="{\"default\":{\"timeout\": 60}, \"remote\": {\"use_ssl\":true,\"verify_certs\":false,\"ca_certs\":\"/path/to/cacert.pem\"}}"

.. WARNING::
   If you use ``client-options`` you must specify options for **every** cluster name defined with ``target-hosts``. Rally will raise an error if there is a mismatch.
