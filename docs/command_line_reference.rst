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
* races: Will show all races that are currently stored. This is needed for the :doc:`tournament mode </tournament>`.
* cars: Will show all cars that are supported by Rally (i.e. Elasticsearch configurations).

To list a specific configuration option, place it after the ``list`` subcommand. For example, ``esrally list pipelines`` will list all pipelines known to Rally.


``compare``
~~~~~~~~~~~

This subcommand is needed for :doc:`tournament mode </tournament>` and its usage is described there.

``configure``
~~~~~~~~~~~~~

This subcommand is needed to :doc:`configure </configuration>` Rally. It is implicitly chosen if you start Rally for the first time but you can rerun this command at any time.

Command Line Flags
------------------

``track-repository``
~~~~~~~~~~~~~~~~~~~~

Selects the track repository that Rally should use to resolve tracks. By default the ``default`` track repository is used, which is available in the Github project `rally-tracks <https://github.com/elastic/rally-tracks>`_. See :doc:`adding tracks </adding_tracks>` on how to add your own track repositories.

``track``
~~~~~~~~~

Selects the track that Rally should run. By default the ``geonames`` track is run. For more details on how tracks work, see :doc:`adding tracks </adding_tracks>`.

``challenge``
~~~~~~~~~~~~~

A track consists of one or more challenges. With this flag you can specify which challenge should be run. If you don't specify a challenge, Rally derives the default challenge itself. To see the default challenge of a track, run ``esrally list tracks``.


``team-repository``
~~~~~~~~~~~~~~~~~~~

Selects the team repository that Rally should use to resolve cars. By default the ``default`` team repository is used, which is available in the Github project `rally-teams <https://github.com/elastic/rally-teams>`_. See the documentation about :doc:`cars </car>` on how to add your own team repositories.


``car``
~~~~~~~

A car defines the Elasticsearch configuration that will be used for the benchmark.

``pipeline``
~~~~~~~~~~~~

Selects the :doc:`pipeline </pipelines>` that Rally should run.

Rally can autodetect the pipeline in most cases. If you specify ``--distribution-version`` it will auto-select the pipeline ``from-distribution`` otherwise it will use ``from-sources-complete``.

``laps``
~~~~~~~~

Allows to run the benchmark for multiple laps (defaults to 1 lap). Each lap corresponds to one full execution of a track but note that the benchmark candidate is not restarted between laps.

.. _clr_enable_driver_profiling:

``enable-driver-profiling``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

This option enables a profiler on all operations that the load test driver performs. It is intended to help track authors spot accidental bottlenecks, especially if they implement their own runners or parameter sources. When this mode is enabled, Rally will enable a profiler in the load driver module. After each task and for each client, Rally will add the profile information to a dedicated profile log file. For example::

   2017-02-09 08:23:24,35 rally.profile INFO
   === Profile START for client [0] and operation [index-append-1000] ===
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

   === Profile END for client [0] and operation [index-append-1000] ===

In this example we can spot quickly that ``Random.seed`` is called excessively, causing an accidental bottleneck in the load test driver.

.. _clr_test_mode:

``test-mode``
~~~~~~~~~~~~~

Allows you to test a track without running it for the whole duration. This mode is only intended for quick sanity checks when creating a track. Please don't rely on these numbers at all (they are meaningless).

If you write your own track, please keep in mind that you need :ref:`prepare your track to support this mode <add_track_test_mode>`.

``telemetry``
~~~~~~~~~~~~~

Activates the provided :doc:`telemetry devices </telemetry>` for this race.

**Example**

 ::

   esrally --telemetry=jfr,jit


This activates Java flight recorder and the JIT compiler telemetry devices.

.. _clr_revision:

``revision``
~~~~~~~~~~~~

If you actively develop Elasticsearch and want to benchmark a source build of Elasticsearch (which will Rally create for you), you can specify the git revision of Elasticsearch that you want to benchmark. But note that Rally does only support Gradle as build tool which effectively means that it will only support this for Elasticsearch 5.0 or better. The default value is ``current``.

You can specify the revision in different formats:

* ``--revision=latest``: Use the HEAD revision from origin/master.
* ``--revision=current``: Use the current revision (i.e. don't alter the local source tree).
* ``--revision=abc123``: Where ``abc123`` is some git revision hash.
* ``--revision=@2013-07-27T10:37:00Z``: Determines the revision that is closest to the provided date. Rally logs to which git revision hash the date has been resolved and if you use Elasticsearch as metrics store (instead of the default in-memory one), :doc:`each metric record will contain the git revision hash also in the meta-data section </metrics>`.

Supported date format: If you specify a date, it has to be ISO-8601 conformant and must start with an ``@`` sign to make it easier for Rally to determine that you actually mean a date.

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

Rally does not only support benchmarking official distributions but can also benchmark snapshot builds. This is option is really just intended for `our benchmarks that are run in continuous integration <https://elasticsearch-benchmarks.elastic.co/>`_ but if you want to, you can use it too. The only supported values are ``release`` (default) and ``snapshot``.

**Example**

 ::

   esrally --distribution-repository=snapshot --distribution-version=6.0.0-SNAPSHOT

This will benchmark the latest 6.0.0 snapshot build of Elasticsearch that is available in the Sonatype repository.

``report-format``
~~~~~~~~~~~~~~~~~

The command line reporter in Rally displays a table with key metrics after a race. With this option you can specify whether this table should be in ``markdown`` format (default) or ``csv``.

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

Default value: ``timeout:60000,request_timeout:60000``

.. warning::
   If you provide your own client options, the default value will not be magically merged. You have to specify all client options explicitly. The only exceptions to this rule is ``ca_cert`` (see below).

**Examples**

Here are a few common examples:

* Enable HTTP compression: ``--client-options="compressed:true"``
* Enable SSL (if you have Shield installed): ``--client-options="use_ssl:true,verify_certs:true"``. Note that you don't need to set ``ca_cert`` (which defines the path to the root certificates). Rally does this automatically for you.
* Enable basic authentication: ``--client-options="basic_auth_user:'user',basic_auth_password:'password'"``. Please avoid the characters ``'``, ``,`` and ``:`` in user name and password as Rally's parsing of these options is currently really simple and there is no possibility to escape characters.

``target-hosts``
~~~~~~~~~~~~~~~~

If you run the ``benchmark-only`` :doc:`pipeline </pipelines>`, then you can specify a comma-delimited list of hosts:port pairs to which Rally should connect. The default value is ``127.0.0.1:9200``.

**Example**

 ::

   esrally --pipeline=benchmark-only --target-hosts=10.17.0.5:9200,10.17.0.6:9200

This will run the benchmark against the hosts 10.17.0.5 and 10.17.0.6 on port 9200. See ``client-options`` if you use Shield and need to authenticate or Rally should use https.

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

``cluster-health``
~~~~~~~~~~~~~~~~~~

Rally checks whether the cluster health is "green" before it runs a benchmark against it. The main reason is that we don't want to benchmark a cluster which is shuffling shards around or might start doing so. If you really need to run a benchmark against a cluster that is "yellow" or "red", then you can explicitly override Rally's default behavior. But please think twice before doing so and rather eliminate the root cause.

**Example**

 ::

   esrally --cluster-health=yellow



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

The required format is ``key`` ":" ``value``. You can choose ``key`` and  ``value`` freely. Note that only one user tag is supported.

**Example**

 ::

   esrally --user-tag="intention:github-issue-1234-baseline"

You can also specify multiple tags. They need to be separated by a comma::

**Example**

 ::

   esrally --user-tag="disk:SSD,data_node_count:4"



When you run ``esrally list races``, this will show up again::

    dm@io:~ $ esrally list races

        ____        ____
       / __ \____ _/ / /_  __
      / /_/ / __ `/ / / / / /
     / _, _/ /_/ / / / /_/ /
    /_/ |_|\__,_/_/_/\__, /
                    /____/
    Recent races:

    Race Timestamp    Track    Challenge            Car       User Tag
    ----------------  -------  -------------------  --------  ------------------------------------
    20160518T122341Z  pmc      append-no-conflicts  defaults  intention:github-issue-1234-baseline
    20160518T112341Z  pmc      append-no-conflicts  defaults  disk:SSD,data_node_count:4

This will help you recognize a specific race when running ``esrally compare``.
