Command Line Reference
======================

Subcommands determine which task Rally performs. Command line flags are used to customize Rally's behavior and not all command line flags can be used for each subcommand. For example, it makes no sense to run ``esrally compare --preserve-install``.

To find out which command line flags are supported by a specific subcommand, just run ``esrally <<subcommand>> --help``.

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
* races: Will show all races that are currently stored. This is basically only needed for the :doc:`tournament mode </tournament>` and it will also only work if you have setup Rally so it supports tournaments.
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

Selects the track repository that Rally should use to resolve tracks. By default the ``default`` track repository is used, which is available on `Github <https://github.com/elastic/rally-tracks>`_. See :doc:`adding benchmarks </adding_benchmarks>` on how to add your own track repositories.

``track``
~~~~~~~~~

Selects the track that Rally should run. By default the ``geonames`` track is run. For more details on how tracks work, see :doc:`adding benchmarks </adding_benchmarks>`.

``challenge``
~~~~~~~~~~~~~

A track consists of one or more challenges. With this flag you can specify which challenge should be run.

``car``
~~~~~~~

A car defines the Elasticsearch configuration that will be used for the benchmark.

``pipeline``
~~~~~~~~~~~~

Selects the :doc:`pipeline </pipelines>` that Rally should run.

``rounds``
~~~~~~~~~~

Allows to run the benchmark for multiple rounds (defaults to 1 round). Note that the benchmark candidate is not restarted between rounds.

``telemetry``
~~~~~~~~~~~~~

Activates the provided :doc:`telemetry devices </telemetry>` for this race.

**Example**

 ::

   esrally --telemetry=jfr,jit


This activates Java flight recorder and the JIT compiler telemetry devices.

.. _command_line_reference_revision:

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

   esrally --pipeline=from-distribution --distribution-version=2.3.3


Rally will then benchmark the official Elasticsearch 2.3.3 distribution.

``distribution-repository``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Rally does not only support benchmarking official distributions but can also benchmark snapshot builds. This is option is really just intended for `our benchmarks that are run in continuous integration <https://elasticsearch-benchmarks.elastic.co/>`_ but if you want to, you can use it too. The only supported values are ``release`` (default) and ``snapshot``.

**Example**

 ::

   esrally --pipeline=from-distribution --distribution-repository=snapshot --distribution-version=5.0.0-SNAPSHOT

This will benchmark the latest 5.0.0 snapshot build of Elasticsearch that is available in the Sonatype repository.

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

**Example**

 ::

   esrally --user-tag=github-issue-1234-baseline


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
    ----------------  -------  -------------------  --------  ------------------------------
    20160518T122341Z  pmc      append-no-conflicts  defaults  github-issue-1234-baseline

This will help you recognize a specific race when running ``esrally compare``.
