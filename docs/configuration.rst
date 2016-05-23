Configuration
=============

Rally has to be configured before it is ready. Just run ``esrally`` after installation. Rally will detect that the configuration is missing and ask a few questions.

If you want to reconfigure Rally at any later time, just run ``esrally configure``.

Simple Configuration
--------------------

By default, Rally will run a simpler configuration routine and autodetect as much settings as possible or choose defaults for you. If you need more control you can run Rally with ``esrally configure --advanced-config``.

Rally can build Elasticsearch either from sources or use an `official binary distribution <https://www.elastic.co/downloads/elasticsearch>`_. If you have Rally build Elasticsearch from sources, it can only be used to benchmark Elasticsearch 5.0 and above. The reason is that with Elasticsearch 5.0 the build tool was switched from Maven to Gradle. As Rally only supports Gradle, it is limited to Elasticsearch 5.0 and above.

If you want to build Elasticsearch from sources you will also need:

* Gradle 2.8+
* git

Let's go through an example step by step: First run `esrally`::

    dm@io:~ $ esrally

        ____        ____
       / __ \____ _/ / /_  __
      / /_/ / __ `/ / / / / /
     / _, _/ /_/ / / / /_/ /
    /_/ |_|\__,_/_/_/\__, /
                    /____/

    Running simple configuration. You can run the advanced configuration with:

      esrally configure --advanced-config

    [✓] Autodetecting available third-party software
      git    : [✓]
      gradle : [✓]
      JDK 8  : [✓]

    [✓] Setting up benchmark data directory in [/Users/dm/.rally/benchmarks] (needs several GB).

As you can see above, Rally autodetects if git, Gradle and JDK 8 are installed. If you don't have git or Gradle, that's no problem, you are just not able to build Elasticsearch from sources. Let's assume you don't have Gradle installed::

    dm@io:~ $ esrally

        ____        ____
       / __ \____ _/ / /_  __
      / /_/ / __ `/ / / / / /
     / _, _/ /_/ / / / /_/ /
    /_/ |_|\__,_/_/_/\__, /
                    /____/

    Running simple configuration. You can run the advanced configuration with:

      esrally configure --advanced-config

    [✓] Autodetecting available third-party software
      git    : [✓]
      gradle : [✕]
      JDK 8  : [✓]

    **********************************************************************************
    You don't have the necessary software to benchmark source builds of Elasticsearch.

    You can still benchmark binary distributions with e.g.:

      esrally --pipeline=from-distribution --distribution-version=5.0.0-alpha2

    See https://esrally.readthedocs.io/en/latest/pipelines.html#from-distribution
    **********************************************************************************

As you can see, Rally tells you that you cannot build Elasticsearch from sources but you can still benchmark official binary distributions.

It's also possible that Rally cannot automatically find your JDK 8 home directory. In that case, it will ask you later in the configuration process.

After the initial detection, setup will go on::

    [✓] Setting up benchmark data directory in [/Users/dm/.rally/benchmarks] (needs several GB).
    Enter your Elasticsearch project directory: [default: '/Users/dm/.rally/benchmarks/src']:

If you are ok with this default, just press "Enter" and Rally will take care of the rest. Otherwise, provide your Elasticsearch project directory here. Please keep in mind that Rally will run builds with Gradle in this directory if you start a benchmark.

If Rally has not found git and Gradle in the first step, it will not ask you for a source directory and just go on.

Now Rally is done::

    [✓] Configuration successfully written to [/Users/dm/.rally/rally.ini]. Happy benchmarking!

    To benchmark the latest version of Elasticsearch with the default benchmark run:

      esrally --revision=latest

    For more help see the user documentation at https://esrally.readthedocs.io

Congratulations! Time to run your first benchmark.

Advanced Configuration
----------------------

If you need more control over a few variables or want to use advanced features like :doc:`tournaments </tournament>`, then you should run the advanced configuration routine. You can invoke it at any time with ``esrally configure --advanced-config``.

Prerequisites
~~~~~~~~~~~~~

When using the advanced configuration, Rally stores its metrics not in-memory but in a dedicated Elasticsearch instance. Therefore, you will also need the following software installed:

* Elasticsearch: a dedicated Elasticsearch instance which acts as the metrics store for Rally. If you don't want to set it up yourself you can also use `Elastic Cloud <https://www.elastic.co/cloud>`_.
* Optional: Kibana (also included in `Elastic Cloud <https://www.elastic.co/cloud>`_).
Preparation
~~~~~~~~~~~

First `install Elasticsearch <https://www.elastic.co/downloads/elasticsearch>`_ 2.3 or higher. A simple out-of-the-box installation with a single node will suffice. Rally uses this instance to store metrics data. It will setup the necessary indices by itself. The configuration procedure of Rally will you ask for host and port of this cluster.

.. note::

   Rally will choose the port range 39200-39300 (HTTP) and 39300-39400 (transport) for the benchmark cluster, so please ensure that this port range is not used by the metrics store.

Optional but recommended is to install also `Kibana <https://www.elastic.co/downloads/kibana>`_. Kibana will not be auto-configured but a sample
dashboard is delivered with Rally in ``$PACKAGE_ROOT/esrally/resources/kibana.json`` which can be imported as follows to Kibana:

1. Create a new Kibana instance pointing to Rally's Elasticsearch data store
2. Create an index pattern ``rally-*`` and use ``trial-timestamp`` as time-field name (you might need to import some data first)
3. Go to Settings > Objects and import ``$PACKAGE_ROOT/esrally/resources/kibana.json``. Note that it assumes that the environment name is "nightly". Otherwise you won't see any data in graphs. You can either provide "nightly" as environment name during the initial configuration of Rally or search and replace it in ``$PACKAGE_ROOT/esrally/resources/kibana.json`` with your environment name before uploading it to Kibana.

Configuration Options
~~~~~~~~~~~~~~~~~~~~~

Rally will ask you a few more things in the advanced setup:

* Elasticsearch project directory: This is the directory where the Elasticsearch sources are located. If you don't actively develop on Elasticsearch you can just leave the default but if you want to benchmark local changes you should point Rally to your project directory. Note that Rally will run builds with Gradle in this directory (it runs ``gradle clean`` and ``gradle :distribution:tar:assemble``).
* JDK 8 root directory: Rally will only ask this if it could not autodetect the JDK 8 home by itself. Just enter the root directory of the JDK you want to use.
* Name for this benchmark environment: You can use the same metrics store for multiple environments (e.g. local, continuous integration etc.) so you can separate metrics from different metrics.
* metrics store settings: Provide the connection details to the Elasticsearch metrics store. This should be an instance that you use just for Rally but it can be a rather small one. A single node cluster with default setting should do it. There is currently no support for choosing the in-memory metrics store when you run the advanced configuration. If you really need it, please raise an issue on Github.



