Configuration
=============

Rally has to be configured once after installation. If you just run ``esrally`` after installing Rally, it will detect that the configuration file is missing and asks you a few questions.

If you want to reconfigure Rally at any later time, just run ``esrally configure`` again.

Simple Configuration
--------------------

By default, Rally will run a simpler configuration routine and autodetect as much settings as possible or choose defaults for you. If you need more control you can run Rally with ``esrally configure --advanced-config``.

Rally can build Elasticsearch either from sources or use an `official binary distribution <https://www.elastic.co/downloads/elasticsearch>`_. If you have Rally build Elasticsearch from sources, it can only be used to benchmark Elasticsearch 5.0 and above. The reason is that with Elasticsearch 5.0 the build tool switched from Maven to Gradle. As Rally utilizes the Gradle Wrapper, it is limited to Elasticsearch 5.0 and above.

Let's go through an example step by step: First run ``esrally``::

    dm@io:~ $ esrally

        ____        ____
       / __ \____ _/ / /_  __
      / /_/ / __ `/ / / / / /
     / _, _/ /_/ / / / /_/ /
    /_/ |_|\__,_/_/_/\__, /
                    /____/

    Running simple configuration. Run the advanced configuration with:

      rally configure --advanced-config

    * Autodetecting available third-party software
      git    : [OK]
      JDK    : [OK]

    * Setting up benchmark data directory in /Users/dm/.rally/benchmarks
    Enter the JDK 10 root directory (Press Enter to skip):

As you can see above, Rally autodetects if git and a JDK are installed. It also searches for a JDK 10; if you don't have it, that's no problem, you are just not able to build Elasticsearch from sources. Let's assume you press Enter and don't specify a path for JDK 10::

    ********************************************************************************
    You don't have a valid JDK 10 installation and cannot benchmark source builds.

    You can still benchmark binary distributions with e.g.:

      rally --distribution-version=6.0.0
    ********************************************************************************

As you can see, Rally tells you that you cannot build Elasticsearch from sources but you can still benchmark official binary distributions.

It's also possible that Rally cannot automatically your JDK home directory. In that case, it will ask you later in the configuration process. If you do not provide a JDK home directory, Rally cannot start Elasticsearch on this machine but you can still use it as a load generator to :doc:`benchmark remote clusters </recipes>`.

If you specify a valid path for JDK 10, Rally will try to autodetect your Elasticsearch project directory (either in the current directory or in ``../elasticsearch``) or will choose a default directory::

    * Setting up benchmark data directory in /Users/dm/.rally/benchmarks
    * Setting up benchmark source directory in /Users/dm/.rally/benchmarks/src/elasticsearch

If a valid path for JDK 10 was not found (or entered), it will not ask you for a source directory and just go on.

Now Rally is done::

    Configuration successfully written to /Users/dm/.rally/rally.ini. Happy benchmarking!

    To benchmark Elasticsearch 6.0.0 with the default benchmark, run:

      esrally --distribution-version=6.0.0

    More info about Rally:

    * Type esrally --help
    * Read the documentation at https://esrally.readthedocs.io/en/latest/
    * Ask a question on the forum at https://discuss.elastic.co/c/elasticsearch/rally

Congratulations! Time to :doc:`run your first benchmark </race>`.

Advanced Configuration
----------------------

If you need more control over a few variables or want to store your metrics in a dedicated Elasticsearch metrics store, then you should run the advanced configuration routine. You can invoke it at any time with ``esrally configure --advanced-config``.

Prerequisites
~~~~~~~~~~~~~

When using the advanced configuration, you can choose that Rally stores its metrics not in-memory but in a dedicated Elasticsearch instance. Therefore, you will also need the following software installed:

* Elasticsearch: a dedicated Elasticsearch instance which acts as the metrics store for Rally. If you don't want to set it up yourself you can also use `Elastic Cloud <https://www.elastic.co/cloud>`_.
* Optional: Kibana (also included in `Elastic Cloud <https://www.elastic.co/cloud>`_).

Preparation
~~~~~~~~~~~

First `install Elasticsearch <https://www.elastic.co/downloads/elasticsearch>`_ 5.0 or higher. A simple out-of-the-box installation with a single node will suffice. Rally uses this instance to store metrics data. It will setup the necessary indices by itself. The configuration procedure of Rally will you ask for host and port of this cluster.

.. note::

   Rally will choose the port range 39200-39300 (HTTP) and 39300-39400 (transport) for the benchmark cluster, so please ensure that this port range is not used by the metrics store.

Optional but recommended is to install also `Kibana <https://www.elastic.co/downloads/kibana>`_. However, note that Kibana will not be auto-configured by Rally.

Configuration Options
~~~~~~~~~~~~~~~~~~~~~

Rally will ask you a few more things in the advanced setup:

* **Benchmark data directory**: Rally stores all benchmark related data in this directory which can take up to several tens of GB. If you want to use a dedicated partition, you can specify a different data directory here.
* **Elasticsearch project directory**: This is the directory where the Elasticsearch sources are located. If you don't actively develop on Elasticsearch you can just leave the default but if you want to benchmark local changes you should point Rally to your project directory. Note that Rally will run builds with the Gradle Wrapper in this directory (it runs ``./gradlew clean`` and ``./gradlew :distribution:tar:assemble``).
* **JDK root directory**: Rally will only ask this if it could not autodetect the JDK home by itself. Just enter the root directory of the JDK you want to use. By default, Rally will choose Java 8 if available and fallback to Java 10.
* **Metrics store type**: You can choose between ``in-memory`` which requires no additional setup or ``elasticsearch`` which requires that you start a dedicated Elasticsearch instance to store metrics but gives you much more flexibility to analyse results.
* **Metrics store settings** (only for metrics store type ``elasticsearch``): Provide the connection details to the Elasticsearch metrics store. This should be an instance that you use just for Rally but it can be a rather small one. A single node cluster with default setting should do it. When using self-signed certificates on the Elasticsearch metrics store, certificate verification can be turned off by setting the ``datastore.ssl.verification_mode`` setting to ``none``. Alternatively you can enter the path to the certificate authority's signing certificate in ``datastore.ssl.certificate_authorities``. Both settings are optional.
* **Name for this benchmark environment** (only for metrics store type ``elasticsearch``): You can use the same metrics store for multiple environments (e.g. local, continuous integration etc.) so you can separate metrics from different environments by choosing a different name.
* whether or not Rally should keep the Elasticsearch benchmark candidate installation including all data by default. This will use lots of disk space so you should wipe ``~/.rally/benchmarks/races`` regularly.

Proxy Configuration
-------------------

Rally downloads all necessary data automatically for you:

* Elasticsearch distributions from elastic.co if you specify ``--distribution-version=SOME_VERSION_NUMBER``
* Elasticsearch source code from Github if you specify a revision number e.g. ``--revision=952097b``
* Track meta-data from Github
* Track data from an S3 bucket

Hence, it needs to connect via http(s) to the outside world. If you are behind a corporate proxy you need to configure Rally and git. As many other Unix programs, Rally relies that the HTTP proxy URL is available in the environment variable ``http_proxy`` (note that this is in lower-case). Hence, you should add this line to your shell profile, e.g. ``~/.bash_profile``::

    export http_proxy=http://proxy.acme.org:8888/

Afterwards, source the shell profile with ``source ~/.bash_profile`` and verify that the proxy URL is correctly set with ``echo $http_proxy``.

Finally, you can set up git::

    git config --global http.proxy $http_proxy

For details, please refer to the `Git config documentation <https://git-scm.com/docs/git-config>`_.

Please verify that the proxy setup for git works correctly by cloning any repository, e.g. the ``rally-tracks`` repository::

    git clone https://github.com/elastic/rally-tracks.git

If the configuration is correct, git will clone this repository. You can delete the folder ``rally-tracks`` after this verification step.

To verify that Rally will connect via the proxy server you can check the log file. If the proxy server is configured successfully, Rally will log the following line on startup::

    Rally connects via proxy URL [http://proxy.acme.org:3128/] to the Internet (picked up from the environment variable [http_proxy]).


.. note::

   Rally will use this proxy server only for downloading benchmark-related data. It will not use this proxy for the actual benchmark.
