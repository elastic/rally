Configuration
=============

Rally Configuration
-------------------

Rally stores its configuration in the file ``~/.rally/rally.ini`` which is automatically created the first time Rally is executed. It comprises the following sections.

meta
~~~~

This section contains meta information about the configuration file.

* ``config.version``: The version of the configuration file format. This property is managed by Rally and should not be changed.

system
~~~~~~

This section contains global information for the current benchmark environment. This information should be identical on all machines where Rally is installed.

* ``env.name`` (default: "local"): The name of this benchmark environment. It is used as meta-data in metrics documents if an Elasticsearch metrics store is configured. Only alphanumeric characters are allowed.
* ``probing.url`` (default: "https://github.com"): This URL is used by Rally to check for a working Internet connection. It's useful to change this to an internal server if all data are hosted inside the corporate network and connections to the outside world are prohibited.
* ``available.cores`` (default: number of logical CPU cores): Determines the number of available CPU cores. Rally aims to create one asyncio event loop per core and will distribute clients evenly across event loops.
* ``async.debug`` (default: false): Enables debug mode on Rally's internal `asyncio event loop <https://docs.python.org/3/library/asyncio-eventloop.html#enabling-debug-mode>`_. This setting is mainly intended for troubleshooting.
* ``passenv`` (default: "PATH"): A comma-separated list of environment variable names that should be passed to the Elasticsearch process.

node
~~~~

This section contains machine-specific information.

* ``root.dir`` (default: "~/.rally/benchmarks"): Rally uses this directory to store all benchmark-related data. It assumes that it has complete control over this directory and any of its subdirectories.
* ``src.root.dir`` (default: "~/.rally/benchmarks/src"): The directory where the source code of Elasticsearch or any plugins is checked out. Only relevant for benchmarks from sources.

source
~~~~~~

This section contains more details about the source tree.

* ``remote.repo.url`` (default: "https://github.com/elastic/elasticsearch.git"): The URL from which to checkout Elasticsearch.
* ``elasticsearch.src.subdir`` (default: "elasticsearch"): The local path, relative to ``src.root.dir``, of the Elasticsearch source tree.
* ``cache`` (default: true): Enables Rally's internal :ref:`source artifact <pipelines_from-sources>` cache (``elasticsearch*.tar.gz`` and optionally ``*.zip`` files for plugins). Artifacts are cached based on their git revision.
* ``cache.days`` (default: 7): The number of days for which an artifact should be kept in the source artifact cache.

benchmarks
~~~~~~~~~~

This section contains details about the benchmark data directory.

* ``local.dataset.cache`` (default: "~/.rally/benchmarks/data"): The directory in which benchmark data sets are stored. Depending on the benchmarks that are executed, this directory may contain hundreds of GB of data.

reporting
~~~~~~~~~

This section defines how metrics are stored.

* ``datastore.type`` (default: "in-memory"): If set to "in-memory" all metrics will be kept in memory while running the benchmark. If set to "elasticsearch" all metrics will instead be written to a persistent metrics store and the data are available for further analysis.
* ``sample.queue.size`` (default: 2^20): The number of metrics samples that can be stored in Rally's in-memory queue.
* ``"metrics.request.downsample.factor`` (default: 1): Determines how many service time and latency samples should be kept in the metrics store. By default all values will be kept. To keep only e.g. every 100th sample, specify 100. This is useful to avoid overwhelming the metrics store in benchmarks with many clients (tens of thousands).
* ``output.processingtime`` (default: false): If set to "true", Rally will show a metric, called "processing time" in the command line report. Contrary to "service time" which is measured as close as possible to the wire, "processing time" also includes Rally's client side processing overhead. Large differences between the service time and the reporting time indicate a high overhead in the client and can thus point to a potential client-side bottleneck which requires investigation.

The following settings are applicable only if ``datastore.type`` is set to "elasticsearch":

* ``datastore.host``: The host name of the metrics store, e.g. "10.17.20.33".
* ``datastore.port``: The port of the metrics store, e.g. "9200".
* ``datastore.secure``: If set to ``false``, Rally assumes a HTTP connection. If set to ``true``, it assumes a HTTPS connection.
* ``datastore.ssl.verification_mode`` (default: "full"): By default the metric store's SSL certificate is checked ("full"). To disable certificate verification set this value to "none".
* ``datastore.ssl.certificate_authorities`` (default: empty): Determines the path on the local file system to the certificate authority's signing certificate.
* ``datastore.user``: Sets the name of the Elasticsearch user for the metrics store.
* ``datastore.password``: Sets the password of the Elasticsearch user for the metrics store.
* ``datastore.probe.cluster_version`` (default: true): Enables automatic detection of the metric store's version.

**Examples**

Define an unprotected metrics store in the local network::

    [reporting]
    datastore.type = elasticsearch
    datastore.host = 192.168.10.17
    datastore.port = 9200
    datastore.secure = false
    datastore.user =
    datastore.password =

Define a secure connection to a metrics store in the local network with a self-signed certificate::

    [reporting]
    datastore.type = elasticsearch
    datastore.host = 192.168.10.22
    datastore.port = 9200
    datastore.secure = true
    datastore.ssl.verification_mode = none
    datastore.user = rally
    datastore.password = the-password-to-your-cluster

Define a secure connection to an Elastic Cloud cluster::

    [reporting]
    datastore.type = elasticsearch
    datastore.host = 123456789abcdef123456789abcdef1.europe-west4.gcp.elastic-cloud.com
    datastore.port = 9243
    datastore.secure = true
    datastore.user = rally
    datastore.password = the-password-to-your-cluster


tracks
~~~~~~

This section defines how :doc:`tracks </track>` are retrieved. All keys are read by Rally using the convention ``<<track-repository-name>>.url``, e.g. ``custom-track-repo.url`` which can be selected the command-line via ``--track-repository="custom-track-repo"``. By default, Rally chooses the track repository specified via ``default.url`` which points to https://github.com/elastic/rally-tracks.

teams
~~~~~

This section defines how :doc:`teams </car>` are retrieved. All keys are read by Rally using the convention ``<<team-repository-name>>.url``, e.g. ``custom-team-repo.url`` which can be selected the command-line via ``--team-repository="custom-team-repo"``. By default, Rally chooses the track repository specified via ``default.url`` which points to https://github.com/elastic/rally-teams.

defaults
~~~~~~~~

This section defines default values for certain command line parameters of Rally.

* ``preserve_benchmark_candidate`` (default: false): Determines whether Elasticsearch installations will be preserved or wiped by default after a benchmark. For preserving an installation for a single benchmark, use the command line flag ``--preserve-install``.

distributions
~~~~~~~~~~~~~

* ``release.cache`` (default: true): Determines whether released Elasticsearch versions should be cached locally.

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

Finally, you can set up git (see also the `Git config documentation <https://git-scm.com/docs/git-config>`_)::

    git config --global http.proxy $http_proxy

Verify that the proxy setup for git works correctly by cloning any repository, e.g. the ``rally-tracks`` repository::

    git clone https://github.com/elastic/rally-tracks.git

If the configuration is correct, git will clone this repository. You can delete the folder ``rally-tracks`` after this verification step.

To verify that Rally will connect via the proxy server you can check the log file. If the proxy server is configured successfully, Rally will log the following line on startup::

    Rally connects via proxy URL [http://proxy.acme.org:3128/] to the Internet (picked up from the environment variable [http_proxy]).


.. note::

   Rally will use this proxy server only for downloading benchmark-related data. It will not use this proxy for the actual benchmark.

.. _logging:

Logging
-------

Logging in Rally is configured in ``~/.rally/logging.json``. For more information about the log file format please refer to the following documents:

* `Python logging cookbook <https://docs.python.org/3/howto/logging-cookbook.html>`_ provides general tips and tricks.
* The Python reference documentation on the `logging configuration schema <https://docs.python.org/3/library/logging.config.html#logging-config-dictschema>`_ explains the file format.
* The `logging handler documentation <https://docs.python.org/3/library/logging.handlers.html>`_ describes how to customize where log output is written to.

By default, Rally will log all output to ``~/.rally/logs/rally.log``.

The log file will not be rotated automatically as this is problematic due to Rally's multi-process architecture. Setup an external tool like `logrotate <https://linux.die.net/man/8/logrotate>`_ to achieve that. See the following example as a starting point for your own ``logrotate`` configuration and ensure to replace the path ``/home/user/.rally/logs/rally.log`` with the proper one::

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

Example
~~~~~~~

With the following configuration Rally will log all output to standard error::

    {
      "version": 1,
      "formatters": {
        "normal": {
          "format": "%(asctime)s,%(msecs)d %(actorAddress)s/PID:%(process)d %(name)s %(levelname)s %(message)s",
          "datefmt": "%Y-%m-%d %H:%M:%S",
          "()": "esrally.log.configure_utc_formatter"
        }
      },
      "filters": {
        "isActorLog": {
          "()": "thespian.director.ActorAddressLogFilter"
        }
      },
      "handlers": {
        "console_log_handler": {
            "class": "logging.StreamHandler",
            "formatter": "normal",
            "filters": ["isActorLog"]
        }
      },
      "root": {
        "handlers": ["console_log_handler"],
        "level": "INFO"
      },
      "loggers": {
        "elasticsearch": {
          "handlers": ["console_log_handler"],
          "level": "WARNING",
          "propagate": false
        }
      }
    }
