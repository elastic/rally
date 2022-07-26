Using Elasticsearch Plugins
===========================

You can have Rally setup an Elasticsearch cluster with plugins for you. Whereas Rally caches downloaded Elasticsearch distributions, plugins will always be installed via the Internet and thus each machine where an Elasticsearch node will be installed, requires an active Internet connection.

Listing plugins
---------------

To see which plugins are available, run ``esrally list elasticsearch-plugins``::

    Available Elasticsearch plugins:

    Name                     Configuration
    -----------------------  ----------------
    analysis-icu
    analysis-kuromoji
    analysis-phonetic
    analysis-smartcn
    analysis-stempel
    analysis-ukrainian
    discovery-azure-classic
    discovery-ec2
    discovery-file
    discovery-gce
    ingest-attachment
    ingest-geoip
    ingest-user-agent
    lang-javascript
    lang-python
    mapper-attachments
    mapper-murmur3
    mapper-size
    repository-azure
    repository-gcs
    repository-hdfs
    repository-s3
    store-smb

As the availability of plugins may change from release to release we recommend that you include the ``--distribution-version`` parameter when listing plugins. By default Rally assumes that you want to benchmark the latest ``main`` version of Elasticsearch.

Running a benchmark with plugins
--------------------------------
In order to tell Rally to install a plugin, use the ``--elasticsearch-plugins`` parameter when starting a race. You can provide multiple plugins (comma-separated) and they will be installed in the order to that you define on the command line.

Example::

    esrally race --track=geonames --distribution-version=7.12.0 --elasticsearch-plugins="analysis-icu,analysis-phonetic"

This will install the plugins ``analysis-icu`` and ``analysis-phonetic`` (in that order). In order to use the features that these plugins provide, you need to write a :doc:`custom track </adding_tracks>`.

Rally will use several techniques to install and configure plugins:

* First, Rally checks whether directory ``plugins/PLUGIN_NAME`` in the currently configured team repository exists. If this is the case, then plugin installation and configuration details will be read from this directory.
* Next, Rally will use the provided plugin name when running the Elasticsearch plugin installer. With this approach we can avoid to create a plugin configuration directory in the team repository for very simple plugins that do not need any configuration.

As mentioned above, Rally also allows you to specify a plugin configuration and you can even combine them. Here are some examples (requires Elasticsearch < 6.3.0 because with 6.3.0 x-pack has turned into a module of Elasticsearch which is treated as a "car" in Rally):

* Run a benchmark with the ``x-pack`` plugin in the ``security`` configuration: ``--elasticsearch-plugins=x-pack:security``
* Run a benchmark with the ``x-pack`` plugin in the ``security`` and the ``graph`` configuration: ``--elasticsearch-plugins=x-pack:security+graph``

.. note::
    To benchmark the ``security`` configuration of ``x-pack`` you need to add the following command line options: ``--client-options="use_ssl:true,verify_certs:false,basic_auth_user:'rally',basic_auth_password:'rally-password'"``

You can also override plugin variables with ``--plugin-params`` which is needed for example if you want to use the ``monitoring-http`` configuration in order to export monitoring data. You can export monitoring data e.g. with the following configuration::

    --elasticsearch-plugins="x-pack:monitoring-http" --plugin-params="monitoring_type:'http',monitoring_host:'some_remote_host',monitoring_port:10200,monitoring_user:'rally',monitoring_password:'m0n1t0r1ng'"

The ``monitoring_user`` and ``monitoring_password`` parameters are optional, the other parameters are mandatory. For more details on the configuration options check the `Monitoring plugin documentation <https://www.elastic.co/guide/en/x-pack/current/monitoring-production.html>`_.

If you are behind a proxy, set the environment variable ``ES_JAVA_OPTS`` accordingly on each target machine as described in the `Elasticsearch plugin documentation <https://www.elastic.co/guide/en/elasticsearch/plugins/current/_other_command_line_parameters.html#_proxy_settings>`_.

Building plugins from sources
-----------------------------

Plugin authors may want to benchmark source builds of their plugins. Your plugin is either:

* built alongside Elasticsearch
* built against a released version of Elasticsearch

Plugins built alongside Elasticsearch
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To make this work, you need to manually edit Rally's configuration file in ``~/.rally/rally.ini``. Suppose, we want to benchmark the plugin "my-plugin". Then you need to add the following entries in the ``source`` section::

    plugin.my-plugin.remote.repo.url = git@github.com:example-org/my-plugin.git
    plugin.my-plugin.src.subdir = elasticsearch-extra/my-plugin
    plugin.my-plugin.build.command = ./gradlew :my-plugin:plugin:assemble
    plugin.my-plugin.build.artifact.subdir = plugin/build/distributions

Let's discuss these properties one by one:

* ``plugin.my-plugin.remote.repo.url`` (optional): This is needed to let Rally checkout the source code of the plugin. If this is a private repo, credentials need to be setup properly. If the source code is already locally available you may not need to define this property. The remote's name is assumed to be "origin" and this is not configurable. Also, only git is supported as revision control system.
* ``plugin.my-plugin.src.subdir`` (mandatory): This is the directory to which the plugin will be checked out relative to ``src.root.dir``. In order to allow to build the plugin alongside Elasticsearch, the plugin needs to reside in a subdirectory of ``elasticsearch-extra`` (see also the `Elasticsearch testing documentation <https://github.com/elastic/elasticsearch/blob/main/TESTING.asciidoc#building-with-extra-plugins>`_.
* ``plugin.my-plugin.build.command`` (mandatory): The full build command to run in order to build the plugin artifact. Note that this command is run from the Elasticsearch source directory as Rally assumes that you want to build your plugin alongside Elasticsearch (otherwise, see the next section).
* ``plugin.my-plugin.build.artifact.subdir`` (mandatory): This is the subdirectory relative to ``plugin.my-plugin.src.subdir`` in which the final plugin artifact is located.

.. warning::
    ``plugin.my-plugin.build.command`` has replaced ``plugin.my-plugin.build.task`` in earlier Rally versions. It now requires the **full** build command.

In order to run a benchmark with ``my-plugin``, you'd invoke Rally as follows: ``esrally race --track=geonames --revision="elasticsearch:some-elasticsearch-revision,my-plugin:some-plugin-revision" --elasticsearch-plugins="my-plugin"`` where you need to replace ``some-elasticsearch-revision`` and ``some-plugin-revision`` with the appropriate :ref:`git revisions <clr_revision>`. Adjust other command line parameters (like track or car) accordingly. In order for this to work, you need to ensure that:

* All prerequisites for source builds are installed.
* The Elasticsearch source revision is compatible with the chosen plugin revision. Note that you do not need to know the revision hash to build against an already released version and can use git tags instead. E.g. if you want to benchmark against Elasticsearch 7.12.0, you can specify ``--revision="elasticsearch:v7.12.0,my-plugin:some-plugin-revision"`` (see e.g. the `Elasticsearch tags on Github <https://github.com/elastic/elasticsearch/tags>`_ or use ``git tag`` in the Elasticsearch source directory on the console).
* If your plugin needs to be configured, create a proper plugin specification (see below).

.. note::
    Rally can build all `Elasticsearch core plugins <https://github.com/elastic/elasticsearch/tree/main/plugins>`_ out of the box without any further configuration.

Plugins based on a released Elasticsearch version
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To make this work, you need to manually edit Rally's configuration file in ``~/.rally/rally.ini``. Suppose, we want to benchmark the plugin "my-plugin". Then you need to add the following entries in the ``source`` section::

    plugin.my-plugin.remote.repo.url = git@github.com:example-org/my-plugin.git
    plugin.my-plugin.src.dir = /path/to/your/plugin/sources
    plugin.my-plugin.build.command = /usr/local/bin/gradle :my-plugin:plugin:assemble
    plugin.my-plugin.build.artifact.subdir = build/distributions

Let's discuss these properties one by one:

* ``plugin.my-plugin.remote.repo.url`` (optional): This is needed to let Rally checkout the source code of the plugin. If this is a private repo, credentials need to be setup properly. If the source code is already locally available you may not need to define this property. The remote's name is assumed to be "origin" and this is not configurable. Also, only git is supported as revision control system.
* ``plugin.my-plugin.src.dir`` (mandatory): This is the absolute directory to which the source code will be checked out.
* ``plugin.my-plugin.build.command`` (mandatory): The full build command to run in order to build the plugin artifact. This command is run from the plugin project's root directory.
* ``plugin.my-plugin.build.artifact.subdir`` (mandatory): This is the subdirectory relative to ``plugin.my-plugin.src.dir`` in which the final plugin artifact is located.

.. warning::
    ``plugin.my-plugin.build.command`` has replaced ``plugin.my-plugin.build.task`` in earlier Rally versions. It now requires the **full** build command.

In order to run a benchmark with ``my-plugin``, you'd invoke Rally as follows: ``esrally race --track=geonames --distribution-version="elasticsearch-version" --revision="my-plugin:some-plugin-revision" --elasticsearch-plugins="my-plugin"`` where you need to replace ``elasticsearch-version`` with the correct release (e.g. 6.0.0) and ``some-plugin-revision`` with the appropriate :ref:`git revisions <clr_revision>`. Adjust other command line parameters (like track or car) accordingly. In order for this to work, you need to ensure that:

* All prerequisites for source builds are installed.
* The Elasticsearch release is compatible with the chosen plugin revision.
* If your plugin needs to be configured, create a proper plugin specification (see below).

Anatomy of a plugin specification
---------------------------------

Simple plugins
~~~~~~~~~~~~~~

You can use Rally to benchmark community-contributed or even your own plugins. In the simplest case, the plugin does not need any custom configuration. Then you just need to add the download URL to your Rally configuration file. Consider we want to benchmark the plugin "my-plugin"::

    [distributions]
    plugin.my-plugin.release.url=https://example.org/my-plugin/releases/{{VERSION}}/my-plugin-{{VERSION}}.zip

Then you can use ``--elasticsearch-plugins=my-plugin`` to run a benchmark with your plugin. Rally will also replace ``{{VERSION}}`` with the distribution version that you have specified on the command line.

Plugins which require configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If the plugin needs a custom configuration we recommend to fork the `official Rally teams repository <https://github.com/elastic/rally-teams>`_ and add your plugin configuration there. Suppose, you want to benchmark "my-plugin" which has the following settings that can be configured in ``elasticsearch.yml``:

* ``myplugin.active``: a boolean which activates the plugin
* ``myplugin.mode``: Either ``simple`` or ``advanced``

We want to support two configurations for this plugin: ``simple`` which will set ``myplugin.mode`` to ``simple`` and ``advanced`` which will set ``myplugin.mode`` to ``advanced``.

First, we need a template configuration. We will call this a "config base" in Rally. We will just need one config base for this example and will call it "default".

In ``$TEAM_REPO_ROOT`` create the directory structure for the plugin and its config base with `mkdir -p myplugin/default/templates/config` and add the following ``elasticsearch.yml`` in the new directory::

    myplugin.active: true
    myplugin.mode={{my_plugin_mode}}

That's it. Later, Rally will just copy all files in ``myplugin/default/templates`` to the home directory of the Elasticsearch node that it configures. First, Rally will always apply the car's configuration and then plugins can add their configuration on top. This also explains why we have created a ``config/elasticsearch.yml``. Rally will just copy this file and replace template variables on the way.

.. note::
    If you create a new customization for a plugin, ensure that the plugin name in the team repository matches the core plugin name. Note that hyphens need to be replaced by underscores (e.g. "x-pack" becomes "x_pack"). The reason is that Rally allows to write custom install hooks and the plugin name will become the root package name of the install hook. However, hyphens are not supported in Python which is why we use underscores instead.


The next step is now to create our two plugin configurations where we will set the variables for our config base "default". Create a file ``simple.ini`` in the ``myplugin`` directory::

    [config]
    # reference our one and only config base here
    base=default

    [variables]
    my_plugin_mode=simple

Similarly, create ``advanced.ini`` in the ``myplugin`` directory::

    [config]
    # reference our one and only config base here
    base=default

    [variables]
    my_plugin_mode=advanced

Rally will now know about ``myplugin`` and its two configurations. Let's check that with ``esrally list elasticsearch-plugins``::

    Available Elasticsearch plugins:

    Name                     Configuration
    -----------------------  ----------------
    analysis-icu
    analysis-kuromoji
    analysis-phonetic
    analysis-smartcn
    analysis-stempel
    analysis-ukrainian
    discovery-azure-classic
    discovery-ec2
    discovery-file
    discovery-gce
    ingest-attachment
    ingest-geoip
    ingest-user-agent
    lang-javascript
    lang-python
    mapper-attachments
    mapper-murmur3
    mapper-size
    myplugin                 simple
    myplugin                 advanced
    repository-azure
    repository-gcs
    repository-hdfs
    repository-s3
    store-smb

As ``myplugin`` is not a core plugin, the Elasticsearch plugin manager does not know from where to install it, so we need to add the download URL to ``~/.rally/rally.ini`` as before::

    [distributions]
    plugin.myplugin.release.url=https://example.org/myplugin/releases/{{VERSION}}/myplugin-{{VERSION}}.zip

Now you can run benchmarks with the custom Elasticsearch plugin, e.g. with ``esrally race --track=geonames --distribution-version=7.12.0 --elasticsearch-plugins="myplugin:simple"``.

For this to work you need ensure two things:

1. The plugin needs to be available for the version that you want to benchmark (7.12.0 in the example above).
2. Rally will choose the most appropriate branch in the team repository before starting the benchmark. See the documentation on :ref:`how branches are mapped to Elasticsearch versions <track-repositories-branch-logic>`.
