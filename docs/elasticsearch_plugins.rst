Using Elasticsearch Plugins
===========================

You can have Rally setup an Elasticsearch cluster with plugins for you. However, there are a couple of restrictions:

* This feature is only supported from Elasticsearch 5.0.0 onwards
* You cannot benchmark source-builds of plugins
* Whereas Rally caches downloaded Elasticsearch distributions, plugins will always be installed via the Internet and thus each machine where an Elasticsearch node will be installed requires an active Internet connection.

Listing plugins
---------------

To see which plugins are available, run ``esrally list elasticsearch-plugins``::

    Available Elasticsearch plugins:

    Name                Configuration
    ------------------  ----------------
    analysis-icu
    analysis-kuromoji
    analysis-phonetic
    analysis-smartcn
    analysis-stempel
    analysis-ukrainian
    ingest-attachment
    ingest-geoip
    ingest-user-agent
    lang-javascript
    lang-python
    mapper-attachments
    mapper-murmur3
    mapper-size
    store-smb
    x-pack              monitoring-local
    x-pack              security

Rally supports plugins only for Elasticsearch 5.0 or better. As the availability of plugins may change from release to release we recommend that you include the ``--distribution-version`` parameter when listing plugins. By default Rally assumes that you want to benchmark the latest master version of Elasticsearch.

Let's see what happens if we run ``esrally list elasticsearch-plugins --distribution-version=2.4.0``::

    No Elasticsearch plugins are available.

As mentioned before, this is expected as only Elasticsearch 5.0 or better is supported.

Running a benchmark with plugins
--------------------------------
In order to tell Rally to install a plugin, use the ``--elasticsearch-plugins`` parameter when starting a race. You can provide multiple plugins (comma-separated) and they will be installed in the order to that you define on the command line.

Example::

    esrally --distribution-version=5.5.0 --elasticsearch-plugins="analysis-icu,analysis-phonetic"

This will install the plugins ``analysis-icu`` and ``analysis-phonetic`` (in that order). In order to use the features that these plugins provide, you need to write a :doc:`custom track </adding_tracks>`.

Rally will use several techniques to install and configure plugins:

* First, Rally checks whether directory ``plugins/PLUGIN_NAME`` in the currently configured team repository exists. If this is the case, then plugin installation and configuration details will be read from this directory.
* Next, Rally will use the provided plugin name when running the Elasticsearch plugin installer. With this approach we can avoid to create a plugin configuration directory in the team repository for very simple plugins that do not need any configuration.

As mentioned above, Rally also allows you to specify a plugin configuration and you can even combine them. Here are some examples:

* Run a benchmark with the ``x-pack`` plugin in the ``security`` configuration: ``--elasticsearch-plugins=xpack:security``
* Run a benchmark with the ``x-pack`` plugin in the ``security`` and the ``graph`` configuration: ``--elasticsearch-plugins=xpack:security+graph``

.. note::
    To benchmark the ``security`` configuration of ``x-pack`` you need to add the following command line options: ``--client-options="use_ssl:true,verify_certs:false,basic_auth_user:'rally',basic_auth_password:'rally-password'" --cluster-health=yellow``

If you are behind a proxy, please set the environment variable ``ES_JAVA_OPTS`` accordingly on each target machine as described in the `Elasticsearch plugin documentation <https://www.elastic.co/guide/en/elasticsearch/plugins/current/_other_command_line_parameters.html#_proxy_settings>`_.

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

In ``$TEAM_REPO_ROOT`` create the directory structure for the plugin and its config base with `mkdir -p myplugin/default/config` and add the following ``elasticsearch.yml`` in the new directory::

    myplugin.active: true
    myplugin.mode={{my_plugin_mode}}

That's it. Later, Rally will just copy all files in ``myplugin/default`` to the home directory of the Elasticsearch node that it configures. First, Rally will always apply the car's configuration and then plugins can add their configuration on top. This also explains why we have created a ``config/elasticsearch.yml``. Rally will just copy this file and replace template variables on the way.

.. note::
    If you create a new customization for a plugin, ensure that the plugin name in the team repository matches the official plugin name. Note that hyphens need to be replaced by underscores (e.g. "x-pack" becomes "x_pack"). The reason is that Rally allows to write custom install hooks and the plugin name will become the root package name of the install hook. However, hyphens are not supported in Python which is why we use underscores instead.


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

    Name                Configuration
    ------------------  ----------------
    analysis-icu
    analysis-kuromoji
    analysis-phonetic
    analysis-smartcn
    analysis-stempel
    analysis-ukrainian
    ingest-attachment
    ingest-geoip
    ingest-user-agent
    lang-javascript
    lang-python
    mapper-attachments
    mapper-murmur3
    mapper-size
    store-smb
    x-pack              monitoring-local
    x-pack              security
    myplugin            simple
    myplugin            advanced

As ``myplugin`` is not an official plugin, the Elasticsearch plugin manager does not know from where to install it, so we need to add the download URL to ``~/.rally/rally.ini`` as before::

    [distributions]
    plugin.myplugin.release.url=https://example.org/myplugin/releases/{{VERSION}}/myplugin-{{VERSION}}.zip

Now you can run benchmarks with the custom Elasticsearch plugin, e.g. with ``esrally --distribution-version=5.5.0 --elasticsearch-plugins="myplugin:simple"``.

For this to work you need ensure two things:

1. The plugin needs to be available for the version that you want to benchmark (5.5.0 in the example above).
2. Rally will choose the most appropriate branch in the team repository before starting the benchmark. In practice, this will most likely be branch "5" for this example. Therefore you need to ensure that your plugin configuration is also available on that branch. See the `README in the team repository <https://github.com/elastic/rally-teams#versioning-scheme>`_ to learn how the versioning scheme works.

