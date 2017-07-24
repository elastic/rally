Benchmarking Elasticsearch Plugins
==================================

You can have Rally setup an Elasticsearch cluster with plugins for you. However, there are a couple of restrictions:

* This feature is only supported from Elasticsearch 5.0.0 onwards
* You cannot benchmark source-builds of plugins
* Whereas Rally caches downloaded Elasticsearch distributions, plugins will always be installed via the Internet and thus each machine where an Elasticsearch node will be installed requires an active Internet connection.

In order to tell Rally to install a plugin, use the ``--elasticsearch-plugins`` parameter. You can provide multiple plugins and they will be installed in the order to that you define on the command line.

Example::

    esrally --distribution-version=5.5.0 --elasticsearch-plugins="analysis-icu,analysis-phonetic"

This will install the plugins ``analysis-icu`` and ``analysis-phonetic`` (in that order). In order to use the features that these plugins provide, you need to write a :doc:`custom track </adding_tracks>`.

Rally will use several techniques to install and configure plugins:

* First, Rally checks whether directory ``plugins/PLUGIN_NAME`` in the currently configured team repository exists. If this is the case, then plugin installation and configuration details will be read from this directory.
* Next, Rally will use the provided plugin name when running the Elasticsearch plugin installer. With this approach we can avoid to create a plugin configuration directory in the team repository for very simple plugins that do not need any configuration.

.. note::
    If you create a new customization for a plugin, ensure that the plugin name in the team repository matches the official plugin name.


If you are behind a proxy, please set the environment variable ``ES_JAVA_OPTS`` accordingly on each target machine as described in the `Elasticsearch plugin documentation <https://www.elastic.co/guide/en/elasticsearch/plugins/current/_other_command_line_parameters.html#_proxy_settings>`_.
