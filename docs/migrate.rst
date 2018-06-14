Migration Guide
===============

Migrating to Rally 1.0.0
------------------------

Handling of JDK versions
^^^^^^^^^^^^^^^^^^^^^^^^

Previously the path to the JDK needed to be configured in Rally's configuration file (``~/.rally/rally.ini``) but this is too inflexible given the increased JDK release cadence. In order to keep up, we define now the allowed runtime JDKs in `rally-teams <https://github.com/elastic/rally-teams/blob/master/cars/v1/vanilla/config.ini>`_ per Elasticsearch version.

To resolve the path to the appropriate JDK you need to define the environment variable ``JAVA_HOME`` on each targeted machine.

You can also set version-specific environment variables, e.g. ``JAVA7_HOME``, ``JAVA8_HOME`` or ``JAVA10_HOME`` which will take precedence over ``JAVA_HOME``.

.. note::

    Rally will choose the highest appropriate JDK per Elasticsearch version. You can use ``--runtime-jdk`` to force a specific JDK version but the path will still be resolved according to the logic above.

Custom Parameter Sources
^^^^^^^^^^^^^^^^^^^^^^^^

In Rally 0.10.0 we have deprecated some parameter names in custom parameter sources. In Rally 1.0.0, these deprecated names have been removed. Therefore you need to replace the following parameter names if you use them in custom parameter sources:

============== ======================= =======================
Operation type Old name                New name
============== ======================= =======================
search         use_request_cache       cache
search         request_params          request-params
search         items_per_page          results-per-page
bulk           action_metadata_present action-metadata-present
force-merge    max_num_segments        max-num-segments
============== ======================= =======================

In Rally 0.9.0 the signature of custom parameter sources has also changed. In Rally 1.0.0 we have removed the backwards compatibility layer so you need to change the signatures.

Old::

    # for parameter sources implemented as functions
    def custom_param_source(indices, params):

    # for parameter sources implemented as classes
    class CustomParamSource:
        def __init__(self, indices, params):


New::

    # for parameter sources implemented as functions
    def custom_param_source(track, params, **kwargs):

    # for parameter sources implemented as classes
    class CustomParamSource:
        def __init__(self, track, params, **kwargs):

You can use the property ``track.indices`` to access indices.

Migrating to Rally 0.11.0
-------------------------

Versioned teams
^^^^^^^^^^^^^^^

.. note::

    You can skip this section if you do not create custom Rally teams.

We have introduced versioned team specifications and consequently the directory structure changes. All cars and plugins need to reside in a version-specific subdirectory now. Up to now the structure of a team repository was as follows::

    .
    ├── cars
    │   ├── 1gheap.ini
    │   ├── 2gheap.ini
    │   ├── defaults.ini
    │   ├── ea
    │   │   └── config
    │   │       └── jvm.options
    │   ├── ea.ini
    │   └── vanilla
    │       └── config
    │           ├── elasticsearch.yml
    │           ├── jvm.options
    │           └── log4j2.properties
    └── plugins
        ├── core-plugins.txt
        └── transport_nio
            ├── default
            │   └── config
            │       └── elasticsearch.yml
            └── transport.ini

Starting with Rally 0.11.0, Rally will look for a directory "v1" within ``cars`` and ``plugins``. The files that should be copied to the Elasticsearch directory, need to be contained in a ``templates`` subdirectory. Therefore, the new structure is as follows::

    .
    ├── cars
    │   └── v1
    │       ├── 1gheap.ini
    │       ├── 2gheap.ini
    │       ├── defaults.ini
    │       ├── ea
    │       │   └── templates
    │       │       └── config
    │       │           └── jvm.options
    │       ├── ea.ini
    │       └── vanilla
    │           └── templates
    │               └── config
    │                   ├── elasticsearch.yml
    │                   ├── jvm.options
    │                   └── log4j2.properties
    └── plugins
        └── v1
            ├── core-plugins.txt
            └── transport_nio
                ├── default
                │   └── templates
                │       └── config
                │           └── elasticsearch.yml
                └── transport.ini

It is also required that you create a file ``variables.ini`` for all your car config bases (optional for mixins). Therefore, the full directory structure is::

    .
    ├── cars
    │   └── v1
    │       ├── 1gheap.ini
    │       ├── 2gheap.ini
    │       ├── defaults.ini
    │       ├── ea
    │       │   └── templates
    │       │       └── config
    │       │           └── jvm.options
    │       ├── ea.ini
    │       └── vanilla
    │           ├── config.ini
    │           └── templates
    │               └── config
    │                   ├── elasticsearch.yml
    │                   ├── jvm.options
    │                   └── log4j2.properties
    └── plugins
        └── v1
            ├── core-plugins.txt
            └── transport_nio
                ├── default
                │   └── templates
                │       └── config
                │           └── elasticsearch.yml
                └── transport.ini

For distribution-based builds, ``config.ini`` file needs to contain a section ``variables`` and a ``release_url`` property::

    [variables]
    release_url=https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-oss-{{VERSION}}.tar.gz


Migrating to Rally 0.10.0
-------------------------

Removal of auto-detection and dependency on Gradle
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We have removed the auto-detection and dependency on Gradle, required until now to build from source, in favor of the `Gradle Wrapper <https://docs.gradle.org/current/userguide/gradle_wrapper.html>`_ which is present in the `Elasticsearch repository <https://github.com/elastic/elasticsearch>`_ for all branches >= 5.0.0.

Use full build command in plugin configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With Rally 0.10.0 we have removed the property :code:`build.task` for plugin definitions, in the :code:`source` section of the Rally configuration file.
Instead, a new property :code:`build.command` has been introduced where the **full build command** needs to be supplied.

The earlier syntax, to build a hypothetical plugin called :code:`my-plugin` `alongside Elasticsearch <elasticsearch_plugins.html#plugins-built-alongside-elasticsearch>`_, required::

    plugin.my-plugin.build.task = :my-plugin:plugin:assemble

This needs to be changed to the full command::

    plugin.my-plugin.build.command = ./gradlew :my-plugin:plugin:assemble

Note that if you are configuring `Plugins based on a released Elasticsearch version <elasticsearch_plugins.html#plugins-based-on-a-released-elasticsearch-version>`_ the command specified in :code:`build.command` will be executed from the plugins root directory. It's likely this directory won't have the Gradle Wrapper so you'll need to specify the full path to a Gradle command e.g.::

    plugin.my-plugin.build.command = /usr/local/bin/gradle :my-plugin:plugin:assemble

Check `Building plugins from sources <elasticsearch_plugins.html#building-plugins-from-sources>`_ for more information.

Removal of operation type ``index``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We have removed the operation type ``index`` which has been deprecated with Rally 0.8.0. Use ``bulk`` instead as operation type.

Removal of the command line parameter ``--cluster-health``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We have removed the command line parameter ``--cluster-health`` which has been deprecated with Rally 0.8.0. When using Rally's standard tracks, specify the expected cluster health as a track parameter instead, e.g.: ``--track-params="cluster_health:'yellow'"``.

Removal of index-automanagement
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We have removed the possibility that Rally automatically deletes and creates indices. Therefore, you need to add the following definitions explicitly at the beginning of a schedule if you want Rally to create declared indices::

        "schedule": [
          {
            "operation": "delete-index"
          },
          {
            "operation": {
              "operation-type": "create-index",
              "settings": {
                "index.number_of_replicas": 0
              }
            }
          },
          {
            "operation": {
              "operation-type": "cluster-health",
              "request-params": {
                "wait_for_status": "green"
              }
            }
          }

The example above also shows how to provide per-challenge index settings. If per-challenge index settings are not required, you can just specify them in the index definition file.

This behavior applies similarly to index templates as well.

Custom Parameter Sources
^^^^^^^^^^^^^^^^^^^^^^^^

We have aligned the internal names between parameter sources and runners with the ones that are specified by the user in the track file. If you have implemented custom parameter sources or runners, adjust the parameter names as follows:

============== ======================= =======================
Operation type Old name                New name
============== ======================= =======================
search         use_request_cache       cache
search         request_params          request-params
search         items_per_page          results-per-page
bulk           action_metadata_present action-metadata-present
force-merge    max_num_segments        max-num-segments
============== ======================= =======================

Migrating to Rally 0.9.0
------------------------

Track Syntax
^^^^^^^^^^^^

With Rally 0.9.0, we have changed the track file format. While the previous format is still supported with deprecation warnings, we recommend that you adapt your tracks as we will remove the deprecated syntax with the next minor release.

Below is an example of a track with the previous syntax::

    {
      "description": "Tutorial benchmark for Rally",
      "data-url": "http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/geonames",
      "indices": [
        {
          "name": "geonames",
          "types": [
            {
              "name": "type",
              "mapping": "mappings.json",
              "documents": "documents.json",
              "document-count": 8647880,
              "uncompressed-bytes": 2790927196
            }
          ]
        }
      ],
      "challenge": {
        "name": "index-only",
        "index-settings": {
          "index.number_of_replicas": 0
        },
        "schedule": [
          {
            "operation": {
              "operation-type": "bulk",
              "bulk-size": 5000
            },
            "warmup-time-period": 120,
            "clients": 8
          }
        ]
      }
    }

Before Rally 0.9.0, indices have been created implicitly. We will remove this ability and thus you need to tell Rally explicitly that you want to create indices. With Rally 0.9.0 your track should look as follows::

    {
      "description": "Tutorial benchmark for Rally",
      "indices": [
        {
          "name": "geonames",
          "body": "index.json",
          "auto-managed": false,
          "types": [ "type" ]
        }
      ],
      "corpora": [
        {
          "name": "geonames",
          "documents": [
            {
              "base-url": "http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/geonames",
              "source-file": "documents.json",
              "document-count": 8647880,
              "uncompressed-bytes": 2790927196
            }
          ]
        }
      ],
      "challenge": {
        "name": "index-only",
        "schedule": [
          {
            "operation": "delete-index"
          },
          {
            "operation": {
              "operation-type": "create-index",
              "settings": {
                "index.number_of_replicas": 0
              }
            }
          },
          {
            "operation": {
              "operation-type": "cluster-health",
              "request-params": {
                "wait_for_status": "green"
              }
            }
          },
          {
            "operation": {
              "operation-type": "bulk",
              "bulk-size": 5000
            },
            "warmup-time-period": 120,
            "clients": 8
          }
        ]
      }
    }

Let's go through the necessary changes one by one.

Define the document corpus separately
"""""""""""""""""""""""""""""""""""""

Previously you had to define the document corpus together with the document type. In order to allow you to reuse existing document corpora across tracks, you now need to specify any document corpora separately::

    "corpora": [
      {
        "name": "geonames",
        "documents": [
          {
            "base-url": "http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/geonames",
            "source-file": "documents.json",
            "document-count": 8647880,
            "uncompressed-bytes": 2790927196
          }
        ]
      }
    ]

Note that this is just a simple example that should cover the most basic case. Be sure to check the :doc:`track reference </track>` for all details.

Change the index definition
"""""""""""""""""""""""""""

The new index definition now looks as follows::

        {
          "name": "geonames",
          "body": "index.json",
          "auto-managed": false,
          "types": [ "type" ]
        }

We have added a ``body`` property to the index and removed the ``mapping`` property from the type. In fact, the only information that we need about the document type is its name, hence it is now a simple list of strings. Just put all type mappings now into the ``mappings`` property of the index definition; see also the `create index API documentation <https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html>`_.

Secondly, we have disabled index auto-management by setting ``auto-managed`` to ``false``. This allows us to define explicit tasks below to manage our index. Note that index auto-management is still working in Rally 0.9.0 but it will be removed with the next minor release Rally 0.10.0.

Explicitly delete and recreate the index
""""""""""""""""""""""""""""""""""""""""

We have also added three tasks at the beginning of the schedule::

          {
            "operation": "delete-index"
          },
          {
            "operation": {
              "operation-type": "create-index",
              "settings": {
                "index.number_of_replicas": 0
              }
            }
          },
          {
            "operation": {
              "operation-type": "cluster-health",
              "request-params": {
                "wait_for_status": "green"
              }
            }
          }

These tasks represent what Rally previously did implicitly.

The first task will delete all indices that have been declared in the ``indices`` section if they existed previously. This ensures that we don't have any leftovers from previous benchmarks.

After that we will create all indices that have been declared in the ``indices`` section. Note that we have also removed the special property ``index-settings`` and moved it to the ``settings`` parameter of ``create-index``. Rally will merge any settings from the index body definition with these settings. This means you should define settings that are always the same in the index body and settings that change from challenge to challenge in the ``settings`` property.

Finally, Rally will check that the cluster health is green. If you want to be able to override the cluster health check parameters from the command line, you can leverage Rally's track parameter feature::

          {
            "operation": {
              "operation-type": "cluster-health",
              "request-params": {
                "wait_for_status": "{{ cluster_health|default('green') }}"
              }
            }
          }

If you don't specify anything on the command line, Rally will use the default value but you can e.g. specify ``--track-params="cluster_health:'yellow'"`` so Rally will check for (at least) a yellow cluster health status.

Note that you can :doc:`customize these operations </track>`.

Custom Parameter Sources
^^^^^^^^^^^^^^^^^^^^^^^^

With Rally 0.9.0, the API for custom parameter sources has changed. Previously, the following syntax was valid::

    # for parameter sources implemented as functions
    def custom_param_source(indices, params):

    # for parameter sources implemented as classes
    class CustomParamSource:
        def __init__(self, indices, params):


With Rally 0.9.0, the signatures need to be changed to::

    # for parameter sources implemented as functions
    def custom_param_source(track, params, **kwargs):

    # for parameter sources implemented as classes
    class CustomParamSource:
        def __init__(self, track, params, **kwargs):

Rally will issue a warning along the lines of ``Parameter source 'custom_param_source' is using deprecated method signature`` if your track is affected. If you need access to the ``indices`` list, you can call ``track.indices`` to retrieve it from the track.
