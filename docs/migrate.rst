Migration Guide
===============

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

We have added a ``body`` property to the index and removed the ``mapping`` property from the type. In fact, the only information that we need about the document type is its name, hence it is now a simple list of strings. Just put all type mappings now into the ``mappings`` property of the index definition. For more details, please refer to the `create index API documentation <https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html>`_.

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

Note that you can customize these operations. Please see the :doc:`track reference </track>` for all details.

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