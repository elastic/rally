Define Custom Workloads: Tracks
===============================

Definition
----------

A track describes one or more benchmarking scenarios. Its structure is described in detail in the :doc:`track reference </track>`.

.. _add_track_create_track:

Creating a track from data in an existing cluster
-------------------------------------------------

If you already have a cluster with data in it you can use the ``create-track`` subcommand of Rally to create a basic Rally track. To create a Rally track with data from the indices ``products`` and ``companies`` that are hosted by a locally running Elasticsearch cluster, issue the following command::

    esrally create-track --track=acme --target-hosts=127.0.0.1:9200 --indices="products,companies" --output-path=~/tracks

If you want to connect to a cluster with TLS and basic authentication enabled, for example via Elastic Cloud, also specify :ref:`--client-options <clr_client_options>` and change ``basic_auth_user`` and ``basic_auth_password`` accordingly::

    esrally create-track --track=acme --target-hosts=abcdef123.us-central-1.gcp.cloud.es.io:9243 --client-options="timeout:60,use_ssl:true,verify_certs:true,basic_auth_user:'elastic',basic_auth_password:'secret-password'" --indices="products,companies" --output-path=~/tracks

The track generator will create a folder with the track's name in the specified output directory::

    > find tracks/acme
    tracks/acme
    tracks/acme/companies-documents.json
    tracks/acme/companies-documents.json.bz2
    tracks/acme/companies-documents-1k.json
    tracks/acme/companies-documents-1k.json.bz2
    tracks/acme/companies.json
    tracks/acme/products-documents.json
    tracks/acme/products-documents.json.bz2
    tracks/acme/products-documents-1k.json
    tracks/acme/products-documents-1k.json.bz2
    tracks/acme/products.json
    tracks/acme/track.json

The files are organized as follows:

* ``track.json`` contains the actual Rally track. For details see the :doc:`track reference<track>`.
* ``companies.json`` and ``products.json`` contain the mapping and settings for the extracted indices.
* ``*-documents.json(.bz2)`` contains the sources of all the documents from the extracted indices. The files suffixed with ``-1k`` contain a smaller version of the document corpus to support :ref:`test mode <add_track_test_mode>`.

Creating a track from scratch
-----------------------------

We will create the track "tutorial" step by step. We store everything in the directory ``~/rally-tracks/tutorial`` but you can choose any other location.

First, get some data. `Geonames <http://www.geonames.org/>`_ provides geo data under a `creative commons license <http://creativecommons.org/licenses/by/3.0/>`_. Download `allCountries.zip <http://download.geonames.org/export/dump/allCountries.zip>`_ (around 300MB), extract it and inspect ``allCountries.txt``.

The file is tab-delimited but to bulk-index data with Elasticsearch we need JSON. Convert the data with the following script::

    import json

    cols = (("geonameid", "int", True),
            ("name", "string", True),
            ("asciiname", "string", False),
            ("alternatenames", "string", False),
            ("latitude", "double", True),
            ("longitude", "double", True),
            ("feature_class", "string", False),
            ("feature_code", "string", False),
            ("country_code", "string", True),
            ("cc2", "string", False),
            ("admin1_code", "string", False),
            ("admin2_code", "string", False),
            ("admin3_code", "string", False),
            ("admin4_code", "string", False),
            ("population", "long", True),
            ("elevation", "int", False),
            ("dem", "string", False),
            ("timezone", "string", False))


    def main():
        with open("allCountries.txt", "rt", encoding="UTF-8") as f:
            for line in f:
                tup = line.strip().split("\t")
                record = {}
                for i in range(len(cols)):
                    name, type, include = cols[i]
                    if tup[i] != "" and include:
                        if type in ("int", "long"):
                            record[name] = int(tup[i])
                        elif type == "double":
                            record[name] = float(tup[i])
                        elif type == "string":
                            record[name] = tup[i]
                print(json.dumps(record, ensure_ascii=False))


    if __name__ == "__main__":
        main()

Store the script as ``toJSON.py`` in the tutorial directory (``~/rally-tracks/tutorial``). Invoke it with ``python3 toJSON.py > documents.json``.

Then store the following mapping file as ``index.json`` in the tutorial directory::

    {
      "settings": {
        "index.number_of_replicas": 0
      },
      "mappings": {
        "docs": {
          "dynamic": "strict",
          "properties": {
            "geonameid": {
              "type": "long"
            },
            "name": {
              "type": "text"
            },
            "latitude": {
              "type": "double"
            },
            "longitude": {
              "type": "double"
            },
            "country_code": {
              "type": "text"
            },
            "population": {
              "type": "long"
            }
          }
        }
      }
    }

.. note::
   This tutorial assumes that you want to benchmark a version of Elasticsearch prior to 7.0.0. If you want to benchmark Elasticsearch 7.0.0 or later you need to remove the mapping type above.


For details on the allowed syntax, see the Elasticsearch documentation on `mappings <https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html>`_ and the `create index API <https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html>`__.

Finally, store the track as ``track.json`` in the tutorial directory::

    {
      "version": 2,
      "description": "Tutorial benchmark for Rally",
      "indices": [
        {
          "name": "geonames",
          "body": "index.json",
          "types": [ "docs" ]
        }
      ],
      "corpora": [
        {
          "name": "rally-tutorial",
          "documents": [
            {
              "source-file": "documents.json",
              "document-count": 11658903,
              "uncompressed-bytes": 1544799789
            }
          ]
        }
      ],
      "schedule": [
        {
          "operation": {
            "operation-type": "delete-index"
          }
        },
        {
          "operation": {
            "operation-type": "create-index"
          }
        },
        {
          "operation": {
            "operation-type": "cluster-health",
            "request-params": {
              "wait_for_status": "green"
            },
            "retry-until-success": true
          }
        },
        {
          "operation": {
            "operation-type": "bulk",
            "bulk-size": 5000
          },
          "warmup-time-period": 120,
          "clients": 8
        },
        {
          "operation": {
            "operation-type": "force-merge"
          }
        },
        {
          "operation": {
            "name": "query-match-all",
            "operation-type": "search",
            "body": {
              "query": {
                "match_all": {}
              }
            }
          },
          "clients": 8,
          "warmup-iterations": 1000,
          "iterations": 1000,
          "target-throughput": 100
        }
      ]
    }


The numbers under the ``documents`` property are needed to verify integrity and provide progress reports. Determine the correct document count with ``wc -l documents.json`` and the size in bytes with ``stat -f "%z" documents.json``.

.. note::
   This tutorial assumes that you want to benchmark a version of Elasticsearch prior to 7.0.0. If you want to benchmark Elasticsearch 7.0.0 or later you need to remove the ``types`` property above.

.. note::

    You can store any supporting scripts along with your track. However, you need to place them in a directory starting with "_", e.g. "_support". Rally loads track plugins (see below) from any directory but will ignore directories starting with "_".

.. note::

    We have defined a `JSON schema for tracks <https://github.com/elastic/rally/blob/master/esrally/resources/track-schema.json>`_ which you can use to check how to define your track. You should also check the tracks provided by Rally for inspiration.

The new track appears when you run ``esrally list tracks --track-path=~/rally-tracks/tutorial``::

    dm@io:~ $ esrally list tracks --track-path=~/rally-tracks/tutorial

        ____        ____
       / __ \____ _/ / /_  __
      / /_/ / __ `/ / / / / /
     / _, _/ /_/ / / / /_/ /
    /_/ |_|\__,_/_/_/\__, /
                    /____/
    Available tracks:

    Name        Description                   Documents    Compressed Size  Uncompressed Size
    ----------  ----------------------------- -----------  ---------------  -----------------
    tutorial    Tutorial benchmark for Rally      11658903  N/A              1.4 GB

You can also show details about your track with ``esrally info --track-path=~/rally-tracks/tutorial``::

    dm@io:~ $ esrally info --track-path=~/rally-tracks/tutorial

        ____        ____
       / __ \____ _/ / /_  __
      / /_/ / __ `/ / / / / /
     / _, _/ /_/ / / / /_/ /
    /_/ |_|\__,_/_/_/\__, /
                    /____/

    Showing details for track [tutorial]:

    * Description: Tutorial benchmark for Rally
    * Documents: 11,658,903
    * Compressed Size: N/A
    * Uncompressed Size: 1.4 GB


    Schedule:
    ----------

    1. delete-index
    2. create-index
    3. cluster-health
    4. bulk (8 clients)
    5. force-merge
    6. query-match-all (8 clients)

Congratulations, you have created your first track! You can test it with ``esrally race --distribution-version=7.0.0 --track-path=~/rally-tracks/tutorial``.

.. _add_track_test_mode:

Adding support for test mode
----------------------------

You can check your track very quickly for syntax errors when you invoke Rally with ``--test-mode``. Rally postprocesses its internal track representation as follows:

* Iteration-based tasks run at most one warmup iteration and one measurement iteration.
* Time-period-based tasks run at most for 10 seconds without warmup.

Rally also postprocesses all data file names. Instead of ``documents.json``, Rally expects ``documents-1k.json`` and assumes the file contains 1.000 documents. You need to prepare these data files though. Pick 1.000 documents for every data file in your track and store them in a file with the suffix ``-1k``. We choose the first 1.000 with ``head -n 1000 documents.json > documents-1k.json``.

Challenges
----------

To specify different workloads in the same track you can use so-called challenges. Instead of specifying the ``schedule`` property on top-level you specify a ``challenges`` array::

    {
      "version": 2,
      "description": "Tutorial benchmark for Rally",
      "indices": [
        {
          "name": "geonames",
          "body": "index.json",
          "types": [ "docs" ]
        }
      ],
      "corpora": [
        {
          "name": "rally-tutorial",
          "documents": [
            {
              "source-file": "documents.json",
              "document-count": 11658903,
              "uncompressed-bytes": 1544799789
            }
          ]
        }
      ],
      "challenges": [
        {
          "name": "index-and-query",
          "default": true,
          "schedule": [
            {
              "operation": {
                "operation-type": "delete-index"
              }
            },
            {
              "operation": {
                "operation-type": "create-index"
              }
            },
            {
              "operation": {
                "operation-type": "cluster-health",
                "request-params": {
                  "wait_for_status": "green"
                },
                "retry-until-success": true
              }
            },
            {
              "operation": {
                "operation-type": "bulk",
                "bulk-size": 5000
              },
              "warmup-time-period": 120,
              "clients": 8
            },
            {
              "operation": {
                "operation-type": "force-merge"
              }
            },
            {
              "operation": {
                "name": "query-match-all",
                "operation-type": "search",
                "body": {
                  "query": {
                    "match_all": {}
                  }
                }
              },
              "clients": 8,
              "warmup-iterations": 1000,
              "iterations": 1000,
              "target-throughput": 100
            }
          ]
        }
      ]
    }

.. note::

    If you define multiple challenges, Rally runs the challenge where ``default`` is set to ``true``. If you want to run a different challenge, provide the command line option ``--challenge=YOUR_CHALLENGE_NAME``.

When should you use challenges? Challenges are useful when you want to run completely different workloads based on the same track but for the majority of cases you should get away without using challenges:

* To run only a subset of the tasks, you can use :ref:`task filtering <clr_include_tasks>`, e.g. ``--include-tasks="create-index,bulk"`` will only run these two tasks in the track above or ``--exclude-tasks="bulk"`` will run all tasks except for ``bulk``.
* To vary parameters, e.g. the number of clients, you can use :ref:`track parameters <clr_track_params>`

Structuring your track
----------------------

``track.json`` is the entry point to a track but you can split your track as you see fit. Suppose you want to add more challenges to the track but keep them in separate files. Create a ``challenges`` directory and store the following in ``challenges/index-and-query.json``::

    {
      "name": "index-and-query",
      "default": true,
      "schedule": [
        {
          "operation": {
            "operation-type": "delete-index"
          }
        },
        {
          "operation": {
            "operation-type": "create-index"
          }
        },
        {
          "operation": {
            "operation-type": "cluster-health",
            "request-params": {
              "wait_for_status": "green"
            },
            "retry-until-success": true
          }
        },
        {
          "operation": {
            "operation-type": "bulk",
            "bulk-size": 5000
          },
          "warmup-time-period": 120,
          "clients": 8
        },
        {
          "operation": {
            "operation-type": "force-merge"
          }
        },
        {
          "operation": {
            "name": "query-match-all",
            "operation-type": "search",
            "body": {
              "query": {
                "match_all": {}
              }
            }
          },
          "clients": 8,
          "warmup-iterations": 1000,
          "iterations": 1000,
          "target-throughput": 100
        }
      ]
    }

Include the new file in ``track.json``::

    {
      "version": 2,
      "description": "Tutorial benchmark for Rally",
      "indices": [
        {
          "name": "geonames",
          "body": "index.json",
          "types": [ "docs" ]
        }
      ],
      "corpora": [
        {
          "name": "rally-tutorial",
          "documents": [
            {
              "source-file": "documents.json",
              "document-count": 11658903,
              "uncompressed-bytes": 1544799789
            }
          ]
        }
      ],
      "challenges": [
        {% include "challenges/index-and-query.json" %}
      ]
    }

We replaced the challenge content with  ``{% include "challenges/index-and-query.json" %}`` which tells Rally to include the challenge from the provided file. You can use ``include`` on arbitrary parts of your track.

To reuse operation definitions across challenges, you can define them in a separate ``operations`` block and refer to them by name in the corresponding challenge::

    {
      "version": 2,
      "description": "Tutorial benchmark for Rally",
      "indices": [
        {
          "name": "geonames",
          "body": "index.json",
          "types": [ "docs" ]
        }
      ],
      "corpora": [
        {
          "name": "rally-tutorial",
          "documents": [
            {
              "source-file": "documents.json",
              "document-count": 11658903,
              "uncompressed-bytes": 1544799789
            }
          ]
        }
      ],
      "operations": [
        {
          "name": "delete",
          "operation-type": "delete-index"
        },
        {
          "name": "create",
          "operation-type": "create-index"
        },
        {
          "name": "wait-for-green",
          "operation-type": "cluster-health",
          "request-params": {
            "wait_for_status": "green"
          },
          "retry-until-success": true
        },
        {
          "name": "bulk-index",
          "operation-type": "bulk",
          "bulk-size": 5000
        },
        {
          "name": "force-merge",
          "operation-type": "force-merge"
        },
        {
          "name": "query-match-all",
          "operation-type": "search",
          "body": {
            "query": {
              "match_all": {}
            }
          }
        }
      ],
      "challenges": [
        {% include "challenges/index-and-query.json" %}
      ]
    }

``challenges/index-and-query.json`` then becomes::

    {
      "name": "index-and-query",
      "default": true,
      "schedule": [
        {
          "operation": "delete"
        },
        {
          "operation": "create"
        },
        {
          "operation": "wait-for-green"
        },
        {
          "operation": "bulk-index",
          "warmup-time-period": 120,
          "clients": 8
        },
        {
          "operation": "force-merge"
        },
        {
          "operation": "query-match-all",
          "clients": 8,
          "warmup-iterations": 1000,
          "iterations": 1000,
          "target-throughput": 100
        }
      ]
    }

Note how we reference to the operations by their name (e.g. ``create``, ``bulk-index``, ``force-merge`` or ``query-match-all``).

.. _track_collect_helper:

You can also use Rally's collect helper to simplify including multiple challenges::

    {% import "rally.helpers" as rally %}
    {
      "version": 2,
      "description": "Tutorial benchmark for Rally",
      "indices": [
        {
          "name": "geonames",
          "body": "index.json",
          "types": [ "docs" ]
        }
      ],
      "corpora": [
        {
          "name": "rally-tutorial",
          "documents": [
            {
              "source-file": "documents.json",
              "document-count": 11658903,
              "uncompressed-bytes": 1544799789
            }
          ]
        }
      ],
      "operations": [
        {
          "name": "delete",
          "operation-type": "delete-index"
        },
        {
          "name": "create",
          "operation-type": "create-index"
        },
        {
          "name": "wait-for-green",
          "operation-type": "cluster-health",
          "request-params": {
            "wait_for_status": "green"
          },
          "retry-until-success": true
        },
        {
          "name": "bulk-index",
          "operation-type": "bulk",
          "bulk-size": 5000
        },
        {
          "name": "force-merge",
          "operation-type": "force-merge"
        },
        {
          "name": "query-match-all",
          "operation-type": "search",
          "body": {
            "query": {
              "match_all": {}
            }
          }
        }
      ],
      "challenges": [
        {{ rally.collect(parts="challenges/*.json") }}
      ]
    }

The changes are:

1. We import helper functions from Rally by adding ``{% import "rally.helpers" as rally %}`` in line 1.
2. We use Rally's ``collect`` helper to find and include all JSON files in the ``challenges`` subdirectory with the statement ``{{ rally.collect(parts="challenges/*.json") }}``.

.. note::

    Rally's log file contains the fully rendered track after it has loaded it successfully.

You can even use `Jinja2 variables <http://jinja.pocoo.org/docs/dev/templates/#assignments>`_ but then you need to import the Rally helpers a bit differently. You also need to declare all variables before the ``import`` statement::

        {% set clients = 16 %}
        {% import "rally.helpers" as rally with context %}

If you use this idiom you can refer to the ``clients`` variable inside your snippets with ``{{ clients }}``.

Sharing your track with others
------------------------------

So far the track is only available on your local machine. To share your track you could check it into version control. To avoid committing the potentially huge data file you can expose it via http (e.g. via an S3 bucket) and reference it in your track with the property ``base-url``. Rally expects that the URL points to the parent path and appends the document file name automatically.

You should also compress your document corpus to save network bandwidth; `pbzip2 <https://linux.die.net/man/1/pbzip2>`_ works well, is backwards compatible with ``bzip2`` and makes use of all available cpu cores for compression and decompression. You can create a compressed archive with the following command::

    pbzip2 -9 -k -m2000 -v documents.json

If you want to support Rally's test mode, also compress your test mode corpus with::

    pbzip2 -9 -k -m2000 -v documents-1k.json

Then upload the generated archives ``documents.json.bz2`` and ``documents-1k.json.bz2`` to the remote location.

Finally, specify the compressed file name in the ``source-file`` property and also add the ``base-url`` property::

    {
      "version": 2,
      "description": "Tutorial benchmark for Rally",
      "corpora": [
        {
          "name": "rally-tutorial",
          "documents": [
            {
              "base-url": "http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/geonames",
              "source-file": "documents.json.bz2",
              "document-count": 11658903,
              "compressed-bytes": 197857614,
              "uncompressed-bytes": 1544799789
            }
          ]
        }
      ],
      ...
    }

Specifying ``compressed-bytes`` (file size of ``documents.json.bz2``) and ``uncompressed-bytes`` (file size of ``documents.json``) is optional but helps Rally to provide progress indicators and also verify integrity.

You've now mastered the basics of track development for Rally. It's time to pat yourself on the back before you dive into the advanced topics!

Advanced topics
---------------

.. _template_language:

Template Language
^^^^^^^^^^^^^^^^^

Rally uses `Jinja2 <http://jinja.pocoo.org/docs/dev/>`_ as a template language so you can use Jinja2 expressions in track files.

Elasticsearch utilizes Mustache formatting in a few places, notably in `search templates <https://www.elastic.co/guide/en/elasticsearch/reference/7.4/search-template.html>`_ and `Watcher templates <https://www.elastic.co/guide/en/elasticsearch/reference/7.4/actions-email.html>`_. If you are using Mustache in your Rally tracks you must `escape them properly <https://jinja.palletsprojects.com/en/2.10.x/templates/#escaping>`_. See :ref:`put_pipeline` for an example.

Extensions
""""""""""

Rally also provides a few extensions to Jinja2:

* ``now``: a global variable that represents the current date and time when the template is evaluated by Rally.
* ``days_ago()``: a `filter <http://jinja.pocoo.org/docs/dev/templates/#filters>`_ that you can use for date calculations.

You can find an example in the ``http_logs`` track::

    {
      "name": "range",
        "index": "logs-*",
        "type": "type",
        "body": {
          "query": {
            "range": {
              "@timestamp": {
                "gte": "now-{{'15-05-1998' | days_ago(now)}}d/d",
                "lt": "now/d"
              }
            }
          }
        }
      }
    }

The data set that is used in the ``http_logs`` track starts on 26-04-1998 but we want to ignore the first few days for this query, so we start on 15-05-1998. The expression ``{{'15-05-1998' | days_ago(now)}}`` yields the difference in days between now and the fixed start date and allows us to benchmark time range queries relative to now with a predetermined data set.

* ``rally.collect(parts)``: a `macro <https://jinja.pocoo.org/docs/dev/templates/#macros>`_ that you can use to join track fragments. See the :ref:`example above<track_collect_helper>`.
* ``rally.exists_set_param(setting_name, value, default_value=None, comma=True)``: a `macro <https://jinja.pocoo.org/docs/dev/templates/#macros>`_ that you can use to set the value of a track parameter without having to check if it exists.

.. important::
    To use macros you must declare ``{% import "rally.helpers" as rally with context %}`` at the top of your track; see :ref:`the docs <track_collect_helper>` for more details and the `geonames track <https://github.com/elastic/rally-tracks/blob/b2f86df5f0c18461fdb64dd9ee1fe16bd3653b9d/geonames/track.json#L1>`_ for an example.

Example:

Suppose you need an operation that specifies the Elasticsearch transient setting ``indices.recovery.max_bytes_per_sec`` if and only if it has been provided as a track parameter.

Your operation could look like::

    {
      "operation": {
        "operation-type": "raw-request",
        "method": "PUT",
        "path": "/_cluster/settings",
        "body": {
          "transient": {
            "cluster.routing.allocation.node_initial_primaries_recoveries": 8
            {{ rally.exists_set_param("indices.recovery.max_bytes_per_sec", es_snapshot_restore_recovery_max_bytes_per_sec) }}
          }
        }
      }
    }

Note the lack of a comma after the first setting ``cluster.routing.allocation.node_initial_primaries_recoveries``. This is intentional since the helper will insert it if the parameter exists (this behavior can be changed using ``comma=False``).

Assuming we pass ``--track-params="es_snapshot_restore_recovery_max_bytes_per_sec:-1"`` the helper will end up rendering the operation as::

    {
      "operation": {
        "operation-type": "raw-request",
        "method": "PUT",
        "path": "/_cluster/settings",
        "body": {
          "transient": {
            "cluster.routing.allocation.node_initial_primaries_recoveries": 8,"indices.recovery.max_bytes_per_sec": -1
          }
        }
      }
    }


The parameter ``default_value`` controls the value to use for the setting if it is undefined. If the setting is undefined and there is no default value, nothing will be added.

.. _adding_tracks_custom_param_sources:

Custom parameter sources
^^^^^^^^^^^^^^^^^^^^^^^^

.. warning::

    Your parameter source is on a performance-critical code-path. Double-check with :ref:`Rally's profiling support <clr_enable_driver_profiling>` that you did not introduce any bottlenecks.


Consider the following operation definition::

    {
      "name": "term",
      "operation-type": "search",
      "body": {
        "query": {
          "term": {
            "body": "physician"
          }
        }
      }
    }

This query is defined statically but if you want to vary parameters, for example to search also for "mechanic" or "nurse, you can write your own "parameter source" in Python.

First, define the name of your parameter source in the operation definition::

    {
      "name": "term",
      "operation-type": "search",
      "param-source": "my-custom-term-param-source"
      "professions": ["mechanic", "physician", "nurse"]
    }

Rally recognizes the parameter source and looks for a file ``track.py`` next to ``track.json``. This file contains the implementation of the parameter source::

    import random


    def random_profession(track, params, **kwargs):
        # choose a suitable index: if there is only one defined for this track
        # choose that one, but let the user always override index and type.
        if len(track.indices) == 1:
            default_index = track.indices[0].name
            if len(track.indices[0].types) == 1:
                default_type = track.indices[0].types[0].name
            else:
                default_type = None
        else:
            default_index = "_all"
            default_type = None

        index_name = params.get("index", default_index)
        type_name = params.get("type", default_type)

        # you must provide all parameters that the runner expects
        return {
            "body": {
                "query": {
                    "term": {
                        "body": "%s" % random.choice(params["professions"])
                    }
                }
            },
            "index": index_name,
            "type": type_name,
            "cache": params.get("cache", False)
        }

    def register(registry):
        registry.register_param_source("my-custom-term-param-source", random_profession)

The example above shows a simple case that is sufficient if the operation to which your parameter source is applied is idempotent and it does not matter whether two clients execute the same operation.

The function ``random_profession`` is the actual parameter source. Rally will bind the name "my-custom-term-param-source" to this function by calling ``register``. ``register`` is called by Rally before the track is executed.

The parameter source function needs to declare the parameters ``track``, ``params`` and ``**kwargs``. ``track`` contains a structured representation of the current track and ``params`` contains all parameters that have been defined in the operation definition in ``track.json``. We use it in the example to read the professions to choose. The third parameter is there to ensure a more stable API as Rally evolves.

We also derive an appropriate index and document type from the track's index definitions but allow the user to override this choice with the ``index`` or ``type`` parameters::

    {
      "name": "term",
      "operation-type": "search",
      "param-source": "my-custom-term-param-source"
      "professions": ["mechanic", "physician", "nurse"],
      "index": "employee*",
      "type": "docs"
    }


If you need more control, you need to implement a class. Below is the implementation of the same parameter source as a class::

    import random


    class TermParamSource:
        def __init__(self, track, params, **kwargs):
            # choose a suitable index: if there is only one defined for this track
            # choose that one, but let the user always override index and type.
            if len(track.indices) == 1:
                default_index = track.indices[0].name
                if len(track.indices[0].types) == 1:
                    default_type = track.indices[0].types[0].name
                else:
                    default_type = None
            else:
                default_index = "_all"
                default_type = None

            # we can eagerly resolve these parameters already in the constructor...
            self._index_name = params.get("index", default_index)
            self._type_name = params.get("type", default_type)
            self._cache = params.get("cache", False)
            # ... but we need to resolve "profession" lazily on each invocation later
            self._params = params
            # Determines whether this parameter source will be "exhausted" at some point or
            # Rally can draw values infinitely from it.
            self.infinite = True

        def partition(self, partition_index, total_partitions):
            return self

        def params(self):
            # you must provide all parameters that the runner expects
            return {
                "body": {
                    "query": {
                        "term": {
                            "body": "%s" % random.choice(self._params["professions"])
                        }
                    }
                },
                "index": self._index_name,
                "type": self._type_name,
                "cache": self._cache
            }


    def register(registry):
        registry.register_param_source("my-custom-term-param-source", TermParamSource)


In ``register`` you bind the name in the track specification to your parameter source implementation class similar to the previous example. ``TermParamSource`` is the actual parameter source and needs to fulfill a few requirements:

* The constructor needs to have the signature ``__init__(self, track, params, **kwargs)``.
* ``partition(self, partition_index, total_partitions)`` is called by Rally to "assign" the parameter source across multiple clients. Typically you can just return ``self``. If each client needs to act differently then you can provide different parameter source instances here as well.
* ``params(self)``: This method returns a dictionary with all parameters that the corresponding "runner" expects. This method will be invoked once for every iteration during the race. In the example, we parameterize the query by randomly selecting a profession from a list.
* ``infinite``: This property helps Rally to determine whether to let the parameter source determine when a task should be finished (when ``infinite`` is ``False``) or whether the task properties (e.g. ``iterations`` or ``time-period``) determine when a task should be finished. In the former case, the parameter source needs to raise ``StopIteration`` to indicate when it is finished.

For cases, where you want to provide a progress indication (this is typically the case when ``infinite`` is ``False``), you can implement a property ``percent_completed`` which returns a floating point value between ``0.0`` and ``1.0``. Rally will query this value before each call to ``params()`` and uses it to indicate progress. However:

* Rally will not check ``percent_completed`` if it can derive progress in any other way.
* The value of ``percent_completed`` is purely informational and does not influence when Rally considers an operation to be completed.

.. note::

    The method ``params(self)`` as well as the property ``percent_completed`` are called on a performance-critical path. Don't do anything that takes a lot of time (avoid any I/O). For searches, you should usually throttle throughput anyway and there it does not matter that much but if the corresponding operation is run without throughput throttling, double-check that your custom parameter source does not introduce a bottleneck.

Custom parameter sources can use the Python standard API but using any additional libraries is not supported.

You can also implement your parameter sources and runners in multiple Python files but the main entry point is always ``track.py``. The root package name of your plugin is the name of your track.

.. _adding_tracks_custom_runners:

Custom runners
^^^^^^^^^^^^^^

.. warning::

    Your runner is on a performance-critical code-path. Double-check with :ref:`Rally's profiling support <clr_enable_driver_profiling>` that you did not introduce any bottlenecks.

Runners execute an operation against Elasticsearch. Rally supports many operations out of the box already, see the :doc:`track reference </track>` for a complete list. If you want to call any other Elasticsearch API, define a custom runner.

Consider we want to use the percolate API with an older version of Elasticsearch which is not supported by Rally. To achieve this, we implement a custom runner in the following steps.

In ``track.json`` set the ``operation-type`` to "percolate" (you can choose this name freely)::


    {
      "name": "percolator_with_content_google",
      "operation-type": "percolate",
      "body": {
        "doc": {
          "body": "google"
        },
        "track_scores": true
      }
    }


Then create a file ``track.py`` next to ``track.json`` and implement the following two functions::

    async def percolate(es, params):
        await es.percolate(
                index="queries",
                doc_type="content",
                body=params["body"]
              )

    def register(registry):
        registry.register_runner("percolate", percolate, async_runner=True)

The function ``percolate`` is the actual runner and takes the following parameters:

* ``es``, is an instance of the Elasticsearch Python client
* ``params`` is a ``dict`` of parameters provided by its corresponding parameter source. Treat this parameter as read-only.

This function can return:

* Nothing at all. Then Rally will assume by default ``1`` and ``"ops"`` (see below).
* A tuple of ``weight`` and a ``unit``, which is usually ``1`` and ``"ops"``. If you run a bulk operation you might return the bulk size here, for example in number of documents or in MB. Then you'd return for example ``(5000, "docs")`` Rally will use these values to store throughput metrics.
* A ``dict`` with arbitrary keys. If the ``dict`` contains the key ``weight`` it is assumed to be numeric and chosen as weight as defined above. The key ``unit`` is treated similarly. All other keys are added to the ``meta`` section of the corresponding service time and latency metrics records.

Similar to a parameter source you also need to bind the name of your operation type to the function within ``register``.

To illustrate how to use custom return values, suppose we want to implement a custom runner that calls the `pending tasks API <https://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-pending.html>`_ and returns the number of pending tasks as additional meta-data::

    async def pending_tasks(es, params):
        response = await es.cluster.pending_tasks()
        return {
            "weight": 1,
            "unit": "ops",
            "pending-tasks-count": len(response["tasks"])
        }

    def register(registry):
        registry.register_runner("pending-tasks", pending_tasks, async_runner=True)


If you need more control, you can also implement a runner class. The example above, implemented as a class looks as follows::

    class PercolateRunner:
        async def __call__(self, es, params):
            await es.percolate(
                index="queries",
                doc_type="content",
                body=params["body"]
            )

        def __repr__(self, *args, **kwargs):
            return "percolate"

    def register(registry):
        registry.register_runner("percolate", PercolateRunner(), async_runner=True)


The actual runner is implemented in the method ``__call__`` and the same return value conventions apply as for functions. For debugging purposes you should also implement ``__repr__`` and provide a human-readable name for your runner. Finally, you need to register your runner in the ``register`` function.

Runners also support Python's `asynchronous context manager <https://docs.python.org/3/reference/datamodel.html#async-context-managers>`_ interface. Rally uses a new context for each request. Implementing the asynchronous context manager interface can be handy for cleanup of resources after executing an operation. Rally uses it, for example, to clear open scrolls.

If you have specified multiple Elasticsearch clusters using :ref:`target-hosts <command_line_reference_advanced_topics>` you can make Rally pass a dictionary of client connections instead of one for the ``default`` cluster in the ``es`` parameter.

To achieve this you need to:

* Use a runner class
* Specify ``multi_cluster = True`` as a class attribute
* Use any of the cluster names specified in :ref:`target-hosts <command_line_reference_advanced_topics>` as a key for the ``es`` dict

Example (assuming Rally has been invoked specifying ``default`` and ``remote`` in `target-hosts`)::

    class CreateIndexInRemoteCluster:
        multi_cluster = True

        async def __call__(self, es, params):
            await es["remote"].indices.create(index="remote-index")

        def __repr__(self, *args, **kwargs):
            return "create-index-in-remote-cluster"

    def register(registry):
        registry.register_runner("create-index-in-remote-cluster", CreateIndexInRemoteCluster(), async_runner=True)


.. note::

    You need to implement ``register`` just once and register all parameter sources and runners there.

For cases, where you want to provide a progress indication, you can implement the two properties ``percent_completed`` which returns a floating point value between ``0.0`` and ``1.0`` and the property ``completed`` which needs to return ``True`` if the runner has completed. This can be useful in cases when it is only possible to determine progress by calling an API, for example when waiting for a recovery to finish.

.. warning::

    Rally will still treat such a runner like any other. If you want to poll status at certain intervals then limit the number of calls by specifying the ``target-throughput`` property on the corresponding task.

.. _adding_tracks_custom_schedulers:

Custom schedulers
^^^^^^^^^^^^^^^^^

.. warning::

    Your scheduler is on a performance-critical code-path. Double-check with :ref:`Rally's profiling support <clr_enable_driver_profiling>` that you did not introduce any bottlenecks.

If you want to rate-limit execution of tasks, you can specify a ``target-throughput`` either as a number to specify the operations per second or, if supported by the operation, as string denoting the target throughput with a different unit. For example, Rally attempts to run this term query 20 times per second::

  {
    "operation": "term",
    "target-throughput": 20
  }

This is identical to::

  {
    "operation": "term",
    "target-throughput": "20 ops/s"
  }

By default, Rally uses a `deterministic distribution <https://en.wikipedia.org/wiki/Degenerate_distribution>`_ to determine when to schedule the next operation. Hence it executes the term query at 0, 50ms, 100ms, 150ms and so on. The scheduler is also aware of the number of clients. Consider this example::

  {
    "operation": "term",
    "target-throughput": 20,
    "clients": 4
  }

If Rally would not take the number of clients into account and would still issue requests (from each of the four clients) at the same points in time (i.e. 0, 50ms, 100ms, 150ms, ...), it would run at a target throughput of 4 * 20 = 80 operations per second. Hence, Rally will automatically reduce the rate at which each client will execute requests. Each client will issue requests at 0, 200ms, 400ms, 600ms, 800ms, 1000ms and so on. Each client issues five requests per second but as there are four of them, we still have a target throughput of 20 operations per second. You should keep this in mind, when writing your own custom schedules.

To create a custom scheduler, create a file ``track.py`` next to ``track.json`` and implement the scheduler class::

    import random

    class RandomScheduler:
        def __init__(self, task, target_throughput):
            self.rate = 1 / target_throughput
            # scale accordingly with the number of clients
            self.variation = task.clients * params.get("variation-millis", 10)

        def next(self, current):
            # roughly matches the target throughput with some random variation
            return current + self.rate + random.randint(-self.variation // 2, self.variation // 2) / 1000.0

The scheduler class also needs to be registered::

    def register(registry):
        registry.register_scheduler("my-random", RandomScheduler)

You can then use your custom scheduler as follows::

  {
    "operation": "term",
    "schedule": "my-random",
    "clients": 10,
    "target-throughput": 100,
    "variation-millis": 1
  }

