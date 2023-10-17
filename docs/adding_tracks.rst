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

Alternatively, to create a Rally track with data from the data streams ``logs-*`` and ``metrics-*``, issue the following command::

    esrally create-track --track=acme --target-hosts=127.0.0.1:9200 --data-streams="metrics-*,logs-*" --output-path=~/tracks

If you want to connect to a cluster with TLS and basic authentication enabled, for example via Elastic Cloud, also specify :ref:`--client-options <clr_client_options>` and change ``basic_auth_user`` and ``basic_auth_password`` accordingly::

    esrally create-track --track=acme --target-hosts=abcdef123.us-central-1.gcp.cloud.es.io:9243 --client-options="use_ssl:true,verify_certs:true,basic_auth_user:'elastic',basic_auth_password:'secret-password'" --indices="products,companies" --output-path=~/tracks

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

.. note::
   This tutorial assumes that you want to benchmark a version of Elasticsearch 7.0.0 or later. If you want to benchmark Elasticsearch prior to 7.0.0 you need to add the mapping type above so ``index.json`` will look like::

      ...
      "mappings": {
        "docs": {
          ...
        }
      }
      ...


For details on the allowed syntax, see the Elasticsearch documentation on `mappings <https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html>`_ and the `create index API <https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html>`__.

Finally, store the track as ``track.json`` in the tutorial directory::

    {
      "version": 2,
      "description": "Tutorial benchmark for Rally",
      "indices": [
        {
          "name": "geonames",
          "body": "index.json"
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


The numbers under the ``documents`` property are needed to verify integrity and provide progress reports. Determine the correct document count with ``wc -l documents.json``. For the size in bytes, use ``stat -f %z documents.json`` on macOS and ``stat -c %s documents.json`` on GNU/Linux.

.. note::
   This tutorial assumes that you want to benchmark a version of Elasticsearch 7.0.0 or later. If you want to benchmark Elasticsearch prior to 7.0.0 you need to add the ``types`` property above so ``track.json`` will look like::

      ...
      "indices": [
        {
          "name": "geonames",
          "body": "index.json",
          "types": [ "docs" ]
        }
      ],
      ...


.. note::

    You can store any supporting scripts along with your track. However, you need to place them in a directory starting with "_", e.g. "_support". Rally loads track plugins (see below) from any directory but will ignore directories starting with "_".

.. note::

    We have defined a `JSON schema for tracks <https://github.com/elastic/rally/blob/master/esrally/resources/track-schema.json>`_ which you can use to check how to define your track. You should also check the tracks provided by Rally for inspiration.

The new track appears when you run ``esrally list tracks --track-path=~/rally-tracks/tutorial``::

    $ esrally list tracks --track-path=~/rally-tracks/tutorial

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

    $ esrally info --track-path=~/rally-tracks/tutorial

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

Congratulations, you have created your first track! You can test it with ``esrally race --distribution-version=7.14.1 --track-path=~/rally-tracks/tutorial``.

.. note::

    To test the track with Elasticsearch prior to 7.0.0 you need to update ``index.json`` and ``track.json`` as specified in notes above and then execute ``esrally race --distribution-version=6.5.3 --track-path=~/rally-tracks/tutorial``.


.. _add_track_test_mode:

Adding support for test mode
----------------------------

You can check your track very quickly for syntax errors when you invoke Rally with ``--test-mode``. Rally postprocesses its internal track representation as follows:

* Iteration-based tasks run at most one warmup iteration and one measurement iteration.
* Time-period-based tasks run at most for 10 seconds without warmup.

Rally also post-processes all data file names. Instead of ``documents.json``, Rally expects ``documents-1k.json`` and assumes the file contains 1.000 documents. You need to prepare these data files though. Pick 1.000 documents for every data file in your track and store them in a file with the suffix ``-1k``. We choose the first 1.000 with ``head -n 1000 documents.json > documents-1k.json``.

If your file has 1000 or fewer documents, you can skip creating the ``-1k`` variant. Rally will use the original file in such case.

Challenges
----------

To specify different workloads in the same track you can use so-called challenges. Instead of specifying the ``schedule`` property on top-level you specify a ``challenges`` array::

    {
      "version": 2,
      "description": "Tutorial benchmark for Rally",
      "indices": [
        {
          "name": "geonames",
          "body": "index.json"
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

.. note::

    To use the track with Elasticsearch prior to 7.0.0 you need to update ``index.json`` and ``track.json`` with index and mapping types accordingly as specified in notes above.

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
          "body": "index.json"
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
          "body": "index.json"
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
          "body": "index.json"
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

So far the track is only available on your local machine. To share your track you could check it into version control. To avoid committing the potentially huge data file you can expose it via HTTP (e.g. via a cloud bucket) and reference it in your track with the property ``base-url``. Rally expects that the URL points to the parent path and appends the document file name automatically.

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

You've now mastered the basics of track development for Rally. It's time to pat yourself on the back before you dive into the :doc:`advanced topics </advanced>`!
