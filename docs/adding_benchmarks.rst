Adding new benchmarks to Rally
==============================

Overview
--------

First of all we need to clarify what a benchmark is. Rally has a few assumptions built-in:

1. Rally sets up a fresh Elasticsearch cluster, i.e. the cluster is entirely under Rally's control.
2. The first step of the benchmark is to index all required documents via the `bulk API <https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html>`_. Rally will measure metrics like indexing throughput in this phase.
3. An optional second step is to run one or more queries against the index. Rally will measure metrics like query latency in this phase.

This is called a "track" in Rally. The most important attributes of a track are:

* One or more indices, each with one or more types
* The queries to issue
* Source URL of the benchmark data
* A list of steps to run, which we'll call "challenge", for example indexing data with a specific number of documents per bulk request or running searches for a defined number of iterations.

Separately from a track, we also have "cars" which define the settings of the benchmark candidate (Elasticsearch), like how much heap memory to use, the number of nodes to start and so on. Rally comes with a set of default tracks and cars which you can use for your own benchmarks (but you don't have to).

Tracks are written as JSON files and are kept in a separate track repository, which is located at https://github.com/elastic/rally-tracks. This repository has separate branches for different Elasticsearch versions and Rally will check out the appropriate branch based on the command line parameter ``--distribution-version``. If the parameter is missing, Rally will assume by default that you are benchmarking the latest version of Elasticsearch and will checkout the master branch of the track repository.

Example track
-------------

Let's create an example track step by step. First of all, we need some data. There are a lot of public data sets available which are interesting for new benchmarks and we also have a
`backlog of benchmarks to add <https://github.com/elastic/rally-tracks/issues>`_.

`Geonames <http://www.geonames.org/>`_ provides geo data under a `creative commons license <http://creativecommons.org/licenses/by/3.0/>`_. We will download `allCountries.zip <http://download.geonames.org/export/dump/allCountries.zip>`_ (around 300MB), extract it and inspect ``allCountries.txt``.

You will note that the file is tab-delimited but we need JSON to bulk-index data with Elasticsearch. So we can use a small script to do the conversion for us::

    import json
    import csv
    
    cols = (('geonameid', 'int'),
           ('name', 'string'),
           ('asciiname', 'string'),
           ('alternatenames', 'string'),
           ('latitude', 'double'),
           ('longitude', 'double'),
           ('feature_class', 'string'),
           ('feature_code', 'string'),
           ('country_code', 'string'),
           ('cc2', 'string'),
           ('admin1_code', 'string'),
           ('admin2_code', 'string'),
           ('admin3_code', 'string'),
           ('admin4_code', 'string'),
           ('population', 'long'),
           ('elevation', 'int'),
           ('dem', 'string'),
           ('timezone', 'string'))
           
    with open('allCountries.txt') as f:
     while True:
       line = f.readline()
       if line == '':
         break
       tup = line.strip().split('\t')
       d = {}
       for i in range(len(cols)):
         name, type = cols[i]
         if tup[i] != '':
           if type in ('int', 'long'):
             d[name] = int(tup[i])
           elif type == 'double':
             d[name] = float(tup[i])
           else:
             d[name] = tup[i]
    
       print(json.dumps(d))

We can invoke the script with ``python3 toJSON.py > documents.json``.

Next we need to compress the JSON file with ``bzip2 -9 -c documents.json > documents.json.bz2``. Upload the data file to a place where it is publicly available. We choose ``http://benchmarks.elastic.co/corpora/geonames`` for this example.

For initial local testing you can place the data file in Rally's data directory, which is located in ``~/.rally/benchmarks/data``. For this example you need to place the data for the "geonames" track in ``~/.rally/benchmarks/data/geonames`` so Rally can pick it up. Additionally, you have to specify the ``--offline`` option when running Rally so it does not try to download any benchmark data.

Next we need a mapping file for our documents. For details on how to write a mapping file, see `the Elasticsearch documentation on mappings <https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html>`_ and look at the `example mapping file <https://github.com/elastic/rally-tracks/blob/master/geonames/mappings.json>`_. Place the mapping file in your ``rally-tracks`` repository in a dedicated folder. This repository is located in ``~/.rally/benchmarks/tracks/default`` and we place the mapping file in ``~/.rally/benchmarks/tracks/default/geonames`` for this track.

The track repository is managed by git, so ensure that you are on the ``master`` branch by running ``git checkout master``. Then add a new JSON file right next to the mapping file. The file has to be called "track.json" and is the actual track specification ::

    {
      "meta": {
        "short-description": "Standard benchmark in Rally (8.6M POIs from Geonames)",
        "description": "This test indexes 8.6M documents (POIs from Geonames, total 2.8 GB json) using 8 client threads and 5000 docs per bulk request against Elasticsearch",
        "data-url": "http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/geonames"
      },
      "indices": [
        {
          "name": "geonames",
          "types": [
            {
              "name": "type",
              "mapping": "mappings.json",
              "documents": "documents.json.bz2",
              "document-count": 8647880,
              "compressed-bytes": 197857614,
              "uncompressed-bytes": 2790927196
            }
          ]
        }
      ],
      "operations": [
        {
          "name": "index-append-default-settings",
          "type": "index",
          "index-settings": {
            "index.number_of_replicas": 0
          },
          "bulk-size": 5000,
          "force-merge": true,
          "clients": {
            "count": 8
          }
        },
        {
          "name": "search",
          "type": "search",
          "warmup-iterations": 1000,
          "iterations": 1000,
          "clients": {
            "count": 1
          },
          "queries": [
            {
              "name": "default",
              "body": {
                "query": {
                  "match_all": {}
                }
              }
            }
          ]
        }
      ],
      "challenges": [
        {
          "name": "append-no-conflicts",
          "description": "",
          "schedule": [
            "index-append-default-settings",
            "search"
          ]
        }
      ]
    }

Finally, you need to commit your changes: ``git commit -a -m "Add geonames track"``.

A few things to note:

* Rally assumes that the challenge that should be run by default is called "append-no-conflicts". If you want to run a different challenge, provide the command line option ``--challenge=YOUR_CHALLENGE_NAME``.
* You can add as many queries as you want. We use the `official Python Elasticsearch client <http://elasticsearch-py.readthedocs.org/>`_ to issue queries.
* The numbers below the ``types`` property are needed to verify integrity and provide progress reports.

.. note::

    We have defined a `JSON schema for tracks <https://github.com/elastic/rally/blob/master/esrally/resources/track-schema.json>`_ which you can use to check how to define your track. You should also check the tracks provided by Rally for inspiration.

When you invoke ``esrally list tracks``, the new track should now appear::

    dm@io:~ $ esrally list tracks
    
        ____        ____
       / __ \____ _/ / /_  __
      / /_/ / __ `/ / / / / /
     / _, _/ /_/ / / / /_/ /
    /_/ |_|\__,_/_/_/\__, /
                    /____/
    Available tracks:
    
    Name        Description                                               Challenges
    ----------  --------------------------------------------------------  -------------------
    geonames    Standard benchmark in Rally (8.6M POIs from Geonames)     append-no-conflicts

Congratulations, you have created your first track! You can test it with ``esrally --track=geonames --offline`` (or whatever the name of your track is) and run specific challenges with ``esrally --track=geonames --challenge=append-fast-with-conflicts --offline``.

If you want to share your track with the community, please read on.

How to contribute a track
-------------------------

First of all, please read Rally's `contributors guide <https://github.com/elastic/rally/blob/master/CONTRIBUTING.md>`_.

If you want to contribute your track, follow these steps:

1. Create a track JSON file and mapping files as described above and place them in a separate folder in the ``rally-tracks`` repository. Please also add a README file in this folder which contains licensing information (respecting the licensing terms of the source data). Note that pull requests for tracks without a license cannot be accepted.
2. Upload the associated data so they can be publicly downloaded via HTTP. The data should be compressed either as .bz2 (recommended) or as .zip.
3. Create a pull request in the `rally-tracks Github repo <https://github.com/elastic/rally-tracks>`_.

Advanced topics
---------------

Template Language
^^^^^^^^^^^^^^^^^

Rally uses `Jinja2 <http://jinja.pocoo.org/docs/dev/>`_ as template language. This allows you to use Jinja2 expressions in track files.


Extension Points
""""""""""""""""

Rally also provides a few extension points to Jinja2:

* ``now``: This is a global variable that represents the current date and time when the template is evaluated by Rally.
* ``days_ago()``: This is a `filter <http://jinja.pocoo.org/docs/dev/templates/#filters>`_ that you can use for date calculations.

You can find an examle in the logging track::

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

The data set that is used in the logging track starts on 26-04-1998 but we want to ignore the first few days for this query, so we start on 15-05-1998. The expression ``{{'15-05-1998' | days_ago(now)}}`` yields the difference in days between now and the fixed start date and allows us to benchmark time range queries relative to now with a predetermined data set.

