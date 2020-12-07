Frequently Asked Questions (FAQ)
================================

A benchmark aborts with ``Couldn't find a tar.gz distribution``. What's the problem?
------------------------------------------------------------------------------------

This error occurs when Rally cannot build an Elasticsearch distribution from source code. The most likely cause is that there is some problem building the Elasticsearch distribution.

To see what's the problem, try building Elasticsearch yourself. First, find out where the source code is located (run ``grep src ~/.rally/rally.ini``). Then change to the directory (``src.root.dir`` + ``elasticsearch.src.subdir`` which is usually ``~/.rally/benchmarks/src/elasticsearch``) and run the following commands::

    ./gradlew clean
    ./gradlew :distribution:tar:assemble

By that you are mimicking what Rally does. Fix any errors that show up here and then retry.

Where does Rally get the benchmark data from?
---------------------------------------------

Rally comes with a set of tracks out of the box which we maintain in the `rally-tracks repository on Github <https://github.com/elastic/rally-tracks>`_. This repository contains the track descriptions. The actual data are stored as compressed files in an S3 bucket.

Will Rally destroy my existing indices?
---------------------------------------

First of all: Please (please, please) do NOT run Rally against your production cluster if you are just getting started with it. You have been warned.

Depending on the track, Rally will delete and create one or more indices. For example, the `geonames track <https://github.com/elastic/rally-tracks/blob/master/geonames/track.json#L9>`_ specifies that Rally should create an index named "geonames" and Rally will assume it can do to this index whatever it wants. Specifically, Rally will check at the beginning of a race if the index "geonames" exists and delete it. After that it creates a new empty "geonames" index and runs the benchmark. So if you benchmark against your own cluster (by specifying the ``benchmark-only`` :doc:`pipeline </pipelines>`) and this cluster contains an index that is called "geonames" you will lose (all) data if you run Rally against it. Rally will neither read nor write (or delete) any other index. So if you apply the usual care nothing bad can happen.

What do `latency` and `service_time` mean and how do they relate to the `took` field that Elasticsearch returns?
----------------------------------------------------------------------------------------------------------------

The ``took`` field is included by Elasticsearch in responses to searches and
bulk indexing and reports the time that Elasticsearch spent processing the
request. This value is measured on the server so it includes neither the time
it took for the request to get from the client to Elasticsearch nor the time it
took for the response to arrive back at the client. In contrast,
``service_time`` is measured by Rally as the length of time from when it
started to send the request to Elasticsearch until it finishing receiving the
response. Therefore ``service_time`` does include the time spent sending the
request and receiving the response.

The explanation of ``latency`` depends on your choice of benchmarking mode:

* **Throughput benchmarking mode**: In this mode, Rally will issue requests as
  fast as it can: as soon as it receives a response to one request it will
  generate and send the next request. In this mode ``latency`` and
  ``service_time`` are identical.

* **Throughput-throttled mode**: You may prefer to run benchmarks that better
  simulate the traffic patterns you experience in your production environment.
  If you define a :ref:`schedule <track_choose_schedule>` (e.g. a target
  throughput) then Rally runs in throughput-throttled mode and generates
  requests according to this schedule regardless of how fast Elasticsearch can
  respond. In this mode the generated requests are first placed in a queue
  within Rally and may stay there for some time. ``latency`` includes the time
  spent in this queue whereas ``service_time`` does not: ``latency`` measures
  the time from generating the request until the response is received whereas
  ``service_time`` measures the time from sending the request to Elasticsearch
  until a response is received.

If you are interested in latency measurement, we recommend you watch the following talks:

"How NOT to Measure Latency" by Gil Tene:

.. raw:: html

    <iframe width="640" height="360" src="https://www.youtube.com/embed/lJ8ydIuPFeU" frameborder="0" allowfullscreen></iframe>

Benchmarking Elasticsearch with Rally by Daniel Mitterdorfer:

.. raw:: html

    <iframe width="640" height="360" src="https://www.youtube.com/embed/HriBY2zoChw?start=2154" frameborder="0" allowfullscreen></iframe>


Where and how long does Rally keep its data?
--------------------------------------------

Rally stores a lot of data (this is just the nature of a benchmark) so you should keep an eye on disk usage. All data are kept in ``~/.rally`` and Rally does not implicitly delete them. These are the most important directories:

* ``~/.rally/logs``: Contains all log files. Logs are rotated daily. If you don't need the logs anymore, you can safely wipe this directory.
* ``~/.rally/benchmarks/races``: telemetry data, Elasticsearch logs and even complete Elasticsearch installations including the data directory if a benchmark failed. If you don't need the data anymore, you can safely wipe this directory.
* ``~/.rally/benchmarks/src``: the Elasticsearch Github repository (only if you had Rally build Elasticsearch from sources at least once).
* ``~/.rally/benchmarks/data``: the benchmark data sets. This directory can get very huge (way more than 100 GB if you want to try all default tracks). You can delete the files in this directory but keep in mind that Rally may needs to download them again.
* ``~/.rally/benchmarks/distributions``: Contains all downloaded Elasticsearch distributions.

There are a few more directories but the ones above are the most disk-hogging ones.

Does Rally spy on me?
---------------------

No. Rally does not collect or send any usage data and also the complete source code is open. We do value your feedback a lot though and if you got any ideas for improvements, found a bug or have any other feedback, head over to `Rally's Discuss forum <https://discuss.elastic.co/tags/c/elastic-stack/elasticsearch/rally>`_ or `raise an issue on Github <https://github.com/elastic/rally>`_.

Do I need an Internet connection?
---------------------------------

You do NOT need Internet access on any node of your Elasticsearch cluster but the machine where you start Rally needs an Internet connection to download track data sets and Elasticsearch distributions. After it has downloaded all data, an Internet connection is not required anymore and you can specify ``--offline``. If Rally detects no active Internet connection, it will automatically enable offline mode and warn you.

We have a dedicated documentation page for :doc:`running Rally offline </offline>` which should cover all necessary details.
