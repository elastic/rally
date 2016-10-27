Frequently Asked Questions (FAQ)
================================

A benchmark aborts with ``Couldn't find a tar.gz distribution``. What's the problem?
------------------------------------------------------------------------------------

This error occurs when Rally cannot build an Elasticsearch distribution from source code. The most likely cause is that there is some problem in the build setup.

To see what's the problem, try building Elasticsearch yourself. First, find out where the source code is located (run ``grep local.src.dir  ~/.rally/rally.ini``). Then change to this directory and run the following commands::

    gradle clean
    gradle :distribution:tar:assemble

By that you are mimicking what Rally does. Fix any errors that show up here and then retry.

Where does Rally get the benchmark data from?
---------------------------------------------

Rally comes with a set of tracks out of the box which we maintain in the `rally-tracks repository on Github <https://github.com/elastic/rally-tracks>`_. This repository contains the track descriptions. The actual data are stored as compressed files in an S3 bucket.

Will Rally destroy my existing indices?
---------------------------------------

First of all: Please (please, please) do NOT run Rally against your production cluster if you are just getting started with it. You have been warned.

Depending on the track, Rally will delete and create one or more indices. For example, the `geonames track <https://github.com/elastic/rally-tracks/blob/master/geonames/track.json#L9>`_ specifies that Rally should create an index named "geonames" and Rally will assume it can do to this index whatever it wants. Specifically, Rally will check at the beginning of a race if the index "geonames" exists and delete it. After that it creates a new empty "geonames" index and runs the benchmark. So if you benchmark against your own cluster (by specifying the ``benchmark-only`` :doc:`pipeline </pipelines>`) and this cluster contains an index that is called "geonames" you will lose (all) data if you run Rally against it. Rally will neither read nor write (or delete) any other index. So if you apply the usual care nothing bad can happen.

Where and how long does Rally keep its data?
--------------------------------------------

Rally stores a lot of data (this is just the nature of a benchmark) so you should keep an eye on disk usage. All data are kept in ``~/.rally`` and Rally does not implicitly delete them. These are the most important directories:

* ``~/.rally/benchmarks/races``: logs, telemetry data or even complete Elasticsearch installations including the data directory if a benchmark failed. If you don't need the logs anymore, you can safely wipe this directory.
* ``~/.rally/benchmarks/src``: the Elasticsearch Github repository (only if you had Rally build Elasticsearch from sources at least once).
* ``~/.rally/benchmarks/data``: the benchmark data sets. This directory can get very huge (way more than 100 GB if you want to try all default tracks). You can delete the files in this directory but keep in mind that Rally may needs to download them again.
* ``~/.rally/benchmarks/distributions``: Contains all downloaded Elasticsearch distributions.

There are a few more directories but the four above are the most disk-hogging ones.

Does Rally spy on me?
---------------------

No. Rally does not collect or send any usage data and also the complete source code is open. We do value your feedback a lot though and if you got any ideas for improvements, found a bug or have any other feedback, please head over to `Rally's Discuss forum <https://discuss.elastic.co/c/elasticsearch/rally>`_ or `raise an issue on Github <https://github.com/elastic/rally>`_.