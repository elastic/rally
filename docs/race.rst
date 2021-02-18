Run a Benchmark: Races
======================

Definition
----------

A "race" in Rally is the execution of a benchmarking experiment. You can choose different benchmarking scenarios (called :doc:`tracks </track>`) for your benchmarks.

List Tracks
-----------

Start by finding out which tracks are available::

    esrally list tracks

This will show the following list::

    Name        Description                                          Documents  Compressed Size    Uncompressed Size    Default Challenge        All Challenges
    ----------  -------------------------------------------------  -----------  -----------------  -------------------  -----------------------  ---------------------------
    geonames    POIs from Geonames                                    11396505  252.4 MB           3.3 GB               append-no-conflicts      append-no-conflicts,appe...
    geopoint    Point coordinates from PlanetOSM                      60844404  481.9 MB           2.3 GB               append-no-conflicts      append-no-conflicts,appe...
    http_logs   HTTP server log data                                 247249096  1.2 GB             31.1 GB              append-no-conflicts      append-no-conflicts,appe...
    nested      StackOverflow Q&A stored as nested docs               11203029  663.1 MB           3.4 GB               nested-search-challenge  nested-search-challenge,...
    noaa        Global daily weather measurements from NOAA           33659481  947.3 MB           9.0 GB               append-no-conflicts      append-no-conflicts,appe...
    nyc_taxis   Taxi rides in New York in 2015                       165346692  4.5 GB             74.3 GB              append-no-conflicts      append-no-conflicts,appe...
    percolator  Percolator benchmark based on AOL queries              2000000  102.7 kB           104.9 MB             append-no-conflicts      append-no-conflicts,appe...
    pmc         Full text benchmark with academic papers from PMC       574199  5.5 GB             21.7 GB              append-no-conflicts      append-no-conflicts,appe...

The first two columns show the name and a description of each track. A track also specifies one or more challenges which describe the workload to run.

Starting a Race
---------------

.. note::
    Do not run Rally as root as Elasticsearch will refuse to start with root privileges.

To start a race you have to define the track and challenge to run. For example::

    esrally race --distribution-version=6.0.0 --track=geopoint --challenge=append-fast-with-conflicts

Rally will then start racing on this track. If you have never started Rally before, it should look similar to the following output::

    dm@io:~ $ esrally race --distribution-version=6.0.0 --track=geopoint --challenge=append-fast-with-conflicts

        ____        ____
       / __ \____ _/ / /_  __
      / /_/ / __ `/ / / / / /
     / _, _/ /_/ / / / /_/ /
    /_/ |_|\__,_/_/_/\__, /
                    /____/

    [INFO] Racing on track [geopoint], challenge [append-fast-with-conflicts] and car ['defaults'] with version [6.0.0].
    [INFO] Downloading Elasticsearch 6.0.0 ... [OK]
    [INFO] Rally will delete the benchmark candidate after the benchmark
    [INFO] Downloading data from [http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/geopoint/documents.json.bz2] (482 MB) to [/Users/dm/.rally/benchmarks/data/geopoint/documents.json.bz2] ... [OK]
    [INFO] Decompressing track data from [/Users/dm/.rally/benchmarks/data/geopoint/documents.json.bz2] to [/Users/dm/.rally/benchmarks/data/geopoint/documents.json] (resulting size: 2.28 GB) ... [OK]
    [INFO] Preparing file offset table for [/Users/dm/.rally/benchmarks/data/geopoint/documents.json] ... [OK]
    Running index-update                                                           [  0% done]


The benchmark will take a while to run, so be patient.

When the race has finished, Rally will show a summary on the command line::

    |                          Metric |         Task |     Value |   Unit |
    |--------------------------------:|-------------:|----------:|-------:|
    |             Total indexing time |              |   124.712 |    min |
    |                Total merge time |              |   21.8604 |    min |
    |              Total refresh time |              |   4.49527 |    min |
    |       Total merge throttle time |              |  0.120433 |    min |
    |                Median CPU usage |              |     546.5 |      % |
    |         Total Young Gen GC time |              |    72.078 |      s |
    |        Total Young Gen GC count |              |        43 |        |
    |           Total Old Gen GC time |              |     3.426 |      s |
    |          Total Old Gen GC count |              |         1 |        |
    |                      Index size |              |   2.26661 |     GB |
    |                   Total written |              |    30.083 |     GB |
    |          Heap used for segments |              |   10.7148 |     MB |
    |        Heap used for doc values |              | 0.0135536 |     MB |
    |             Heap used for terms |              |   9.22965 |     MB |
    |            Heap used for points |              |   0.78789 |     MB |
    |     Heap used for stored fields |              |  0.683708 |     MB |
    |                   Segment count |              |       115 |        |
    |                  Min Throughput | index-update |   59210.4 | docs/s |
    |                 Mean Throughput | index-update |   60110.3 | docs/s |
    |               Median Throughput | index-update |   65276.2 | docs/s |
    |                  Max Throughput | index-update |   76516.6 | docs/s |
    |       50.0th percentile latency | index-update |   556.269 |     ms |
    |       90.0th percentile latency | index-update |   852.779 |     ms |
    |       99.0th percentile latency | index-update |   1854.31 |     ms |
    |       99.9th percentile latency | index-update |   2972.96 |     ms |
    |      99.99th percentile latency | index-update |   4106.91 |     ms |
    |        100th percentile latency | index-update |   4542.84 |     ms |
    |  50.0th percentile service time | index-update |   556.269 |     ms |
    |  90.0th percentile service time | index-update |   852.779 |     ms |
    |  99.0th percentile service time | index-update |   1854.31 |     ms |
    |  99.9th percentile service time | index-update |   2972.96 |     ms |
    | 99.99th percentile service time | index-update |   4106.91 |     ms |
    |   100th percentile service time | index-update |   4542.84 |     ms |
    |                  Min Throughput |  force-merge |  0.221067 |  ops/s |
    |                 Mean Throughput |  force-merge |  0.221067 |  ops/s |
    |               Median Throughput |  force-merge |  0.221067 |  ops/s |
    |                  Max Throughput |  force-merge |  0.221067 |  ops/s |
    |        100th percentile latency |  force-merge |   4523.52 |     ms |
    |   100th percentile service time |  force-merge |   4523.52 |     ms |


    ----------------------------------
    [INFO] SUCCESS (took 1624 seconds)
    ----------------------------------


.. note::
    You can save this report also to a file by using ``--report-file=/path/to/your/report.md`` and save it as CSV with ``--report-format=csv``.

What did Rally just do?

* It downloaded and started Elasticsearch 6.0.0
* It downloaded the relevant data for the geopoint track
* It ran the actual benchmark
* And finally it reported the results

If you are curious about the operations that Rally has run, inspect the `geopoint track specification <https://github.com/elastic/rally-tracks/blob/5/geopoint/track.json>`_ or start to :doc:`write your own tracks </adding_tracks>`. You can also configure Rally to :doc:`store all data samples in Elasticsearch </configuration>` so you can analyze the results with Kibana. Finally, you may want to :doc:`change the Elasticsearch configuration </car>`.


