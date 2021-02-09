Quickstart
==========

Rally is developed for Unix and is actively tested on Linux and MacOS. Rally supports `benchmarking Elasticsearch clusters running on Windows <http://esrally.readthedocs.io/en/stable/recipes.html#benchmarking-an-existing-cluster>`_ but Rally itself needs to be installed on machines running Unix.

Install
-------

Install Python 3.8+ including ``pip3``, git 1.9+ and an `appropriate JDK to run Elasticsearch <https://www.elastic.co/support/matrix#matrix_jvm>`_ Be sure that ``JAVA_HOME`` points to that JDK. Then run the following command, optionally prefixed by ``sudo`` if necessary::

    pip3 install esrally


If you have any trouble or need more detailed instructions, look in the :doc:`detailed installation guide </install>`.

Run your first race
-------------------

Now we're ready to run our first :doc:`race </glossary>`::

    esrally race --distribution-version=6.5.3 --track=geonames

This will download Elasticsearch 6.5.3 and run the `geonames <https://github.com/elastic/rally-tracks/tree/master/geonames>`_ :doc:`track </glossary>` against it. After the race, a :doc:`summary report </summary_report>` is written to the command line:::


    ------------------------------------------------------
        _______             __   _____
       / ____(_)___  ____ _/ /  / ___/_________  ________
      / /_  / / __ \/ __ `/ /   \__ \/ ___/ __ \/ ___/ _ \
     / __/ / / / / / /_/ / /   ___/ / /__/ /_/ / /  /  __/
    /_/   /_/_/ /_/\__,_/_/   /____/\___/\____/_/   \___/
    ------------------------------------------------------

    |   Lap |                                                          Metric |                   Task |     Value |    Unit |
    |------:|----------------------------------------------------------------:|-----------------------:|----------:|--------:|
    |   All |                      Cumulative indexing time of primary shards |                        |   54.5878 |     min |
    |   All |              Min cumulative indexing time across primary shards |                        |   10.7519 |     min |
    |   All |           Median cumulative indexing time across primary shards |                        |   10.9219 |     min |
    |   All |              Max cumulative indexing time across primary shards |                        |   11.1754 |     min |
    |   All |             Cumulative indexing throttle time of primary shards |                        |         0 |     min |
    |   All |     Min cumulative indexing throttle time across primary shards |                        |         0 |     min |
    |   All |  Median cumulative indexing throttle time across primary shards |                        |         0 |     min |
    |   All |     Max cumulative indexing throttle time across primary shards |                        |         0 |     min |
    |   All |                         Cumulative merge time of primary shards |                        |   20.4128 |     min |
    |   All |                        Cumulative merge count of primary shards |                        |       136 |         |
    |   All |                 Min cumulative merge time across primary shards |                        |   3.82548 |     min |
    |   All |              Median cumulative merge time across primary shards |                        |    4.1088 |     min |
    |   All |                 Max cumulative merge time across primary shards |                        |   4.38148 |     min |
    |   All |                Cumulative merge throttle time of primary shards |                        |   1.17975 |     min |
    |   All |        Min cumulative merge throttle time across primary shards |                        |    0.1169 |     min |
    |   All |     Median cumulative merge throttle time across primary shards |                        |   0.26585 |     min |
    |   All |        Max cumulative merge throttle time across primary shards |                        |  0.291033 |     min |
    |   All |                       Cumulative refresh time of primary shards |                        |    7.0317 |     min |
    |   All |                      Cumulative refresh count of primary shards |                        |       420 |         |
    |   All |               Min cumulative refresh time across primary shards |                        |   1.37088 |     min |
    |   All |            Median cumulative refresh time across primary shards |                        |    1.4076 |     min |
    |   All |               Max cumulative refresh time across primary shards |                        |   1.43343 |     min |
    |   All |                         Cumulative flush time of primary shards |                        |  0.599417 |     min |
    |   All |                        Cumulative flush count of primary shards |                        |        10 |         |
    |   All |                 Min cumulative flush time across primary shards |                        | 0.0946333 |     min |
    |   All |              Median cumulative flush time across primary shards |                        |  0.118767 |     min |
    |   All |                 Max cumulative flush time across primary shards |                        |   0.14145 |     min |
    |   All |                                                Median CPU usage |                        |     284.4 |       % |
    |   All |                                         Total Young Gen GC time |                        |    12.868 |       s |
    |   All |                                        Total Young Gen GC count |                        |        17 |         |
    |   All |                                           Total Old Gen GC time |                        |     3.803 |       s |
    |   All |                                          Total Old Gen GC count |                        |         2 |         |
    |   All |                                                      Store size |                        |   3.17241 |      GB |
    |   All |                                                   Translog size |                        |   2.62736 |      GB |
    |   All |                                                      Index size |                        |   5.79977 |      GB |
    |   All |                                                   Total written |                        |   22.8536 |      GB |
    |   All |                                          Heap used for segments |                        |   18.8885 |      MB |
    |   All |                                        Heap used for doc values |                        | 0.0322647 |      MB |
    |   All |                                             Heap used for terms |                        |   17.7184 |      MB |
    |   All |                                             Heap used for norms |                        | 0.0723877 |      MB |
    |   All |                                            Heap used for points |                        |  0.277171 |      MB |
    |   All |                                     Heap used for stored fields |                        |  0.788307 |      MB |
    |   All |                                                   Segment count |                        |        94 |         |
    |   All |                                                  Min Throughput |           index-append |   38089.5 |  docs/s |
    |   All |                                                 Mean Throughput |           index-append |   38325.2 |  docs/s |
    |   All |                                               Median Throughput |           index-append |   38613.9 |  docs/s |
    |   All |                                                  Max Throughput |           index-append |   40693.3 |  docs/s |
    |   All |                                         50th percentile latency |           index-append |   803.417 |      ms |
    |   All |                                         90th percentile latency |           index-append |    1913.7 |      ms |
    |   All |                                         99th percentile latency |           index-append |   3591.23 |      ms |
    |   All |                                       99.9th percentile latency |           index-append |   6176.23 |      ms |
    |   All |                                        100th percentile latency |           index-append |   6642.97 |      ms |
    |   All |                                    50th percentile service time |           index-append |   803.417 |      ms |
    |   All |                                    90th percentile service time |           index-append |    1913.7 |      ms |
    |   All |                                    99th percentile service time |           index-append |   3591.23 |      ms |
    |   All |                                  99.9th percentile service time |           index-append |   6176.23 |      ms |
    |   All |                                   100th percentile service time |           index-append |   6642.97 |      ms |
    |   All |                                                      error rate |           index-append |         0 |       % |
    |   All |                                                            ...  |                    ... |       ... |     ... |
    |   All |                                                            ...  |                    ... |       ... |     ... |
    |   All |                                                  Min Throughput | large_prohibited_terms |         2 |   ops/s |
    |   All |                                                 Mean Throughput | large_prohibited_terms |         2 |   ops/s |
    |   All |                                               Median Throughput | large_prohibited_terms |         2 |   ops/s |
    |   All |                                                  Max Throughput | large_prohibited_terms |         2 |   ops/s |
    |   All |                                         50th percentile latency | large_prohibited_terms |   344.429 |      ms |
    |   All |                                         90th percentile latency | large_prohibited_terms |   353.187 |      ms |
    |   All |                                         99th percentile latency | large_prohibited_terms |    377.22 |      ms |
    |   All |                                        100th percentile latency | large_prohibited_terms |   392.918 |      ms |
    |   All |                                    50th percentile service time | large_prohibited_terms |   341.177 |      ms |
    |   All |                                    90th percentile service time | large_prohibited_terms |   349.979 |      ms |
    |   All |                                    99th percentile service time | large_prohibited_terms |   374.958 |      ms |
    |   All |                                   100th percentile service time | large_prohibited_terms |    388.62 |      ms |
    |   All |                                                      error rate | large_prohibited_terms |         0 |       % |


    ----------------------------------
    [INFO] SUCCESS (took 1862 seconds)
    ----------------------------------


Next steps
----------

Now you can check :doc:`how to run benchmarks </race>`, get a better understanding how to interpret the numbers in the :doc:`summary report </summary_report>`, :doc:`configure </configuration>` Rally to better suit your needs or start to :doc:`create your own tracks </adding_tracks>`. Be sure to check also some :doc:`tips and tricks </recipes>` to help you understand how to solve specific problems in Rally.

Also run ``esrally --help`` to see what options are available and keep the :doc:`command line reference </command_line_reference>` handy for more detailed explanations of each option.
