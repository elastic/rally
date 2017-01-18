Quickstart
==========

Install
-------

Install Python 3.4+ including ``pip3``, JDK 8 and git 1.9+. Then run the following command, optionally prefixed by ``sudo`` if necessary::

    pip3 install esrally


If you have any trouble or need more detailed instructions, please look in the :doc:`detailed installation guide </install>`.

Configure
---------

Just invoke ``esrally configure``.

For more detailed instructions and a detailed walkthrough see the :doc:`configuration guide </configuration>`.

Run your first race
-------------------

Now we're ready to run our first race::

    esrally --distribution-version=5.0.0

This will download Elasticsearch 5.0.0 and run Rally's default track against it. After the race, a summary report is written to the command line:::

    ------------------------------------------------------
        _______             __   _____
       / ____(_)___  ____ _/ /  / ___/_________  ________
      / /_  / / __ \/ __ `/ /   \__ \/ ___/ __ \/ ___/ _ \
     / __/ / / / / / /_/ / /   ___/ / /__/ /_/ / /  /  __/
    /_/   /_/_/ /_/\__,_/_/   /____/\___/\____/_/   \___/
    ------------------------------------------------------

    |                         Metric |            Operation |     Value |   Unit |
    |-------------------------------:|---------------------:|----------:|-------:|
    |                  Indexing time |                      |   28.0997 |    min |
    |                     Merge time |                      |   6.84378 |    min |
    |                   Refresh time |                      |   3.06045 |    min |
    |                     Flush time |                      |  0.106517 |    min |
    |            Merge throttle time |                      |   1.28193 |    min |
    |               Median CPU usage |                      |     471.6 |      % |
    |             Total Young Gen GC |                      |    16.237 |      s |
    |               Total Old Gen GC |                      |     1.796 |      s |
    |                     Index size |                      |   2.60124 |     GB |
    |                Totally written |                      |   11.8144 |     GB |
    |         Heap used for segments |                      |   14.7326 |     MB |
    |       Heap used for doc values |                      |  0.115917 |     MB |
    |            Heap used for terms |                      |   13.3203 |     MB |
    |            Heap used for norms |                      | 0.0734253 |     MB |
    |           Heap used for points |                      |    0.5793 |     MB |
    |    Heap used for stored fields |                      |  0.643608 |     MB |
    |                  Segment count |                      |        97 |        |
    |                 Min Throughput |         index-append |   31925.2 | docs/s |
    |              Median Throughput |         index-append |   39137.5 | docs/s |
    |                 Max Throughput |         index-append |   39633.6 | docs/s |
    |      50.0th percentile latency |         index-append |   872.513 |     ms |
    |      90.0th percentile latency |         index-append |   1457.13 |     ms |
    |      99.0th percentile latency |         index-append |   1874.89 |     ms |
    |       100th percentile latency |         index-append |   2711.71 |     ms |
    | 50.0th percentile service time |         index-append |   872.513 |     ms |
    | 90.0th percentile service time |         index-append |   1457.13 |     ms |
    | 99.0th percentile service time |         index-append |   1874.89 |     ms |
    |  100th percentile service time |         index-append |   2711.71 |     ms |
    |                           ...  |                  ... |       ... |    ... |
    |                           ...  |                  ... |       ... |    ... |
    |                 Min Throughput |     painless_dynamic |   2.53292 |  ops/s |
    |              Median Throughput |     painless_dynamic |   2.53813 |  ops/s |
    |                 Max Throughput |     painless_dynamic |   2.54401 |  ops/s |
    |      50.0th percentile latency |     painless_dynamic |    172208 |     ms |
    |      90.0th percentile latency |     painless_dynamic |    310401 |     ms |
    |      99.0th percentile latency |     painless_dynamic |    341341 |     ms |
    |      99.9th percentile latency |     painless_dynamic |    344404 |     ms |
    |       100th percentile latency |     painless_dynamic |    344754 |     ms |
    | 50.0th percentile service time |     painless_dynamic |    393.02 |     ms |
    | 90.0th percentile service time |     painless_dynamic |   407.579 |     ms |
    | 99.0th percentile service time |     painless_dynamic |   430.806 |     ms |
    | 99.9th percentile service time |     painless_dynamic |   457.352 |     ms |
    |  100th percentile service time |     painless_dynamic |   459.474 |     ms |

    ----------------------------------
    [INFO] SUCCESS (took 2634 seconds)
    ----------------------------------


Next steps
----------

Now you can check :doc:`how to run benchmarks </race>` or :doc:`how to define your own benchmarks </adding_tracks>`. Be sure to check also the available :doc:`recipes </recipes>` to help you understand how to solve specific problems in Rally.

Also run ``esrally --help`` to see what options are available and keep the :doc:`command line reference </command_line_reference>` handy for more detailed explanations of each option.

