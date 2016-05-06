Tournaments
===========

Suppose, we want to analyze the impact of a performance improvement. First, we need a baseline measurement. We can use the command line parameter ``--user-tag`` to provide a key-value pair to document the intent of a race. After we've run both races, we want to know about the performance impact of a change. With Rally we can analyze differences of two given races easily. First of all, we need to find two races to compare by issuing ``esrally list races``::

    dm@io:~ $ esrally list races

        ____        ____
       / __ \____ _/ / /_  __
      / /_/ / __ `/ / / / / /
     / _, _/ /_/ / / / /_/ /
    /_/ |_|\__,_/_/_/\__, /
                    /____/
    Recent races:

    Race Timestamp     Track     Track setups    User Tag
    -----------------  --------  --------------  ------------------------------
    20160502T191011Z   geonames  defaults        intention:reduce_alloc_1234
    20160502T190127Z   geonames  defaults        intention:baseline_github_1234
    20160502T185632Z   tiny      defaults
    20160502T185619Z   tiny      defaults
    20160502T185604Z   tiny      defaults
    20160502T185551Z   tiny      defaults
    20160502T185538Z   tiny      defaults
    20160502T185525Z   tiny      defaults
    20160502T185511Z   tiny      defaults
    20160502T185459Z   tiny      defaults

We can see that the user tag helps us to recognize races. We want to compare the two most recent races and have to provide the two race timestamps in the next step::

    dm@io:~ $ esrally compare --baseline=20160502T190127Z --contender=20160502T191011Z

        ____        ____
       / __ \____ _/ / /_  __
      / /_/ / __ `/ / / / / /
     / _, _/ /_/ / / / /_/ /
    /_/ |_|\__,_/_/_/\__, /
                    /____/

    Comparing baseline
      Race timestamp: 2016-05-02 19:01:27
      Track setup: defaults

    with contender
      Race timestamp: 2016-05-02 19:10:11
      Track setup: defaults

    ------------------------------------------------------
        _______             __   _____
       / ____(_)___  ____ _/ /  / ___/_________  ________
      / /_  / / __ \/ __ `/ /   \__ \/ ___/ __ \/ ___/ _ \
     / __/ / / / / / /_/ / /   ___/ / /__/ /_/ / /  /  __/
    /_/   /_/_/ /_/\__,_/_/   /____/\___/\____/_/   \___/
    ------------------------------------------------------
                                                      Metric    Baseline    Contender               Diff
    --------------------------------------------------------  ----------  -----------  -----------------
                            Min Indexing Throughput [docs/s]       19501        19118  -383.00000
                         Median Indexing Throughput [docs/s]       20232      19927.5  -304.45833
                            Max Indexing Throughput [docs/s]       21172        20849  -323.00000
                                         Indexing time [min]     55.7989       56.335    +0.53603
                                            Merge time [min]     12.9766      13.3115    +0.33495
                                          Refresh time [min]     5.20067      5.20097    +0.00030
                                            Flush time [min]   0.0648667    0.0681833    +0.00332
                                   Merge throttle time [min]    0.796417     0.879267    +0.08285
                   Query latency term (50.0 percentile) [ms]     2.10049      2.15421    +0.05372
                   Query latency term (90.0 percentile) [ms]     2.77537      2.84168    +0.06630
                  Query latency term (100.0 percentile) [ms]     4.52081      5.15368    +0.63287
            Query latency country_agg (50.0 percentile) [ms]     112.049      110.385    -1.66392
            Query latency country_agg (90.0 percentile) [ms]     128.426      124.005    -4.42138
           Query latency country_agg (100.0 percentile) [ms]     155.989      133.797   -22.19185
                 Query latency scroll (50.0 percentile) [ms]     16.1226      14.4974    -1.62519
                 Query latency scroll (90.0 percentile) [ms]     17.2383      15.4079    -1.83043
                Query latency scroll (100.0 percentile) [ms]     18.8419      18.4241    -0.41784
     Query latency country_agg_cached (50.0 percentile) [ms]     1.70223      1.64502    -0.05721
     Query latency country_agg_cached (90.0 percentile) [ms]     2.34819      2.04318    -0.30500
    Query latency country_agg_cached (100.0 percentile) [ms]     3.42547      2.86814    -0.55732
                Query latency default (50.0 percentile) [ms]     5.89058      5.83409    -0.05648
                Query latency default (90.0 percentile) [ms]     6.71282      6.64662    -0.06620
               Query latency default (100.0 percentile) [ms]     7.65307       7.3701    -0.28297
                 Query latency phrase (50.0 percentile) [ms]     1.82687      1.83193    +0.00506
                 Query latency phrase (90.0 percentile) [ms]     2.63714      2.46286    -0.17428
                Query latency phrase (100.0 percentile) [ms]     5.39892      4.22367    -1.17525
                                Median CPU usage (index) [%]     668.025       679.15   +11.12499
                                Median CPU usage (stats) [%]      143.75        162.4   +18.64999
                               Median CPU usage (search) [%]       223.1        229.2    +6.10000
                                      Total Young Gen GC [s]      39.447       40.456    +1.00900
                                        Total Old Gen GC [s]       7.108        7.703    +0.59500
                                             Index size [GB]     3.25475      3.25098    -0.00377
                                        Totally written [GB]     17.8434      18.3143    +0.47083
                                 Heap used for segments [MB]     21.7504      21.5901    -0.16037
                               Heap used for doc values [MB]     0.16436      0.13905    -0.02531
                                    Heap used for terms [MB]     20.0293      19.9159    -0.11345
                                    Heap used for norms [MB]    0.105469    0.0935669    -0.01190
                                   Heap used for points [MB]    0.773487     0.772155    -0.00133
                                   Heap used for points [MB]    0.677795     0.669426    -0.00837
                                               Segment count         136          121   -15.00000
                         Indices Stats(90.0 percentile) [ms]     3.16053      3.21023    +0.04969
                         Indices Stats(99.0 percentile) [ms]     5.29526      3.94132    -1.35393
                        Indices Stats(100.0 percentile) [ms]     5.64971      7.02374    +1.37403
                           Nodes Stats(90.0 percentile) [ms]     3.19611      3.15251    -0.04360
                           Nodes Stats(99.0 percentile) [ms]     4.44111      4.87003    +0.42892
                          Nodes Stats(100.0 percentile) [ms]     5.22527      5.66977    +0.44450

