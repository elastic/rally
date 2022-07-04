<p align="center"><img alt="Rally logo" src="https://raw.githubusercontent.com/elastic/rally/master/docs/rally-logo.svg" width="350px"></p>

<h1 align="center">Rally</h1>

Rally is the macrobenchmarking framework for Elasticsearch

## What is Rally?

You want to benchmark Elasticsearch? Then Rally is for you. It can help you with the following tasks:

* Setup and teardown of an Elasticsearch cluster for benchmarking
* Management of benchmark data and specifications even across Elasticsearch versions
* Running benchmarks and recording results
* Finding performance problems by attaching so-called telemetry devices
* Comparing performance results

We have also put considerable effort in Rally to ensure that benchmarking data are reproducible.

## Quick Start

Rally is developed for Unix and is actively tested on Linux and macOS. Rally supports [benchmarking Elasticsearch clusters running on Windows](http://esrally.readthedocs.io/en/stable/recipes.html#benchmarking-an-existing-cluster) but Rally itself needs to be installed on machines running Unix.

### Installing Rally

**Note**: If you actively develop on Elasticsearch, we recommend that you `install Rally in development mode <https://esrally.readthedocs.io/en/latest/developing.html#installation-instructions-for-development>`_ instead as Elasticsearch is fast moving and Rally always adapts accordingly to the latest main version.

Install Python 3.8+ including `pip3`, git 1.9+ and an [appropriate JDK to run Elasticsearch](https://www.elastic.co/support/matrix#matrix_jvm>). Be sure that `JAVA_HOME` points to that JDK. Then run the following command, optionally prefixed by `sudo` if necessary:

    pip3 install esrally


If you have any trouble or need more detailed instructions, please look in the [detailed installation guide](https://esrally.readthedocs.io/en/latest/install.html>).

## Run your first race

Now we're ready to run our first race:

    esrally race --distribution-version=6.0.0 --track=geonames

This will download Elasticsearch 6.0.0 and run Rally's default track - the [geonames track](https://github.com/elastic/rally-tracks/tree/master/geonames>) - against it. After the race, a summary report is written to the command line:

    ------------------------------------------------------
        _______             __   _____
       / ____(_)___  ____ _/ /  / ___/_________  ________
      / /_  / / __ \/ __ `/ /   \__ \/ ___/ __ \/ ___/ _ \
     / __/ / / / / / /_/ / /   ___/ / /__/ /_/ / /  /  __/
    /_/   /_/_/ /_/\__,_/_/   /____/\___/\____/_/   \___/
    ------------------------------------------------------

    |                         Metric |                 Task |     Value |   Unit |
    |-------------------------------:|---------------------:|----------:|-------:|
    |            Total indexing time |                      |   28.0997 |    min |
    |               Total merge time |                      |   6.84378 |    min |
    |             Total refresh time |                      |   3.06045 |    min |
    |               Total flush time |                      |  0.106517 |    min |
    |      Total merge throttle time |                      |   1.28193 |    min |
    |               Median CPU usage |                      |     471.6 |      % |
    |             Total Young Gen GC |                      |    16.237 |      s |
    |               Total Old Gen GC |                      |     1.796 |      s |
    |                     Index size |                      |   2.60124 |     GB |
    |                  Total written |                      |   11.8144 |     GB |
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


## Getting help

* Quick help: `esrally --help`
* Look in [Rally's user guide](https://esrally.readthedocs.io/>) for more information
* Ask questions about Rally in the [Rally Discuss forum](https://discuss.elastic.co/tags/c/elastic-stack/elasticsearch/rally).
* File improvements or bug reports in our [Github repo](https://github.com/elastic/rally/issues>).

## How to Contribute

See all details in the [contributor guidelines](https://github.com/elastic/rally/blob/master/CONTRIBUTING.md>).


## License

This software is licensed under the Apache License, version 2 ("ALv2"), quoted below.

Copyright 2015-2021 Elasticsearch <https://www.elastic.co>

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
