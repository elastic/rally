### 0.4.0

#### Breaking changes in 0.4.0

The track format has changed a bit due a more flexible approach in how benchmarks are executed:
 
* Operations are defined in the `operations` section, execution details like number of warmup iterations, warmup time etc. are defined as part of the `schedule`.
* Each query needs to be defined as a separate operation and referenced in the `schedule`
* You can (and in fact should) specify a `warmup-time-period` (defined in sections) for bulk index operations. The warmup time period is specified in seconds.

For details please refer to the updated [JSON schema for Rally tracks](https://github.com/elastic/rally/blob/master/esrally/resources/track-schema.json).

Hint: This is just relevant for you, if you have defined your own tracks. We already took care of updating the [official Rally tracks](https://github.com/elastic/rally-tracks).

[All changes](https://github.com/elastic/rally/issues?q=milestone%3A0.4.0+is%3Aclosed)

### 0.3.0

#### Breaking changes in 0.3

We have [separated the previously known "track setup" into two parts](https://github.com/elastic/rally/issues/101):

* Challenges: Which describe what happens during a benchmark (whether to index or search and with which parameters)
* Cars: Which describe the benchmark candidate settings (e.g. heap size, logging configuration etc.)

This influences the command line interface in a couple of ways:

* To list all known cars, we have added a new command `esrally list cars`. To select a challenge, use now `--challenge` instead of `--track-setup` and also specify a car now with `--car`.
* Tournaments created by older versions of Rally are incompatible
* Rally must now be invoked with only one challenge and only one car (previously it was possible to specify multiple track setups)

We have also [moved tracks](https://github.com/elastic/rally/issues/69) to a [dedicated repository](https://github.com/elastic/rally-tracks). This allows you to support tracks for multiple versions of Elasticsearch but also requires that all users have `git` installed.
 
[All changes](https://github.com/elastic/rally/issues?q=milestone%3A0.3.0+is%3Aclosed)

#### Simplified configuration in 0.3

We have spent a lot of time to simplify first time setup of Rally. For starters, you are not required to setup your own metrics store if you don't need it. 
However, you are then just able to run individual benchmarks but you cannot compare results or visualize anything in Kibana. If you don't need this, it is recommended that you
remove the configuration directory and run `esrally configure`. Rally will notify you on its first start of this change and guide you through the process.

Please raise a ticket in case of problems.

### 0.2.1

* Add a [tournament mode](https://github.com/elastic/rally/issues/57). More information in the [user docs](https://esrally.readthedocs.io/en/latest/tournament.html)
* [External benchmarks can now specify target hosts and ports](https://github.com/elastic/rally/issues/83)
* Ability to [add a user-defined tag as metric meta-data](https://github.com/elastic/rally/issues/84)
* [Support .gzipped benchmark data](https://github.com/elastic/rally/issues/87) contributed by @monk-ee. Thanks!
* [Support for perf profiler](https://github.com/elastic/rally/issues/28)
* [Add a fulltext benchmark](https://github.com/elastic/rally/issues/38)

[All changes](https://github.com/elastic/rally/issues?q=milestone%3A0.2.1+is%3Aclosed)

### 0.2.0

Major changes:

* Rally can now [benchmark a binary Elasticsearch distribution](rally/issues#63) (starting with Elasticsearch 5.0.0-alpha1).
* Reporting improvements for [query latency](elastic/rally#10) and [indexing throughput](elastic/rally#59) on the command line.
* We store [benchmark environment data](elastic/rally#54) alongside metrics.
* A new percolator track](elastic/rally#74) contributed by [Martijn van Groningen](https://github.com/martijnvg). Thanks!

[All changes](https://github.com/elastic/rally/issues?q=milestone%3A0.2.0+is%3Aclosed)

### 0.1.0

Major changes:

* Added a [JIT profiler](https://github.com/elastic/rally/issues/43). This allows to check warmup times but also in-depth inspection which
optimizations were performed by the JIT compiler. If the HotSpot disassembler library is available, the logs will also contain the 
disassembled JIT compiler output which can be used for low-level analysis. We recommend to use 
[JITWatch](https://github.com/AdoptOpenJDK/jitwatch) for analysis.
* Added [pipeline support](https://github.com/elastic/rally/issues/61). Pipelines allow to define more flexibly which steps Rally executes
during a benchmark. One of the use-cases for this is to run a benchmark based on a released build of Elasticsearch rather than building it
ourselves.

[All changes](https://github.com/elastic/rally/issues?q=milestone%3A0.1.0+is%3Aclosed)

### 0.0.3

Major changes:

* Migrated the metrics data store from file-based to a dedicated Elasticsearch instance. Graphical reports can be created with 
  Kibana (optional but recommended). It is necessary to setup an Elasticsearch cluster to store metrics data (a single node 
  is sufficient). The cluster will be configured automatically by Rally. For details please see the [README](README.rst).
  
  Related issues: #8, #21, #46, 
  
[All changes](https://github.com/elastic/rally/issues?q=milestone%3A0.0.3+is%3Aclosed)
