### 0.6.0

#### Highlights

* [#258](https://github.com/elastic/rally/issues/258): Make 'race' self-contained

#### Enhancements

* [#284](https://github.com/elastic/rally/issues/284) (Breaking): Disallow previously deprecated usage of track properties in meta block
* [#283](https://github.com/elastic/rally/issues/283): Store race results in a format optimized for reporting
* [#279](https://github.com/elastic/rally/issues/279) (Breaking): Separate race and metrics indices
* [#276](https://github.com/elastic/rally/issues/276): Add affected index to meta-data returned by bulk index runner
* [#275](https://github.com/elastic/rally/issues/275): Allow to define per-challenge cluster-settings
* [#267](https://github.com/elastic/rally/issues/267): Provide a progress-indication for downloads
* [#266](https://github.com/elastic/rally/issues/266): Make the gc telemetry device Java 9 compatible
* [#246](https://github.com/elastic/rally/issues/246): Rally should print a warning if there are no measurement samples
* [#223](https://github.com/elastic/rally/issues/223): Allow unlimited number of pages for scroll queries
* [#222](https://github.com/elastic/rally/issues/222): Report number of hits, not just pages, for scroll queries
* [#220](https://github.com/elastic/rally/issues/220) (Breaking): Allow monthly indices for metrics
* [#138](https://github.com/elastic/rally/issues/138) (Breaking): Improve support Elasticsearch 5 as metrics store

#### Bug Fixes

* [#285](https://github.com/elastic/rally/issues/285): Rally is stuck for benchmarks with a very large number of requests
* [#280](https://github.com/elastic/rally/issues/280): The file-reader parameter source does not seem to pass additional parameters

#### Doc Changes

* [#288](https://github.com/elastic/rally/issues/288): Clarify usage of remote track repositories in docs
* [#287](https://github.com/elastic/rally/issues/287): Document throughput semantics for each operation
* [#274](https://github.com/elastic/rally/issues/274): Explain summary report output
* [#271](https://github.com/elastic/rally/issues/271): Document how to implement custom runner classes
* [#264](https://github.com/elastic/rally/issues/264): Documentation on settings and warning message when using a documents archive file

### 0.5.3

#### Highlights

* [#251](https://github.com/elastic/rally/issues/251): Support for non-deterministic distribution of operations

#### Enhancements

* [#260](https://github.com/elastic/rally/issues/260): Have bulk parameter source provide the bulk size
* [#249](https://github.com/elastic/rally/issues/249): Saving tournament report
* [#155](https://github.com/elastic/rally/issues/155): Improve document count handling

#### Bug Fixes

* [#263](https://github.com/elastic/rally/issues/263): Race condition when rolling log files
* [#261](https://github.com/elastic/rally/issues/261): Unable to determine valid external socket address
* [#253](https://github.com/elastic/rally/issues/253): Cannot determine CPU info for POWER8 chips
* [#242](https://github.com/elastic/rally/issues/242): Rally does not detect the distribution version correctly for externally provisioned clusters
* [#235](https://github.com/elastic/rally/issues/235): Allow Python files in a track directory that are unrelated to the track plugin

#### Doc Changes

* [#256](https://github.com/elastic/rally/issues/256): Document additional decompression options
* [#247](https://github.com/elastic/rally/issues/247): Description of available operation types in the docs
* [#241](https://github.com/elastic/rally/issues/241): Clearly document which Elasticsearch versions are supported

### 0.5.2

#### Enhancements

* [#244](https://github.com/elastic/rally/issues/244): Show a nice error message when user tries to run a non-existing challenge on an existing track

#### Bug Fixes

* [#245](https://github.com/elastic/rally/issues/245): Connection behind a proxy fails. has_internet_connection() returns False

### 0.5.1

#### Enhancements

* [#234](https://github.com/elastic/rally/issues/234): Add request error rate to summary report

### 0.5.0

#### Highlights

* [#238](https://github.com/elastic/rally/issues/238): Simplify gathering of facts for benchmark results sharing
* [#216](https://github.com/elastic/rally/issues/216): Add possibility to profile load driver internals
* [#184](https://github.com/elastic/rally/issues/184): Allow to benchmark a single-machine cluster remotely

#### Enhancements

* [#207](https://github.com/elastic/rally/issues/207): Improve response checks in bulk API runner
* [#205](https://github.com/elastic/rally/issues/205): Make track configuration more modular
* [#200](https://github.com/elastic/rally/issues/200): Allow root.dir to be set during configure
* [#199](https://github.com/elastic/rally/issues/199): Let track authors decide on the default challenge
* [#191](https://github.com/elastic/rally/issues/191): Support target-interval for operations

#### Bug Fixes

* [#225](https://github.com/elastic/rally/issues/225): Final score report blank when metrics store specified
* [#221](https://github.com/elastic/rally/issues/221): Scrolls fail against Elasticsearch master
* [#203](https://github.com/elastic/rally/issues/203): Index time metrics are not aware of laps
* [#202](https://github.com/elastic/rally/issues/202): Bulk index source reads data for all indices

#### Doc Changes

* [#224](https://github.com/elastic/rally/issues/224): Issue(s) with --user-tags
* [#214](https://github.com/elastic/rally/issues/214): Improve documentation of "parallel"
* [#213](https://github.com/elastic/rally/issues/213): Document how to support --test-mode in a track
* [#208](https://github.com/elastic/rally/issues/208): Add FAQ item to explain latency, service time and their relation to "took"

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
