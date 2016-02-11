Below is a list of the shortcomings we are aware of and other areas to investigate. This list is roughly prioritized.

### Support for running nightly benchmarks:

* The original version printed more information like full path to java, the mappings being used, how many indexing threads (in the python client) are used, git hash tested, etc. -> Check whether this information has been lost and add it again if necessary.
* Verification of rally against the original version (compare log files (single-threaded test)?)
  
### Conceptual Topics

* Test thoroughly for bottlenecks in every place (I/O, CPU, benchmark driver, metrics gathering, etc. etc.)
* Account for warmup, multiple benchmark iterations (-> also check JIT compiler logs of benchmark candidate)
* Account for coordinated omission
* Metrics reporting (latency distribution, not mean)
* Physically isolate benchmark driver from benchmark candidate
* Add ability to dig deeper (flamegraphs etc.)

### Usability

* Warn if there is not enough disk space on your ROOT_DIR (-> for data files)
* Simulate "no space left on device" while running. Ensure that Rally will work the next time after freeing up enough space.

### Reporting

* Write command line summary also as report to disk
* Time series data for a single benchmark run (Marvel in local benchmark mode?)
* More metrics / graphs (see also 'Conceptual Topics')
* Add support for JSON output

### Internal Refactorings

* Get metrics gathering out of tracks, maybe the benchmark can trigger that but metrics should be gathered by something in metrics.py ("Sensor"?)
 
### Further Ideas

* Support additional JVM options for the benchmark candidate by setting "ES_GC_OPTS" (e.g. for benchmarking with G1)  
* Clear documentation in one place on how the benchmark candidate was invoked: Java version, Java options, GC options, ES config file
* Introduce a tournament mode (candidate vs. baseline)
* Add scalability benchmarks
* Add benchmarks running on Found
* Measure single-shot latency of search requests (what's the latency of a query that is just executed once without any caching)
