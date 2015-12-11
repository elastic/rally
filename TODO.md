Below is a list of the shortcomings we are aware of and other areas to investigate. This list is roughly prioritized.

### Support for running nightly benchmarks:

* Reenable timing of builds (currently we don't even run gradle assembly as it is taking up too much time)
* The original version printed more information like full path to java, the mappings being used, how many indexing threads (in the python client) are used, git hash tested, etc. -> Check whether this information has been lost and add it again if necessary.
* Verification of rally against the original version (compare log files (single-threaded test)?)
* Check number of cores and use that many indexing threads (instead of always 8) 
    
### Backtesting

* Maven build
* Choose Java and build tool based on timestamp, commit id, ... . -> Already fleshed out in gear/gear.py
* Iteration loop around race control

### Multi Benchmark Support

There is not much missing for this one; the structure is already in place.

* Pick up benchmarks automatically
* Proper reporting for multiple benchmarks -> Spice up reporting by allowing multiple benchmarks with a menu structure like in http://getbootstrap.com/examples/navbar/
* Also note that pickling previous results will be broken (assume just one benchmark)
* Idea: Allow to provide benchmark files externally -> separation between Rally and benchmarks - can we just use a descriptive format for benchmarks?
* Split actual benchmarks and Rally: extract data, mappings and possible queries and download them from one common place like http://benchmarks.elastic.co/corpora/geonames/documents.json.bz2
  
### Conceptual Topics

* Test thoroughly for bottlenecks in every place (I/O, CPU, benchmark driver, metrics gathering, etc. etc.)
* Account for warmup, multiple benchmark iterations (-> also check JIT compiler logs of benchmark candidate)
* Randomization of order in which benchmarks are run
* Account for coordinated omission
* Check metric gathering, i.e. what's the resolution of the Python time API? Are we staying well above resolution so we don't get fooled by noise? Can we use another API?
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
