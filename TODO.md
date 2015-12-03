### Immediate

* Check if build logs are working and configure a proper separation between build log, the benchmark output and metrics output
* Reenable scenario with 2 nodes (currently cluster does not turn green in this case - see logging_track.py)

---

### After that

* Support for running locally on dev machines:
    * Introduce subcommands (run, setup) -> use https://docs.python.org/3.5/library/argparse.html#module-argparse for that
    * Externalize configuration -> hardcoded values must get out of config.py
    * Easier setup / configuration and clear docs on how to get started (use setup.py for installing dependencies!)
    * Support for downloading the benchmark file directly from rally (without boto!)
    * Nice to have: Command line reporter showing a metrics summary (in addition or instead of graphs)

* Tests
* [Pydocs](http://sphinxcontrib-napoleon.readthedocs.org/en/latest/example_google.html)

* Support for running nightly benchmarks:
    * Verification of rally against the original version (compare log files (single-threaded test)?)
    * EC2 benchmarks
    * Various minor bits like uploading the report to S3
    * Reenable timing of builds (currently we don't even run gradle assembly as it is taking up too much time)
    
* Backtesting support:
    * Maven build
    * Choose Java and build tool based on timestamp, commit id, ... . -> Already fleshed out in gear/gear.py
    * Iteration loop around race control

* Triggering via Jenkins (at random times, how?)

* Rally auto-update (run some kind of "pre-script"? Maybe also in Jenkins?)

* Support for multiple benchmarks (not much missing for this one, structure already in place),
    * Pick up benchmarks automatically
    * Add an iteration loop in race control so iterates over multiple benchmarks
    * Proper reporting for multiple benchmarks -> Spice up reporting by allowing multiple benchmarks with a menu structure
    * Also note that pickling previous results will be broken
  like in http://getbootstrap.com/examples/navbar/)
  
* Open up the repo
* How can we split benchmark development from rally? (-> logging benchmark shouldn't be directly in rally)

### Internal Refactorings

* Get metrics gathering out of tracks, maybe the benchmark can trigger that but metrics should be gathered by something in metrics.py ("Sensor"?)

 
### Further Ideas

* Support additional JVM options for the benchmark candidate by setting "ES_GC_OPTS" (e.g. for benchmarking with G1)  
* Warn if there is not enough disk space on your ROOT_DIR (-> for data files)
* Introduce a tournament mode (candidate vs. baseline)
* Conceptual topics:
    * Test thoroughly for bottlenecks in every place (I/O, CPU, benchmark driver, metrics gathering, etc. etc.)
    * Account for warmup, multiple benchmark iterations (-> also check JIT compiler logs of benchmark candidate)
    * Randomization of order in which benchmarks are run
    * Account for coordinated omission
    * Check metric gathering, i.e. what's the resolution of the Python time API? Are we staying well above resolution so we don't get fooled by noise? Can we use another API?
    * Metrics reporting (latency distribution, not mean)
    * Physically isolate benchmark driver from benchmark candidate
    * Add ability to dig deeper (flamegraphs etc.)
* Add scalability benchmarks
* Time series data for a single benchmark run (Marvel in local benchmark mode?)

TODO dm: Create Github issues
