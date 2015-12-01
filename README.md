Rally is the macrobenchmarking framework for Elasticsearch

### Prerequisites

* Python 3.4+ available as `python3` on the path
* Python Elasticsearch client. Install via `pip3 install elasticsearch`
* (Optional) Python psutil library. Install via `pip3 install psutil`
* JDK 8+
* Gradle 2.8+
* git

Rally is only tested on Mac OS X and Linux.

### Getting started

* Clone this repo: `git clone git@github.com:elastic/rally.git`
* Open `rally/config.py` in the editor of your choice and change the configuration. This is not really convenient but we're getting 
there, promise. :) The idea is to have a setup script that will ask for those values and put them in `~.rally/rally.cfg`.
* Run rally from the root project directory: `python3 rally/rally.py`.

### Command Line Options

Rally provides a list of supported command line options when it is invoked with `--help`. 

### Key Components of Rally

Note: This is just important if you want to hack on Rally and to some extend if you want to add new benchmarks. It is not that interesting if you are just using it.
 
* `Series`: represents a class of benchmarking scenarios, e.g. a logging benchmark. It defines the data set to use.
* `Track`: A track is a concrete benchmark configuration, e.g. the logging benchmark with Elasticsearch default settings.
* `Mechanic`: A mechanic can build and prepare a benchmark candidate for the race. It checks out the source, builds Elasticsearch, provisions and starts the cluster.
* `Race Control`: Race control is responsible for proper setup of the race.
* `Telemetry`: Telemetry allows us to gather metrics during the race. 
* `Driver`: drives the race, i.e. it is executing the benchmark according to the track specification.
* `Reporter`: A reporter tells us how the race went (currently only after the fact).

When implementing a new benchmark, create a new file in `track` and subclass `Series` and `Track`. See `track/logging_track.py` for an example.
Currently, race control does not pick up the new benchmark automatically but adding support for that is coming soon. 

TODO dm: Add a nice diagram for a graphical overview of the key components and their dependencies 