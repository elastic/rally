## Rally

Rally is the macrobenchmarking framework for Elasticsearch

### Prerequisites

* Python 3.4+ available as `python3` on the path (verify with: `python3 --version` which should print `Python 3.4.0` (or higher))
* `pip3` available on the path (verify with `pip3 --version`)
* JDK 8+
* Gradle 2.8+
* git
* unzip (install via `apt-get install unzip` on  Debian based distributions or check your distribution's documentation)

Rally is only tested on Mac OS X and Linux.

### Getting Started

1. Clone this repo: `git clone git@github.com:elastic/rally.git`
2. Install Rally and its dependencies: `python3 setup.py develop`. Depending on your local setup and file system permission it might be necessary to use `sudo` in this step. `sudo`ing is required as this script will install two Python libraries which Rally needs to run:`psutil` to gather process metrics and `elasticsearch` to connect to the benchmark cluster. Additionally, the setup procedure will set symlinks to the script `esrally` so it can be easily invoked. If you don't want do that, see the section below for an alternative. Note: this step will change once Rally is available in the official Python package repos.
3. Configure Rally: `esrally configure`. It will prompt you for some values and write them to the config file `~/.rally/rally.ini`.
4. Run Rally: `esrally`. It is now properly set up and will run the benchmarks.

### Non-sudo Install

If you don't want to use `sudo` when installing Rally, installation is still possible but a little more involved:
 
1. Specify the `--user` option when installing Rally (step 2 above), so the command to be issued is: `python3 setup.py develop --user`
2. Check the output of the install script or lookup the [Python documentation on the variable site.USER_BASE](https://docs.python.org/3.5/library/site.html#site.USER_BASE) to find out where the script is located. On Linux, this is typically `~/.local/bin`.

You can now either add `~/.local/bin` to your path or invoke Rally via `~/.local/bin/esrally` instead of just `esrally`.

### Command Line Options

Rally has a list of supported command line options. Just run `esrally --help`.

#### Telemetry

Rally can add telemetry during the race. Currently, only [Java Flight Recorder](http://docs.oracle.com/javacomponents/jmc-5-5/jfr-runtime-guide/index.html) is supported. 

To see the list of available telemetry devices, use `esrally list-telemetry`. To enable telemetry devices, run Rally with the `--telemetry` option, e.g.: `esrally --telemetry=jfr` enables the Java Flight Recorder based profiler.


### Key Components of Rally

Note: This is just important if you want to hack on Rally and to some extent if you want to add new benchmarks. It is not that interesting if you are just using it.

* `Race Control`: is responsible for proper execution of the race. It sets up all components and acts as a high-level controller.
* `Mechanic`: can build and prepare a benchmark candidate for the race. It checks out the source, builds Elasticsearch, provisions and starts the cluster.
* `Track`: is a concrete benchmarking scenario, e.g. the logging benchmark. It defines the data set to use.
* `TrackSetup`: is a concrete system configuration for a benchmark, e.g. Elasticsearch default settings. Note: There are some lose ends in the code due to the porting efforts. The implementation is very likely to change significantly.
* `Driver`: drives the race, i.e. it is executing the benchmark according to the track specification.
* `Reporter`: A reporter tells us how the race went (currently only after the fact).

When implementing a new benchmark, create a new file in `track` and create a new `Track` and one or more `TrackSetup` instances. See `track/countries_track.py` for an example.
Currently, race control does not pick up the new benchmark automatically but adding support for that is coming soon.
 
### How to Contribute
 
See all details in the [contributor guidelines](CONTRIBUTING.md).
 
### License
 
This software is licensed under the Apache License, version 2 ("ALv2"), quoted below.

Copyright 2015 Elasticsearch <https://www.elastic.co>

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
