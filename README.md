Rally is the macrobenchmarking framework for Elasticsearch

### Prerequisites

* Python 3.4+ available as `python3` on the path
* JDK 8+
* Gradle 2.8+
* git

Rally is only tested on Mac OS X and Linux.

### Getting started

* Clone this repo: `git clone git@github.com:elastic/rally.git`
* Verify the Python installation: `python3 --version` should print `Python 3.4.0` (or higher)
* Install Rally and its dependencies: `python3 setup.py develop`. Depending on your local setup and file system permission it might be necessary to use `sudo` in this step. `sudo`ing is required as this script will install two Python libraries which Rally needs to run:`psutil` to gather process metrics and `elasticsearch` to connect to the benchmark cluster. Note: this step will change when Rally is available in the Python package repos.
* Run Rally: `esrally`. The first time it will prompt you for some values and write them to the config file `~/.rally/rally.ini`.
* Rerun Rally: `esrally`. It is now properly set up and will run the benchmarks.

Caveat: The diagrams are intended for performance regression testing so they only show a single data point per build. After the first build only a single data point is shown (will change in the future).  

### Command Line Options

Rally has a list of supported command line options. Just run `esrally --help`.

### Key Components of Rally

Note: This is just important if you want to hack on Rally and to some extent if you want to add new benchmarks. It is not that interesting if you are just using it.

* `Race Control`: is responsible for proper execution of the race. It sets up all components and acts as a high-level controller.
* `Mechanic`: can build and prepare a benchmark candidate for the race. It checks out the source, builds Elasticsearch, provisions and starts the cluster.
* `Track`: is a concrete benchmarking scenario, e.g. the logging benchmark. It defines the data set to use.
* `TrackSetup`: is a concrete system configuration for a benchmark, e.g. Elasticsearch default settings. Note: There are some lose ends in the code due to the porting efforts. The implementation is very likely to change significantly.
* `Driver`: drives the race, i.e. it is executing the benchmark according to the track specification.
* `Reporter`: A reporter tells us how the race went (currently only after the fact).

When implementing a new benchmark, create a new file in `track` and subclass `Track` and `TrackSetup`. See `track/countries_track.py` for an example.
Currently, race control does not pick up the new benchmark automatically but adding support for that is coming soon. 
 
### License
 
Copyright 2015 Elasticsearch
 
Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 
  http://www.apache.org/licenses/LICENSE-2.0
 
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.