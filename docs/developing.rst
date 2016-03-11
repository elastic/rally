Developing Rally
================

Installation Instructions for Development
-----------------------------------------

If you want to hack on Rally or `add new benchmarks <adding_benchmarks.rst>`, you have to install Rally in development mode::


    # Clone the repo
    git clone https://github.com/elastic/rally.git
    cd rally
    # Install Rally in development mode, may require sudo. For detailed instructions, please see the README
    python3 setup.py develop

For more detailed instructions, please refer to the `README <https://github.com/elastic/rally/blob/master/README.rst>`.

Key Components of Rally
-----------------------

To get a rough understanding of Rally, it makes sense to get to know its key components:

* `Race Control`: is responsible for proper execution of the race. It sets up all components and acts as a high-level controller.
* `Mechanic`: can build and prepare a benchmark candidate for the race. It checks out the source, builds Elasticsearch, provisions and 
starts the cluster.
* `Track`: is a concrete benchmarking scenario, e.g. the logging benchmark. It defines the data set to use.
* `TrackSetup`: is a concrete system configuration for a benchmark, e.g. Elasticsearch default settings. Note: There are some lose ends 
in the code due to the porting efforts. The implementation is very likely to change significantly.
* `Driver`: drives the race, i.e. it is executing the benchmark according to the track specification.
* `Reporter`: A reporter tells us how the race went (currently only after the fact).

When implementing a new benchmark, create a new file in `track` and create a new `Track` and one or more `TrackSetup` instances. 
See `track/geonames_track.py` for an example. The new track will be picked up automatically. You can run Rally with your track 
by issuing `esrally --track=your-track-name`. All available tracks can be listed with `esrally list tracks`.

How to contribute code
----------------------

First of all, please read the `contributors guide <https://github.com/elastic/rally/blob/master/CONTRIBUTING.md>`.

We strive to be PEP-8 compliant but don't follow it to the letter.