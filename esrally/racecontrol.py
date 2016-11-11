import logging
import shutil
import sys
import collections

import tabulate
import thespian.actors
from esrally import config, exceptions, paths, track, driver, reporter, metrics, time, PROGRAM_NAME
from esrally.mechanic import mechanic
from esrally.utils import console, io, convert

logger = logging.getLogger("rally.racecontrol")

pipelines = collections.OrderedDict()


class Pipeline:
    """
    Describes a whole execution pipeline. A pipeline can consist of one or more steps. Each pipeline should contain roughly of the following
    steps:

    * Prepare the benchmark candidate: It can build Elasticsearch from sources, download a ZIP from somewhere etc.
    * Launch the benchmark candidate: This can be done directly, with tools like Ansible or it can assume the candidate is already launched
    * Run the benchmark
    * Report results
    """

    def __init__(self, name, description, target, stable=True):
        """
        Creates a new pipeline.

        :param name: A short name of the pipeline. This name will be used to reference it from the command line.
        :param description: A human-readable description what the pipeline does.
        :param target: A function that implements this pipeline
        :param stable True iff the pipeline is considered production quality.
        """
        self.name = name
        self.description = description
        self.target = target
        self.stable = stable
        pipelines[name] = self

    def __del__(self):
        if pipelines is not None:
            pipelines.pop(self.name, default=None)

    def __call__(self, cfg):
        self.target(cfg)


class Benchmark:
    def __init__(self, cfg, mechanic, metrics_store):
        self.cfg = cfg
        self.mechanic = mechanic
        self.metrics_store = metrics_store
        self.cluster = None
        self.actor_system = None
        self.track = None

    def setup(self):
        self.mechanic.prepare_candidate()
        self.cluster = self.mechanic.start_engine()
        self.track = track.load_track(self.cfg)
        metrics.race_store(self.cfg).store_race(self.track)
        self.actor_system = thespian.actors.ActorSystem()

    def run(self, lap):
        self.metrics_store.lap = lap
        main_driver = self.actor_system.createActor(driver.Driver)
        self.cluster.on_benchmark_start()
        result = self.actor_system.ask(main_driver,
                                       driver.StartBenchmark(self.cfg, self.track, self.metrics_store.meta_info, self.metrics_store.lap))
        if isinstance(result, driver.BenchmarkComplete):
            logger.info("Benchmark is complete.")
            logger.info("Notifying cluster.")
            self.cluster.on_benchmark_stop()
            logger.info("Bulk adding data to metrics store.")
            self.metrics_store.bulk_add(result.metrics)
            logger.info("Flushing metrics data...")
            self.metrics_store.flush()
            logger.info("Flushing done")
        elif isinstance(result, driver.BenchmarkFailure):
            raise exceptions.RallyError(result.message, result.cause)
        else:
            raise exceptions.RallyError("Driver has returned no metrics but instead [%s]. Terminating race without result." % str(result))

    def teardown(self):
        logger.info("Stopping engine.")
        self.mechanic.stop_engine(self.cluster)
        logger.info("Closing metrics store.")
        self.metrics_store.close()
        logger.info("Summarizing results.")
        reporter.summarize(self.cfg, self.track)
        logger.info("Sweeping")
        self.sweep()

    def sweep(self):
        invocation_root = self.cfg.opts("system", "invocation.root.dir")
        track_name = self.cfg.opts("benchmarks", "track")
        challenge_name = self.cfg.opts("benchmarks", "challenge")
        car_name = self.cfg.opts("benchmarks", "car")

        log_root = paths.Paths(self.cfg).log_root()
        archive_path = "%s/logs-%s-%s-%s.zip" % (invocation_root, track_name, challenge_name, car_name)
        io.compress(log_root, archive_path)
        console.println("")
        console.info("Archiving logs in %s" % archive_path)
        shutil.rmtree(log_root)


class LapCounter:
    def __init__(self, track, laps, cfg):
        self.track = track
        self.laps = laps
        self.cfg = cfg
        self.lap_timer = time.Clock.stop_watch()
        self.lap_timer.start()
        self.lap_times = 0

    def before_lap(self, lap):
        if self.laps > 1:
            msg = "Lap [%d/%d]" % (lap, self.laps)
            console.println(console.format.bold(msg), logger=logger.info)
            console.println(console.format.underline_for(msg))

    def after_lap(self, lap):
        if self.laps > 1:
            lap_time = self.lap_timer.split_time() - self.lap_times
            self.lap_times += lap_time
            hl, ml, sl = convert.seconds_to_hour_minute_seconds(lap_time)
            reporter.summarize(self.cfg, track=self.track, lap=lap)
            console.println("")
            if lap < self.laps:
                remaining = (self.laps - lap) * self.lap_times / lap
                hr, mr, sr = convert.seconds_to_hour_minute_seconds(remaining)
                console.info("Lap time %02d:%02d:%02d (ETA: %02d:%02d:%02d)" % (hl, ml, sl, hr, mr, sr), logger=logger)
            else:
                console.info("Lap time %02d:%02d:%02d" % (hl, ml, sl), logger=logger)
            console.println("")


def print_race_info(cfg):
    track_name = cfg.opts("benchmarks", "track")
    challenge_name = cfg.opts("benchmarks", "challenge")
    selected_car_name = cfg.opts("benchmarks", "car")
    console.info("Racing on track [%s], challenge [%s] and car [%s]" % (track_name, challenge_name, selected_car_name))
    # just ensure it is optically separated
    console.println("")


def race(benchmark, cfg):
    laps = cfg.opts("benchmarks", "laps")
    print_race_info(cfg)
    benchmark.setup()
    lap_counter = LapCounter(benchmark.track, laps, cfg)

    for lap in range(1, laps + 1):
        lap_counter.before_lap(lap)
        benchmark.run(lap)
        lap_counter.after_lap(lap)

    benchmark.teardown()


# Poor man's curry
def from_sources_complete(cfg):
    metrics_store = metrics.metrics_store(cfg, read_only=False)
    return race(Benchmark(cfg, mechanic.create(cfg, metrics_store, sources=True, build=True), metrics_store), cfg)


def from_sources_skip_build(cfg):
    metrics_store = metrics.metrics_store(cfg, read_only=False)
    return race(Benchmark(cfg, mechanic.create(cfg, metrics_store, sources=True, build=False), metrics_store), cfg)


def from_distribution(cfg):
    metrics_store = metrics.metrics_store(cfg, read_only=False)
    return race(Benchmark(cfg, mechanic.create(cfg, metrics_store, distribution=True), metrics_store), cfg)


def benchmark_only(cfg):
    # We'll use a special car name for external benchmarks.
    cfg.add(config.Scope.benchmark, "benchmarks", "car", "external")
    metrics_store = metrics.metrics_store(cfg, read_only=False)
    return race(Benchmark(cfg, mechanic.create(cfg, metrics_store, external=True), metrics_store), cfg)


def docker(cfg):
    metrics_store = metrics.metrics_store(cfg, read_only=False)
    return race(Benchmark(cfg, mechanic.create(cfg, metrics_store, docker=True), metrics_store), cfg)


Pipeline("from-sources-complete",
         "Builds and provisions Elasticsearch, runs a benchmark and reports results.", from_sources_complete)

Pipeline("from-sources-skip-build",
         "Provisions Elasticsearch (skips the build), runs a benchmark and reports results.", from_sources_skip_build)

Pipeline("from-distribution",
         "Downloads an Elasticsearch distribution, provisions it, runs a benchmark and reports results.", from_distribution)

Pipeline("benchmark-only",
         "Assumes an already running Elasticsearch instance, runs a benchmark and reports results", benchmark_only)

# Very experimental Docker pipeline. Should only be used with great care and is also not supported on all platforms.
Pipeline("docker",
         "Runs a benchmark against the official Elasticsearch Docker container and reports results", docker, stable=False)


def available_pipelines():
    return [[pipeline.name, pipeline.description] for pipeline in pipelines.values() if pipeline.stable]


def list_pipelines():
    console.println("Available pipelines:\n")
    console.println(tabulate.tabulate(available_pipelines(), headers=["Name", "Description"]))


def run(cfg):
    name = cfg.opts("system", "pipeline")
    if len(name) == 0:
        distribution_version = cfg.opts("source", "distribution.version")
        if len(distribution_version) > 0:
            name = "from-distribution"
        else:
            name = "from-sources-complete"
        logger.info("User specified no pipeline. Automatically derived pipeline [%s]." % name)
    else:
        logger.info("User specified pipeline [%s]." % name)

    try:
        pipeline = pipelines[name]
    except KeyError:
        raise exceptions.SystemSetupError(
            "Unknown pipeline [%s]. List the available pipelines with %s list pipelines." % (name, PROGRAM_NAME))
    try:
        pipeline(cfg)
    except exceptions.RallyError as e:
        # just pass on our own errors. It should be treated differently on top-level
        raise e
    except BaseException:
        tb = sys.exc_info()[2]
        raise exceptions.RallyError("This race ended with a fatal crash.").with_traceback(tb)
