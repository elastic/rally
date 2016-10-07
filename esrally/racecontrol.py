import logging
import shutil
import sys

import tabulate
import thespian.actors
from esrally import config, driver, exceptions, paths, track, reporter, metrics, PROGRAM_NAME
from esrally.mechanic import mechanic
from esrally.utils import console, io

logger = logging.getLogger("rally.racecontrol")

pipelines = {}


class Pipeline:
    """
    Describes a whole execution pipeline. A pipeline can consist of one or more steps. Each pipeline should contain roughly of the following
    steps:

    * Prepare the benchmark candidate: It can build Elasticsearch from sources, download a ZIP from somewhere etc.
    * Launch the benchmark candidate: This can be done directly, with tools like Ansible or it can assume the candidate is already launched
    * Run the benchmark
    * Report results
    """

    def __init__(self, name, description, target):
        """
        Creates a new pipeline.

        :param name: A short name of the pipeline. This name will be used to reference it from the command line.
        :param description: A human-readable description what the pipeline does.
        :param target: A function that implements this pipeline
        """
        self.name = name
        self.description = description
        self.target = target
        pipelines[name] = self

    def __call__(self, cfg):
        self.target(cfg)


def sweep(cfg):
    invocation_root = cfg.opts("system", "invocation.root.dir")
    track_name = cfg.opts("benchmarks", "track")
    challenge_name = cfg.opts("benchmarks", "challenge")
    car_name = cfg.opts("benchmarks", "car")

    log_root = paths.Paths(cfg).log_root()
    archive_path = "%s/logs-%s-%s-%s.zip" % (invocation_root, track_name, challenge_name, car_name)
    io.compress(log_root, archive_path)
    console.println("\nLogs for this race are archived in %s" % archive_path)
    shutil.rmtree(log_root)


def benchmark(cfg, mechanic, metrics_store):
    track_name = cfg.opts("benchmarks", "track")
    challenge_name = cfg.opts("benchmarks", "challenge")
    selected_car_name = cfg.opts("benchmarks", "car")

    console.println("Racing on track [%s] and challenge [%s] with car [%s]" % (track_name, challenge_name, selected_car_name))

    mechanic.prepare_candidate()
    cluster = mechanic.start_engine()

    t = track.load_track(cfg)
    metrics.race_store(cfg).store_race(t)

    actors = thespian.actors.ActorSystem()
    main_driver = actors.createActor(driver.Driver)

    cluster.on_benchmark_start()
    result = actors.ask(main_driver, driver.StartBenchmark(cfg, t, metrics_store.meta_info))
    if isinstance(result, driver.BenchmarkComplete):
        cluster.on_benchmark_stop()
        metrics_store.bulk_add(result.metrics)
        mechanic.stop_engine(cluster)
        metrics_store.close()
        reporter.summarize(cfg, t)
        sweep(cfg)
    elif isinstance(result, driver.BenchmarkFailure):
        raise exceptions.RallyError(result.message, result.cause)
    else:
        raise exceptions.RallyError("Driver has returned no metrics but instead [%s]. Terminating race without result." % str(result))


# Poor man's curry
def from_sources_complete(cfg):
    metrics_store = metrics.metrics_store(cfg, read_only=False)
    return benchmark(cfg, mechanic.create(cfg, metrics_store, sources=True, build=True), metrics_store)


def from_sources_skip_build(cfg):
    metrics_store = metrics.metrics_store(cfg, read_only=False)
    return benchmark(cfg, mechanic.create(cfg, metrics_store, sources=True, build=False), metrics_store)


def from_distribution(cfg):
    metrics_store = metrics.metrics_store(cfg, read_only=False)
    return benchmark(cfg, mechanic.create(cfg, metrics_store, distribution=True), metrics_store)


def benchmark_only(cfg):
    # We'll use a special car name for external benchmarks.
    cfg.add(config.Scope.benchmark, "benchmarks", "car", "external")
    metrics_store = metrics.metrics_store(cfg, read_only=False)
    return benchmark(cfg, mechanic.create(cfg, metrics_store, external=True), metrics_store)


Pipeline("from-sources-complete",
         "Builds and provisions Elasticsearch, runs a benchmark and reports results.", from_sources_complete)

Pipeline("from-sources-skip-build",
         "Provisions Elasticsearch (skips the build), runs a benchmark and reports results.", from_sources_skip_build)

Pipeline("from-distribution",
         "Downloads an Elasticsearch distribution, provisions it, runs a benchmark and reports results.", from_distribution)

Pipeline("benchmark-only",
         "Assumes an already running Elasticsearch instance, runs a benchmark and reports results", benchmark_only)


def list_pipelines():
    console.println("Available pipelines:\n")
    console.println(
        tabulate.tabulate([[pipeline.name, pipeline.description] for pipeline in pipelines.values()], headers=["Name", "Description"]))


def run(cfg):
    name = cfg.opts("system", "pipeline")
    try:
        pipeline = pipelines[name]
    except KeyError:
        raise exceptions.SystemSetupError(
            "Unknown pipeline [%s]. You can list the available pipelines with %s list pipelines." % (name, PROGRAM_NAME))
    try:
        pipeline(cfg)
    except exceptions.RallyError as e:
        # just pass on our own errors. It should be treated differently on top-level
        raise e
    except BaseException:
        tb = sys.exc_info()[2]
        raise exceptions.RallyError("This race ended early with a fatal crash. For details please see the logs.").with_traceback(tb)
