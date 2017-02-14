import collections
import logging
import sys

import tabulate
from esrally import actor, config, exceptions, track, driver, reporter, metrics, time, PROGRAM_NAME
from esrally.mechanic import mechanic
from esrally.utils import console, convert

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

    def __call__(self, cfg):
        self.target(cfg)


class Benchmark:
    def __init__(self, cfg, sources=False, build=False, distribution=False, external=False, docker=False):
        self.cfg = cfg
        self.track = track.load_track(self.cfg)
        self.metrics_store = metrics.metrics_store(
            self.cfg,
            track=self.track.name,
            challenge=self.track.find_challenge_or_default(self.cfg.opts("track", "challenge.name")).name,
            read_only=False
        )
        self.race_store = metrics.race_store(self.cfg)
        self.sources = sources
        self.build = build
        self.distribution = distribution
        self.external = external
        self.docker = docker
        self.actor_system = None
        self.mechanic = None

    def setup(self):
        # at this point an actor system has to run and we should only join
        self.actor_system = actor.bootstrap_actor_system(try_join=True)
        self.mechanic = self.actor_system.createActor(mechanic.MechanicActor,
                                                      targetActorRequirements={"coordinator": True},
                                                      globalName="/rally/mechanic/coordinator")
        logger.info("Asking mechanic to start the engine.")
        result = self.actor_system.ask(self.mechanic,
                                       mechanic.StartEngine(
                                           self.cfg, self.metrics_store.open_context,
                                           self.sources, self.build, self.distribution, self.external, self.docker))
        if isinstance(result, mechanic.EngineStarted):
            logger.info("Mechanic has started engine successfully.")
            self.metrics_store.meta_info = result.system_meta_info
            cluster = result.cluster_meta_info
            self.race_store.store_race(self.track, cluster.hosts, cluster.revision, cluster.distribution_version)
            console.info("Racing on track [%s], challenge [%s] and car [%s]" %
                         (self.track,
                          self.track.find_challenge_or_default(self.cfg.opts("track", "challenge.name")),
                          self.cfg.opts("mechanic", "car.name")))
            # just ensure it is optically separated
            console.println("")
        elif isinstance(result, mechanic.Failure):
            logger.info("Starting engine has failed. Reason [%s]." % result.message)
            raise exceptions.RallyError(result.message)
        else:
            raise exceptions.RallyError("Mechanic has not started engine but instead [%s]. Terminating race without result." % str(result))

    def run(self, lap):
        """
        Runs the provided lap of a benchmark.

        :param lap: The current lap number.
        :return: True iff the benchmark may go on. False iff the user has cancelled the benchmark.
        """
        self.metrics_store.lap = lap
        logger.info("Notifying mechanic of benchmark start.")
        # we could use #tell() here but then the ask call to driver below will fail because it returns the response that mechanic
        # sends (see http://godaddy.github.io/Thespian/doc/using.html#sec-6-6-1).
        self.actor_system.ask(self.mechanic, mechanic.OnBenchmarkStart(lap))
        logger.info("Asking driver to start benchmark.")
        main_driver = self.actor_system.createActor(driver.Driver,
                                                    targetActorRequirements={"coordinator": True},
                                                    globalName="/rally/driver/coordinator")
        try:
            result = self.actor_system.ask(main_driver,
                                           driver.StartBenchmark(self.cfg, self.track, self.metrics_store.meta_info, lap))
        except KeyboardInterrupt:
            result = self.actor_system.ask(main_driver, driver.BenchmarkCancelled())
            logger.info("User has cancelled the benchmark.")

        if isinstance(result, driver.BenchmarkComplete):
            logger.info("Benchmark is complete.")
            logger.info("Bulk adding request metrics to metrics store.")
            self.metrics_store.bulk_add(result.metrics)
            stop_result = self.actor_system.ask(self.mechanic, mechanic.OnBenchmarkStop())
            if isinstance(stop_result, mechanic.BenchmarkStopped):
                logger.info("Bulk adding system metrics to metrics store.")
                self.metrics_store.bulk_add(stop_result.system_metrics)
            else:
                raise exceptions.RallyError("Mechanic has returned no metrics but instead [%s]. Terminating race without result." %
                                            str(stop_result))

            logger.info("Flushing metrics data...")
            self.metrics_store.flush()
            logger.info("Flushing done")
        elif isinstance(result, driver.BenchmarkCancelled):
            logger.info("User has cancelled the benchmark.")
            return False
        elif isinstance(result, driver.BenchmarkFailure):
            logger.info("Driver has reported a benchmark failure.")
            raise exceptions.RallyError(result.message, result.cause)
        else:
            raise exceptions.RallyError("Driver has returned no metrics but instead [%s]. Terminating race without result." % str(result))
        return True

    def teardown(self, cancelled=False):
        logger.info("Asking mechanic to stop the engine.")
        result = self.actor_system.ask(self.mechanic, mechanic.StopEngine())
        if isinstance(result, mechanic.EngineStopped):
            logger.info("Mechanic has stopped engine successfully.")
            logger.info("Bulk adding system metrics to metrics store.")
            self.metrics_store.bulk_add(result.system_metrics)
        elif isinstance(result, mechanic.Failure):
            logger.info("Stopping engine has failed. Reason [%s]." % result.message)
            raise exceptions.RallyError(result.message, result.cause)
        else:
            raise exceptions.RallyError("Mechanic has not stopped engine but instead [%s]. Terminating race without result." % str(result))

        self.metrics_store.flush()
        if not cancelled:
            reporter.summarize(self.race_store, self.metrics_store, self.cfg, self.track)
        self.metrics_store.close()


class LapCounter:
    def __init__(self, race_store, metrics_store, track, laps, cfg):
        self.race_store = race_store
        self.metrics_store = metrics_store
        self.track = track
        self.laps = laps
        self.cfg = cfg
        self.lap_timer = time.Clock.stop_watch()
        self.lap_timer.start()
        self.lap_times = 0

    def before_lap(self, lap):
        logger.info("Starting lap [%d/%d]" % (lap, self.laps))
        if self.laps > 1:
            msg = "Lap [%d/%d]" % (lap, self.laps)
            console.println(console.format.bold(msg))
            console.println(console.format.underline_for(msg))

    def after_lap(self, lap):
        logger.info("Finished lap [%d/%d]" % (lap, self.laps))
        if self.laps > 1:
            lap_time = self.lap_timer.split_time() - self.lap_times
            self.lap_times += lap_time
            hl, ml, sl = convert.seconds_to_hour_minute_seconds(lap_time)
            reporter.summarize(self.race_store, self.metrics_store, self.cfg, track=self.track, lap=lap)
            console.println("")
            if lap < self.laps:
                remaining = (self.laps - lap) * self.lap_times / lap
                hr, mr, sr = convert.seconds_to_hour_minute_seconds(remaining)
                console.info("Lap time %02d:%02d:%02d (ETA: %02d:%02d:%02d)" % (hl, ml, sl, hr, mr, sr), logger=logger)
            else:
                console.info("Lap time %02d:%02d:%02d" % (hl, ml, sl), logger=logger)
            console.println("")


def race(benchmark):
    cfg = benchmark.cfg
    laps = cfg.opts("race", "laps")
    benchmark.setup()
    lap_counter = LapCounter(benchmark.race_store, benchmark.metrics_store, benchmark.track, laps, cfg)
    cancelled = False
    try:
        for lap in range(1, laps + 1):
            lap_counter.before_lap(lap)
            may_continue = benchmark.run(lap)
            if may_continue:
                lap_counter.after_lap(lap)
            else:
                cancelled = True
                # Early termination due to cancellation by the user
                break
    finally:
        benchmark.teardown(cancelled)


def set_default_hosts(cfg, host="127.0.0.1", port=9200):
    configured_hosts = cfg.opts("client", "hosts", mandatory=False)
    if configured_hosts:
        logger.info("Using configured hosts %s" % configured_hosts)
    else:
        logger.info("Setting default host to [%s:%d]" % (host, port))
        cfg.add(config.Scope.benchmark, "client", "hosts", [{"host": host, "port": port}])


# Poor man's curry
def from_sources_complete(cfg):
    port = cfg.opts("provisioning", "node.http.port")
    set_default_hosts(cfg, port=port)
    return race(Benchmark(cfg, sources=True, build=True))


def from_sources_skip_build(cfg):
    port = cfg.opts("provisioning", "node.http.port")
    set_default_hosts(cfg, port=port)
    return race(Benchmark(cfg, sources=True, build=False))


def from_distribution(cfg):
    port = cfg.opts("provisioning", "node.http.port")
    set_default_hosts(cfg, port=port)
    return race(Benchmark(cfg, distribution=True))


def benchmark_only(cfg):
    set_default_hosts(cfg)
    # We'll use a special car name for external benchmarks.
    cfg.add(config.Scope.benchmark, "mechanic", "car.name", "external")
    return race(Benchmark(cfg, external=True))


def docker(cfg):
    set_default_hosts(cfg)
    return race(Benchmark(cfg, docker=True))


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
    name = cfg.opts("race", "pipeline")
    if len(name) == 0:
        distribution_version = cfg.opts("mechanic", "distribution.version")
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
    except KeyboardInterrupt:
        logger.info("User has cancelled the benchmark.")
    except BaseException:
        tb = sys.exc_info()[2]
        raise exceptions.RallyError("This race ended with a fatal crash.").with_traceback(tb)
