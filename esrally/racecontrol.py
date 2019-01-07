# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import collections
import logging
import sys

import tabulate
import thespian.actors

from esrally import actor, config, exceptions, track, driver, mechanic, reporter, metrics, time, DOC_LINK, PROGRAM_NAME
from esrally.utils import console, convert, opts

# benchmarks with external candidates are really scary and we should warn users.
BOGUS_RESULTS_WARNING = """
************************************************************************
************** WARNING: A dark dungeon lies ahead of you  **************
************************************************************************

Rally does not have control over the configuration of the benchmarked
Elasticsearch cluster.

Be aware that results may be misleading due to problems with the setup.
Rally is also not able to gather lots of metrics at all (like CPU usage
of the benchmarked cluster) or may even produce misleading metrics (like
the index size).

************************************************************************
****** Use this pipeline only if you are aware of the tradeoffs.  ******
*************************** Watch your step! ***************************
************************************************************************
"""

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


class Setup:
    def __init__(self, cfg, sources=False, build=False, distribution=False, external=False, docker=False):
        self.cfg = cfg
        self.sources = sources
        self.build = build
        self.distribution = distribution
        self.external = external
        self.docker = docker


class Success:
    pass


class BenchmarkActor(actor.RallyActor):
    def __init__(self):
        super().__init__()
        self.cfg = None
        self.race = None
        self.metrics_store = None
        self.race_store = None
        self.lap_counter = None
        self.cancelled = False
        self.error = False
        self.start_sender = None
        self.mechanic = None
        self.main_driver = None

    def receiveMsg_PoisonMessage(self, msg, sender):
        self.logger.info("BenchmarkActor got notified of poison message [%s] (forwarding).", (str(msg)))
        self.error = True
        self.send(self.start_sender, msg)

    def receiveUnrecognizedMessage(self, msg, sender):
        self.logger.info("BenchmarkActor received unknown message [%s] (ignoring).", (str(msg)))

    @actor.no_retry("race control")
    def receiveMsg_Setup(self, msg, sender):
        self.setup(msg, sender)

    @actor.no_retry("race control")
    def receiveMsg_EngineStarted(self, msg, sender):
        self.logger.info("Mechanic has started engine successfully.")
        self.metrics_store.meta_info = msg.system_meta_info
        cluster = msg.cluster_meta_info
        self.race.cluster = cluster
        if self.race.challenge.auto_generated:
            console.info("Racing on track [{}] and car {} with version [{}].\n"
                         .format(self.race.track_name, self.race.car, self.race.cluster.distribution_version))
        else:
            console.info("Racing on track [{}], challenge [{}] and car {} with version [{}].\n"
                         .format(self.race.track_name, self.race.challenge_name, self.race.car, self.race.cluster.distribution_version))
        # start running we assume that each race has at least one lap
        self.run()

    @actor.no_retry("race control")
    def receiveMsg_TaskFinished(self, msg, sender):
        self.logger.info("Task has finished.")
        self.logger.info("Bulk adding request metrics to metrics store.")
        self.metrics_store.bulk_add(msg.metrics)
        # We choose *NOT* to reset our own metrics store's timer as this one is only used to collect complete metrics records from
        # other stores (used by driver and mechanic). Hence there is no need to reset the timer in our own metrics store.
        self.send(self.mechanic, mechanic.ResetRelativeTime(msg.next_task_scheduled_in))

    @actor.no_retry("race control")
    def receiveMsg_BenchmarkCancelled(self, msg, sender):
        self.cancelled = True
        # even notify the start sender if it is the originator. The reason is that we call #ask() which waits for a reply.
        # We also need to ask in order to avoid races between this notification and the following ActorExitRequest.
        self.send(self.start_sender, msg)

    @actor.no_retry("race control")
    def receiveMsg_BenchmarkFailure(self, msg, sender):
        self.logger.info("Received a benchmark failure from [%s] and will forward it now.", sender)
        self.error = True
        self.send(self.start_sender, msg)

    @actor.no_retry("race control")
    def receiveMsg_BenchmarkComplete(self, msg, sender):
        self.logger.info("Benchmark is complete.")
        self.logger.info("Bulk adding request metrics to metrics store.")
        self.metrics_store.bulk_add(msg.metrics)
        self.send(self.main_driver, thespian.actors.ActorExitRequest())
        self.main_driver = None
        self.send(self.mechanic, mechanic.OnBenchmarkStop())

    @actor.no_retry("race control")
    def receiveMsg_BenchmarkStopped(self, msg, sender):
        self.logger.info("Bulk adding system metrics to metrics store.")
        self.metrics_store.bulk_add(msg.system_metrics)
        self.logger.debug("Flushing metrics data...")
        self.metrics_store.flush()
        self.logger.debug("Flushing done")
        self.lap_counter.after_lap()
        if self.lap_counter.has_more_laps():
            self.run()
        else:
            self.teardown()

    @actor.no_retry("race control")
    def receiveMsg_EngineStopped(self, msg, sender):
        self.logger.info("Mechanic has stopped engine successfully.")
        self.logger.info("Bulk adding system metrics to metrics store.")
        self.metrics_store.bulk_add(msg.system_metrics)
        self.metrics_store.flush()
        if not self.cancelled and not self.error:
            final_results = reporter.calculate_results(self.metrics_store, self.race)
            self.race.add_final_results(final_results)
            reporter.summarize(self.race, self.cfg)
            self.race_store.store_race(self.race)
        else:
            self.logger.info("Suppressing output of summary report. Cancelled = [%r], Error = [%r].", self.cancelled, self.error)
        self.metrics_store.close()
        self.send(self.start_sender, Success())

    def setup(self, msg, sender):
        self.start_sender = sender
        self.cfg = msg.cfg
        # to load the track we need to know the correct cluster distribution version. Usually, this value should be set but there are rare
        # cases (external pipeline and user did not specify the distribution version) where we need to derive it ourselves. For source
        # builds we always assume "master"
        if not msg.sources and not self.cfg.exists("mechanic", "distribution.version"):
            distribution_version = mechanic.cluster_distribution_version(self.cfg)
            if not distribution_version:
                raise exceptions.SystemSetupError("A distribution version is required. Please specify it with --distribution-version.")
            self.logger.info("Automatically derived distribution version [%s]", distribution_version)
            self.cfg.add(config.Scope.benchmark, "mechanic", "distribution.version", distribution_version)

        t = track.load_track(self.cfg)
        challenge_name = self.cfg.opts("track", "challenge.name")
        challenge = t.find_challenge_or_default(challenge_name)
        if challenge is None:
            raise exceptions.SystemSetupError("Track [%s] does not provide challenge [%s]. List the available tracks with %s list tracks."
                                              % (t.name, challenge_name, PROGRAM_NAME))
        if challenge.user_info:
            console.info(challenge.user_info)
        self.race = metrics.create_race(self.cfg, t, challenge)

        self.metrics_store = metrics.metrics_store(
            self.cfg,
            track=self.race.track_name,
            challenge=self.race.challenge_name,
            read_only=False
        )
        self.lap_counter = LapCounter(self.race, self.metrics_store, self.cfg)
        self.race_store = metrics.race_store(self.cfg)
        self.logger.info("Asking mechanic to start the engine.")
        cluster_settings = challenge.cluster_settings
        self.mechanic = self.createActor(mechanic.MechanicActor, targetActorRequirements={"coordinator": True})
        self.send(self.mechanic, mechanic.StartEngine(self.cfg, self.metrics_store.open_context, cluster_settings, msg.sources, msg.build,
                                                      msg.distribution, msg.external, msg.docker))

    def run(self):
        self.lap_counter.before_lap()
        lap = self.lap_counter.current_lap
        self.metrics_store.lap = lap
        self.logger.info("Telling mechanic of benchmark start.")
        self.send(self.mechanic, mechanic.OnBenchmarkStart(lap))
        self.main_driver = self.createActor(driver.DriverActor, targetActorRequirements={"coordinator": True})
        self.logger.info("Telling driver to start benchmark.")
        self.send(self.main_driver, driver.StartBenchmark(self.cfg, self.race.track, self.metrics_store.meta_info, lap))

    def teardown(self):
        self.logger.info("Asking mechanic to stop the engine.")
        self.send(self.mechanic, mechanic.StopEngine())


class LapCounter:
    def __init__(self, current_race, metrics_store, cfg):
        self.race = current_race
        self.metrics_store = metrics_store
        self.cfg = cfg
        self.lap_timer = time.Clock.stop_watch()
        self.lap_timer.start()
        self.lap_times = 0
        self.current_lap = 0
        self.logger = logging.getLogger(__name__)

    def has_more_laps(self):
        return self.current_lap < self.race.total_laps

    def before_lap(self):
        self.current_lap += 1
        self.logger.info("Starting lap [%d/%d]", self.current_lap, self.race.total_laps)
        if self.race.total_laps > 1:
            msg = "Lap [%d/%d]" % (self.current_lap, self.race.total_laps)
            console.println(console.format.bold(msg))
            console.println(console.format.underline_for(msg))

    def after_lap(self):
        self.logger.info("Finished lap [%d/%d]", self.current_lap, self.race.total_laps)
        if self.race.total_laps > 1:
            lap_time = self.lap_timer.split_time() - self.lap_times
            self.lap_times += lap_time
            hl, ml, sl = convert.seconds_to_hour_minute_seconds(lap_time)
            lap_results = reporter.calculate_results(self.metrics_store, self.race, self.current_lap)
            self.race.add_lap_results(lap_results)
            reporter.summarize(self.race, self.cfg, lap=self.current_lap)
            console.println("")
            if self.current_lap < self.race.total_laps:
                remaining = (self.race.total_laps - self.current_lap) * self.lap_times / self.current_lap
                hr, mr, sr = convert.seconds_to_hour_minute_seconds(remaining)
                console.info("Lap time %02d:%02d:%02d (ETA: %02d:%02d:%02d)" % (hl, ml, sl, hr, mr, sr), logger=self.logger)
            else:
                console.info("Lap time %02d:%02d:%02d" % (hl, ml, sl), logger=self.logger)
            console.println("")


def race(cfg, sources=False, build=False, distribution=False, external=False, docker=False):
    logger = logging.getLogger(__name__)
    # at this point an actor system has to run and we should only join
    actor_system = actor.bootstrap_actor_system(try_join=True)
    benchmark_actor = actor_system.createActor(BenchmarkActor, targetActorRequirements={"coordinator": True})
    try:
        result = actor_system.ask(benchmark_actor, Setup(cfg, sources, build, distribution, external, docker))
        if isinstance(result, Success):
            logger.info("Benchmark has finished successfully.")
        # may happen if one of the load generators has detected that the user has cancelled the benchmark.
        elif isinstance(result, actor.BenchmarkCancelled):
            logger.info("User has cancelled the benchmark (detected by actor).")
        elif isinstance(result, actor.BenchmarkFailure):
            logger.error("A benchmark failure has occurred")
            raise exceptions.RallyError(result.message, result.cause)
        else:
            raise exceptions.RallyError("Got an unexpected result during benchmarking: [%s]." % str(result))
    except KeyboardInterrupt:
        logger.info("User has cancelled the benchmark (detected by race control).")
        # notify the coordinator so it can properly handle this state. Do it blocking so we don't have a race between this message
        # and the actor exit request.
        actor_system.ask(benchmark_actor, actor.BenchmarkCancelled())
    finally:
        logger.info("Telling benchmark actor to exit.")
        actor_system.tell(benchmark_actor, thespian.actors.ActorExitRequest())


def set_default_hosts(cfg, host="127.0.0.1", port=9200):
    logger = logging.getLogger(__name__)
    configured_hosts = cfg.opts("client", "hosts")
    if len(configured_hosts.default) != 0:
        logger.info("Using configured hosts %s", configured_hosts.default)
    else:
        logger.info("Setting default host to [%s:%d]", host, port)
        default_host_object = opts.TargetHosts("{}:{}".format(host,port))
        cfg.add(config.Scope.benchmark, "client", "hosts", default_host_object)


# Poor man's curry
def from_sources_complete(cfg):
    port = cfg.opts("provisioning", "node.http.port")
    set_default_hosts(cfg, port=port)
    return race(cfg, sources=True, build=True)


def from_sources_skip_build(cfg):
    port = cfg.opts("provisioning", "node.http.port")
    set_default_hosts(cfg, port=port)
    return race(cfg, sources=True, build=False)


def from_distribution(cfg):
    port = cfg.opts("provisioning", "node.http.port")
    set_default_hosts(cfg, port=port)
    return race(cfg, distribution=True)


def benchmark_only(cfg):
    console.println(BOGUS_RESULTS_WARNING)
    set_default_hosts(cfg)
    # We'll use a special car name for external benchmarks.
    cfg.add(config.Scope.benchmark, "mechanic", "car.names", ["external"])
    return race(cfg, external=True)


def docker(cfg):
    set_default_hosts(cfg)
    return race(cfg, docker=True)


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
    logger = logging.getLogger(__name__)
    name = cfg.opts("race", "pipeline")

    if len(name) == 0:
        # assume from-distribution pipeline if distribution.version has been specified and --pipeline cli arg not set
        if cfg.exists("mechanic", "distribution.version"):
            name = "from-distribution"
        else:
            name = "from-sources-complete"
        logger.info("User specified no pipeline. Automatically derived pipeline [%s].", name)
        cfg.add(config.Scope.applicationOverride, "race", "pipeline", name)
    else:
        if (cfg.exists("mechanic", "distribution.version") and
                name in ["from-sources-complete", "from-sources-skip-build", "benchmark-only"]):
            raise exceptions.SystemSetupError(
                "--distribution-version can only be used together with pipeline from-distribution, """
                "but you specified {}.\n"
                "If you intend to benchmark an externally provisioned cluster, don't specify --distribution-version otherwise\n"
                "please read the docs for from-distribution pipeline at "
                "{}/pipelines.html#from-distribution".format(name, DOC_LINK))
        logger.info("User specified pipeline [%s].", name)

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
