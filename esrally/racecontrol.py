# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import collections
import logging
import os
import sys

import tabulate
import thespian.actors

from esrally import (
    PROGRAM_NAME,
    actor,
    config,
    doc_link,
    driver,
    exceptions,
    mechanic,
    metrics,
    reporter,
    track,
    version,
)
from esrally.utils import console, opts, versions

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
    def __init__(self, cfg, sources=False, distribution=False, external=False, docker=False):
        self.cfg = cfg
        self.sources = sources
        self.distribution = distribution
        self.external = external
        self.docker = docker


class Success:
    pass


class BenchmarkActor(actor.RallyActor):
    def __init__(self):
        super().__init__()
        self.cfg = None
        self.start_sender = None
        self.mechanic = None
        self.main_driver = None
        self.coordinator = None

    def receiveMsg_PoisonMessage(self, msg, sender):
        self.logger.debug("BenchmarkActor got notified of poison message [%s] (forwarding).", (str(msg)))
        if self.coordinator:
            self.coordinator.error = True
        self.send(self.start_sender, msg)

    def receiveUnrecognizedMessage(self, msg, sender):
        self.logger.debug("BenchmarkActor received unknown message [%s] (ignoring).", (str(msg)))

    @actor.no_retry("race control")  # pylint: disable=no-value-for-parameter
    def receiveMsg_Setup(self, msg, sender):
        self.start_sender = sender
        self.cfg = msg.cfg
        self.coordinator = BenchmarkCoordinator(msg.cfg)
        self.coordinator.setup(sources=msg.sources)
        self.logger.info("Asking mechanic to start the engine.")
        self.mechanic = self.createActor(mechanic.MechanicActor, targetActorRequirements={"coordinator": True})
        self.send(
            self.mechanic,
            mechanic.StartEngine(
                self.cfg, self.coordinator.metrics_store.open_context, msg.sources, msg.distribution, msg.external, msg.docker
            ),
        )

    @actor.no_retry("race control")  # pylint: disable=no-value-for-parameter
    def receiveMsg_EngineStarted(self, msg, sender):
        self.logger.info("Mechanic has started engine successfully.")
        self.coordinator.race.team_revision = msg.team_revision
        self.main_driver = self.createActor(driver.DriverActor, targetActorRequirements={"coordinator": True})
        self.logger.info("Telling driver to prepare for benchmarking.")
        self.send(self.main_driver, driver.PrepareBenchmark(self.cfg, self.coordinator.current_track))

    @actor.no_retry("race control")  # pylint: disable=no-value-for-parameter
    def receiveMsg_PreparationComplete(self, msg, sender):
        self.coordinator.on_preparation_complete(msg.distribution_flavor, msg.distribution_version, msg.revision)
        self.logger.info("Telling driver to start benchmark.")
        self.send(self.main_driver, driver.StartBenchmark())

    @actor.no_retry("race control")  # pylint: disable=no-value-for-parameter
    def receiveMsg_TaskFinished(self, msg, sender):
        self.coordinator.on_task_finished(msg.metrics)
        # We choose *NOT* to reset our own metrics store's timer as this one is only used to collect complete metrics records from
        # other stores (used by driver and mechanic). Hence there is no need to reset the timer in our own metrics store.
        self.send(self.mechanic, mechanic.ResetRelativeTime(msg.next_task_scheduled_in))

    @actor.no_retry("race control")  # pylint: disable=no-value-for-parameter
    def receiveMsg_BenchmarkCancelled(self, msg, sender):
        self.coordinator.cancelled = True
        # even notify the start sender if it is the originator. The reason is that we call #ask() which waits for a reply.
        # We also need to ask in order to avoid races between this notification and the following ActorExitRequest.
        self.send(self.start_sender, msg)

    @actor.no_retry("race control")  # pylint: disable=no-value-for-parameter
    def receiveMsg_BenchmarkFailure(self, msg, sender):
        self.logger.info("Received a benchmark failure from [%s] and will forward it now.", sender)
        self.coordinator.error = True
        self.send(self.start_sender, msg)

    @actor.no_retry("race control")  # pylint: disable=no-value-for-parameter
    def receiveMsg_BenchmarkComplete(self, msg, sender):
        self.coordinator.on_benchmark_complete(msg.metrics)
        self.send(self.main_driver, thespian.actors.ActorExitRequest())
        self.main_driver = None
        self.logger.info("Asking mechanic to stop the engine.")
        self.send(self.mechanic, mechanic.StopEngine())

    @actor.no_retry("race control")  # pylint: disable=no-value-for-parameter
    def receiveMsg_EngineStopped(self, msg, sender):
        self.logger.info("Mechanic has stopped engine successfully.")
        self.send(self.start_sender, Success())


class BenchmarkCoordinator:
    def __init__(self, cfg):
        self.logger = logging.getLogger(__name__)
        self.cfg = cfg
        self.race = None
        self.metrics_store = None
        self.race_store = None
        self.cancelled = False
        self.error = False
        self.track_revision = None
        self.current_track = None
        self.current_challenge = None

    def setup(self, sources=False):
        # to load the track we need to know the correct cluster distribution version. Usually, this value should be set
        # but there are rare cases (external pipeline and user did not specify the distribution version) where we need
        # to derive it ourselves. For source builds we always assume "main"
        if not sources and not self.cfg.exists("mechanic", "distribution.version"):
            distribution_version = mechanic.cluster_distribution_version(self.cfg)
            self.logger.info("Automatically derived distribution version [%s]", distribution_version)
            self.cfg.add(config.Scope.benchmark, "mechanic", "distribution.version", distribution_version)
            min_es_version = versions.Version.from_string(version.minimum_es_version())
            specified_version = versions.Version.from_string(distribution_version)
            if specified_version < min_es_version:
                raise exceptions.SystemSetupError(f"Cluster version must be at least [{min_es_version}] but was [{distribution_version}]")

        self.current_track = track.load_track(self.cfg, install_dependencies=True)
        self.track_revision = self.cfg.opts("track", "repository.revision", mandatory=False)
        challenge_name = self.cfg.opts("track", "challenge.name")
        self.current_challenge = self.current_track.find_challenge_or_default(challenge_name)
        if self.current_challenge is None:
            raise exceptions.SystemSetupError(
                "Track [{}] does not provide challenge [{}]. List the available tracks with {} list tracks.".format(
                    self.current_track.name, challenge_name, PROGRAM_NAME
                )
            )
        if self.current_challenge.user_info:
            console.info(self.current_challenge.user_info)
        self.race = metrics.create_race(self.cfg, self.current_track, self.current_challenge, self.track_revision)

        self.metrics_store = metrics.metrics_store(
            self.cfg, track=self.race.track_name, challenge=self.race.challenge_name, read_only=False
        )
        self.race_store = metrics.race_store(self.cfg)

    def on_preparation_complete(self, distribution_flavor, distribution_version, revision):
        self.race.distribution_flavor = distribution_flavor
        self.race.distribution_version = distribution_version
        self.race.revision = revision
        # store race initially (without any results) so other components can retrieve full metadata
        self.race_store.store_race(self.race)
        if self.race.challenge.auto_generated:
            console.info(
                "Racing on track [{}] and car {} with version [{}].\n".format(
                    self.race.track_name, self.race.car, self.race.distribution_version
                )
            )
        else:
            console.info(
                "Racing on track [{}], challenge [{}] and car {} with version [{}].\n".format(
                    self.race.track_name, self.race.challenge_name, self.race.car, self.race.distribution_version
                )
            )

    def on_task_finished(self, new_metrics):
        self.logger.info("Bulk adding request metrics to metrics store.")
        self.metrics_store.bulk_add(new_metrics)

    def on_benchmark_complete(self, new_metrics):
        self.logger.info("Benchmark is complete.")
        self.logger.info("Bulk adding request metrics to metrics store.")
        self.metrics_store.bulk_add(new_metrics)
        self.metrics_store.flush()
        if not self.cancelled and not self.error:
            final_results = metrics.calculate_results(self.metrics_store, self.race)
            self.race.add_results(final_results)
            self.race_store.store_race(self.race)
            metrics.results_store(self.cfg).store_results(self.race)
            reporter.summarize(final_results, self.cfg)
        else:
            self.logger.info("Suppressing output of summary report. Cancelled = [%r], Error = [%r].", self.cancelled, self.error)
        self.metrics_store.close()


def race(cfg, sources=False, distribution=False, external=False, docker=False):
    logger = logging.getLogger(__name__)
    # at this point an actor system has to run and we should only join
    actor_system = actor.bootstrap_actor_system(try_join=True)
    benchmark_actor = actor_system.createActor(BenchmarkActor, targetActorRequirements={"coordinator": True})
    try:
        result = actor_system.ask(benchmark_actor, Setup(cfg, sources, distribution, external, docker))
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
        raise exceptions.UserInterrupted("User has cancelled the benchmark (detected by race control).") from None
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
        default_host_object = opts.TargetHosts(f"{host}:{port}")
        cfg.add(config.Scope.benchmark, "client", "hosts", default_host_object)


# Poor man's curry
def from_sources(cfg):
    port = cfg.opts("provisioning", "node.http.port")
    set_default_hosts(cfg, port=port)
    return race(cfg, sources=True)


def from_distribution(cfg):
    port = cfg.opts("provisioning", "node.http.port")
    set_default_hosts(cfg, port=port)
    return race(cfg, distribution=True)


def benchmark_only(cfg):
    set_default_hosts(cfg)
    # We'll use a special car name for external benchmarks.
    cfg.add(config.Scope.benchmark, "mechanic", "car.names", ["external"])
    return race(cfg, external=True)


def docker(cfg):
    set_default_hosts(cfg)
    return race(cfg, docker=True)


Pipeline("from-sources", "Builds and provisions Elasticsearch, runs a benchmark and reports results.", from_sources)

Pipeline(
    "from-distribution", "Downloads an Elasticsearch distribution, provisions it, runs a benchmark and reports results.", from_distribution
)

Pipeline("benchmark-only", "Assumes an already running Elasticsearch instance, runs a benchmark and reports results", benchmark_only)

# Very experimental Docker pipeline. Should only be used with great care and is also not supported on all platforms.
Pipeline("docker", "Runs a benchmark against the official Elasticsearch Docker container and reports results", docker, stable=False)


def available_pipelines():
    return [[pipeline.name, pipeline.description] for pipeline in pipelines.values() if pipeline.stable]


def list_pipelines():
    console.println("Available pipelines:\n")
    console.println(tabulate.tabulate(available_pipelines(), headers=["Name", "Description"]))


def run(cfg):
    logger = logging.getLogger(__name__)
    name = cfg.opts("race", "pipeline")
    race_id = cfg.opts("system", "race.id")
    console.info(f"Race id is [{race_id}]", logger=logger)
    if len(name) == 0:
        # assume from-distribution pipeline if distribution.version has been specified and --pipeline cli arg not set
        if cfg.exists("mechanic", "distribution.version"):
            name = "from-distribution"
        else:
            name = "from-sources"
        logger.info("User specified no pipeline. Automatically derived pipeline [%s].", name)
        cfg.add(config.Scope.applicationOverride, "race", "pipeline", name)
    else:
        logger.info("User specified pipeline [%s].", name)

    if os.environ.get("RALLY_RUNNING_IN_DOCKER", "").upper() == "TRUE":
        # in this case only benchmarking remote Elasticsearch clusters makes sense
        if name != "benchmark-only":
            raise exceptions.SystemSetupError(
                "Only the [benchmark-only] pipeline is supported by the Rally Docker image.\n"
                "Add --pipeline=benchmark-only in your Rally arguments and try again.\n"
                "For more details read the docs for the benchmark-only pipeline in {}\n".format(doc_link("pipelines.html#benchmark-only"))
            )

    try:
        pipeline = pipelines[name]
    except KeyError:
        raise exceptions.SystemSetupError(
            "Unknown pipeline [%s]. List the available pipelines with %s list pipelines." % (name, PROGRAM_NAME)
        )
    try:
        pipeline(cfg)
    except exceptions.RallyError as e:
        # just pass on our own errors. It should be treated differently on top-level
        raise e
    except KeyboardInterrupt:
        logger.info("User has cancelled the benchmark.")
        raise exceptions.UserInterrupted("User has cancelled the benchmark (detected by race control).") from None
    except BaseException:
        tb = sys.exc_info()[2]
        raise exceptions.RallyError("This race ended with a fatal crash.").with_traceback(tb)
