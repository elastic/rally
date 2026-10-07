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

import asyncio
import collections
import logging
import os
import sys

import tabulate

from esrally import (
    PROGRAM_NAME,
    actor,
    client,
    config,
    doc_link,
    driver,
    exceptions,
    mechanic,
    metrics,
    reporter,
    track,
    types,
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
        :param stable True if the pipeline is considered production quality.
        """
        self.name = name
        self.description = description
        self.target = target
        self.stable = stable
        pipelines[name] = self

    def __call__(self, cfg: types.Config):
        self.target(cfg)


class BenchmarkCoordinator:
    def __init__(self, cfg: types.Config):
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
            hosts_cfg = self.cfg.opts("client", "hosts")
            options_cfg = self.cfg.opts("client", "options")
            hosts = hosts_cfg.default_or_first
            client_options = options_cfg.default_or_first
            (
                distribution_flavor,
                distribution_version,
                distribution_build_hash,
                serverless_operator,
            ) = client.factory.cluster_distribution_version(hosts, client_options)

            self.logger.info(
                "Automatically derived distribution flavor [%s], version [%s], and build hash [%s]",
                distribution_flavor,
                distribution_version,
                distribution_build_hash,
            )
            self.cfg.add(config.Scope.benchmark, "mechanic", "distribution.version", distribution_version)
            self.cfg.add(config.Scope.benchmark, "mechanic", "distribution.flavor", distribution_flavor)
            if versions.is_serverless(distribution_flavor):
                if not self.cfg.exists("driver", "serverless.mode"):
                    self.cfg.add(config.Scope.benchmark, "driver", "serverless.mode", True)

                if not self.cfg.exists("driver", "serverless.operator"):
                    self.cfg.add(config.Scope.benchmark, "driver", "serverless.operator", serverless_operator)
                console.info(f"Detected Elasticsearch Serverless mode with operator=[{serverless_operator}].")
            else:
                min_es_version = versions.Version.from_string(version.minimum_es_version())
                specified_version = versions.Version.from_string(distribution_version)
                if specified_version < min_es_version:
                    raise exceptions.SystemSetupError(
                        f"Cluster version must be at least [{min_es_version}] but was [{distribution_version}]"
                    )

        loaded_track = track.load_track(self.cfg, install_dependencies=True)
        self.current_track = loaded_track
        self.track_revision = self.cfg.opts("track", "repository.revision", mandatory=False)
        # Resolve the challenge and validate track parameters before provisioning the engine so that
        # invalid parameters fail fast. Track plugins were loaded (and any validators registered)
        # during load_track above. Shared with ``esrally validate-track``.
        self.current_challenge = track.resolve_challenge_and_invoke_validators(loaded_track, self.cfg)
        if self.current_challenge.user_info:
            console.info(self.current_challenge.user_info)
        for message in self.current_challenge.serverless_info:
            console.info(message)
        self.race = metrics.create_race(self.cfg, self.current_track, self.current_challenge, self.track_revision)

        self.metrics_store = metrics.metrics_store(
            self.cfg, track=self.race.track_name, challenge=self.race.challenge_name, read_only=False
        )
        self.race_store = metrics.race_store(self.cfg)

    def on_preparation_complete(
        self, distribution_flavor, distribution_version, revision, target_id=None, target_platform=None, target_auth_type=None
    ):
        self.race.distribution_flavor = distribution_flavor
        self.race.distribution_version = distribution_version
        self.race.revision = revision
        if target_id is not None:
            self.race.target_id = target_id
        if target_platform is not None:
            self.race.target_platform = target_platform
        if target_auth_type is not None:
            self.race.target_auth_type = target_auth_type
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


class RaceCoordinator:
    """
    Runs a race: starts the benchmark candidate, prepares and runs the benchmark, and reports results.

    It runs in the main process (the one that owns the user's terminal) and coordinates Rally's actors:

    * the mechanic (``mechanic.MechanicCoordinator``) which starts ``NodeMechanicActor`` instances on target hosts;
    * the ``DriverActor`` which coordinates ``Worker`` actors on load driver hosts.
    """

    # how often race control polls the driver for progress and results of finished tasks
    POLL_INTERVAL_SECONDS = 1
    # how long race control waits for the driver to stop all workers when the benchmark is cancelled
    CANCEL_TIMEOUT_SECONDS = 30

    def __init__(self, cfg: types.Config, sources=False, distribution=False, external=False, docker=False):
        self.logger = logging.getLogger(__name__)
        self.cfg = cfg
        self.sources = sources
        self.distribution = distribution
        self.external = external
        self.docker = docker
        self.coordinator = BenchmarkCoordinator(cfg)
        self.mechanic = mechanic.MechanicCoordinator(cfg)
        self.main_driver = None
        self.progress = console.progress()

    def setup(self):
        self.coordinator.setup(sources=self.sources)

    async def run(self):
        """
        Runs the race. ``setup()`` must have been called before.

        :raise asyncio.CancelledError: if the race has been cancelled (by the user).
        """
        try:
            await self._race()
            self.logger.info("Benchmark has finished successfully.")
        except asyncio.CancelledError:
            self.logger.info("User has cancelled the benchmark (detected by race control).")
            self.coordinator.cancelled = True
            await self._cancel_driver()
            raise
        except BaseException as e:
            cause = actor.unwrap(e)
            if isinstance(cause, actor.BenchmarkCancelled):
                # may happen if one of the load generators has detected that the user has cancelled the benchmark.
                self.logger.info("User has cancelled the benchmark (detected by actor).")
                self.coordinator.cancelled = True
            elif isinstance(cause, actor.BenchmarkFailure):
                self.logger.error("A benchmark failure has occurred")
                self.coordinator.error = True
                raise exceptions.RallyError(cause.message, cause.cause) from None
            elif _is_ray_error(cause):
                self.logger.error("A Rally actor has died unexpectedly", exc_info=True)
                self.coordinator.error = True
                raise exceptions.RallyError("A Rally actor has died unexpectedly.", str(cause)) from None
            else:
                self.coordinator.error = True
                raise
        finally:
            await self._stop()

    async def _race(self):
        self.logger.info("Asking mechanic to start the engine.")
        self.coordinator.race.team_revision = await self.mechanic.start_engine(
            self.coordinator.metrics_store.open_context, self.sources, self.distribution, self.external, self.docker
        )
        self.logger.info("Mechanic has started engine successfully.")
        self.main_driver = actor.create_actor(driver.DriverActor, self.cfg, self.mechanic.node_mechanics, host="localhost", name="driver")
        self.logger.info("Telling driver to prepare for benchmarking.")
        preparation = await self.main_driver.prepare_benchmark.remote(self.coordinator.current_track)
        await actor.await_actor_output()
        self.coordinator.on_preparation_complete(
            preparation.distribution_flavor,
            preparation.distribution_version,
            preparation.revision,
            target_id=preparation.target_id,
            target_platform=preparation.target_platform,
            target_auth_type=preparation.target_auth_type,
        )
        self.logger.info("Telling driver to start benchmark.")
        benchmark = asyncio.ensure_future(self.main_driver.run_benchmark.remote())
        try:
            while not benchmark.done():
                await asyncio.wait({benchmark}, timeout=RaceCoordinator.POLL_INTERVAL_SECONDS)
                await self._poll()
        finally:
            if not benchmark.done():
                benchmark.cancel()
        metrics_of_last_task = benchmark.result()
        # make sure that we have received all results and progress updates
        await self._poll()
        self.coordinator.on_benchmark_complete(metrics_of_last_task)

    async def _poll(self):
        status = await self.main_driver.poll.remote()
        for event in status.progress:
            if event is None:
                self.progress.finish()
            else:
                self.progress.print(*event)
        for task_finished in status.finished_tasks:
            self.coordinator.on_task_finished(task_finished.metrics)
        await self.mechanic.check_health()

    async def _cancel_driver(self):
        if self.main_driver is None:
            return
        try:
            await actor.await_with_timeout(self.main_driver.cancel.remote(), RaceCoordinator.CANCEL_TIMEOUT_SECONDS)
        except Exception as e:  # pylint: disable=broad-exception-caught
            self.logger.warning("Could not cancel the benchmark gracefully: %s", e)

    async def _stop(self):
        if self.main_driver is not None:
            self.logger.info("Telling driver to stop.")
            main_driver = self.main_driver
            self.main_driver = None
            await actor.stop_actor(main_driver, name="driver")
        self.logger.info("Asking mechanic to stop the engine.")
        await self.mechanic.stop_engine()
        self.logger.info("Mechanic has stopped engine successfully.")


def _is_ray_error(e: BaseException) -> bool:
    return any(c.__module__.startswith("ray.") for c in type(e).__mro__)


def race(cfg: types.Config, sources=False, distribution=False, external=False, docker=False):
    logger = logging.getLogger(__name__)
    race_coordinator = RaceCoordinator(cfg, sources, distribution, external, docker)
    try:
        race_coordinator.setup()
        actor.run_async(race_coordinator.run)
    except KeyboardInterrupt:
        logger.info("User has cancelled the benchmark (detected by race control).")
        raise exceptions.UserInterrupted("User has cancelled the benchmark (detected by race control).") from None


def prepare_track(cfg: types.Config):
    logger = logging.getLogger(__name__)
    track_description = cfg.opts("track", "track.name", mandatory=False) or cfg.opts("track", "track.path", mandatory=False)
    assert track_description is not None, "track description missing"
    logger.info("Preparing track [%s] ...", track_description)
    console.println(f"Preparing track [{track_description}] ...")
    try:
        # load the track in the coordinating process so track parameters are validated before preparing corpora
        t = track.load_track(cfg, install_dependencies=True)
        track.resolve_challenge_and_invoke_validators(t, cfg)
        actor.run_async(lambda: _prepare_track(cfg, t))
    except KeyboardInterrupt:
        logger.info("User has cancelled track preparation.")
        raise exceptions.UserInterrupted("User has cancelled track preparation.") from None
    logger.info("Track [%s] has been prepared successfully.", t.name)
    console.println(f"Track [{t.name}] has been prepared successfully.")


async def _prepare_track(cfg: types.Config, t):
    logger = logging.getLogger(__name__)
    track_preparation_actor = actor.create_actor(driver.TrackPreparationActor, cfg, host="localhost", name="track-preparator")
    try:
        # dependencies were already installed by this process, which runs on the same machine
        await track_preparation_actor.prepare_track.remote(t, install_dependencies=False)
        await actor.await_actor_output()
    except asyncio.CancelledError:
        raise
    except BaseException as e:
        cause = actor.unwrap(e)
        if isinstance(cause, actor.BenchmarkFailure):
            logger.error("A track preparation failure has occurred")
            raise exceptions.RallyError(cause.message, cause.cause) from None
        raise
    finally:
        logger.info("Telling track preparation actor to stop.")
        await actor.stop_actor(track_preparation_actor, name="track-preparator")


def set_default_hosts(cfg: types.Config, host="127.0.0.1", port=9200):
    logger = logging.getLogger(__name__)
    configured_hosts = cfg.opts("client", "hosts")
    if len(configured_hosts.default_or_first) != 0:
        logger.info("Using configured hosts %s", configured_hosts.default_or_first)
    else:
        logger.info("Setting default host to [%s:%d]", host, port)
        default_host_object = opts.TargetHosts(f"{host}:{port}")
        cfg.add(config.Scope.benchmark, "client", "hosts", default_host_object)


# Poor man's curry
def from_sources(cfg: types.Config):
    port = cfg.opts("provisioning", "node.http.port")
    set_default_hosts(cfg, port=port)
    return race(cfg, sources=True)


def from_distribution(cfg: types.Config):
    port = cfg.opts("provisioning", "node.http.port")
    set_default_hosts(cfg, port=port)
    return race(cfg, distribution=True)


def benchmark_only(cfg: types.Config):
    if not cfg.opts("driver", "multi.cluster", mandatory=False):
        set_default_hosts(cfg)
    # We'll use a special car name for external benchmarks.
    cfg.add(config.Scope.benchmark, "mechanic", "car.names", ["external"])
    return race(cfg, external=True)


def docker(cfg: types.Config):
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


def run(cfg: types.Config):
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
                "For more details read the docs at {}\n".format(doc_link("pipelines.html"))
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
