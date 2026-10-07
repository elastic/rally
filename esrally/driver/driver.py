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
import concurrent.futures
import itertools
import logging
import math
import multiprocessing
import queue
import sys
import threading
import time
from dataclasses import dataclass
from io import BytesIO
from typing import Callable, Optional

from esrally import (
    PROGRAM_NAME,
    actor,
    client,
    config,
    exceptions,
    metrics,
    paths,
    telemetry,
    track,
    types,
)
from esrally.client import delete_api_keys
from esrally.driver import runner, scheduler
from esrally.track import TrackProcessorRegistry, load_track, load_track_plugins
from esrally.utils import console, convert, net
from esrally.utils.error_behavior import OnErrorBehavior


##################################
#
# Data exchanged between actors
#
##################################
@dataclass(frozen=True)
class WorkerTask:
    """
    Unit of work that should be completed by the low-level TaskExecutionActor
    """

    func: Callable
    params: dict


@dataclass(frozen=True)
class PreparationComplete:
    """
    Result of the preparation of a benchmark.
    """

    distribution_flavor: str
    distribution_version: str
    revision: str
    target_id: Optional[str] = None
    target_platform: Optional[str] = None
    target_auth_type: Optional[str] = None


@dataclass(frozen=True)
class TaskFinished:
    """
    Request metrics of a finished task.
    """

    metrics: Optional[bytes]
    next_task_scheduled_in: float


@dataclass(frozen=True)
class DriverStatus:
    """
    What happened in the driver since race control polled it last time.

    :param progress: Progress updates, i.e. arguments for ``CmdLineProgressReporter.print()``, or ``None`` for a call to
                     ``CmdLineProgressReporter.finish()``.
    :param finished_tasks: Tasks that have finished.
    """

    progress: list[Optional[tuple[str, str]]]
    finished_tasks: list[TaskFinished]


class RecordingProgressReporter:
    """
    A replacement for ``CmdLineProgressReporter`` that records progress so that race control can print it in the process
    that owns the user's terminal.
    """

    def __init__(self):
        self.events: list[Optional[tuple[str, str]]] = []

    def print(self, message, progress):
        # only the most recent update of the current line matters
        if self.events and self.events[-1] is not None:
            self.events[-1] = (message, progress)
        else:
            self.events.append((message, progress))

    def finish(self):
        if self.events and self.events[-1] is None:
            return
        self.events.append(None)

    def drain(self) -> list[Optional[tuple[str, str]]]:
        events = self.events
        self.events = []
        return events


def _share(obj):
    """
    Puts an object into Ray's object store so that it is serialized only once when it is passed to several actors.
    """
    import ray  # pylint: disable=import-outside-toplevel

    return ray.put(obj)


class DriverActor(actor.RallyActorBase):
    """
    Coordinates all workers. This is a thin actor wrapper around ``Driver`` which does the actual work.

    Race control calls ``prepare_benchmark()`` and then ``run_benchmark()``, and polls ``poll()`` regularly in the meantime.
    """

    TICK_INTERVAL_SECONDS = 1

    # post-process request metrics every N seconds and send it to the metrics store
    POST_PROCESS_INTERVAL_SECONDS = 30

    def __init__(self, cfg: types.Config, node_mechanics: Optional[list] = None):
        """
        :param cfg: Rally internal configuration object.
        :param node_mechanics: Handles of the ``NodeMechanicActor`` instances whose relative time should be reset together
                               with the driver's.
        """
        super().__init__(name="driver", cfg=cfg)
        self.cfg = cfg
        self.node_mechanics = node_mechanics or []
        self.progress = RecordingProgressReporter()
        self.driver = Driver(self, cfg, progress_reporter=self.progress)
        self.status = "init"
        # set by Driver
        self.cluster_details = {}
        self.target_platform = None
        self.target_auth_type = None
        self.workers: dict[int, object] = {}
        # futures of the Worker#run() calls, which only complete when the worker stops or fails.
        self.worker_runs: dict[int, tuple[object, concurrent.futures.Future]] = {}
        self.track_preparators: list = []
        self._track_preparation: Optional[asyncio.Future] = None
        self._shared_track = None
        self._finished_tasks: list[TaskFinished] = []
        self._done = asyncio.Event()
        self._result = None
        self._cancelled = False
        self._tick_task: Optional[asyncio.Task] = None
        self._reset_task: Optional[asyncio.Task] = None

    def _fail(self, failure):
        super()._fail(failure)
        self._done.set()

    # ---- called by race control ----

    @actor.convert_failures("driver")
    async def prepare_benchmark(self, t) -> PreparationComplete:
        self.driver.prepare_benchmark(t)
        await self._track_preparation
        return self._preparation_complete()

    @actor.convert_failures("driver")
    async def run_benchmark(self) -> Optional[bytes]:
        """
        Runs the benchmark and returns the request metrics of the last task when it is complete.
        """
        if self._failure is None:
            self.status = "running"
            self._shared_track = _share(self.driver.track)
            self.driver.start_benchmark()
            self._tick_task = asyncio.create_task(self._tick_loop())
        await self._done.wait()
        if self._failure is not None:
            raise self._failure
        if self._cancelled:
            raise actor.BenchmarkCancelled()
        return self._result

    def poll(self) -> DriverStatus:
        finished_tasks = self._finished_tasks
        self._finished_tasks = []
        return DriverStatus(progress=self.progress.drain(), finished_tasks=finished_tasks)

    async def cancel(self):
        """
        Cancels the benchmark: stops all workers. ``run_benchmark()`` raises ``BenchmarkCancelled``.
        """
        self.logger.info("Main driver received a notification that the benchmark has been cancelled.")
        self._cancelled = True
        self.status = "exiting"
        await self._stop_workers()
        self._done.set()

    async def stop(self):
        """
        Stops the driver and all actors it has created.
        """
        self.logger.info("Main driver is stopping and will terminate all load generators.")
        self.status = "exiting"
        for task in (self._tick_task, self._reset_task, self._track_preparation):
            if task is not None and not task.done():
                task.cancel()
        await self._stop_workers()
        for preparator in self.track_preparators:
            actor.kill_actor(preparator)
        self.track_preparators = []
        self.driver.close()
        self._done.set()

    # ---- called by workers ----

    def update_samples(self, worker_id, samples):
        self.driver.update_samples(samples)

    @actor.report_failures("driver")
    def joinpoint_reached(self, worker_id, worker_timestamp, task_allocations):
        self.driver.joinpoint_reached(worker_id, worker_timestamp, task_allocations)

    # ---- called by Driver ----

    def create_client(self, host, cfg: types.Config, worker_id):
        worker = actor.create_actor(Worker, self.self_handle, worker_id, cfg, host=host, name=f"worker-{worker_id}")
        self.workers[worker_id] = worker
        return worker

    def start_worker(self, driver, worker_id, cfg: types.Config, track, allocations, client_contexts=None):
        shared_track = self._shared_track if self._shared_track is not None else track
        run = driver.run.remote(shared_track, allocations, client_contexts)
        self.worker_runs[worker_id] = (run, run.future())

    def drive_at(self, driver, client_start_timestamp):
        driver.drive_at.remote(client_start_timestamp)

    def complete_current_task(self, driver):
        driver.complete_current_task.remote()

    def on_task_finished(self, metrics, next_task_scheduled_in):
        if self._reset_task is not None:
            self._reset_task.cancel()
        if next_task_scheduled_in > 0:
            self._reset_task = asyncio.get_running_loop().create_task(self._reset_relative_time_after(next_task_scheduled_in))
        else:
            self._reset_relative_time()
        self._finished_tasks.append(TaskFinished(metrics, next_task_scheduled_in))

    def prepare_track(self, hosts, cfg: types.Config, track):
        self._track_preparation = asyncio.ensure_future(self._prepare_track(hosts, cfg, track))

    def on_benchmark_complete(self, metrics):
        self._result = metrics
        self._done.set()

    # ---- internals ----

    def _preparation_complete(self) -> PreparationComplete:
        cluster_version = self.cluster_details["version"] if self.cluster_details else {}
        # manually compiled versions don't expose build_flavor but Rally expects a value in telemetry devices
        # we should default to trial/basic, but let's default to oss for now to avoid breaking the charts
        build_flavor = cluster_version.get("build_flavor", "oss")
        build_version = cluster_version.get("number", build_flavor)
        build_hash = cluster_version.get("build_hash", build_flavor)
        # Determine target_id (cluster_name from GET /)
        target_id = self.cluster_details.get("cluster_name") if self.cluster_details else None
        return PreparationComplete(
            build_flavor,
            build_version,
            build_hash,
            target_id=target_id,
            target_platform=self.target_platform,
            target_auth_type=self.target_auth_type,
        )

    async def _prepare_track(self, hosts, cfg: types.Config, track):
        self.logger.info("Starting prepare track process on hosts [%s]", hosts)
        for host in hosts:
            await actor.require_node_async(host)
        self.track_preparators = [
            actor.create_actor(TrackPreparationActor, cfg, host=host, name=f"track-preparator-{idx}") for idx, host in enumerate(hosts)
        ]
        try:
            await asyncio.gather(*[p.prepare_track.remote(track, install_dependencies=True) for p in self.track_preparators])
        finally:
            for preparator in self.track_preparators:
                actor.kill_actor(preparator)
            self.track_preparators = []

    async def _reset_relative_time_after(self, seconds):
        await asyncio.sleep(seconds)
        self._reset_relative_time()

    def _reset_relative_time(self):
        self.driver.reset_relative_time()
        for node_mechanic in self.node_mechanics:
            node_mechanic.reset_relative_time.remote()

    @actor.report_failures("driver")
    async def _tick_loop(self):
        post_process_timer = 0
        while True:
            await asyncio.sleep(DriverActor.TICK_INTERVAL_SECONDS)
            if self.status == "exiting" or self.driver.finished():
                return
            self._check_workers()
            post_process_timer += DriverActor.TICK_INTERVAL_SECONDS
            if post_process_timer >= DriverActor.POST_PROCESS_INTERVAL_SECONDS:
                post_process_timer = 0
                self.driver.post_process_samples()
            self.driver.update_progress_message()

    def _check_workers(self):
        # Worker#run() only returns when a worker is stopped. If it completes otherwise, the worker has failed or died.
        for worker_id, (_, future) in list(self.worker_runs.items()):
            if not future.done():
                continue
            del self.worker_runs[worker_id]
            if self.status == "exiting":
                self.logger.debug("Worker [%d] has exited.", worker_id)
                continue
            exception = future.exception()
            cause = actor.unwrap(exception) if exception is not None else None
            if isinstance(cause, actor.BenchmarkCancelled):
                self.logger.info("Worker [%d] has cancelled the benchmark.", worker_id)
                self._cancelled = True
                self._done.set()
            elif isinstance(cause, actor.BenchmarkFailure):
                self.logger.error("Main driver received a fatal exception from load generator [%d]. Shutting down.", worker_id)
                self._fail(cause)
            else:
                self.logger.error("Worker [%d] has exited prematurely. Aborting benchmark.", worker_id)
                self._fail(actor.BenchmarkFailure(f"Worker [{worker_id}] has exited prematurely.", str(cause) if cause else None))

    async def _stop_workers(self):
        workers = self.workers
        self.workers = {}
        await asyncio.gather(*[actor.stop_actor(w, name=f"worker-{worker_id}") for worker_id, w in workers.items()])


def load_local_config(coordinator_config) -> types.Config:
    cfg = config.auto_load_local_config(
        coordinator_config,
        additional_sections=[
            # only copy the relevant bits
            "track",
            "driver",
            "client",
            # due to distribution version...
            "mechanic",
            "telemetry",
        ],
    )
    # set root path (normally done by the main entry point)
    cfg.add(config.Scope.application, "node", "rally.root", paths.rally_root())
    return cfg


class TaskExecutionActor(actor.RallyActorBase):
    """
    Executes the (potentially long-running) tasks of track processors, e.g. downloading or decompressing data.
    """

    def __init__(self, cfg: types.Config, executor_id: int = 0):
        super().__init__(name=f"task-executor-{executor_id}", cfg=cfg)
        self.cfg = load_local_config(cfg)

    @actor.convert_failures("task executor")
    def bootstrap(self, track_name):
        track.load_track_plugins(self.cfg, track_name)

    @actor.convert_failures("task executor")
    def execute(self, task: WorkerTask):
        task.func(**task.params)


class TrackPreparationActor(actor.RallyActorBase):
    """
    Prepares a track on one host by running the tasks of all track processors on a pool of ``TaskExecutionActor``
    instances on the same host.
    """

    def __init__(self, cfg: types.Config):
        super().__init__(name="track-preparator", cfg=cfg)
        # load node-specific config to have correct paths available
        self.cfg = load_local_config(cfg)
        self.executors: list = []
        self.logger.info("Track Preparator started")

    @actor.convert_failures("track preparator")
    async def prepare_track(self, t, install_dependencies: bool = False):
        """
        :param t: The track to prepare.
        :param install_dependencies: Whether to install the track's dependencies. This happens once per host, so it is
                                     not necessary if the process that loaded the track runs on the same host.
        """
        try:
            if install_dependencies:
                load_track(self.cfg, install_dependencies=True)
            data_root_dir = self.cfg.opts("benchmarks", "local.dataset.cache")
            tpr = TrackProcessorRegistry(self.cfg)
            self.logger.info("Preparing track [%s]", t.name)
            self.logger.info("Reloading track [%s] to ensure plugins are up-to-date.", t.name)
            # the track might have been loaded on a different machine (the coordinator machine) so we force a track
            # update to ensure we use the latest version of plugins.
            load_track(self.cfg)
            load_track_plugins(self.cfg, t.name, register_track_processor=tpr.register_track_processor, force_update=True)
            # processors run one after the other, tasks of a processor run in parallel
            for processor in tpr.processors:
                tasks = [WorkerTask(func, params) for func, params in processor.on_prepare_track(t, data_root_dir)]
                await self._run_tasks(t.name, tasks)
            self.logger.info("Track [%s] is prepared.", t.name)
        finally:
            self._kill_executors()

    async def stop(self):
        self._kill_executors()

    async def _run_tasks(self, track_name, tasks):
        if not tasks:
            return
        await self._ensure_executors(track_name, min(num_cores(self.cfg), len(tasks)))
        pending = list(tasks)

        async def feed(executor):
            while pending:
                await executor.execute.remote(pending.pop())

        await asyncio.gather(*[feed(executor) for executor in self.executors])

    async def _ensure_executors(self, track_name, count):
        new_executors = [
            actor.create_actor(TaskExecutionActor, self.cfg, len(self.executors) + idx, strategy=self.this_node_strategy())
            for idx in range(count - len(self.executors))
        ]
        self.executors.extend(new_executors)
        await asyncio.gather(*[executor.bootstrap.remote(track_name) for executor in new_executors])

    def _kill_executors(self):
        for executor in self.executors:
            actor.kill_actor(executor)
        self.executors = []


def num_cores(cfg: types.Config):
    return int(cfg.opts("system", "available.cores", mandatory=False, default_value=multiprocessing.cpu_count()))


ApiKey = collections.namedtuple("ApiKey", ["id", "secret"])


@dataclass
class ClientContext:
    client_id: int
    parent_worker_id: int
    api_key: Optional[ApiKey] = None


class EsClients(dict):
    """
    A cluster-name-to-client dict returned by create_es_clients().
    Exposes a ``default`` property that returns the 'default' client, or the
    first available client when no 'default' key is present (multi-cluster mode
    with named clusters).
    """

    @property
    def default(self):
        if "default" in self:
            return self["default"]
        return next(iter(self.values()), None)


class Driver:
    def __init__(self, driver_actor, config: types.Config, es_client_factory_class=client.EsClientFactory, progress_reporter=None):
        """
        Coordinates all workers. It is technology-agnostic, i.e. it does not know anything about actors. To allow us to hook in an actor,
        we provide a ``target`` parameter which will be called whenever some event has occurred. The ``target`` can use this to send
        appropriate messages.

        :param target: A target that will be notified of important events.
        :param config: The current config object.
        :param progress_reporter: Reports the progress of the benchmark. Prints on the console by default.
        """
        self.logger = logging.getLogger(__name__)
        self.driver_actor = driver_actor
        self.config = config
        self.es_client_factory = es_client_factory_class
        self.default_sync_es_client = None
        self.track = None
        self.challenge = None
        self.metrics_store = None
        self.load_driver_hosts = []
        self.workers = []
        # which client ids are assigned to which workers?
        self.clients_per_worker = {}
        self.client_contexts = {}
        self.generated_api_key_ids = []

        self.progress_reporter = progress_reporter if progress_reporter is not None else console.progress()
        self.progress_counter = 0
        self.quiet = False
        self.allocations = None
        self.raw_samples = []
        self.most_recent_sample_per_client = {}
        self.sample_post_processor = None

        self.number_of_steps = 0
        self.currently_completed = 0
        self.workers_completed_current_step = {}
        self.current_step = -1
        self.tasks_per_join_point = None
        self.complete_current_task_sent = False

        self.telemetry = None

    def create_es_clients(self):
        all_hosts = self.config.opts("client", "hosts").all_hosts
        distribution_version = self.config.opts("mechanic", "distribution.version", mandatory=False)
        distribution_flavor = self.config.opts("mechanic", "distribution.flavor", mandatory=False)
        all_client_options = self.config.opts("client", "options").all_client_options
        es = {}
        for cluster_name, cluster_hosts in all_hosts.items():
            cluster_client_options = dict(all_client_options[cluster_name])
            # Use retries to avoid aborts on long living connections for telemetry devices
            cluster_client_options["retry_on_timeout"] = True
            es[cluster_name] = self.es_client_factory(
                cluster_hosts, cluster_client_options, distribution_version=distribution_version, distribution_flavor=distribution_flavor
            ).create()
        return EsClients(es)

    def prepare_telemetry(self, es, enable, index_names, data_stream_names, build_hash, serverless_mode, serverless_operator):
        enabled_devices = self.config.opts("telemetry", "devices")
        telemetry_params = self.config.opts("telemetry", "params")
        log_root = paths.race_root(self.config)

        es_default = es.default
        default_client_options = self.config.opts("client", "options").default_or_first

        if enable:
            devices = [
                telemetry.NodeStats(telemetry_params, es, self.metrics_store),
                telemetry.ExternalEnvironmentInfo(es_default, self.metrics_store),
                telemetry.ClusterEnvironmentInfo(es_default, self.metrics_store, build_hash, default_client_options),
                telemetry.JvmStatsSummary(es_default, self.metrics_store),
                telemetry.IndexStats(es_default, self.metrics_store),
                telemetry.MlBucketProcessingTime(es_default, self.metrics_store),
                telemetry.MasterNodeStats(telemetry_params, es_default, self.metrics_store),
                telemetry.SegmentStats(log_root, es_default),
                telemetry.CcrStats(telemetry_params, es, self.metrics_store),
                telemetry.RecoveryStats(telemetry_params, es, self.metrics_store),
                telemetry.ShardStats(telemetry_params, es, self.metrics_store),
                telemetry.TransformStats(telemetry_params, es, self.metrics_store),
                telemetry.SearchableSnapshotsStats(telemetry_params, es, self.metrics_store),
                telemetry.DataStreamStats(telemetry_params, es, self.metrics_store),
                telemetry.IngestPipelineStats(es, self.metrics_store),
                telemetry.DiskUsageStats(telemetry_params, es_default, self.metrics_store, index_names, data_stream_names),
                telemetry.BlobStoreStats(telemetry_params, es, self.metrics_store),
                telemetry.GeoIpStats(es_default, self.metrics_store),
            ]
        else:
            devices = []
        self.telemetry = telemetry.Telemetry(
            enabled_devices,
            devices=devices,
            serverless_mode=serverless_mode,
            serverless_operator=serverless_operator,
        )

    def wait_for_rest_api(self, es):
        es_default = es.default
        self.logger.info("Checking if REST API is available.")
        if client.wait_for_rest_layer(es_default, max_attempts=40):
            self.logger.info("REST API is available.")
        else:
            self.logger.error("REST API layer is not yet available. Stopping benchmark.")
            raise exceptions.SystemSetupError("Elasticsearch REST API layer is not available.")

    def retrieve_cluster_info(self, es):
        try:
            es_default = es.default
            return es_default.info() if es_default else None
        except BaseException:
            self.logger.exception("Could not retrieve cluster info on benchmark start")
            return None

    def retrieve_build_hash_from_nodes_info(self, es):
        try:
            es_default = es.default
            if not es_default:
                return None
            nodes_info = es_default.nodes.info(filter_path="**.build_hash")
            nodes = nodes_info["nodes"]
            # assumption: build hash is the same across all the nodes
            first_node_id = next(iter(nodes))
            return nodes[first_node_id]["build_hash"]
        except BaseException:
            self.logger.exception("Could not retrieve build hash from nodes info")
            return None

    def create_api_key(self, es, client_id):
        self.logger.debug("Creating ES API key for client [%s].", client_id)
        try:
            api_key = client.create_api_key(es, client_id)
            self.logger.debug("ES API key created for client [%s].", client_id)
            # Store the API key ID for deletion upon benchmark completion
            self.generated_api_key_ids.append(api_key["id"])
            return api_key
        except Exception as e:
            self.logger.error("Unable to create API keys. Stopping benchmark.")
            raise exceptions.SystemSetupError(e.message)

    def prepare_benchmark(self, t):
        self.track = t
        self.challenge = select_challenge(self.config, self.track)
        self.quiet = self.config.opts("system", "quiet.mode", mandatory=False, default_value=False)
        downsample_factor = int(self.config.opts("reporting", "metrics.request.downsample.factor", mandatory=False, default_value=1))
        self.metrics_store = metrics.metrics_store(cfg=self.config, track=self.track.name, challenge=self.challenge.name, read_only=False)

        self.sample_post_processor = SamplePostprocessor(
            self.metrics_store, downsample_factor, self.track.meta_data, self.challenge.meta_data
        )

        es_clients = self.create_es_clients()
        self.default_sync_es_client = es_clients.default

        skip_rest_api_check = self.config.opts("mechanic", "skip.rest.api.check")
        uses_static_responses = self.config.opts("client", "options").uses_static_responses
        multi_cluster = self.config.opts("driver", "multi.cluster", mandatory=False)
        serverless_mode = convert.to_bool(self.config.opts("driver", "serverless.mode", mandatory=False, default_value=False))
        serverless_operator = convert.to_bool(self.config.opts("driver", "serverless.operator", mandatory=False, default_value=False))
        build_hash = None
        if skip_rest_api_check:
            self.logger.info("Skipping REST API check as requested explicitly.")
        elif uses_static_responses:
            self.logger.info("Skipping REST API check as static responses are used.")
        else:
            if serverless_mode and not serverless_operator:
                self.logger.info("Skipping REST API check while targetting serverless cluster with a public user.")
            else:
                self.wait_for_rest_api(es_clients)
            self.driver_actor.cluster_details = self.retrieve_cluster_info(es_clients)
            if serverless_mode:
                # overwrite static serverless version number
                self.driver_actor.cluster_details["version"]["number"] = "serverless"
                if serverless_operator:
                    # overwrite build hash if running as operator
                    build_hash = self.retrieve_build_hash_from_nodes_info(es_clients)
                    self.logger.info("Retrieved actual build hash [%s] from serverless cluster.", build_hash)
                    self.driver_actor.cluster_details["version"]["build_hash"] = build_hash

            # Determine target_platform while we still have the API response object (with response headers)
            if serverless_mode:
                self.driver_actor.target_platform = "serverless"
            else:
                target_platform = "on-prem"
                try:
                    cluster_details = self.driver_actor.cluster_details
                    meta = getattr(cluster_details, "meta", None)
                    if meta is not None and "x-found-handling-cluster" in getattr(meta, "headers", {}):
                        target_platform = "hosted"
                except Exception:
                    pass
                self.driver_actor.target_platform = target_platform

            # Determine target_auth_type from default client options
            default_client_options = self.config.opts("client", "options").default_or_first
            if default_client_options.get("api_key"):
                self.driver_actor.target_auth_type = "api_key"
            elif default_client_options.get("basic_auth_user") or default_client_options.get("basic_auth"):
                self.driver_actor.target_auth_type = "basic"

        # Avoid issuing any requests to the target cluster when static responses are enabled. The results
        # are not useful and attempts to connect to a non-existing cluster just lead to exception traces in logs.
        self.prepare_telemetry(
            es_clients,
            enable=not uses_static_responses and not multi_cluster,
            index_names=self.track.index_names(),
            data_stream_names=self.track.data_stream_names(),
            build_hash=build_hash,
            serverless_mode=serverless_mode,
            serverless_operator=serverless_operator,
        )

        for host in self.config.opts("driver", "load_driver_hosts"):
            host_config = {
                # for simplicity we assume that all benchmark machines have the same specs
                "cores": num_cores(self.config)
            }
            if host != "localhost":
                host_config["host"] = net.resolve(host)
            else:
                host_config["host"] = host

            self.load_driver_hosts.append(host_config)

        self.driver_actor.prepare_track([h["host"] for h in self.load_driver_hosts], self.config, self.track)

    def start_benchmark(self):
        self.logger.info("Benchmark is about to start.")
        # ensure relative time starts when the benchmark starts.
        self.reset_relative_time()
        self.logger.info("Attaching cluster-level telemetry devices.")
        self.telemetry.on_benchmark_start()
        self.logger.info("Cluster-level telemetry devices are now attached.")

        allocator = Allocator(self.challenge.schedule)
        self.allocations = allocator.allocations
        self.number_of_steps = len(allocator.join_points) - 1
        self.tasks_per_join_point = allocator.tasks_per_joinpoint

        self.logger.info(
            "Benchmark consists of [%d] steps executed by [%d] clients.",
            self.number_of_steps,
            len(self.allocations),  # type: ignore[arg-type]  # TODO remove the below ignore when introducing type hints
        )
        # avoid flooding the log if there are too many clients
        if allocator.clients < 128:
            self.logger.debug("Allocation matrix:\n%s", "\n".join([str(a) for a in self.allocations]))

        create_api_keys = self.config.opts("client", "options").default_or_first.get("create_api_key_per_client", None)
        worker_assignments = calculate_worker_assignments(self.load_driver_hosts, allocator.clients)
        worker_id = 0
        for assignment in worker_assignments:
            host = assignment["host"]
            for clients in assignment["workers"]:
                # don't assign workers without any clients
                if len(clients) > 0:
                    self.logger.debug("Allocating worker [%d] on [%s] with [%d] clients.", worker_id, host, len(clients))
                    worker = self.driver_actor.create_client(host, self.config, worker_id)

                    client_allocations = ClientAllocations()
                    worker_client_contexts = {}
                    for client_id in clients:
                        client_allocations.add(client_id, self.allocations[client_id])
                        self.clients_per_worker[client_id] = worker_id
                        client_context = ClientContext(client_id=client_id, parent_worker_id=worker_id)

                        if create_api_keys:
                            resp = self.create_api_key(self.default_sync_es_client, client_id)
                            client_context.api_key = ApiKey(id=resp["id"], secret=resp["api_key"])

                        worker_client_contexts[client_id] = client_context
                        self.client_contexts[worker_id] = worker_client_contexts
                    self.driver_actor.start_worker(
                        worker, worker_id, self.config, self.track, client_allocations, client_contexts=worker_client_contexts
                    )
                    self.workers.append(worker)
                    worker_id += 1

        self.update_progress_message()

    def joinpoint_reached(self, worker_id, worker_local_timestamp, task_allocations):
        self.currently_completed += 1
        self.workers_completed_current_step[worker_id] = (worker_local_timestamp, time.perf_counter())
        self.logger.debug(
            "[%d/%d] workers reached join point [%d/%d].",
            self.currently_completed,
            len(self.workers),
            self.current_step + 1,
            self.number_of_steps,
        )
        if self.currently_completed == len(self.workers):
            self.logger.info("All workers completed their tasks until join point [%d/%d].", self.current_step + 1, self.number_of_steps)
            # we can go on to the next step
            self.currently_completed = 0
            self.complete_current_task_sent = False
            # make a copy and reset early to avoid any race conditions from clients that reach a join point already while we are sending...
            workers_curr_step = self.workers_completed_current_step
            self.workers_completed_current_step = {}
            self.update_progress_message(task_finished=True)
            # clear per step
            self.most_recent_sample_per_client = {}
            self.current_step += 1

            self.logger.debug("Postprocessing samples...")
            self.post_process_samples()
            if self.finished():
                self.telemetry.on_benchmark_stop()
                self.logger.info("All steps completed.")
                # Some metrics store implementations return None because no external representation is required.
                # pylint: disable=assignment-from-none
                m = self.metrics_store.to_externalizable(clear=True)
                self.logger.debug("Closing metrics store...")
                self.metrics_store.close()
                # immediately clear as we don't need it anymore and it can consume a significant amount of memory
                self.metrics_store = None
                if self.generated_api_key_ids:
                    self.logger.debug("Deleting auto-generated client API keys...")
                    try:
                        delete_api_keys(self.default_sync_es_client, self.generated_api_key_ids)
                    except exceptions.RallyError:
                        console.warn(
                            "Unable to delete auto-generated API keys. You may need to manually delete them. "
                            "Please check the logs for details."
                        )
                self.logger.debug("Sending benchmark results...")
                self.driver_actor.on_benchmark_complete(m)
            else:
                self.move_to_next_task(workers_curr_step)
        else:
            self.may_complete_current_task(task_allocations)

    def move_to_next_task(self, workers_curr_step):
        if self.config.opts("track", "test.mode.enabled"):
            # don't wait if test mode is enabled and start the next task immediately.
            waiting_period = 0
        else:
            # start the next task in one second (relative to master's timestamp)
            #
            # Assumption: We don't have a lot of clock skew between reaching the join point and sending the next task
            #             (it doesn't matter too much if we're a few ms off).
            waiting_period = 1.0
        # Some metrics store implementations return None because no external representation is required.
        # pylint: disable=assignment-from-none
        m = self.metrics_store.to_externalizable(clear=True)
        self.driver_actor.on_task_finished(m, waiting_period)
        # Using a perf_counter here is fine also in the distributed case as we subtract it from `master_received_msg_at` making it
        # a relative instead of an absolute value.
        start_next_task = time.perf_counter() + waiting_period
        for worker_id, worker in enumerate(self.workers):
            worker_ended_task_at, master_received_msg_at = workers_curr_step[worker_id]
            worker_start_timestamp = worker_ended_task_at + (start_next_task - master_received_msg_at)
            self.logger.debug(
                "Scheduling next task for worker id [%d] at their timestamp [%f] (master timestamp [%f])",
                worker_id,
                worker_start_timestamp,
                start_next_task,
            )
            self.driver_actor.drive_at(worker, worker_start_timestamp)

    def may_complete_current_task(self, task_allocations):
        any_joinpoints_completing_parent = [a for a in task_allocations if a.task.any_task_completes_parent]
        joinpoints_completing_parent = [a for a in task_allocations if a.task.preceding_task_completes_parent]

        # If 'completed-by' is set to 'any', then we *do* want to check for completion by
        # any client and *not* wait until the remaining runner has completed. This way the 'parallel' task will exit
        # on the completion of _any_ client for any task, i.e. given a contrived track with two tasks to execute inside
        # a parallel block:
        #   * parallel:
        #     * bulk-1, with clients 8
        #     * bulk-2, with clients: 8
        #
        # 1. Both 'bulk-1' and 'bulk-2' execute in parallel
        # 2. 'bulk-1' client[0]'s runner is first to complete and reach the next joinpoint successfully
        # 3. 'bulk-1' will now cause the parent task to complete and _not_ wait for all 8 clients' runner to complete,
        # or for 'bulk-2' at all
        #
        # The reasoning for the distinction between 'any_joinpoints_completing_parent' & 'joinpoints_completing_parent'
        # is to simplify logic, otherwise we'd need to implement some form of state machine involving actor-to-actor
        # communication.

        if len(any_joinpoints_completing_parent) > 0 and not self.complete_current_task_sent:
            self.logger.info(
                "Any task before join point [%s] is able to complete the parent structure. Telling all clients to exit immediately.",
                any_joinpoints_completing_parent[0].task,
            )

            self.complete_current_task_sent = True
            for worker in self.workers:
                self.driver_actor.complete_current_task(worker)

        # If we have a specific 'completed-by' task specified, then we want to make sure that all clients for that task
        # are able to complete their runners as expected before completing the parent
        elif len(joinpoints_completing_parent) > 0 and not self.complete_current_task_sent:
            # while this list could contain multiple items, it should always be the same task (but multiple
            # different clients) so any item is sufficient.
            current_join_point = joinpoints_completing_parent[0].task
            self.logger.info(
                "Tasks before join point [%s] are able to complete the parent structure. Checking "
                "if all [%d] clients have finished yet.",
                current_join_point,
                len(current_join_point.clients_executing_completing_task),
            )
            pending_client_ids = []
            for client_id in current_join_point.clients_executing_completing_task:
                # We assume that all clients have finished if their corresponding worker has finished
                worker_id = self.clients_per_worker[client_id]
                if worker_id not in self.workers_completed_current_step:
                    pending_client_ids.append(client_id)

            # are all clients executing said task already done? if so we need to notify the remaining clients
            if len(pending_client_ids) == 0:
                # As we are waiting for other clients to finish, we would send this message over and over again.
                # Hence we need to memorize whether we have already sent it for the current step.
                self.complete_current_task_sent = True
                self.logger.info("All affected clients have finished. Notifying all clients to complete their current tasks.")
                for worker in self.workers:
                    self.driver_actor.complete_current_task(worker)
            else:
                if len(pending_client_ids) > 32:
                    self.logger.info("[%d] clients did not yet finish.", len(pending_client_ids))
                else:
                    self.logger.info("Client id(s) [%s] did not yet finish.", ",".join(map(str, pending_client_ids)))

    def reset_relative_time(self):
        self.logger.debug("Resetting relative time of request metrics store.")
        self.metrics_store.reset_relative_time()

    def finished(self):
        return self.current_step == self.number_of_steps

    def close(self):
        self.progress_reporter.finish()
        if self.metrics_store and self.metrics_store.opened:
            self.metrics_store.close()

    def update_samples(self, samples):
        if len(samples) > 0:
            self.raw_samples += samples
            # We need to check all samples, they will be from different clients
            for s in samples:
                self.most_recent_sample_per_client[s.client_id] = s

    def update_progress_message(self, task_finished=False):
        if not self.quiet and self.current_step >= 0:
            tasks = ",".join([t.name for t in self.tasks_per_join_point[self.current_step]])

            if task_finished:
                total_progress = 1.0
            else:
                # we only count clients which actually contribute to progress. If clients are executing tasks eternally in a parallel
                # structure, we should not count them. The reason is that progress depends entirely on the client(s) that execute the
                # task that is completing the parallel structure.
                progress_per_client = [
                    s.percent_completed for s in self.most_recent_sample_per_client.values() if s.percent_completed is not None
                ]

                num_clients = max(len(progress_per_client), 1)
                total_progress = sum(progress_per_client) / num_clients
            self.progress_reporter.print("Running %s" % tasks, "[%3d%% done]" % (round(total_progress * 100)))
            if task_finished:
                self.progress_reporter.finish()

    def post_process_samples(self):
        # we do *not* do this here to avoid concurrent updates (actors run on a single event loop) but rather to make it clear that we use
        # only a snapshot and that new data will go to a new sample set.
        raw_samples = self.raw_samples
        self.raw_samples = []
        self.sample_post_processor(raw_samples)


class SamplePostprocessor:
    def __init__(self, metrics_store, downsample_factor, track_meta_data, challenge_meta_data):
        self.logger = logging.getLogger(__name__)
        self.metrics_store = metrics_store
        self.track_meta_data = track_meta_data
        self.challenge_meta_data = challenge_meta_data
        self.throughput_calculator = ThroughputCalculator()
        self.downsample_factor = downsample_factor

    def put_sample(self, sample, name, unit, meta_data):
        self.metrics_store.put_value_cluster_level(
            name=name,
            value=convert.seconds_to_ms(getattr(sample, name)),
            unit=unit,
            task=sample.task.name,
            operation=sample.operation_name,
            operation_type=sample.operation_type,
            sample_type=sample.sample_type,
            absolute_time=sample.absolute_time,
            relative_time=sample.relative_time,
            meta_data=meta_data,
        )

    def __call__(self, raw_samples):
        if len(raw_samples) == 0:
            return
        total_start = time.perf_counter()
        start = total_start
        final_sample_count = 0
        for idx, sample in enumerate(raw_samples):
            if idx % self.downsample_factor == 0:
                final_sample_count += 1
                client_id_meta_data = {"client_id": sample.client_id}
                meta_data = self.merge(
                    self.track_meta_data,
                    self.challenge_meta_data,
                    sample.operation_meta_data,
                    sample.task.meta_data,
                    sample.request_meta_data,
                    client_id_meta_data,
                )
                self.put_sample(
                    sample,
                    "latency",
                    "ms",
                    meta_data,
                )
                self.put_sample(
                    sample,
                    "service_time",
                    "ms",
                    meta_data,
                )
                self.put_sample(
                    sample,
                    "processing_time",
                    "ms",
                    meta_data,
                )

                for timing in sample.dependent_timings:
                    self.put_sample(
                        timing,
                        "service_time",
                        "ms",
                        self.merge(timing.request_meta_data, client_id_meta_data),
                    )

        end = time.perf_counter()
        self.logger.debug("Storing latency and service time took [%f] seconds.", (end - start))
        start = end
        aggregates = self.throughput_calculator.calculate(raw_samples)
        end = time.perf_counter()
        self.logger.debug("Calculating throughput took [%f] seconds.", (end - start))
        start = end
        for (task, cluster_name), samples in aggregates.items():
            cluster_meta = {"cluster": cluster_name} if cluster_name is not None else {}
            meta_data = self.merge(self.track_meta_data, self.challenge_meta_data, task.operation.meta_data, task.meta_data, cluster_meta)
            for absolute_time, relative_time, sample_type, throughput, throughput_unit in samples:
                self.metrics_store.put_value_cluster_level(
                    name="throughput",
                    value=throughput,
                    unit=throughput_unit,
                    task=task.name,
                    operation=task.operation.name,
                    operation_type=task.operation.type,
                    sample_type=sample_type,
                    absolute_time=absolute_time,
                    relative_time=relative_time,
                    meta_data=meta_data,
                )
        end = time.perf_counter()
        self.logger.debug("Storing throughput took [%f] seconds.", (end - start))
        start = end
        # this will be a noop for the in-memory metrics store. If we use an ES metrics store however, this will ensure that we already send
        # the data and also clear the in-memory buffer. This allows users to see data already while running the benchmark. In cases where
        # it does not matter (i.e. in-memory) we will still defer this step until the end.
        #
        # Don't force refresh here in the interest of short processing times. We don't need to query immediately afterwards so there is
        # no need for frequent refreshes.
        self.metrics_store.flush(refresh=False)
        end = time.perf_counter()
        self.logger.debug("Flushing the metrics store took [%f] seconds.", (end - start))
        self.logger.debug(
            "Postprocessing [%d] raw samples (downsampled to [%d] samples) took [%f] seconds in total.",
            len(raw_samples),
            final_sample_count,
            (end - total_start),
        )

    def merge(self, *args):
        result = {}
        for arg in args:
            if arg is not None:
                result.update(arg)
        return result


def calculate_worker_assignments(host_configs, client_count):
    """
    Assigns clients to workers on the provided hosts.

    :param host_configs: A list of dicts where each dict contains the host name (key: ``host``) and the number of
                         available CPU cores (key: ``cores``).
    :param client_count: The number of clients that should be used at most.
    :return: A list of dicts containing the host (key: ``host``) and a list of workers (key ``workers``). Each entry
             in that list contains another list with the clients that should be assigned to these workers.
    """
    assignments = []
    client_idx = 0
    host_count = len(host_configs)
    clients_per_host = math.ceil(client_count / host_count)
    remaining_clients = client_count
    for host_config in host_configs:
        # the last host might not need to simulate as many clients as the rest of the hosts as we eagerly
        # assign clients to hosts.
        clients_on_this_host = min(clients_per_host, remaining_clients)
        assignment = {
            "host": host_config["host"],
            "workers": [],
        }
        assignments.append(assignment)

        workers_on_this_host = host_config["cores"]
        clients_per_worker = [0] * workers_on_this_host

        # determine how many clients each worker should simulate
        for c in range(clients_on_this_host):
            clients_per_worker[c % workers_on_this_host] += 1

        # assign client ids to workers
        for client_count_for_worker in clients_per_worker:
            worker_assignment = []
            assignment["workers"].append(worker_assignment)
            for c in range(client_idx, client_idx + client_count_for_worker):
                worker_assignment.append(c)
            client_idx += client_count_for_worker

        remaining_clients -= clients_on_this_host

    assert remaining_clients == 0

    return assignments


ClientAllocation = collections.namedtuple("ClientAllocation", ["client_id", "task"])


class ClientAllocations:
    def __init__(self):
        self.allocations = []

    def add(self, client_id, tasks):
        self.allocations.append({"client_id": client_id, "tasks": tasks})

    def is_joinpoint(self, task_index):
        return all(isinstance(t.task, JoinPoint) for t in self.tasks(task_index))

    def tasks(self, task_index, remove_empty=True):
        current_tasks = []
        for allocation in self.allocations:
            tasks_at_index = allocation["tasks"][task_index]
            if remove_empty and tasks_at_index is not None:
                current_tasks.append(ClientAllocation(allocation["client_id"], tasks_at_index))
        return current_tasks


class Worker(actor.RallyActorBase):
    """
    The actual worker that applies load against the cluster(s).

    It runs the tasks of its clients on the actor's event loop and regularly sends measurements to the driver so it can
    consolidate them.
    """

    # how often samples are sent to the driver
    SAMPLE_PUSH_INTERVAL_SECONDS = 5
    # how long a worker waits for its clients to finish their current request when it is stopped
    STOP_TIMEOUT_SECONDS = 30

    def __init__(self, driver_actor, worker_id, cfg: types.Config):
        """
        :param driver_actor: Handle of the ``DriverActor`` that coordinates this worker.
        :param worker_id: Unique (numeric) id of the worker.
        :param cfg: Rally internal configuration object of the coordinator.
        """
        super().__init__(name=f"worker-{worker_id}", cfg=cfg)
        self.driver_actor = driver_actor
        self.worker_id = worker_id
        # load node-specific config to have correct paths available
        self.config: types.Config = load_local_config(cfg)
        load_track(self.config, install_dependencies=False)
        self.logger.debug("Worker[%d] has Python load path %s after bootstrap.", self.worker_id, sys.path)
        self.track = None
        self.client_allocations = None
        self.client_contexts = None
        self.current_task_index = 0
        self.next_task_index = 0
        self.on_error: OnErrorBehavior = OnErrorBehavior.CONTINUE
        # set when the worker is stopped; checked by clients after each request
        self.cancel = threading.Event()
        # used to indicate that we want to prematurely consider this completed. This is *not* due to cancellation
        # but a regular event in a benchmark and used to model task dependency of parallel tasks.
        self.complete = threading.Event()
        self.sampler = None
        self.sample_push_interval = Worker.SAMPLE_PUSH_INTERVAL_SECONDS
        self.sample_queue_size = None
        self._step: Optional[asyncio.Task] = None
        self._scheduled_drive: Optional[asyncio.TimerHandle] = None
        self._stopped = asyncio.Event()

    def _fail(self, failure):
        super()._fail(failure)
        self._stopped.set()

    @actor.convert_failures("worker")
    async def run(self, t, client_allocations, client_contexts):
        """
        Starts executing the first step of the benchmark. Subsequent steps are started by ``drive_at()``.

        :param t: The track to use.
        :param client_allocations: A structure describing which clients need to run which tasks.
        :param client_contexts: A dict of ``ClientContext`` objects keyed by client ID.
        :return: only when the worker is stopped. Raises ``BenchmarkFailure`` if the worker failed.
        """
        self.logger.info("Worker[%d] is about to start.", self.worker_id)
        self.on_error = self.config.opts("driver", "on.error")
        self.sample_queue_size = int(self.config.opts("reporting", "sample.queue.size", mandatory=False, default_value=1 << 20))
        self.track = t
        track.set_absolute_data_path(self.config, self.track)
        self.client_allocations = client_allocations
        self.client_contexts = client_contexts
        self.current_task_index = 0
        # we need to send samples more often in test mode
        if self.config.opts("track", "test.mode.enabled"):
            self.sample_push_interval = 0.5
        loop = asyncio.get_running_loop()
        loop.set_debug(self.config.opts("system", "async.debug", mandatory=False, default_value=False))
        loop.set_exception_handler(self._logging_exception_handler)
        runner.register_default_runners(self.config)
        if self.track.has_plugins:
            track.load_track_plugins(self.config, self.track.name, runner.register_runner, scheduler.register_scheduler)
        sample_pusher = asyncio.create_task(self._push_samples_periodically())
        try:
            self._drive()
            await self._stopped.wait()
        finally:
            sample_pusher.cancel()
        if self._failure is not None:
            raise self._failure

    def drive_at(self, client_start_timestamp):
        """
        Starts the next step at the provided time.

        :param client_start_timestamp: When to start the next step, as a ``time.perf_counter()`` value of this worker.
        """
        sleep_time = client_start_timestamp - time.perf_counter()
        self.logger.debug(
            "Worker[%d] is continuing its work at task index [%d] on [%f], that is in [%f] seconds.",
            self.worker_id,
            self.current_task_index,
            client_start_timestamp,
            sleep_time,
        )
        self._scheduled_drive = asyncio.get_running_loop().call_later(max(sleep_time, 0), self._drive)

    def complete_current_task(self):
        # finish now ASAP. Remaining samples will be sent with the next periodic push. We will also need to skip to the next
        # JoinPoint. But if we are already at a JoinPoint at the moment, there is nothing to do.
        if self.at_joinpoint():
            self.logger.info(
                "Worker[%s] has been asked to complete its current task but is currently at join point at index [%d]. Ignoring.",
                str(self.worker_id),
                self.current_task_index,
            )
        else:
            self.logger.info(
                "Worker[%s] has been asked to complete its current task. Completing tasks at index [%d].",
                str(self.worker_id),
                self.current_task_index,
            )
            self.complete.set()

    async def stop(self):
        """
        Stops the worker: clients finish their current request and ``run()`` returns.
        """
        self.logger.debug("Worker[%s] is stopping.", str(self.worker_id))
        self.cancel.set()
        if self._scheduled_drive is not None:
            self._scheduled_drive.cancel()
        step = self._step
        if step is not None and not step.done():
            try:
                await asyncio.wait_for(asyncio.shield(step), Worker.STOP_TIMEOUT_SECONDS)
            except asyncio.TimeoutError:
                self.logger.warning("Worker[%s] did not stop within [%d] seconds.", str(self.worker_id), Worker.STOP_TIMEOUT_SECONDS)
                step.cancel()
        self._stopped.set()
        self.logger.debug("Worker[%s] has stopped.", str(self.worker_id))

    def _logging_exception_handler(self, loop, context):
        self.logger.error("Uncaught exception in event loop: %s", context)

    @actor.report_failures("worker")
    def _drive(self):
        self._scheduled_drive = None
        if self._stopped.is_set() or self.cancel.is_set():
            return
        task_allocations = self.current_tasks_and_advance()
        # skip non-tasks in the task list
        while len(task_allocations) == 0:
            task_allocations = self.current_tasks_and_advance()

        if self.at_joinpoint():
            self.logger.debug("Worker[%d] reached join point at index [%d].", self.worker_id, self.current_task_index)
            self._push_samples()
            self.complete.clear()
            self.sampler = None
            # Using perf_counter here is fine even in the distributed case. Although we "leak" this value to other
            # machines, we will only ever interpret this value on the same machine (see `drive_at()` and the
            # implementation in `Driver#joinpoint_reached()`).
            self.driver_actor.joinpoint_reached.remote(self.worker_id, time.perf_counter(), task_allocations)
        elif self.complete.is_set():
            # There may be a situation where there are more (parallel) tasks than workers. If we were asked to complete all tasks, we not
            # only need to complete actively running tasks but actually all scheduled tasks until we reach the next join point.
            self.logger.info(
                "Worker[%d] skips tasks at index [%d] because it has been asked to complete all tasks until next join point.",
                self.worker_id,
                self.current_task_index,
            )
            asyncio.get_running_loop().call_soon(self._drive)
        else:
            self.logger.debug("Worker[%d] is executing tasks at index [%d].", self.worker_id, self.current_task_index)
            self.sampler = Sampler(start_timestamp=time.perf_counter(), buffer_size=self.sample_queue_size)
            executor = AsyncIoAdapter(
                self.config,
                self.track,
                task_allocations,
                self.sampler,
                self.cancel,
                self.complete,
                self.on_error,
                self.client_contexts,
                self.worker_id,
            )
            self._step = asyncio.get_running_loop().create_task(self._run_step(executor))

    async def _run_step(self, executor):
        try:
            await executor.run()
        except asyncio.CancelledError:
            raise
        except BaseException as e:
            self.logger.exception("Worker[%s] has detected a benchmark failure. Notifying master...", str(self.worker_id))
            # the exception might be user-defined and not be on the load path of the master driver. Hence, it cannot be
            # deserialized on the receiver so we convert it here to a plain string.
            self._fail(actor.BenchmarkFailure(f"Error in load generator [{self.worker_id}]", str(e)))
            return
        if self.cancel.is_set():
            self.logger.info("Worker[%s] has stopped executing tasks because it is stopping.", str(self.worker_id))
            return
        self.logger.debug("Worker[%s] is ready for the next task.", str(self.worker_id))
        # the next step uses a new sampler
        self._push_samples()
        self._drive()

    @actor.report_failures("worker")
    async def _push_samples_periodically(self):
        while True:
            await asyncio.sleep(self.sample_push_interval)
            current_samples = self._push_samples()
            if current_samples:
                most_recent_sample = current_samples[-1]
                if most_recent_sample.percent_completed is not None:
                    self.logger.debug(
                        "Worker[%s] is executing [%s] (%.2f%% complete).",
                        str(self.worker_id),
                        most_recent_sample.task,
                        most_recent_sample.percent_completed * 100.0,
                    )
                else:
                    # TODO: This could be misleading given that one worker could execute more than one task...
                    self.logger.debug(
                        "Worker[%s] is executing [%s] (dependent eternal task).", str(self.worker_id), most_recent_sample.task
                    )
            else:
                self.logger.debug("Worker[%s] is executing (no samples).", str(self.worker_id))

    def at_joinpoint(self):
        return self.client_allocations.is_joinpoint(self.current_task_index)

    def current_tasks_and_advance(self):
        self.current_task_index = self.next_task_index
        current = self.client_allocations.tasks(self.current_task_index)
        self.next_task_index += 1
        self.logger.debug("Worker[%d] is at task index [%d].", self.worker_id, self.current_task_index)
        return current

    def _push_samples(self):
        if self.sampler:
            samples = self.sampler.samples
            if len(samples) > 0:
                self.driver_actor.update_samples.remote(self.worker_id, samples)
            return samples
        return None


class Sampler:
    """
    Encapsulates management of gathered samples.
    """

    def __init__(self, start_timestamp, buffer_size=16384):
        self.start_timestamp = start_timestamp
        self.q = queue.Queue(maxsize=buffer_size)
        self.logger = logging.getLogger(__name__)

    def add(
        self,
        task,
        client_id,
        sample_type,
        meta_data,
        absolute_time,
        request_start,
        latency,
        service_time,
        processing_time,
        throughput,
        ops,
        ops_unit,
        time_period,
        percent_completed,
        dependent_timing=None,
    ):
        try:
            self.q.put_nowait(
                Sample(
                    client_id,
                    absolute_time,
                    request_start,
                    self.start_timestamp,
                    task,
                    sample_type,
                    meta_data,
                    latency,
                    service_time,
                    processing_time,
                    throughput,
                    ops,
                    ops_unit,
                    time_period,
                    percent_completed,
                    dependent_timing,
                )
            )
        except queue.Full:
            self.logger.warning("Dropping sample for [%s] due to a full sampling queue.", task.operation.name)

    @property
    def samples(self):
        samples = []
        try:
            while True:
                samples.append(self.q.get_nowait())
        except queue.Empty:
            pass
        return samples


class Sample:
    def __init__(
        self,
        client_id,
        absolute_time,
        request_start,
        task_start,
        task,
        sample_type,
        request_meta_data,
        latency,
        service_time,
        processing_time,
        throughput,
        total_ops,
        total_ops_unit,
        time_period,
        percent_completed,
        dependent_timing=None,
        operation_name=None,
        operation_type=None,
    ):
        self.client_id = client_id
        self.absolute_time = absolute_time
        self.request_start = request_start
        self.task_start = task_start
        self.task = task
        self.sample_type = sample_type
        self.request_meta_data = request_meta_data
        self.latency = latency
        self.service_time = service_time
        self.processing_time = processing_time
        self.throughput = throughput
        self.total_ops = total_ops
        self.total_ops_unit = total_ops_unit
        self.time_period = time_period
        self._dependent_timing = dependent_timing
        self._operation_name = operation_name
        self._operation_type = operation_type
        # may be None for eternal tasks!
        self.percent_completed = percent_completed

    @property
    def operation_name(self):
        return self._operation_name if self._operation_name else self.task.operation.name

    @property
    def operation_type(self):
        return self._operation_type if self._operation_type else self.task.operation.type

    @property
    def operation_meta_data(self):
        return self.task.operation.meta_data

    @property
    def relative_time(self):
        return self.request_start - self.task_start

    @property
    def dependent_timings(self):
        if self._dependent_timing:
            for t in self._dependent_timing:
                if t:
                    yield from self._dependent_sample(t, self.request_meta_data)

    def _dependent_sample(self, dependent_timing, meta_data):
        """Recursively flatten dependent timings while accumulating parent-level metadata."""
        if not isinstance(dependent_timing, dict):
            return
        timing = dependent_timing.get("dependent_timing")
        current_meta_data = self._merge(meta_data, self._metadata_without_dependent_timing(dependent_timing))

        if isinstance(timing, dict):
            yield Sample(
                self.client_id,
                timing["absolute_time"],
                timing["request_start"],
                self.task_start,
                self.task,
                self.sample_type,
                current_meta_data,
                0,
                timing["service_time"],
                0,
                0,
                self.total_ops,
                self.total_ops_unit,
                self.time_period,
                self.percent_completed,
                None,
                timing["operation"],
                timing["operation-type"],
            )
        elif timing:
            for sub_timing in timing:
                if sub_timing:
                    yield from self._dependent_sample(sub_timing, current_meta_data)

    def _metadata_without_dependent_timing(self, dependent_timing):
        """Extract metadata fields, excluding the nested dependent timing payload."""
        return {k: v for k, v in dependent_timing.items() if k != "dependent_timing"}

    def __repr__(self, *args, **kwargs):
        return (
            f"[{self.absolute_time}; {self.relative_time}] [client [{self.client_id}]] [{self.task}] "
            f"[{self.sample_type}]: [{self.latency}s] request latency, [{self.service_time}s] service time, "
            f"[{self.total_ops} {self.total_ops_unit}]"
        )

    def _merge(self, *args):
        result = {}
        for arg in args:
            if arg is not None:
                result.update(arg)
        return result


def select_challenge(config: types.Config, t):
    challenge_name = config.opts("track", "challenge.name")
    selected_challenge = t.find_challenge_or_default(challenge_name)

    if not selected_challenge:
        raise exceptions.SystemSetupError(
            "Unknown challenge [%s] for track [%s]. You can list the available tracks and their "
            "challenges with %s list tracks." % (challenge_name, t.name, PROGRAM_NAME)
        )
    return selected_challenge


class ThroughputCalculator:
    class TaskStats:
        """
        Stores per task numbers needed for throughput calculation in between multiple calculations.
        """

        def __init__(self, bucket_interval, sample_type, start_time):
            self.unprocessed = []
            self.total_count = 0
            self.interval = 0
            self.bucket_interval = bucket_interval
            # the first bucket is complete after one bucket interval is over
            self.bucket = bucket_interval
            self.sample_type = sample_type
            self.has_samples_in_sample_type = False
            # start relative to the beginning of our (calculation) time slice.
            self.start_time = start_time

        @property
        def throughput(self):
            return self.total_count / self.interval

        def maybe_update_sample_type(self, current_sample_type):
            if self.sample_type < current_sample_type:
                self.sample_type = current_sample_type
                self.has_samples_in_sample_type = False

        def update_interval(self, absolute_sample_time):
            self.interval = max(absolute_sample_time - self.start_time, self.interval)

        def can_calculate_throughput(self):
            return self.interval > 0 and self.interval >= self.bucket

        def can_add_final_throughput_sample(self):
            return self.interval > 0 and not self.has_samples_in_sample_type

        def finish_bucket(self, new_total):
            self.unprocessed = []
            self.total_count = new_total
            self.has_samples_in_sample_type = True
            self.bucket = int(self.interval) + self.bucket_interval

    def __init__(self):
        self.task_stats = {}

    def calculate(self, samples, bucket_interval_secs=1):
        """
            Calculates global throughput based on samples gathered from multiple load generators.
            Samples are grouped by task, and in case of multi-cluster mode additionally by cluster.

        :param samples: A list containing all samples from all load generators.
        :param bucket_interval_secs: The bucket interval for aggregations.
        :return: A global view of throughput samples.
        """

        samples_per_task = {}
        # Group samples by (task, cluster_name) so multi-cluster throughput is calculated per cluster.
        for sample in samples:
            cluster_name = sample.request_meta_data.get("cluster") if sample.request_meta_data else None
            k = (sample.task, cluster_name)
            if k not in samples_per_task:
                samples_per_task[k] = []
            samples_per_task[k].append(sample)

        global_throughput = {}
        # with open("raw_samples_new.csv", "a") as sample_log:
        # print("client_id,absolute_time,relative_time,operation,sample_type,total_ops,time_period", file=sample_log)
        for k, v in samples_per_task.items():
            if k not in global_throughput:
                global_throughput[k] = []
            # sort all samples by time
            if k in self.task_stats:
                samples = itertools.chain(v, self.task_stats[k].unprocessed)
            else:
                samples = v
            current_samples = sorted(samples, key=lambda s: s.absolute_time)

            # Calculate throughput based on service time if the runner does not provide one, otherwise use it as is and
            # only transform the values into the expected structure.
            first_sample = current_samples[0]
            if first_sample.throughput is None:
                task_throughput = self.calculate_task_throughput(k, current_samples, bucket_interval_secs)
            else:
                task_throughput = self.map_task_throughput(current_samples)
            global_throughput[k].extend(task_throughput)

        return global_throughput

    def calculate_task_throughput(self, task_key, current_samples, bucket_interval_secs):
        task_throughput = []

        if task_key not in self.task_stats:
            first_sample = current_samples[0]
            self.task_stats[task_key] = ThroughputCalculator.TaskStats(
                bucket_interval=bucket_interval_secs,
                sample_type=first_sample.sample_type,
                start_time=first_sample.absolute_time - first_sample.time_period,
            )
        current = self.task_stats[task_key]
        count = current.total_count
        last_sample = None
        for sample in current_samples:
            last_sample = sample
            # print("%d,%f,%f,%s,%s,%d,%f" %
            #       (sample.client_id, sample.absolute_time, sample.relative_time, sample.operation, sample.sample_type,
            #        sample.total_ops, sample.time_period), file=sample_log)

            # once we have seen a new sample type, we stick to it.
            current.maybe_update_sample_type(sample.sample_type)

            # we need to store the total count separately and cannot update `current.total_count` immediately here
            # because we would count all raw samples in `unprocessed` twice. Hence, we'll only update
            # `current.total_count` when we have calculated a new throughput sample.
            count += sample.total_ops
            current.update_interval(sample.absolute_time)

            if current.can_calculate_throughput():
                current.finish_bucket(count)
                task_throughput.append(
                    (
                        sample.absolute_time,
                        sample.relative_time,
                        current.sample_type,
                        current.throughput,
                        # we calculate throughput per second
                        f"{sample.total_ops_unit}/s",
                    )
                )
            else:
                current.unprocessed.append(sample)

        # also include the last sample if we don't have one for the current sample type, even if it is below the bucket
        # interval (mainly needed to ensure we show throughput data in test mode)
        if last_sample is not None and current.can_add_final_throughput_sample():
            current.finish_bucket(count)
            task_throughput.append(
                (
                    last_sample.absolute_time,
                    last_sample.relative_time,
                    current.sample_type,
                    current.throughput,
                    f"{last_sample.total_ops_unit}/s",
                )
            )

        return task_throughput

    def map_task_throughput(self, current_samples):
        throughput = []
        for sample in current_samples:
            throughput.append(
                (
                    sample.absolute_time,
                    sample.relative_time,
                    sample.sample_type,
                    sample.throughput,
                    f"{sample.total_ops_unit}/s",
                )
            )
        return throughput


class AsyncIoAdapter:
    def __init__(
        self, cfg: types.Config, track, task_allocations, sampler, cancel, complete, on_error: OnErrorBehavior, client_contexts, worker_id
    ):
        self.cfg = cfg
        self.track = track
        self.task_allocations = task_allocations
        self.sampler = sampler
        self.cancel = cancel
        self.complete = complete
        self.on_error = on_error
        self.client_contexts = client_contexts
        self.parent_worker_id = worker_id
        self.profiling_enabled = self.cfg.opts("driver", "profiling")
        self.assertions_enabled = self.cfg.opts("driver", "assertions")
        self.debug_event_loop = self.cfg.opts("system", "async.debug", mandatory=False, default_value=False)
        self.logger = logging.getLogger(__name__)

    def __call__(self, *args, **kwargs):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            loop.set_debug(self.debug_event_loop)
            loop.set_exception_handler(self._logging_exception_handler)
            asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.run())
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            loop.close()

    def _logging_exception_handler(self, loop, context):
        self.logger.error("Uncaught exception in event loop: %s", context)

    async def run(self):
        def es_clients(client_id, all_hosts, all_client_options, distribution_version, distribution_flavor):
            es = {}
            context = self.client_contexts.get(client_id)
            api_key = context.api_key
            for cluster_name, cluster_hosts in all_hosts.items():
                es[cluster_name] = client.EsClientFactory(
                    cluster_hosts,
                    all_client_options[cluster_name],
                    distribution_version=distribution_version,
                    distribution_flavor=distribution_flavor,
                ).create_async(api_key=api_key, client_id=client_id)
            return EsClients(es)

        if self.assertions_enabled:
            self.logger.info("Task assertions enabled")
        runner.enable_assertions(self.assertions_enabled)

        all_hosts = self.cfg.opts("client", "hosts").all_hosts
        client_options = self.cfg.opts("client", "options")
        distribution_version = self.cfg.opts("mechanic", "distribution.version", mandatory=False)
        distribution_flavor = self.cfg.opts("mechanic", "distribution.flavor", mandatory=False)
        all_client_options = client_options.all_client_options

        clients = []
        for client_id, task_allocation in self.task_allocations:
            es = es_clients(
                client_id,
                all_hosts,
                all_client_options,
                distribution_version=distribution_version,
                distribution_flavor=distribution_flavor,
            )
            clients.append(es)

        task_names = [t.task.task.name for t in self.task_allocations]

        awaitables = []
        if self.cfg.opts("driver", "multi.cluster", mandatory=False) and len(all_hosts) > 1:
            # Multi-cluster: run all clusters in parallel for this step
            for cluster_name in all_hosts:
                params_per_task = {}
                for (client_id, task_allocation), es in zip(self.task_allocations, clients):
                    task = task_allocation.task
                    if task not in params_per_task:
                        param_source = track.operation_parameters(self.track, task)
                        params_per_task[task] = param_source
                    schedule = schedule_for(task_allocation, params_per_task[task])
                    es_single = EsClients({"default": es[cluster_name]})
                    async_executor = AsyncExecutor(
                        client_id,
                        task,
                        schedule,
                        es_single,
                        self.sampler,
                        self.cancel,
                        self.complete,
                        task.error_behavior(self.on_error),
                        cluster_name=cluster_name,
                    )
                    final_executor = AsyncProfiler(async_executor) if self.profiling_enabled else async_executor
                    awaitables.append(final_executor())
            self.logger.info(
                "Worker[%s] executing tasks %s against clusters [%s] in parallel",
                self.parent_worker_id,
                task_names,
                ", ".join(all_hosts),
            )
        else:
            # Single cluster: current behavior
            params_per_task = {}
            for (client_id, task_allocation), es in zip(self.task_allocations, clients):
                task = task_allocation.task
                if task not in params_per_task:
                    param_source = track.operation_parameters(self.track, task)
                    params_per_task[task] = param_source
                schedule = schedule_for(task_allocation, params_per_task[task])
                async_executor = AsyncExecutor(
                    client_id,
                    task,
                    schedule,
                    es,
                    self.sampler,
                    self.cancel,
                    self.complete,
                    task.error_behavior(self.on_error),
                    cluster_name=None,
                )
                final_executor = AsyncProfiler(async_executor) if self.profiling_enabled else async_executor
                awaitables.append(final_executor())
            self.logger.info("Worker[%s] executing tasks: %s", self.parent_worker_id, task_names)

        run_start = time.perf_counter()
        try:
            await asyncio.gather(*awaitables)
        finally:
            run_end = time.perf_counter()
            self.logger.info(
                "Worker[%s] finished executing tasks %s in %f seconds",
                self.parent_worker_id,
                task_names,
                (run_end - run_start),
            )
            # Do not shut down async generators here: run() may execute on a long-lived event loop (the one of a
            # Worker actor) that executes more steps later. Async generators of this step are finalized when they are
            # garbage-collected.
            for c in clients:
                for conn in c.values():
                    await conn.close()
            transport_close_end = time.perf_counter()
            self.logger.debug("Total time to close transports: %f seconds.", (transport_close_end - run_end))


class AsyncProfiler:
    def __init__(self, target):
        """
        :param target: The actual executor which should be profiled.
        """
        self.target = target
        self.profile_logger = logging.getLogger("rally.profile")

    async def __call__(self, *args, **kwargs):
        # initialize lazily, we don't need it in the majority of cases
        # pylint: disable=import-outside-toplevel
        import io as python_io

        import yappi

        yappi.start()
        try:
            return await self.target(*args, **kwargs)
        finally:
            yappi.stop()
            s = python_io.StringIO()
            yappi.get_func_stats().print_all(
                out=s, columns={0: ("name", 140), 1: ("ncall", 8), 2: ("tsub", 8), 3: ("ttot", 8), 4: ("tavg", 8)}
            )

            profile = "\n=== Profile START ===\n"
            profile += s.getvalue()
            profile += "=== Profile END ==="
            self.profile_logger.info(profile)


class AsyncExecutor:
    def __init__(
        self,
        client_id,
        task,
        schedule,
        es,
        sampler,
        cancel,
        complete,
        on_error: OnErrorBehavior,
        cluster_name=None,
    ):
        """
        Executes tasks according to the schedule for a given operation.

        :param task: The task that is executed.
        :param schedule: The schedule for this task.
        :param es: Elasticsearch client that will be used to execute the operation.
        :param sampler: A container to store raw samples.
        :param cancel: A shared boolean that indicates we need to cancel execution.
        :param complete: A shared boolean that indicates we need to prematurely complete execution.
        :param on_error: An Enum of type `OnErrorBehaviour` specifying how the load generator should behave on errors.
        :param cluster_name: Optional name of the target cluster (for multi-cluster reporting).
        """
        self.client_id = client_id
        self.task = task
        self.op = task.operation
        self.schedule_handle = schedule
        self.es = es
        self.sampler = sampler
        self.cancel = cancel
        self.complete = complete
        self.on_error = on_error
        self.cluster_name = cluster_name
        self.logger = logging.getLogger(__name__)

    async def __call__(self, *args, **kwargs):
        any_task_completes_parent = self.task.any_completes_parent
        task_completes_parent = self.task.completes_parent
        total_start = time.perf_counter()
        # lazily initialize the schedule
        self.logger.debug("Initializing schedule for client id [%s].", self.client_id)
        schedule = self.schedule_handle()
        # Start the schedule's timer early so the warmup period is independent of any deferred start due to ramp-up
        self.schedule_handle.start()
        rampup_wait_time = self.schedule_handle.ramp_up_wait_time
        if rampup_wait_time:
            self.logger.debug("client id [%s] waiting [%.2f]s for ramp-up.", self.client_id, rampup_wait_time)
            await asyncio.sleep(rampup_wait_time)

        self.logger.debug("Entering main loop for client id [%s].", self.client_id)
        # noinspection PyBroadException
        try:
            async for expected_scheduled_time, sample_type, percent_completed, runner, params in schedule:
                if self.cancel.is_set():
                    self.logger.info("User cancelled execution.")
                    break
                absolute_expected_schedule_time = total_start + expected_scheduled_time
                throughput_throttled = expected_scheduled_time > 0
                if throughput_throttled:
                    rest = absolute_expected_schedule_time - time.perf_counter()
                    if rest > 0:
                        await asyncio.sleep(rest)

                absolute_processing_start = time.time()
                processing_start = time.perf_counter()
                self.schedule_handle.before_request(processing_start)
                es_client = self.es.default
                with es_client.new_request_context() as request_context:
                    total_ops, total_ops_unit, request_meta_data = await execute_single(runner, self.es, params, self.on_error)
                    request_start = request_context.request_start
                    request_end = request_context.request_end

                processing_end = time.perf_counter()
                service_time = request_end - request_start
                processing_time = processing_end - processing_start
                time_period = request_end - total_start
                self.schedule_handle.after_request(processing_end, total_ops, total_ops_unit, request_meta_data)
                # Allow runners to override the throughput calculation in very specific circumstances. Usually, Rally
                # assumes that throughput is the "amount of work" (determined by the "weight") per unit of time
                # (determined by the elapsed time period). However, in certain cases (e.g. shard recovery or other
                # long running operations where there is a dedicated stats API to determine progress), it is
                # advantageous if the runner calculates throughput directly. The following restrictions apply:
                #
                # * Only one client must call that runner (when throughput is calculated, it is aggregated across
                #   all clients but if the runner provides it, we take the value as is).
                # * The runner should be rate-limited as each runner call will result in one throughput sample.
                #
                throughput = request_meta_data.pop("throughput", None)
                # Do not calculate latency separately when we run unthrottled. This metric is just confusing then.
                latency = request_end - absolute_expected_schedule_time if throughput_throttled else service_time
                # If this task completes the parent task we should *not* check for completion by another client but
                # instead continue until our own runner has completed. We need to do this because the current
                # worker (process) could run multiple clients that execute the same task. We do not want all clients to
                # finish this task as soon as the first of these clients has finished but rather continue until the last
                # client has finished that task.
                if task_completes_parent:
                    completed = runner.completed
                else:
                    completed = self.complete.is_set() or runner.completed
                # last sample should bump progress to 100% if externally completed.
                if completed:
                    progress = 1.0
                elif runner.percent_completed:
                    progress = runner.percent_completed
                else:
                    progress = percent_completed

                sample_meta = dict(request_meta_data) if request_meta_data else {}
                if self.cluster_name is not None:
                    sample_meta["cluster"] = self.cluster_name
                self.sampler.add(
                    self.task,
                    self.client_id,
                    sample_type,
                    sample_meta,
                    absolute_processing_start,
                    request_start,
                    latency,
                    service_time,
                    processing_time,
                    throughput,
                    total_ops,
                    total_ops_unit,
                    time_period,
                    progress,
                    request_meta_data.pop("dependent_timing", None),
                )

                if completed:
                    self.logger.info("Task [%s] is considered completed due to external event.", self.task)
                    break
        except BaseException as e:
            self.logger.exception("Could not execute schedule")
            raise exceptions.RallyError(f"Cannot run task [{self.task}]: {e}") from None
        finally:
            # Actively set it if this task completes its parent
            if task_completes_parent:
                self.logger.info(
                    "Task [%s] completes parent. Client id [%s] is finished executing it and signals completion.",
                    self.task,
                    self.client_id,
                )
                self.complete.set()
            elif any_task_completes_parent:
                self.logger.info(
                    "Task [%s] completes parent. Client id [%s] is finished executing it and signals completion of all "
                    "remaining clients, immediately.",
                    self.task,
                    self.client_id,
                )
                self.complete.set()


async def execute_single(runner, es, params, on_error: OnErrorBehavior):
    """
    Invokes the given runner once and provides the runner's return value in a uniform structure.

    :return: a triple of: total number of operations, unit of operations, a dict of request meta data (may be None).
    """
    # pylint: disable=import-outside-toplevel
    import elasticsearch

    def _parse_headers(e):
        tracked_headers = [
            {
                "request_id": "X-Cloud-Request-Id",
                "handling_node": "X-Found-Handling-Instance",
                "handling_cluster": "X-Found-Handling-Cluster",
            }
        ]
        headers_meta = {}
        if hasattr(e, "errors") and e.errors and len(e.errors) > 0 and hasattr(e.errors[0], "headers"):
            for header in tracked_headers:
                for key, value in header.items():
                    if value in e.errors[0].headers:
                        headers_meta[key] = e.errors[0].headers[value]
        return headers_meta

    is_connection_error = False

    try:
        async with runner:
            return_value = await runner(es, params)
        if isinstance(return_value, tuple) and len(return_value) == 2:
            total_ops, total_ops_unit = return_value
            request_meta_data = {"success": True}
        elif isinstance(return_value, dict):
            total_ops = return_value.pop("weight", 1)
            total_ops_unit = return_value.pop("unit", "ops")
            request_meta_data = return_value
            if "success" not in request_meta_data:
                request_meta_data["success"] = True
        else:
            total_ops = 1
            total_ops_unit = "ops"
            request_meta_data = {"success": True}
    except elasticsearch.TransportError as e:
        # we *specifically* want to distinguish connection refused (a node died?) from connection timeouts
        # pylint: disable=unidiomatic-typecheck
        is_connection_error = type(e) is elasticsearch.ConnectionError

        total_ops = 0
        total_ops_unit = "ops"
        request_meta_data = {"success": False, "error-type": "transport"}
        # For the 'errors' attribute, errors are ordered from
        # most recently raised (index=0) to least recently raised (index=N)
        #
        # If an HTTP status code is available with the error it
        # will be stored under 'status'. If HTTP headers are available
        # they are stored under 'headers'.
        if e.errors:
            if hasattr(e.errors[0], "status"):
                request_meta_data["http-status"] = e.errors[0].status
            request_meta_data.update(_parse_headers(e))
        # connection timeout errors don't provide a helpful description
        if isinstance(e, elasticsearch.ConnectionTimeout):
            request_meta_data["error-description"] = "network connection timed out"
        else:
            error_description = e.message
            request_meta_data["error-description"] = error_description
    except elasticsearch.ApiError as e:
        total_ops = 0
        total_ops_unit = "ops"
        request_meta_data = {"success": False, "error-type": "api"}
        error_message = ""

        # Some runners return a raw response, causing the 'error' property to be a string literal of the bytes/BytesIO object,
        # we should avoid bubbling that up
        # e.g. ApiError(413, '<_io.BytesIO object at 0xffffaf146a70>')
        # errors="replace" so binary response bodies (e.g. OTLP ingest returns binary protobuf
        # error frames) don't crash the driver with UnicodeDecodeError. Undecodable bytes become
        # U+FFFD in the logged/recorded message.
        if isinstance(e.body, bytes):
            # could be an empty body
            if error_body := e.body.decode("utf-8", errors="replace"):
                error_message = error_body
            else:
                # to be consistent with an empty 'e.error'
                error_message = str(None)
        elif isinstance(e.body, BytesIO):
            # could be an empty body
            if error_body := e.body.read().decode("utf-8", errors="replace"):
                error_message = error_body
            else:
                # to be consistent with an empty 'e.error'
                error_message = str(None)
        # fallback to 'error' property if the body isn't bytes/BytesIO
        else:
            if isinstance(e.error, bytes):
                error_message = e.error.decode("utf-8", errors="replace")
            elif isinstance(e.error, BytesIO):
                error_message = e.error.read().decode("utf-8", errors="replace")
            else:
                # if the 'error' is empty, we get back str(None)
                error_message = e.error

        if isinstance(e.info, bytes):
            error_info = e.info.decode("utf-8", errors="replace")
        elif isinstance(e.info, BytesIO):
            error_info = e.info.read().decode("utf-8", errors="replace")
        else:
            error_info = e.info

        if error_info:
            error_message += f" ({error_info})"

        error_description = error_message

        request_meta_data["error-description"] = error_description
        if e.status_code:
            request_meta_data["http-status"] = e.status_code
        request_meta_data.update(_parse_headers(e))

    except KeyError as e:
        logging.getLogger(__name__).exception("Cannot execute runner [%s]; most likely due to missing parameters.", str(runner))
        msg = "Cannot execute [%s]. Provided parameters are: %s. Error: [%s]." % (str(runner), list(params.keys()), str(e))
        raise exceptions.SystemSetupError(msg)

    if not request_meta_data["success"]:
        if on_error == OnErrorBehavior.ABORT or (is_connection_error and on_error != OnErrorBehavior.CONTINUE_ON_NETWORK):
            msg = "Request returned an error. Error type: %s" % request_meta_data.get("error-type", "Unknown")

            if description := request_meta_data.get("error-description"):
                msg += f", Description: {description}"

            if http_status := request_meta_data.get("http-status"):
                msg += f", HTTP Status: {http_status}"

            raise exceptions.RallyAssertionError(msg)

    return total_ops, total_ops_unit, request_meta_data


class JoinPoint:
    def __init__(self, id, clients_executing_completing_task=None, any_task_completes_parent=None):
        """

        :param id: The join point's id.
        :param clients_executing_completing_task: An array of client indices which execute a task that can prematurely complete its parent
        element. Provide 'None' or an empty array if no task satisfies this predicate.
        """
        if clients_executing_completing_task is None:
            clients_executing_completing_task = []
        if any_task_completes_parent is None:
            any_task_completes_parent = []
        self.id = id
        self.any_task_completes_parent = any_task_completes_parent
        self.clients_executing_completing_task = clients_executing_completing_task
        self.num_clients_executing_completing_task = len(clients_executing_completing_task)
        self.preceding_task_completes_parent = self.num_clients_executing_completing_task > 0

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return isinstance(other, type(self)) and self.id == other.id

    def __repr__(self, *args, **kwargs):
        return "JoinPoint(%s)" % self.id


class TaskAllocation:
    def __init__(self, task, client_index_in_task, global_client_index, total_clients):
        """

        :param task: The current task which is always a leaf task.
        :param client_index_in_task: The task-specific index for the allocated client.
        :param global_client_index:  The globally unique index for the allocated client across
                                     all concurrently executed tasks.
        :param total_clients: The total number of clients executing tasks concurrently.
        """
        self.task = task
        self.client_index_in_task = client_index_in_task
        self.global_client_index = global_client_index
        self.total_clients = total_clients

    def __hash__(self):
        return hash(self.task) ^ hash(self.global_client_index)

    def __eq__(self, other):
        return isinstance(other, type(self)) and self.task == other.task and self.global_client_index == other.global_client_index

    def __repr__(self, *args, **kwargs):
        return (
            f"TaskAllocation [{self.client_index_in_task}/{self.task.clients}] for {self.task} "
            f"and [{self.global_client_index}/{self.total_clients}] in total"
        )


class Allocator:
    """
    Decides which operations runs on which client and how to partition them.
    """

    def __init__(self, schedule):
        self.schedule = schedule

    @property
    def allocations(self):
        """
        Calculates an allocation matrix consisting of two dimensions. The first dimension is the client. The second dimension are the task
         this client needs to run. The matrix shape is rectangular (i.e. it is not ragged). There are three types of entries in the matrix:

          1. Normal tasks: They need to be executed by a client.
          2. Join points: They are used as global coordination points which all clients need to reach until the benchmark can go on. They
                          indicate that a client has to wait until the master signals it can go on.
          3. `None`: These are inserted by the allocator to keep the allocation matrix rectangular. Clients have to skip `None` entries
                     until one of the other entry types are encountered.

        :return: An allocation matrix with the structure described above.
        """
        max_clients = self.clients
        allocations = [None] * max_clients
        for client_index in range(max_clients):
            allocations[client_index] = []
        join_point_id = 0
        # start with an artificial join point to allow master to coordinate that all clients start at the same time
        next_join_point = JoinPoint(join_point_id)
        for client_index in range(max_clients):
            allocations[client_index].append(next_join_point)
        join_point_id += 1

        for task in self.schedule:
            start_client_index = 0
            clients_executing_completing_task = []
            any_task_completes_parent = []
            for sub_task in task:
                for client_index in range(start_client_index, start_client_index + sub_task.clients):
                    # this is the actual client that will execute the task. It may differ from the logical one in case we over-commit (i.e.
                    # more tasks than actually available clients)
                    physical_client_index = client_index % max_clients
                    if sub_task.completes_parent:
                        clients_executing_completing_task.append(physical_client_index)
                    elif sub_task.any_completes_parent:
                        any_task_completes_parent.append(physical_client_index)

                    ta = TaskAllocation(
                        task=sub_task,
                        client_index_in_task=client_index - start_client_index,
                        global_client_index=client_index,
                        # if task represents a parallel structure this is the total number of clients
                        # executing sub-tasks concurrently.
                        total_clients=task.clients,
                    )
                    allocations[physical_client_index].append(ta)
                start_client_index += sub_task.clients
            # uneven distribution between tasks and clients, e.g. there are 5 (parallel) tasks but only 2 clients. Then, one of them
            # executes three tasks, the other one only two. So we need to fill in a `None` for the second one.
            if start_client_index % max_clients > 0:
                # pin the index range to [0, max_clients). This simplifies the code below.
                start_client_index = start_client_index % max_clients
                for client_index in range(start_client_index, max_clients):
                    allocations[client_index].append(None)

            # let all clients join after each task, then we go on
            next_join_point = JoinPoint(join_point_id, clients_executing_completing_task, any_task_completes_parent)
            for client_index in range(max_clients):
                allocations[client_index].append(next_join_point)
            join_point_id += 1
        return allocations

    @property
    def join_points(self):
        """
        :return: A list of all join points for this allocations.
        """
        return [allocation for allocation in self.allocations[0] if isinstance(allocation, JoinPoint)]

    @property
    def tasks_per_joinpoint(self):
        """

        Calculates a flat list of all tasks that are run in between join points.

        Consider the following schedule (2 clients):

        1. task1 and task2 run by both clients in parallel
        2. join point
        3. task3 run by client 1
        4. join point

        The results in: [{task1, task2}, {task3}]

        :return: A list of sets containing all tasks.
        """
        tasks = []
        current_tasks = set()

        allocs = self.allocations
        # assumption: the shape of allocs is rectangular (i.e. each client contains the same number of elements)
        for idx in range(0, len(allocs[0])):
            for client in range(0, self.clients):
                allocation = allocs[client][idx]
                if isinstance(allocation, TaskAllocation):
                    current_tasks.add(allocation.task)
                elif isinstance(allocation, JoinPoint) and len(current_tasks) > 0:
                    tasks.append(current_tasks)
                    current_tasks = set()

        return tasks

    @property
    def clients(self):
        """
        :return: The maximum number of clients involved in executing the given schedule.
        """
        max_clients = 1
        for task in self.schedule:
            max_clients = max(max_clients, task.clients)
        return max_clients


#######################################
#
# Scheduler related stuff
#
#######################################


# Runs a concrete schedule on one worker client
# Needs to determine the runners and concrete iterations per client.
def schedule_for(task_allocation, parameter_source):
    """
    Calculates a client's schedule for a given task.

    :param task_allocation: The task allocation that should be executed by this schedule.
    :param parameter_source: The parameter source that should be used for this task.
    :return: A generator for the operations the given client needs to perform for this task.
    """
    logger = logging.getLogger(__name__)
    task = task_allocation.task
    op = task.operation
    sched = scheduler.scheduler_for(task)

    # We cannot use the global client index here because we need to support parallel execution of tasks
    # with multiple clients. Consider the following scenario:
    #
    # * Clients 0-3 bulk index into indexA
    # * Clients 4-7 bulk index into indexB
    #
    # Now we need to ensure that we start partitioning parameters correctly in both cases. And that means we
    # need to start from (client) index 0 in both cases instead of 0 for indexA and 4 for indexB.
    client_index = task_allocation.client_index_in_task

    # guard all logging statements with the client index and only emit them for the first client. This information is
    # repetitive and floods the log with many clients.
    if client_index == 0:
        logger.debug("Choosing [%s] for [%s].", sched, task)
    runner_for_op = runner.runner_for(op.type)
    params_for_op = parameter_source.partition(client_index, task.clients)
    if hasattr(sched, "parameter_source"):
        if client_index == 0:
            logger.debug("Setting parameter source [%s] for scheduler [%s]", params_for_op, sched)
        sched.parameter_source = params_for_op

    if requires_time_period_schedule(task, runner_for_op, params_for_op):
        warmup_time_period = task.warmup_time_period if task.warmup_time_period else 0
        if client_index == 0:
            logger.info(
                "Creating time-period based schedule with [%s] distribution for [%s] with a warmup period of [%s] "
                "seconds and a time period of [%s] seconds.",
                task.schedule,
                task.name,
                str(warmup_time_period),
                str(task.time_period),
            )
        loop_control = TimePeriodBased(warmup_time_period, task.time_period)
    else:
        warmup_iterations = task.warmup_iterations if task.warmup_iterations else 0
        if task.iterations:
            iterations = task.iterations
        elif params_for_op.infinite:
            # this is usually the case if the parameter source provides a constant
            iterations = 1
        else:
            iterations = None
        if client_index == 0:
            logger.debug(
                "Creating iteration-count based schedule with [%s] distribution for [%s] with [%s] warmup "
                "iterations and [%s] iterations.",
                task.schedule,
                task.name,
                str(warmup_iterations),
                str(iterations),
            )
        loop_control = IterationBased(warmup_iterations, iterations)

    if client_index == 0:
        if loop_control.infinite:
            logger.debug("Parameter source will determine when the schedule for [%s] terminates.", task.name)
        else:
            logger.debug("%s schedule will determine when the schedule for [%s] terminates.", str(loop_control), task.name)

    return ScheduleHandle(task_allocation, sched, loop_control, runner_for_op, params_for_op)


def requires_time_period_schedule(task, task_runner, params):
    if task.warmup_time_period is not None or task.time_period is not None:
        return True
    # user has explicitly requested iterations
    if task.warmup_iterations is not None or task.iterations is not None:
        return False
    # the runner determines completion
    if task_runner.completed is not None:
        return True
    # If the parameter source ends after a finite amount of iterations, we will run with a time-based schedule
    return not params.infinite


class ScheduleHandle:
    def __init__(self, task_allocation, sched, task_progress_control, runner, params):
        """
        Creates a generator that will yield individual task invocations for the provided schedule.

        :param task_allocation: The task allocation for which the schedule is generated.
        :param sched: The scheduler for this task.
        :param task_progress_control: Controls how and how often this generator will loop.
        :param runner: The runner for a given operation.
        :param params: The parameter source for a given operation.
        :return: A generator for the corresponding parameters.
        """
        self.task_allocation = task_allocation
        self.operation_type = task_allocation.task.operation.type
        self.sched = sched
        self.task_progress_control = task_progress_control
        self.runner = runner
        self.params = params
        # TODO: Can we offload the parameter source execution to a different thread / process? Is this too heavy-weight?
        # from concurrent.futures import ThreadPoolExecutor
        # import asyncio
        # self.io_pool_exc = ThreadPoolExecutor(max_workers=1)
        # self.loop = asyncio.get_event_loop()

    @property
    def ramp_up_wait_time(self):
        """
        :return: the number of seconds to wait until this client should start so load can gradually ramp-up.
        """
        ramp_up_time_period = self.task_allocation.task.ramp_up_time_period
        if ramp_up_time_period:
            return ramp_up_time_period * (self.task_allocation.global_client_index / self.task_allocation.total_clients)
        else:
            return 0

    def start(self):
        self.task_progress_control.start()

    def before_request(self, now):
        self.sched.before_request(now)

    def after_request(self, now, weight, unit, request_meta_data):
        self.sched.after_request(now, weight, unit, request_meta_data)

    def params_with_operation_type(self):
        p = self.params.params()
        p.update({"operation-type": self.operation_type})
        return p

    async def __call__(self):
        next_scheduled = 0
        if self.task_progress_control.infinite:
            param_source_knows_progress = hasattr(self.params, "percent_completed")
            while True:
                try:
                    next_scheduled = self.sched.next(next_scheduled)
                    # does not contribute at all to completion. Hence, we cannot define completion.
                    percent_completed = self.params.percent_completed if param_source_knows_progress else None
                    # current_params = await self.loop.run_in_executor(self.io_pool_exc, self.params.params)
                    yield (
                        next_scheduled,
                        self.task_progress_control.sample_type,
                        percent_completed,
                        self.runner,
                        self.params_with_operation_type(),
                    )
                    self.task_progress_control.next()
                except StopIteration:
                    return
        else:
            while not self.task_progress_control.completed:
                try:
                    next_scheduled = self.sched.next(next_scheduled)
                    # current_params = await self.loop.run_in_executor(self.io_pool_exc, self.params.params)
                    yield (
                        next_scheduled,
                        self.task_progress_control.sample_type,
                        self.task_progress_control.percent_completed,
                        self.runner,
                        self.params_with_operation_type(),
                    )
                    self.task_progress_control.next()
                except StopIteration:
                    return


class TimePeriodBased:
    def __init__(self, warmup_time_period, time_period):
        self._warmup_time_period = warmup_time_period
        self._time_period = time_period
        if warmup_time_period is not None and time_period is not None:
            self._duration = self._warmup_time_period + self._time_period
        else:
            self._duration = None
        self._start = None
        self._now = None

    def start(self):
        self._now = time.perf_counter()
        self._start = self._now

    @property
    def _elapsed(self):
        return self._now - self._start

    @property
    def sample_type(self):
        return metrics.SampleType.Warmup if self._elapsed < self._warmup_time_period else metrics.SampleType.Normal

    @property
    def infinite(self):
        return self._time_period is None

    @property
    def percent_completed(self):
        return self._elapsed / self._duration

    @property
    def completed(self):
        return self._now >= (self._start + self._duration)

    def next(self):
        self._now = time.perf_counter()

    def __str__(self):
        return "time-period-based"


class IterationBased:
    def __init__(self, warmup_iterations, iterations):
        self._warmup_iterations = warmup_iterations
        self._iterations = iterations
        if warmup_iterations is not None and iterations is not None:
            self._total_iterations = self._warmup_iterations + self._iterations
            if self._total_iterations == 0:
                raise exceptions.RallyAssertionError("Operation must run at least for one iteration.")
        else:
            self._total_iterations = None
        self._it = None

    def start(self):
        self._it = 0

    @property
    def sample_type(self):
        return metrics.SampleType.Warmup if self._it < self._warmup_iterations else metrics.SampleType.Normal

    @property
    def infinite(self):
        return self._iterations is None

    @property
    def percent_completed(self):
        return (self._it + 1) / self._total_iterations

    @property
    def completed(self):
        return self._it >= self._total_iterations

    def next(self):
        self._it += 1

    def __str__(self):
        return "iteration-count-based"
