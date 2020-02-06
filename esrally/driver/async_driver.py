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


import logging
from esrally.driver import driver, runner, scheduler
import time
import asyncio
import threading

from esrally import exceptions, metrics, track, client, PROGRAM_NAME, telemetry
from esrally.utils import console, convert
import concurrent.futures


# TODO: Inline this code later
class PseudoActor:
    def __init__(self, cfg, current_race):
        self.cfg = cfg
        self.race = current_race

    def on_cluster_details_retrieved(self, cluster_details):
        #self.cluster_details = cluster_details
        pass

    def on_benchmark_complete(self, metrics_store):
        # TODO: Should we do this in race control instead?
        from esrally import reporter
        final_results = metrics.calculate_results(metrics_store, self.race)
        metrics.results_store(self.cfg).store_results(self.race)
        reporter.summarize(final_results, self.cfg)


def race(cfg):
    logger = logging.getLogger(__name__)
    # TODO: Taken from BenchmarkActor#setup()
    t = track.load_track(cfg)
    track_revision = cfg.opts("track", "repository.revision", mandatory=False)
    challenge_name = cfg.opts("track", "challenge.name")
    challenge = t.find_challenge_or_default(challenge_name)
    if challenge is None:
        raise exceptions.SystemSetupError("Track [%s] does not provide challenge [%s]. List the available tracks with %s list tracks."
                                          % (t.name, challenge_name, PROGRAM_NAME))
    if challenge.user_info:
        console.info(challenge.user_info)
    current_race = metrics.create_race(cfg, t, challenge, track_revision)

    metrics_store = metrics.metrics_store(
        cfg,
        track=current_race.track_name,
        challenge=current_race.challenge_name,
        read_only=False
    )
    race_store = metrics.race_store(cfg)

    a = PseudoActor(cfg, current_race)

    d = AsyncDriver(a, cfg)
    logger.info("Preparing benchmark...")
    cluster_info = d.prepare_benchmark(t)
    # TODO: ensure we execute the code in after_track_prepared (from the original actor)
    # def after_track_prepared(self):
    #     cluster_version = self.cluster_details["version"] if self.cluster_details else {}
    #     for child in self.children:
    #         self.send(child, thespian.actors.ActorExitRequest())
    #     self.children = []
    #     self.send(self.start_sender, driver.PreparationComplete(
    #         # older versions (pre 6.3.0) don't expose build_flavor because the only (implicit) flavor was "oss"
    #         cluster_version.get("build_flavor", "oss"),
    #         cluster_version.get("number", "Unknown"),
    #         cluster_version.get("build_hash", "Unknown")
    #     ))
    logger.info("Running benchmark...")
    d.start_benchmark()


# TODO: Move to time.py
class Timer:
    def __init__(self, fn, interval, stop_event):
        self.stop_event = stop_event
        self.fn = fn
        self.interval = interval
        # check at least once a second whether we need to exit
        self.wakeup_interval = min(self.interval, 1)

    def __call__(self, *args, **kwargs):
        while not self.stop_event.is_set():
            self.fn(*args, **kwargs)
            # allow early exit even if a longer sleeping period is requested
            for _ in range(self.interval):
                if self.stop_event.is_set():
                    break
                time.sleep(self.wakeup_interval)


class AsyncDriver:
    def __init__(self, target, config, es_client_factory_class=client.EsClientFactory):
        """
        Coordinates all workers. It is technology-agnostic, i.e. it does not know anything about actors. To allow us to hook in an actor,
        we provide a ``target`` parameter which will be called whenever some event has occurred. The ``target`` can use this to send
        appropriate messages.

        :param target: A target that will be notified of important events.
        :param config: The current config object.
        """
        self.logger = logging.getLogger(__name__)
        self.target = target
        self.config = config
        self.es_client_factory = es_client_factory_class
        self.track = None
        self.challenge = None
        self.metrics_store = None
        self.drivers = []

        self.progress_reporter = console.progress()
        self.progress_counter = 0
        self.quiet = False
        self.allocations = None
        self.raw_samples = []
        self.throughput_calculator = None
        self.most_recent_sample_per_client = {}

        self.number_of_steps = 0
        self.currently_completed = 0
        self.clients_completed_current_step = {}
        self.current_step = -1
        self.tasks_per_join_point = None
        self.complete_current_task_sent = False
        self.current_tasks = []

        self.telemetry = None
        self.es_clients = None

        self._finished = False
        self.abort_on_error = self.config.opts("driver", "on.error") == "abort"
        self.profiling_enabled = self.config.opts("driver", "profiling")
        self.use_uvloop = self.config.opts("driver", "uvloop")
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)
        self.stop_timer_tasks = threading.Event()
        self.sampler = None

    def create_es_clients(self, sync=True):
        all_hosts = self.config.opts("client", "hosts").all_hosts
        es = {}
        for cluster_name, cluster_hosts in all_hosts.items():
            all_client_options = self.config.opts("client", "options").all_client_options
            cluster_client_options = dict(all_client_options[cluster_name])
            # Use retries to avoid aborts on long living connections for telemetry devices
            cluster_client_options["retry-on-timeout"] = True

            client_factory = self.es_client_factory(cluster_hosts, cluster_client_options)
            if sync:
                es[cluster_name] = client_factory.create()
            else:
                es[cluster_name] = client_factory.create_async()
        return es

    def prepare_telemetry(self):
        enabled_devices = self.config.opts("telemetry", "devices")
        telemetry_params = self.config.opts("telemetry", "params")

        es = self.es_clients
        es_default = self.es_clients["default"]
        self.telemetry = telemetry.Telemetry(enabled_devices, devices=[
            telemetry.NodeStats(telemetry_params, es, self.metrics_store),
            telemetry.ExternalEnvironmentInfo(es_default, self.metrics_store),
            telemetry.ClusterEnvironmentInfo(es_default, self.metrics_store),
            telemetry.JvmStatsSummary(es_default, self.metrics_store),
            telemetry.IndexStats(es_default, self.metrics_store),
            telemetry.MlBucketProcessingTime(es_default, self.metrics_store),
            telemetry.CcrStats(telemetry_params, es, self.metrics_store),
            telemetry.RecoveryStats(telemetry_params, es, self.metrics_store)
        ])

    def wait_for_rest_api(self):
        skip_rest_api_check = self.config.opts("mechanic", "skip.rest.api.check")
        if skip_rest_api_check:
            self.logger.info("Skipping REST API check.")
        else:
            es_default = self.es_clients["default"]
            self.logger.info("Checking if REST API is available.")
            if client.wait_for_rest_layer(es_default, max_attempts=40):
                self.logger.info("REST API is available.")
            else:
                self.logger.error("REST API layer is not yet available. Stopping benchmark.")
                raise exceptions.SystemSetupError("Elasticsearch REST API layer is not available.")

    def retrieve_cluster_info(self):
        try:
            return self.es_clients["default"].info()
        except BaseException:
            self.logger.exception("Could not retrieve cluster info on benchmark start")
            return None

    def prepare_benchmark(self, t):
        self.track = t
        self.challenge = driver.select_challenge(self.config, self.track)
        self.quiet = self.config.opts("system", "quiet.mode", mandatory=False, default_value=False)
        self.throughput_calculator = driver.ThroughputCalculator()
        self.metrics_store = metrics.metrics_store(cfg=self.config,
                                                   track=self.track.name,
                                                   challenge=self.challenge.name,
                                                   read_only=False)
        self.es_clients = self.create_es_clients()
        self.wait_for_rest_api()
        self.prepare_telemetry()

        if self.track.has_plugins:
            track.track_repo(self.config, fetch=True, update=True)
            # we also need to load track plugins eagerly as the respective parameter sources could require
            track.load_track_plugins(self.config, runner.register_runner, scheduler.register_scheduler)
        track.prepare_track(self.track, self.config)

        return self.retrieve_cluster_info()

    def start_benchmark(self):
        self.logger.info("Benchmark is about to start.")
        # ensure relative time starts when the benchmark starts.
        self.reset_relative_time()
        self.logger.info("Attaching cluster-level telemetry devices.")
        self.telemetry.on_benchmark_start()
        self.logger.info("Cluster-level telemetry devices are now attached.")
        # TODO: Turn the intervals into constants
        self.pool.submit(Timer(fn=self.update_samples, interval=1, stop_event=self.stop_timer_tasks))
        self.pool.submit(Timer(fn=self.post_process_samples, interval=30, stop_event=self.stop_timer_tasks))
        self.pool.submit(Timer(fn=self.update_progress_message, interval=1, stop_event=self.stop_timer_tasks))

        # needed because a new thread does not have an event loop (see https://stackoverflow.com/questions/48725890/)
        if self.use_uvloop:
            self.logger.info("Using uvloop to perform asyncio.")
            import uvloop
            uvloop.install()
        else:
            self.logger.info("Using standard library implementation to perform asyncio.")

        loop = asyncio.new_event_loop()
        loop.set_debug(True)
        asyncio.set_event_loop(loop)
        loop.set_exception_handler(debug_exception_handler)

        track.set_absolute_data_path(self.config, self.track)
        runner.register_default_runners()
        # TODO: I think we can skip this here - it has already been done earlier in prepare_benchmark()
        if self.track.has_plugins:
            track.load_track_plugins(self.config, runner.register_runner, scheduler.register_scheduler)
        success = False
        try:
            benchmark_runner = AsyncProfiler(self.run_benchmark) if self.profiling_enabled else self.run_benchmark
            loop.run_until_complete(benchmark_runner())

            self._finished = True
            self.telemetry.on_benchmark_stop()
            self.logger.info("All steps completed.")
            success = True
        finally:
            self.stop_timer_tasks.set()
            self.pool.shutdown()
            loop.close()
            self.progress_reporter.finish()
            if success:
                self.target.on_benchmark_complete(self.metrics_store)
            self.logger.debug("Closing metrics store...")
            self.metrics_store.close()
            # immediately clear as we don't need it anymore and it can consume a significant amount of memory
            del self.metrics_store

    async def run_benchmark(self):
        # avoid: aiohttp.internal WARNING The object should be created from async function
        es = self.create_es_clients(sync=False)
        try:
            cancel = threading.Event()
            # used to indicate that we want to prematurely consider this completed. This is *not* due to cancellation but a regular event in
            # a benchmark and used to model task dependency of parallel tasks.
            complete = threading.Event()

            for task in self.challenge.schedule:
                for sub_task in task:
                    self.logger.info("Running task [%s] with [%d] clients...", sub_task.name, sub_task.clients)
                    #console.println("Running task [{}] with [{}] clients...".format(sub_task.name, sub_task.clients), logger=self.logger.info)

                    # TODO: We need to restructure this later on: We could have only one sampler for the whole benchmark but then we need to
                    #       provide the current task to the sampler. This would also simplify #update_samples(). We also need to move the
                    #       join point (done, pending = await asyncio.wait(aws)) below one level out so we can actually run all sub-tasks of
                    #       a task in parallel. At the moment we'd run one after the other (which is plain wrong)
                    self.current_tasks = [sub_task]
                    self.sampler = driver.Sampler(None, task, start_timestamp=time.perf_counter())
                    aws = []
                    # TODO: This is lacking support for one (sub)task being able to complete a complete parallel
                    #       structure. We can probably achieve that by waiting for the task in question and then
                    #       cancelling all other ongoing clients.
                    for client_id in range(sub_task.clients):
                        schedule = driver.schedule_for(self.track, sub_task, client_id)
                        e = AsyncExecutor(client_id, sub_task, schedule, es, self.sampler, cancel, complete, self.abort_on_error)
                        aws.append(e())
                    # join point
                    done, pending = await asyncio.wait(aws)
                    self.logger.info("All clients have finished running task [%s]", sub_task.name)
                    # drain the active samples before we move on to the next task
                    self.update_samples()
                    self.post_process_samples()
                    self.reset_relative_time()
                    self.update_progress_message(task_finished=True)


                    #for client_index in range(start_client_index, start_client_index + sub_task.clients):
                    # this is the actual client that will execute the task. It may differ from the logical one in case we over-commit (i.e.
                    # more tasks than actually available clients)
                    #     physical_client_index = client_index % max_clients
                    #     if sub_task.completes_parent:
                    #         clients_executing_completing_task.append(physical_client_index)
                    #     allocations[physical_client_index].append(TaskAllocation(sub_task, client_index - start_client_index))
                    # start_client_index += sub_task.clients
        finally:
            await asyncio.get_event_loop().shutdown_asyncgens()
            await es["default"].transport.close()

    def reset_relative_time(self):
        self.logger.debug("Resetting relative time of request metrics store.")
        self.metrics_store.reset_relative_time()

    def close(self):
        self.progress_reporter.finish()
        if self.metrics_store and self.metrics_store.opened:
            self.metrics_store.close()

    def update_samples(self):
        if self.sampler:
            samples = self.sampler.samples
            self.logger.info("Adding [%d] new samples.", len(samples))
            if len(samples) > 0:
                self.raw_samples += samples
                # We need to check all samples, they will be from different clients
                for s in samples:
                    self.most_recent_sample_per_client[s.client_id] = s
            self.logger.info("Done adding [%d] new samples.", len(samples))
        else:
            self.logger.info("No sampler defined yet. Skipping update of samples.")

    def update_progress_message(self, task_finished=False):
        if not self.quiet and len(self.current_tasks) > 0:
            tasks = ",".join([t.name for t in self.current_tasks])

            if task_finished:
                total_progress = 1.0
            else:
                # we only count clients which actually contribute to progress. If clients are executing tasks eternally in a parallel
                # structure, we should not count them. The reason is that progress depends entirely on the client(s) that execute the
                # task that is completing the parallel structure.
                progress_per_client = [s.percent_completed
                                       for s in self.most_recent_sample_per_client.values() if s.percent_completed is not None]

                num_clients = max(len(progress_per_client), 1)
                total_progress = sum(progress_per_client) / num_clients
            self.progress_reporter.print("Running %s" % tasks, "[%3d%% done]" % (round(total_progress * 100)))
            if task_finished:
                self.progress_reporter.finish()

    def post_process_samples(self):
        if len(self.raw_samples) == 0:
            return
        total_start = time.perf_counter()
        start = total_start
        # we do *not* do this here to avoid concurrent updates (actors are single-threaded) but rather to make it clear that we use
        # only a snapshot and that new data will go to a new sample set.
        raw_samples = self.raw_samples
        self.raw_samples = []
        for sample in raw_samples:
            meta_data = self.merge(
                self.track.meta_data,
                self.challenge.meta_data,
                sample.operation.meta_data,
                sample.task.meta_data,
                sample.request_meta_data)

            self.metrics_store.put_value_cluster_level(name="latency", value=sample.latency_ms, unit="ms", task=sample.task.name,
                                                       operation=sample.operation.name, operation_type=sample.operation.type,
                                                       sample_type=sample.sample_type, absolute_time=sample.absolute_time,
                                                       relative_time=sample.relative_time, meta_data=meta_data)

            self.metrics_store.put_value_cluster_level(name="service_time", value=sample.service_time_ms, unit="ms", task=sample.task.name,
                                                       operation=sample.task.name, operation_type=sample.operation.type,
                                                       sample_type=sample.sample_type, absolute_time=sample.absolute_time,
                                                       relative_time=sample.relative_time, meta_data=meta_data)

        end = time.perf_counter()
        self.logger.debug("Storing latency and service time took [%f] seconds.", (end - start))
        start = end
        aggregates = self.throughput_calculator.calculate(raw_samples)
        end = time.perf_counter()
        self.logger.debug("Calculating throughput took [%f] seconds.", (end - start))
        start = end
        for task, samples in aggregates.items():
            meta_data = self.merge(
                self.track.meta_data,
                self.challenge.meta_data,
                task.operation.meta_data,
                task.meta_data
            )
            for absolute_time, relative_time, sample_type, throughput, throughput_unit in samples:
                self.metrics_store.put_value_cluster_level(name="throughput", value=throughput, unit=throughput_unit, task=task.name,
                                                           operation=task.operation.name, operation_type=task.operation.type,
                                                           sample_type=sample_type, absolute_time=absolute_time,
                                                           relative_time=relative_time, meta_data=meta_data)
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
        self.logger.debug("Postprocessing [%d] raw samples took [%f] seconds in total.", len(raw_samples), (end - total_start))

    def merge(self, *args):
        result = {}
        for arg in args:
            if arg is not None:
                result.update(arg)
        return result


def debug_exception_handler(loop, context):
    logging.getLogger(__name__).error("Uncaught exception in event loop!! %s", context)


class AsyncFoo:
    def __init__(self, loop):
        from concurrent.futures import ThreadPoolExecutor
        self.io_pool_exc = ThreadPoolExecutor()
        self.loop = loop

    async def __call__(self, task):
        yield self.loop.run_in_executor(self.io_pool_exc, task)


class BoundAsyncFoo:
    def __init__(self, e, t):
        self.loop = e.loop
        self.io_pool_exc = e.io_pool_exc
        self.t = t

    async def __call__(self):
        yield self.loop.run_in_executor(self.io_pool_exc, self.t)


class AsyncProfiler:
    def __init__(self, target):
        """
        :param target: The actual executor which should be profiled.
        """
        self.target = target
        self.profile_logger = logging.getLogger("rally.profile")

    async def __call__(self, *args, **kwargs):
        import yappi
        import io as python_io
        yappi.start()
        try:
            return await self.target(*args, **kwargs)
        finally:
            yappi.stop()
            s = python_io.StringIO()
            yappi.get_func_stats().print_all(out=s, columns={
                0: ("name", 200),
                1: ("ncall", 5),
                2: ("tsub", 8),
                3: ("ttot", 8),
                4: ("tavg", 8)
            })

            profile = "\n=== Profile START ===\n"
            profile += s.getvalue()
            profile += "=== Profile END ==="
            self.profile_logger.info(profile)


class AsyncExecutor:
    def __init__(self, client_id, task, schedule, es, sampler, cancel, complete, abort_on_error=False):
        """
        Executes tasks according to the schedule for a given operation.

        :param task: The task that is executed.
        :param schedule: The schedule for this task.
        :param es: Elasticsearch client that will be used to execute the operation.
        :param sampler: A container to store raw samples.
        :param cancel: A shared boolean that indicates we need to cancel execution.
        :param complete: A shared boolean that indicates we need to prematurely complete execution.
        """
        self.client_id = client_id
        self.task = task
        self.op = task.operation
        self.schedule_handle = schedule
        self.es = es
        self.sampler = sampler
        self.cancel = cancel
        self.complete = complete
        self.abort_on_error = abort_on_error
        self.logger = logging.getLogger(__name__)

    async def __call__(self, *args, **kwargs):
        total_start = time.perf_counter()
        # lazily initialize the schedule
        self.logger.debug("Initializing schedule for client id [%s].", self.client_id)
        schedule = self.schedule_handle()
        self.logger.debug("Entering main loop for client id [%s].", self.client_id)
        # noinspection PyBroadException
        try:
            async for expected_scheduled_time, sample_type, percent_completed, runner, params in schedule:
                #self.logger.info("Next iteration in main loop for client id %s (%s %% completed)", self.client_id, percent_completed)
                if self.cancel.is_set():
                    self.logger.info("User cancelled execution.")
                    break
                absolute_expected_schedule_time = total_start + expected_scheduled_time
                throughput_throttled = expected_scheduled_time > 0
                if throughput_throttled:
                    rest = absolute_expected_schedule_time - time.perf_counter()
                    if rest > 0:
                        await asyncio.sleep(rest)
                start = time.perf_counter()
                total_ops, total_ops_unit, request_meta_data = await execute_single(runner, self.es, params, self.abort_on_error)
                stop = time.perf_counter()
                service_time = stop - start
                # Do not calculate latency separately when we don't throttle throughput. This metric is just confusing then.
                latency = stop - absolute_expected_schedule_time if throughput_throttled else service_time
                # last sample should bump progress to 100% if externally completed.
                completed = self.complete.is_set() or runner.completed
                if completed:
                    progress = 1.0
                elif runner.percent_completed:
                    progress = runner.percent_completed
                else:
                    progress = percent_completed
                self.sampler.add(sample_type, request_meta_data, convert.seconds_to_ms(latency), convert.seconds_to_ms(service_time),
                                 total_ops, total_ops_unit, (stop - total_start), progress, client_id=self.client_id)

                if completed:
                    self.logger.info("Task is considered completed due to external event.")
                    break
        except BaseException:
            self.logger.exception("Could not execute schedule")
            raise
        finally:
            # Actively set it if this task completes its parent
            if self.task.completes_parent:
                self.complete.set()


async def execute_single(runner, es, params, abort_on_error=False):
    """
    Invokes the given runner once and provides the runner's return value in a uniform structure.

    :return: a triple of: total number of operations, unit of operations, a dict of request meta data (may be None).
    """
    import elasticsearch
    try:
        # TODO: Make all runners async-aware - Can we run async runners as a "regular" function (to avoid duplicate implementations)?
        with runner:
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
        total_ops = 0
        total_ops_unit = "ops"
        request_meta_data = {
            "success": False,
            "error-type": "transport"
        }
        # The ES client will sometimes return string like "N/A" or "TIMEOUT" for connection errors.
        if isinstance(e.status_code, int):
            request_meta_data["http-status"] = e.status_code
        if e.info:
            request_meta_data["error-description"] = "%s (%s)" % (e.error, e.info)
        else:
            request_meta_data["error-description"] = e.error
    except KeyError as e:
        logging.getLogger(__name__).exception("Cannot execute runner [%s]; most likely due to missing parameters.", str(runner))
        msg = "Cannot execute [%s]. Provided parameters are: %s. Error: [%s]." % (str(runner), list(params.keys()), str(e))
        raise exceptions.SystemSetupError(msg)

    if abort_on_error and not request_meta_data["success"]:
        msg = "Request returned an error. Error type: %s" % request_meta_data.get("error-type", "Unknown")
        description = request_meta_data.get("error-description")
        if description:
            msg += ", Description: %s" % description
        raise exceptions.RallyAssertionError(msg)
    return total_ops, total_ops_unit, request_meta_data
