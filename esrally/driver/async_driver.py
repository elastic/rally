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


import asyncio
import concurrent.futures
import logging
import threading
import time

from esrally import exceptions, metrics, paths, track, client, telemetry
from esrally.driver import driver, runner, scheduler
from esrally.utils import console


class Timer:
    """
    A general purpose timer that periodically runs tasks that have been added via ``add_task``. Stop the timer via
    the ``stop`` method. Note that tasks can be called only once per second at most.

    """
    class Task:
        def __init__(self, fn, interval, wakeup_interval):
            self.fn = fn
            self.interval = interval
            self.wakeup_interval = wakeup_interval
            self.current = 0

        def may_run(self):
            self.current += self.wakeup_interval
            if self.current >= self.interval:
                self.current = 0
                self.fn()

        def __repr__(self):
            return "timer task for {} firing every {}s.".format(str(self.fn), self.interval)

    def __init__(self, wakeup_interval=1):
        """
        :param wakeup_interval: The interval in seconds in which the timer will check whether it has been stopped or
                                schedule tasks. Default: 1 second.
        """
        self.stop_event = threading.Event()
        self.tasks = []
        self.wakeup_interval = wakeup_interval
        self.logger = logging.getLogger(__name__)

    def add_task(self, fn, interval):
        self.tasks.append(Timer.Task(fn, interval, self.wakeup_interval))

    def stop(self):
        self.stop_event.set()

    def __call__(self, *args, **kwargs):
        while not self.stop_event.is_set():
            for t in self.tasks:
                self.logger.debug("Invoking [%s]", t)
                t.may_run()
            # allow early exit even if a longer sleeping period is requested
            if self.stop_event.is_set():
                self.logger.debug("Stopping timer due to external event.")
                break
            time.sleep(self.wakeup_interval)


class AsyncDriver:
    def __init__(self, config, track, challenge, es_client_factory_class=client.EsClientFactory):
        self.logger = logging.getLogger(__name__)
        self.config = config
        self.track = track
        self.challenge = challenge
        self.es_client_factory = es_client_factory_class
        self.metrics_store = None

        self.progress_reporter = console.progress()
        self.throughput_calculator = driver.ThroughputCalculator()
        self.raw_samples = []
        self.most_recent_sample_per_client = {}

        self.current_tasks = []

        self.telemetry = None
        self.es_clients = None

        self.quiet = self.config.opts("system", "quiet.mode", mandatory=False, default_value=False)
        # TODO: Change the default value to `False` once this implementation becomes the default
        self.debug_event_loop = self.config.opts("system", "async.debug", mandatory=False, default_value=True)
        self.abort_on_error = self.config.opts("driver", "on.error") == "abort"
        self.profiling_enabled = self.config.opts("driver", "profiling")
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
        log_root = paths.race_root(self.config)

        es = self.es_clients
        es_default = self.es_clients["default"]
        self.telemetry = telemetry.Telemetry(enabled_devices, devices=[
            telemetry.NodeStats(telemetry_params, es, self.metrics_store),
            telemetry.ExternalEnvironmentInfo(es_default, self.metrics_store),
            telemetry.ClusterEnvironmentInfo(es_default, self.metrics_store),
            telemetry.JvmStatsSummary(es_default, self.metrics_store),
            telemetry.IndexStats(es_default, self.metrics_store),
            telemetry.MlBucketProcessingTime(es_default, self.metrics_store),
            telemetry.SegmentStats(log_root, es_default),
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
        # noinspection PyBroadException
        try:
            return self.es_clients["default"].info()
        except BaseException:
            self.logger.exception("Could not retrieve cluster info on benchmark start")
            return None

    def setup(self):
        if self.track.has_plugins:
            # no need to fetch the track once more; it has already been updated
            track.track_repo(self.config, fetch=False, update=False)
            # load track plugins eagerly to initialize the respective parameter sources
            track.load_track_plugins(self.config, runner.register_runner, scheduler.register_scheduler)
        track.prepare_track(self.track, self.config)

        self.metrics_store = metrics.metrics_store(cfg=self.config,
                                                   track=self.track.name,
                                                   challenge=self.challenge.name,
                                                   read_only=False)
        self.es_clients = self.create_es_clients()
        self.wait_for_rest_api()
        self.prepare_telemetry()

        cluster_info = self.retrieve_cluster_info()
        cluster_version = cluster_info["version"] if cluster_info else {}
        return cluster_version.get("build_flavor", "oss"), cluster_version.get("number"), cluster_version.get("build_hash")

    def run(self):
        self.logger.info("Benchmark is about to start.")
        # ensure relative time starts when the benchmark starts.
        self.reset_relative_time()
        self.logger.info("Attaching cluster-level telemetry devices.")
        self.telemetry.on_benchmark_start()
        self.logger.info("Cluster-level telemetry devices are now attached.")

        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)

        timer = Timer()
        timer.add_task(fn=self.update_samples, interval=1)
        timer.add_task(fn=self.post_process_samples, interval=30)
        timer.add_task(fn=self.update_progress_message, interval=1)

        pool.submit(timer)

        # needed because a new thread (that is not the main thread) does not have an event loop
        loop = asyncio.new_event_loop()
        loop.set_debug(self.debug_event_loop)
        asyncio.set_event_loop(loop)
        loop.set_exception_handler(self._logging_exception_handler)

        track.set_absolute_data_path(self.config, self.track)
        runner.register_default_runners()
        # We can skip this here as long as we run in the same process; it has already been done in #setup()
        # if self.track.has_plugins:
        #     track.load_track_plugins(self.config, runner.register_runner, scheduler.register_scheduler)
        try:
            benchmark_runner = driver.AsyncProfiler(self._run_benchmark) if self.profiling_enabled else self._run_benchmark
            loop.run_until_complete(benchmark_runner())
            self.telemetry.on_benchmark_stop()
            self.logger.info("All steps completed.")
            return self.metrics_store.to_externalizable()
        finally:
            self.logger.debug("Stopping timer...")
            timer.stop()
            pool.shutdown()
            self.logger.debug("Closing event loop...")
            loop.close()
            self.progress_reporter.finish()
            self.logger.debug("Closing metrics store...")
            self.metrics_store.close()
            # immediately clear as we don't need it anymore and it can consume a significant amount of memory
            self.metrics_store = None

    def _logging_exception_handler(self, loop, context):
        self.logger.error("Uncaught exception in event loop: %s", context)

    async def _run_benchmark(self):
        # avoid: aiohttp.internal WARNING The object should be created from async function
        es = self.create_es_clients(sync=False)
        try:
            cancel = threading.Event()
            # allow to buffer more events than by default as we expect to have way more clients.
            self.sampler = driver.Sampler(start_timestamp=time.perf_counter(), buffer_size=65536)

            for task in self.challenge.schedule:
                self.current_tasks = []
                aws = []
                for sub_task in task:
                    self.current_tasks.append(sub_task)
                    self.logger.info("Running task [%s] with [%d] clients...", sub_task.name, sub_task.clients)
                    # A parameter source should only be created once per task - it is partitioned later on per client.
                    param_source = track.operation_parameters(self.track, task)
                    for client_id in range(sub_task.clients):
                        schedule = driver.schedule_for(sub_task, client_id, param_source)
                        # used to indicate that we want to prematurely consider this completed. This is *not* due to
                        # cancellation but a regular event in a benchmark and used to model task dependency of parallel tasks.
                        complete = threading.Event()
                        e = driver.AsyncExecutor(client_id, sub_task, schedule, es, self.sampler, cancel, complete, self.abort_on_error)
                        aws.append(e())
                # join point
                _ = await asyncio.gather(*aws)
                self.logger.info("All clients have finished running task [%s]", task.name)
                # drain the active samples before we move on to the next task
                self.update_samples()
                self.post_process_samples()
                self.reset_relative_time()
                self.update_progress_message(task_finished=True)
        finally:
            await asyncio.get_event_loop().shutdown_asyncgens()
            for e in es.values():
                await e.transport.close()

    def reset_relative_time(self):
        self.logger.debug("Resetting relative time of request metrics store.")
        self.metrics_store.reset_relative_time()

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
        # we do *not* do this here to avoid concurrent updates (we are single-threaded) but rather to make it clear that we use
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

            self.metrics_store.put_value_cluster_level(name="processing_time", value=sample.processing_time_ms,
                                                       unit="ms", task=sample.task.name,
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
