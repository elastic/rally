import concurrent.futures
import threading
import datetime
import json
import logging
import queue
import socket
import time

import thespian.actors
from esrally import actor, exceptions, metrics, track, client, PROGRAM_NAME
from esrally.driver import runner, scheduler
from esrally.utils import convert, console, versions, io

logger = logging.getLogger("rally.driver")
profile_logger = logging.getLogger("rally.profile")


##################################
#
# Messages sent between drivers
#
##################################
class StartBenchmark:
    """
    Starts a benchmark.
    """

    def __init__(self, config, track, metrics_meta_info, lap):
        """
        :param config: Rally internal configuration object.
        :param track: The track to use.
        :param metrics_meta_info: meta info for the metrics store.
        :param lap: The current lap.
        """
        self.lap = lap
        self.config = config
        self.track = track
        self.metrics_meta_info = metrics_meta_info


class StartLoadGenerator:
    """
    Starts a load generator.
    """

    def __init__(self, client_id, config, track, tasks):
        """
        :param client_id: Client id of the load generator.
        :param config: Rally internal configuration object.
        :param track: The track to use.
        :param tasks: Tasks to run.
        """
        self.client_id = client_id
        self.config = config
        self.track = track
        self.tasks = tasks


class Drive:
    """
    Tells a load generator to drive (either after a join point or initially).
    """

    def __init__(self, client_start_timestamp):
        self.client_start_timestamp = client_start_timestamp


class UpdateSamples:
    """
    Used to send samples from a load generator node to the master.
    """

    def __init__(self, client_id, samples):
        self.client_id = client_id
        self.samples = samples


class JoinPointReached:
    """
    Tells the master that a load generator has reached a join point. Used for coordination across multiple load generators.
    """

    def __init__(self, client_id, task):
        self.client_id = client_id
        self.client_local_timestamp = time.perf_counter()
        self.task = task


class BenchmarkComplete:
    """
    Indicates that the benchmark is complete.
    """

    def __init__(self, metrics):
        self.metrics = metrics


# Workaround for https://github.com/godaddy/Thespian/issues/22
class BenchmarkFailure:
    """
    Indicates a failure in the benchmark execution due to an exception
    """

    def __init__(self, message, cause=None):
        self.message = message
        self.cause = cause


class BenchmarkCancelled:
    """
    Indicates that the benchmark has been cancelled (by the user).
    """
    pass


class Driver(actor.RallyActor):
    WAKEUP_INTERVAL_SECONDS = 1
    """
    Coordinates all worker drivers.
    """

    def __init__(self):
        super().__init__()
        actor.RallyActor.configure_logging(logger)
        self.config = None
        self.track = None
        self.challenge = None
        # Elasticsearch client
        self.es = None
        self.metrics_store = None
        self.raw_samples = []
        self.currently_completed = 0
        self.clients_completed_current_step = {}
        self.current_step = -1
        self.number_of_steps = 0
        self.start_sender = None
        self.allocations = None
        self.join_points = None
        self.ops_per_join_point = None
        self.drivers = []
        self.progress_reporter = console.progress()
        self.progress_counter = 0
        self.quiet = False
        self.most_recent_sample_per_client = {}
        self.status = "init"

    def receiveMessage(self, msg, sender):
        try:
            logger.debug("Driver#receiveMessage(msg = [%s] sender = [%s])" % (str(type(msg)), str(sender)))
            if isinstance(msg, StartBenchmark):
                self.start_benchmark(msg, sender)
            elif isinstance(msg, JoinPointReached):
                self.joinpoint_reached(msg)
            elif isinstance(msg, UpdateSamples):
                self.update_samples(msg)
            elif isinstance(msg, thespian.actors.WakeupMessage):
                if not self.finished():
                    self.update_progress_message()
                    self.wakeupAfter(datetime.timedelta(seconds=Driver.WAKEUP_INTERVAL_SECONDS))
            elif isinstance(msg, BenchmarkFailure):
                logger.error("Main driver received a fatal exception from a load generator. Shutting down.")
                self.metrics_store.close()
                self.send(self.start_sender, msg)
                self.send(self.myAddress, thespian.actors.ActorExitRequest())
            elif isinstance(msg, BenchmarkCancelled):
                logger.info("Main driver received a notification that the benchmark has been cancelled. Shutting down.")
                self.progress_reporter.finish()
                self.metrics_store.close()
                self.send(self.start_sender, msg)
                self.send(self.myAddress, thespian.actors.ActorExitRequest())
            elif isinstance(msg, thespian.actors.ActorExitRequest):
                logger.info("Main driver received ActorExitRequest and will terminate all load generators.")
                self.status = "exiting"
                for driver in self.drivers:
                    self.send(driver, thespian.actors.ActorExitRequest())
                logger.info("Main driver has notified all load generators of termination.")
            elif isinstance(msg, thespian.actors.ChildActorExited):
                driver_index = self.drivers.index(msg.childAddress)
                if self.status == "exiting":
                    logger.info("Load generator [%d] has exited." % driver_index)
                else:
                    logger.error("Load generator [%d] has exited prematurely. Aborting benchmark." % driver_index)
                    self.send(self.start_sender, BenchmarkFailure("Load generator [%d] has exited prematurely." % driver_index))
                    self.send(self.myAddress, thespian.actors.ActorExitRequest())
            else:
                logger.info("Main driver received unknown message [%s] (ignoring)." % (str(msg)))
        except BaseException as e:
            logger.exception("Main driver encountered a fatal exception. Shutting down.")
            if self.metrics_store:
                self.metrics_store.close()
            self.status = "exiting"
            for driver in self.drivers:
                self.send(driver, thespian.actors.ActorExitRequest())
            self.send(self.start_sender, BenchmarkFailure("Could not execute benchmark", e))
            self.send(self.myAddress, thespian.actors.ActorExitRequest())

    def start_benchmark(self, msg, sender):
        self.start_sender = sender
        self.config = msg.config
        self.track = msg.track

        track_name = self.track.name
        challenge_name = self.track.find_challenge_or_default(self.config.opts("track", "challenge.name")).name
        selected_car_name = self.config.opts("mechanic", "car.name")

        logger.info("Preparing track [%s]" % track_name)
        # TODO #257: Reconsider this in case we distribute drivers. *For now* the driver will only be on a single machine, so we're safe.
        track.prepare_track(self.track, self.config)

        logger.info("Benchmark for track [%s], challenge [%s] and car [%s] is about to start." %
                    (track_name, challenge_name, selected_car_name))
        self.quiet = self.config.opts("system", "quiet.mode", mandatory=False, default_value=False)
        self.es = client.EsClientFactory(self.config.opts("client", "hosts"), self.config.opts("client", "options")).create()
        self.metrics_store = metrics.InMemoryMetricsStore(cfg=self.config, meta_info=msg.metrics_meta_info, lap=msg.lap)
        invocation = self.config.opts("system", "time.start")
        expected_cluster_health = self.config.opts("benchmarks", "cluster.health")
        self.metrics_store.open(invocation, track_name, challenge_name, selected_car_name)

        self.challenge = select_challenge(self.config, self.track)
        for template in self.track.templates:
            setup_template(self.es, template)

        for index in self.track.indices:
            setup_index(self.es, index, self.challenge.index_settings)
        wait_for_status(self.es, expected_cluster_health)
        allocator = Allocator(self.challenge.schedule)
        self.allocations = allocator.allocations
        self.number_of_steps = len(allocator.join_points) - 1
        self.ops_per_join_point = allocator.operations_per_joinpoint

        logger.info("Benchmark consists of [%d] steps executed by (at most) [%d] clients as specified by the allocation matrix:\n%s" %
                    (self.number_of_steps, len(self.allocations), self.allocations))

        for client_id in range(allocator.clients):
            self.drivers.append(
                self.createActor(LoadGenerator,
                                 globalName="/rally/driver/worker/%s" % str(client_id),
                                 targetActorRequirements={"coordinator": True}))
        for client_id, driver in enumerate(self.drivers):
            logger.info("Starting load generator [%d]." % client_id)
            self.send(driver, StartLoadGenerator(client_id, self.config, self.track, self.allocations[client_id]))

        self.update_progress_message()
        self.wakeupAfter(datetime.timedelta(seconds=Driver.WAKEUP_INTERVAL_SECONDS))

    def joinpoint_reached(self, msg):
        self.currently_completed += 1
        self.clients_completed_current_step[msg.client_id] = (msg.client_local_timestamp, time.perf_counter())
        logger.info("[%d/%d] drivers reached join point [%d/%d]." %
                    (self.currently_completed, len(self.drivers), self.current_step + 1, self.number_of_steps))
        if self.currently_completed == len(self.drivers):
            logger.info("All drivers completed their operations until join point [%d/%d]." %
                        (self.current_step + 1, self.number_of_steps))
            # we can go on to the next step
            self.currently_completed = 0
            # make a copy and reset early to avoid any race conditions from clients that reach a join point already while we are sending...
            clients_curr_step = self.clients_completed_current_step
            self.clients_completed_current_step = {}
            self.update_progress_message(task_finished=True)
            # clear per step
            self.most_recent_sample_per_client = {}
            self.current_step += 1
            if self.finished():
                logger.info("All steps completed. Shutting down.")
                # we're done here
                for driver in self.drivers:
                    self.send(driver, thespian.actors.ActorExitRequest())
                logger.info("Postprocessing samples...")
                self.post_process_samples()
                logger.info("Sending benchmark results...")
                # TODO #257: In case we are not on a single machine, spilling to disk will not work. Check and reimplement if necessary.
                # Spill to disk to guard against a too large representation of metrics store.
                self.send(self.start_sender, BenchmarkComplete(self.metrics_store.to_externalizable(spill_to_disk=True)))
                logger.info("Closing metrics store...")
                self.metrics_store.close()
                # immediately clear as we don't need it anymore and it can consume a significant amount of memory
                del self.metrics_store
                logger.info("Terminating main driver actor.")
                self.send(self.myAddress, thespian.actors.ActorExitRequest())
            else:
                if self.config.opts("track", "test.mode.enabled"):
                    # don't wait if test mode is enabled and start the next task immediately.
                    start_next_task = time.perf_counter()
                else:
                    # start the next task in five seconds (relative to master's timestamp)
                    #
                    # Assumption: We don't have a lot of clock skew between reaching the join point and sending the next task
                    #             (it doesn't matter too much if we're a few ms off).
                    start_next_task = time.perf_counter() + 5.0
                for client_id, driver in enumerate(self.drivers):
                    client_ended_task_at, master_received_msg_at = clients_curr_step[client_id]
                    client_start_timestamp = client_ended_task_at + (start_next_task - master_received_msg_at)
                    logger.info("Scheduling next task for client id [%d] at their timestamp [%f] (master timestamp [%f])" %
                                (client_id, client_start_timestamp, start_next_task))
                    self.send(driver, Drive(client_start_timestamp))

    def finished(self):
        return self.current_step == self.number_of_steps

    def update_samples(self, msg):
        self.raw_samples += msg.samples
        if len(msg.samples) > 0:
            most_recent = msg.samples[-1]
            self.most_recent_sample_per_client[most_recent.client_id] = most_recent

    def post_process_samples(self):
        logger.info("Storing latency and service time... ")
        for sample in self.raw_samples:
            meta_data = self.merge(
                self.track.meta_data,
                self.challenge.meta_data,
                sample.operation.meta_data,
                sample.task.meta_data,
                sample.request_meta_data)

            self.metrics_store.put_value_cluster_level(name="latency", value=sample.latency_ms, unit="ms", operation=sample.operation.name,
                                                       operation_type=sample.operation.type, sample_type=sample.sample_type,
                                                       absolute_time=sample.absolute_time, relative_time=sample.relative_time,
                                                       meta_data=meta_data)

            self.metrics_store.put_value_cluster_level(name="service_time", value=sample.service_time_ms, unit="ms",
                                                       operation=sample.operation.name, operation_type=sample.operation.type,
                                                       sample_type=sample.sample_type, absolute_time=sample.absolute_time,
                                                       relative_time=sample.relative_time, meta_data=meta_data)

        logger.info("Calculating throughput... ")
        aggregates = calculate_global_throughput(self.raw_samples)
        logger.info("Storing throughput... ")
        for task, samples in aggregates.items():
            meta_data = self.merge(
                self.track.meta_data,
                self.challenge.meta_data,
                task.operation.meta_data,
                task.meta_data
            )
            op = task.operation
            for absolute_time, relative_time, sample_type, throughput, throughput_unit in samples:
                self.metrics_store.put_value_cluster_level(name="throughput", value=throughput, unit=throughput_unit,
                                                           operation=op.name, operation_type=op.type, sample_type=sample_type,
                                                           absolute_time=absolute_time, relative_time=relative_time, meta_data=meta_data)

    def merge(self, *args):
        result = {}
        for arg in args:
            if arg is not None:
                result.update(arg)
        return result

    def update_progress_message(self, task_finished=False):
        if not self.quiet and self.current_step >= 0:
            ops = ",".join([op.name for op in self.ops_per_join_point[self.current_step]])

            if task_finished:
                total_progress = 1.0
            else:
                num_clients = max(len(self.most_recent_sample_per_client), 1)
                total_progress = sum([s.percent_completed for s in self.most_recent_sample_per_client.values()]) / num_clients
            self.progress_reporter.print("Running %s" % ops, "[%3d%% done]" % (round(total_progress * 100)))
            if task_finished:
                self.progress_reporter.finish()


class LoadGenerator(actor.RallyActor):
    """
    The actual driver that applies load against the cluster.

    It will also regularly send measurements to the master node so it can consolidate them.
    """

    WAKEUP_INTERVAL_SECONDS = 5

    def __init__(self):
        super().__init__()
        actor.RallyActor.configure_logging(logger)
        self.master = None
        self.client_id = None
        self.es = None
        self.config = None
        self.track = None
        self.tasks = None
        self.current_task = 0
        self.start_timestamp = None
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        # cancellation via future does not work, hence we use our own mechanism with a shared variable and polling
        self.cancel = threading.Event()
        self.executor_future = None
        self.sampler = None
        self.start_driving = False
        self.wakeup_interval = LoadGenerator.WAKEUP_INTERVAL_SECONDS

    def receiveMessage(self, msg, sender):
        try:
            logger.debug("LoadGenerator[%s]#receiveMessage(msg = [%s], sender = [%s])" % (str(self.client_id), str(type(msg)), str(sender)))
            if isinstance(msg, StartLoadGenerator):
                logger.info("LoadGenerator[%d] is about to start." % msg.client_id)
                self.master = sender
                self.client_id = msg.client_id
                self.es = client.EsClientFactory(msg.config.opts("client", "hosts"), msg.config.opts("client", "options")).create()
                self.config = msg.config
                self.track = msg.track
                self.tasks = msg.tasks
                self.current_task = 0
                self.cancel.clear()
                # we need to wake up more often in test mode
                if self.config.opts("track", "test.mode.enabled"):
                    self.wakeup_interval = 0.5
                self.start_timestamp = time.perf_counter()
                track.load_track_plugins(self.config, runner.register_runner, scheduler.register_scheduler)
                self.drive()
            elif isinstance(msg, Drive):
                logger.debug("LoadGenerator[%d] is continuing its work at task index [%d] on [%f]." %
                             (self.client_id, self.current_task, msg.client_start_timestamp))
                self.start_driving = True
                self.wakeupAfter(datetime.timedelta(seconds=time.perf_counter() - msg.client_start_timestamp))
            elif isinstance(msg, thespian.actors.WakeupMessage):
                # it would be better if we could send ourselves a message at a specific time, simulate this with a boolean...
                if self.start_driving:
                    logger.info("LoadGenerator[%s] starts driving now." % str(self.client_id))
                    self.start_driving = False
                    self.drive()
                else:
                    current_samples = self.send_samples()
                    if self.cancel.is_set():
                        logger.info("LoadGenerator[%s] has detected that benchmark has been cancelled. Notifying master..." %
                                    str(self.client_id))
                        self.send(self.master, BenchmarkCancelled())
                    elif self.executor_future is not None and self.executor_future.done():
                        e = self.executor_future.exception(timeout=0)
                        if e:
                            logger.info("LoadGenerator[%s] has detected a benchmark failure. Notifying master..." % str(self.client_id))
                            self.send(self.master, BenchmarkFailure("Error in load generator [%d]" % self.client_id, e))
                        else:
                            logger.info("LoadGenerator[%s] is ready for the next task." % str(self.client_id))
                            self.executor_future = None
                            self.drive()
                    else:
                        if current_samples and len(current_samples) > 0:
                            most_recent_sample = current_samples[-1]
                            logger.info("LoadGenerator[%s] is executing [%s] (%.2f%% complete)." %
                                        (str(self.client_id), most_recent_sample.task, most_recent_sample.percent_completed * 100.0))
                        else:
                            logger.info("LoadGenerator[%s] is executing (no samples)." % (str(self.client_id)))
                        self.wakeupAfter(datetime.timedelta(seconds=self.wakeup_interval))
            elif isinstance(msg, thespian.actors.ActorExitRequest):
                logger.info("LoadGenerator[%s] is exiting due to ActorExitRequest." % str(self.client_id))
                if self.executor_future is not None and self.executor_future.running():
                    self.cancel.set()
                    self.pool.shutdown()
            else:
                logger.info("LoadGenerator[%d] received unknown message [%s] (ignoring)." % (self.client_id, str(msg)))
        except Exception as e:
            logger.exception("Fatal error in LoadGenerator[%d]" % self.client_id)
            self.send(self.master, BenchmarkFailure("Fatal error in load generator [%d]" % self.client_id, e))

    def drive(self):
        profiling_enabled = self.config.opts("driver", "profiling")
        task = None
        # skip non-tasks in the task list
        while task is None:
            task = self.tasks[self.current_task]
            self.current_task += 1

        if isinstance(task, JoinPoint):
            logger.info("LoadGenerator[%d] reached join point [%s]." % (self.client_id, task))
            # clients that don't execute tasks don't need to care about waiting
            if self.executor_future is not None:
                self.executor_future.result()
            self.send_samples()
            self.cancel.clear()
            self.executor_future = None
            self.sampler = None
            self.send(self.master, JoinPointReached(self.client_id, task))
        elif isinstance(task, track.Task):
            logger.info("LoadGenerator[%d] is executing [%s]." % (self.client_id, task))
            self.sampler = Sampler(self.client_id, task, self.start_timestamp)
            schedule = schedule_for(self.track, task, self.client_id)
            self.executor_future = self.pool.submit(execute_schedule,
                                                    self.cancel, self.client_id, task.operation, schedule, self.es, self.sampler,
                                                    profiling_enabled)
            self.wakeupAfter(datetime.timedelta(seconds=self.wakeup_interval))
        else:
            raise exceptions.RallyAssertionError("Unknown task type [%s]" % type(task))

    def send_samples(self):
        if self.sampler:
            samples = self.sampler.samples
            if len(samples) > 0:
                self.send(self.master, UpdateSamples(self.client_id, samples))
            return samples
        return None


class Sampler:
    """
    Encapsulates management of gathered samples.
    """

    def __init__(self, client_id, task, start_timestamp):
        self.client_id = client_id
        self.task = task
        self.start_timestamp = start_timestamp
        self.q = queue.Queue(maxsize=16384)

    def add(self, sample_type, request_meta_data, latency_ms, service_time_ms, total_ops, total_ops_unit, time_period, percent_completed):
        try:
            self.q.put_nowait(Sample(self.client_id, time.time(), time.perf_counter() - self.start_timestamp, self.task,
                                     sample_type, request_meta_data, latency_ms, service_time_ms, total_ops, total_ops_unit, time_period,
                                     percent_completed))
        except queue.Full:
            logger.warning("Dropping sample for [%s] due to a full sampling queue." % self.task.operation.name)

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
    def __init__(self, client_id, absolute_time, relative_time, task, sample_type, request_meta_data, latency_ms, service_time_ms,
                 total_ops, total_ops_unit, time_period, percent_completed):
        self.client_id = client_id
        self.absolute_time = absolute_time
        self.relative_time = relative_time
        self.task = task
        self.sample_type = sample_type
        self.request_meta_data = request_meta_data
        self.latency_ms = latency_ms
        self.service_time_ms = service_time_ms
        self.total_ops = total_ops
        self.total_ops_unit = total_ops_unit
        self.time_period = time_period
        self.percent_completed = percent_completed

    @property
    def operation(self):
        return self.task.operation

    def __repr__(self, *args, **kwargs):
        return "[%f; %f] [client [%s]] [%s] [%s]: [%f] ms request latency, [%f] ms service time, [%d %s]" % \
               (self.absolute_time, self.relative_time, self.client_id, self.task, self.sample_type, self.latency_ms, self.service_time_ms,
                self.total_ops, self.total_ops_unit)


def select_challenge(config, t):
    challenge_name = config.opts("track", "challenge.name")
    selected_challenge = t.find_challenge_or_default(challenge_name)

    if not selected_challenge:
        raise exceptions.SystemSetupError("Unknown challenge [%s] for track [%s]. You can list the available tracks and their "
                                          "challenges with %s list tracks." % (challenge_name, t.name, PROGRAM_NAME))
    return selected_challenge


def setup_template(es, template, source=io.FileSource):
    if es.indices.exists_template(template.name):
        es.indices.delete_template(template.name)
    if template.delete_matching_indices:
        es.indices.delete(index=template.pattern)
    with source(template.template_file, "rt") as f:
        template_content = f.read()
    logger.info("create index template [%s] matching indices [%s] with content:\n%s" % (template.name, template.pattern, template_content))
    es.indices.put_template(name=template.name, body=template_content)


def setup_index(es, index, index_settings, source=io.FileSource):
    if index.auto_managed:
        if es.indices.exists(index=index.name):
            logger.warning("Index [%s] already exists. Deleting it." % index.name)
            es.indices.delete(index=index.name)
        logger.info("Creating index [%s]" % index.name)
        # first we merge the index settings and the mappings for all types
        body = {}
        body['settings'] = index_settings
        if ('mappings' not in body):
            body['mappings'] = {}
        for type in index.types:
            with source(type.mapping_file, "rt") as f:
                mappings = f.read()
            body['mappings'].update(json.loads(mappings))
        # create the index with mappings and settings
        logger.info("create index [%s] with body [%s]" % (index.name, body))
        es.indices.create(index=index.name, body=body)
    else:
        logger.info("Skipping index [%s] as it is managed by the user." % index.name)


def wait_for_status(es, expected_cluster_status):
    """
    Synchronously waits until the cluster reaches the provided status. Upon timeout a LaunchError is thrown.

    :param es Elasticsearch client
    :param expected_cluster_status the cluster status that should be reached.
    """
    logger.info("Wait for cluster status [%s]" % expected_cluster_status)
    start = time.perf_counter()
    reached_cluster_status, relocating_shards = _do_wait(es, expected_cluster_status)
    stop = time.perf_counter()
    logger.info("Cluster reached status [%s] within [%.1f] sec." % (reached_cluster_status, (stop - start)))
    logger.info("Cluster health: [%s]" % str(es.cluster.health()))
    logger.info("Shards:\n%s" % es.cat.shards(v=True))


def _do_wait(es, expected_cluster_status, sleep=time.sleep):
    import elasticsearch
    from enum import Enum
    from functools import total_ordering

    @total_ordering
    class ClusterHealthStatus(Enum):
        UNKNOWN = 0
        RED = 1
        YELLOW = 2
        GREEN = 3

        def __lt__(self, other):
            if self.__class__ is other.__class__:
                return self.value < other.value
            return NotImplemented

    def status(v):
        try:
            return ClusterHealthStatus[v.upper()]
        except (KeyError, AttributeError):
            return ClusterHealthStatus.UNKNOWN

    reached_cluster_status = None
    relocating_shards = -1
    major, minor, patch, suffix = versions.components(es.info()["version"]["number"])
    if major < 5:
        use_wait_for_relocating_shards = True
    elif major == 5 and minor == 0 and patch == 0 and suffix and suffix.startswith("alpha"):
        use_wait_for_relocating_shards = True
    else:
        use_wait_for_relocating_shards = False

    max_attempts = 10
    for attempt in range(max_attempts):
        try:
            # Is this the last attempt? Then just retrieve the status
            if attempt + 1 == max_attempts:
                result = es.cluster.health()
            elif use_wait_for_relocating_shards:
                result = es.cluster.health(wait_for_status=expected_cluster_status, timeout="3s",
                                           params={"wait_for_relocating_shards": 0})
            else:
                result = es.cluster.health(wait_for_status=expected_cluster_status, timeout="3s", wait_for_no_relocating_shards=True)
        except (socket.timeout, elasticsearch.exceptions.ConnectionError):
            pass
        except elasticsearch.exceptions.TransportError as e:
            if e.status_code == 408:
                logger.info("Timed out waiting for cluster health status. Retrying shortly...")
                sleep(0.5)
            else:
                raise e
        else:
            reached_cluster_status = result["status"]
            relocating_shards = result["relocating_shards"]
            logger.info("GOT: %s" % str(result))
            logger.info("ALLOC:\n%s" % es.cat.allocation(v=True))
            logger.info("RECOVERY:\n%s" % es.cat.recovery(v=True))
            logger.info("SHARDS:\n%s" % es.cat.shards(v=True))
            if status(reached_cluster_status) >= status(expected_cluster_status) and relocating_shards == 0:
                return reached_cluster_status, relocating_shards
            else:
                sleep(0.5)
    if status(reached_cluster_status) < status(expected_cluster_status):
        msg = "Cluster did not reach status [%s]. Last reached status: [%s]" % (expected_cluster_status, reached_cluster_status)
    else:
        msg = "Cluster reached status [%s] which is equal or better than the expected status [%s] but there were [%d] relocating shards " \
              "and we require zero relocating shards (Use the /_cat/shards API to check which shards are relocating.)" % \
              (reached_cluster_status, expected_cluster_status, relocating_shards)
    logger.error(msg)
    raise exceptions.RallyAssertionError(msg)


def calculate_global_throughput(samples, bucket_interval_secs=1):
    """
    Calculates global throughput based on samples gathered from multiple load generators.

    :param samples: A list containing all samples from all load generators.
    :param bucket_interval_secs: The bucket interval for aggregations.
    :return: A global view of throughput samples.
    """
    samples_per_task = {}
    # first we group all warmup / measurement samples by operation.
    for sample in samples:
        k = sample.task
        if k not in samples_per_task:
            samples_per_task[k] = []
        samples_per_task[k].append(sample)

    global_throughput = {}
    # with open("raw_samples.csv", "w") as sample_log:
    #    print("client_id,absolute_time,relative_time,operation,sample_type,total_ops,time_period", file=sample_log)
    for k, v in samples_per_task.items():
        task = k
        if task not in global_throughput:
            global_throughput[task] = []
        # sort all samples by time
        current_samples = sorted(v, key=lambda s: s.absolute_time)

        total_count = 0
        interval = 0
        current_bucket = 0
        current_sample_type = current_samples[0].sample_type
        sample_count_for_current_sample_type = 0
        start_time = current_samples[0].absolute_time - current_samples[0].time_period
        for sample in current_samples:
            # print("%d,%f,%f,%s,%s,%d,%f" %
            # (sample.client_id, sample.absolute_time, sample.relative_time, sample.operation, sample.sample_type,
            #  sample.total_ops, sample.time_period), file=sample_log)

            # once we have seen a new sample type, we stick to it.
            if current_sample_type < sample.sample_type:
                current_sample_type = sample.sample_type
                sample_count_for_current_sample_type = 0

            total_count += sample.total_ops
            interval = max(sample.absolute_time - start_time, interval)

            # avoid division by zero
            if interval > 0 and interval >= current_bucket:
                sample_count_for_current_sample_type += 1
                current_bucket = int(interval) + bucket_interval_secs
                throughput = (total_count / interval)
                # we calculate throughput per second
                global_throughput[task].append(
                    (sample.absolute_time, sample.relative_time, current_sample_type, throughput, "%s/s" % sample.total_ops_unit))
        # also include the last sample if we don't have one for the current sample type, even if it is below the bucket interval
        # (mainly needed to ensure we show throughput data in test mode)
        if interval > 0 and sample_count_for_current_sample_type == 0:
            throughput = (total_count / interval)
            global_throughput[task].append(
                (sample.absolute_time, sample.relative_time, current_sample_type, throughput, "%s/s" % sample.total_ops_unit))

    return global_throughput


def execute_schedule(cancel, client_id, op, schedule, es, sampler, enable_profiling=False):
    """
    Executes tasks according to the schedule for a given operation.

    :param cancel: A shared boolean that indicates we need to cancel execution.
    :param client_id: The id of the client that executes the operation.
    :param op: The operation that is executed.
    :param schedule: The schedule for this operation.
    :param es: Elasticsearch client that will be used to execute the operation.
    :param sampler: A container to store raw samples.
    :param enable_profiling: Enables a Python profiler for this execution (default: False).
    """
    if enable_profiling:
        logger.debug("Enabling Python profiler for [%s]" % str(op))
        import cProfile, pstats
        import io as python_io
        profiler = cProfile.Profile()
        profiler.enable()
    else:
        logger.debug("Python profiler for [%s] is disabled." % str(op))

    total_start = time.perf_counter()
    # noinspection PyBroadException
    try:
        for expected_scheduled_time, sample_type, percent_completed, runner, params in schedule:
            if cancel.is_set():
                logger.info("User cancelled execution.")
                break
            absolute_expected_schedule_time = total_start + expected_scheduled_time
            throughput_throttled = expected_scheduled_time > 0
            if throughput_throttled:
                rest = absolute_expected_schedule_time - time.perf_counter()
                if rest > 0:
                    time.sleep(rest)
            start = time.perf_counter()
            total_ops, total_ops_unit, request_meta_data = execute_single(runner, es, params)
            stop = time.perf_counter()

            service_time = stop - start
            # Do not calculate latency separately when we don't throttle throughput. This metric is just confusing then.
            latency = stop - absolute_expected_schedule_time if throughput_throttled else service_time
            sampler.add(sample_type, request_meta_data, convert.seconds_to_ms(latency), convert.seconds_to_ms(service_time), total_ops,
                        total_ops_unit, (stop - total_start), percent_completed)
    except BaseException:
        logger.exception("Could not execute schedule")
        raise
    finally:
        if enable_profiling:
            profiler.disable()
            s = python_io.StringIO()
            sortby = 'cumulative'
            ps = pstats.Stats(profiler, stream=s).sort_stats(sortby)
            ps.print_stats()

            profile = "\n=== Profile START for client [%s] and operation [%s] ===\n" % (str(client_id), str(op))
            profile += s.getvalue()
            profile += "=== Profile END for client [%s] and operation [%s] ===" % (str(client_id), str(op))
            profile_logger.info(profile)


def execute_single(runner, es, params):
    """
    Invokes the given runner once and provides the runner's return value in a uniform structure.

    :return: a triple of: total number of operations, unit of operations, a dict of request meta data (may be None).
    """
    import elasticsearch
    try:
        with runner:
            return_value = runner(es, params)
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
            "error-description": e.error
        }
        # The ES client will return N/A for connection errors
        if e.status_code != "N/A":
            request_meta_data["http-status"] = e.status_code
    except KeyError as e:
        logger.exception("Cannot execute runner [%s]; most likely due to missing parameters." % str(runner))
        msg = "Cannot execute [%s]. Provided parameters are: %s. Error: [%s]." % (str(runner), list(params.keys()), str(e))
        raise exceptions.SystemSetupError(msg)

    return total_ops, total_ops_unit, request_meta_data


class JoinPoint:
    def __init__(self, id):
        self.id = id

    def __eq__(self, other):
        return self.id == other.id

    def __repr__(self, *args, **kwargs):
        return "JoinPoint(%s)" % self.id


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
            for sub_task in task:
                for client_index in range(start_client_index, start_client_index + sub_task.clients):
                    allocations[client_index % max_clients].append(sub_task)
                start_client_index += sub_task.clients

            # uneven distribution between tasks and clients, e.g. there are 5 (parallel) tasks but only 2 clients. Then, one of them
            # executes three tasks, the other one only two. So we need to fill in a `None` for the second one.
            if start_client_index % max_clients > 0:
                # pin the index range to [0, max_clients). This simplifies the code below.
                start_client_index = start_client_index % max_clients
                for client_index in range(start_client_index, max_clients):
                    allocations[client_index].append(None)

            # let all clients join after each task, then we go on
            next_join_point = JoinPoint(join_point_id)
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
    def operations_per_joinpoint(self):
        """

        Calculates a flat list of all unique operations that are run in between join points.

        Consider the following schedule (2 clients):

        1. op1 and op2 run by both clients in parallel
        2. join point
        3. op3 run by client 1
        4. join point

        The results in: [{op1, op2}, {op3}]

        :return: A list of sets containing all operations.
        """
        ops = []
        current_ops = set()

        allocs = self.allocations
        # assumption: the shape of allocs is rectangular (i.e. each client contains the same number of elements)
        for idx in range(0, len(allocs[0])):
            for client in range(0, self.clients):
                task = allocs[client][idx]
                if isinstance(task, track.Task):
                    current_ops.add(task.operation)
                elif isinstance(task, JoinPoint) and len(current_ops) > 0:
                    ops.append(current_ops)
                    current_ops = set()

        return ops

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
def schedule_for(current_track, task, client_index):
    """
    Calculates a client's schedule for a given task.

    :param current_track: The current track.
    :param task: The task that should be executed.
    :param client_index: The current client index.  Must be in the range [0, `task.clients').
    :return: A generator for the operations the given client needs to perform for this task.
    """
    op = task.operation
    num_clients = task.clients
    sched = scheduler.scheduler_for(task.schedule, task.params)
    logger.info("Choosing [%s] for [%s]." % (sched, task))
    runner_for_op = runner.runner_for(op.type)
    params_for_op = track.operation_parameters(current_track, op).partition(client_index, num_clients)

    if task.warmup_time_period is not None or task.time_period is not None:
        warmup_time_period = task.warmup_time_period if task.warmup_time_period else 0
        logger.info("Creating time-period based schedule with [%s] distribution for [%s] with a warmup period of [%s] seconds and a "
                    "time period of [%s] seconds." % (task.schedule, op, str(warmup_time_period), str(task.time_period)))
        return time_period_based(sched, warmup_time_period, task.time_period, runner_for_op, params_for_op)
    else:
        logger.info("Creating iteration-count based schedule with [%s] distribution for [%s] with [%d] warmup iterations and "
                    "[%d] iterations." % (task.schedule, op, task.warmup_iterations, task.iterations))
        return iteration_count_based(sched, task.warmup_iterations // num_clients, task.iterations // num_clients,
                                     runner_for_op, params_for_op)


def time_period_based(sched, warmup_time_period, time_period, runner, params):
    """
    Calculates the necessary schedule for time period based operations.

    :param sched: The scheduler for this task. Must not be None.
    :param warmup_time_period: The time period in seconds that is considered for warmup. Must not be None; provide zero instead.
    :param time_period: The time period in seconds that is considered for measurement. May be None.
    :param runner: The runner for a given operation.
    :param params: The parameter source for a given operation.
    :return: A generator for the corresponding parameters.
    """
    next_scheduled = 0
    start = time.perf_counter()
    if time_period is None:
        iterations = params.size()
        for it in range(0, iterations):
            sample_type = metrics.SampleType.Warmup if time.perf_counter() - start < warmup_time_period else metrics.SampleType.Normal
            percent_completed = (it + 1) / iterations
            yield (next_scheduled, sample_type, percent_completed, runner, params.params())
            next_scheduled = sched.next(next_scheduled)
    else:
        end = start + warmup_time_period + time_period
        it = 0

        while time.perf_counter() < end:
            now = time.perf_counter()
            sample_type = metrics.SampleType.Warmup if now - start < warmup_time_period else metrics.SampleType.Normal
            percent_completed = (now - start) / (warmup_time_period + time_period)
            yield (next_scheduled, sample_type, percent_completed, runner, params.params())
            next_scheduled = sched.next(next_scheduled)
            it += 1


def iteration_count_based(sched, warmup_iterations, iterations, runner, params):
    """
    Calculates the necessary schedule based on a given number of iterations.

    :param sched: The scheduler for this task. Must not be None.
    :param warmup_iterations: The number of warmup iterations to run. 0 if no warmup should be performed.
    :param iterations: The number of measurement iterations to run.
    :param runner: The runner for a given operation.
    :param params: The parameter source for a given operation.
    :return: A generator for the corresponding parameters.
    """
    next_scheduled = 0
    total_iterations = warmup_iterations + iterations
    if total_iterations == 0:
        raise exceptions.RallyAssertionError("Operation must run at least for one iteration.")
    for it in range(0, total_iterations):
        sample_type = metrics.SampleType.Warmup if it < warmup_iterations else metrics.SampleType.Normal
        percent_completed = (it + 1) / total_iterations
        yield (next_scheduled, sample_type, percent_completed, runner, params.params())
        next_scheduled = sched.next(next_scheduled)
