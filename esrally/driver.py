import concurrent.futures
import datetime
import json
import logging
import queue
import random
import socket
import time

import elasticsearch
import thespian.actors
from esrally import exceptions, metrics, track, client, PROGRAM_NAME
from esrally.utils import convert, console, io

logger = logging.getLogger("rally.driver")


##################################
#
# Messages sent between drivers
#
##################################
class StartBenchmark:
    """
    Starts a benchmark.
    """

    def __init__(self, config, track, metrics_meta_info):
        """
        :param config: Rally internal configuration object.
        :param track: The track to use.
        :param metrics_meta_info: meta info for the metrics store.
        """
        self.config = config
        self.track = track
        self.metrics_meta_info = metrics_meta_info


class StartLoadGenerator:
    """
    Starts a load generator.
    """

    def __init__(self, client_id, config, indices, tasks):
        """
        :param client_id: Client id of the load generator.
        :param config: Rally internal configuration object.
        :param indices: Affected indices.
        :param tasks: Tasks to run.
        """
        self.client_id = client_id
        self.config = config
        self.indices = indices
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
    def __init__(self, message, cause):
        self.message = message
        self.cause = cause


class Driver(thespian.actors.Actor):
    WAKEUP_INTERVAL_SECONDS = 1
    """
    Coordinates all worker drivers.
    """

    def __init__(self):
        super().__init__()
        self.config = None
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

    def receiveMessage(self, msg, sender):
        try:
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
                for driver in self.drivers:
                    self.send(driver, thespian.actors.ActorExitRequest())
                self.send(self.start_sender, msg)
                self.send(self.myAddress, thespian.actors.ActorExitRequest())
        except Exception as e:
            logger.exception("Main driver encountered a fatal exception. Shutting down.")
            self.metrics_store.close()
            for driver in self.drivers:
                self.send(driver, thespian.actors.ActorExitRequest())
            self.send(self.start_sender, BenchmarkFailure("Could not execute benchmark", e))
            self.send(self.myAddress, thespian.actors.ActorExitRequest())

    def start_benchmark(self, msg, sender):
        self.start_sender = sender
        self.config = msg.config
        current_track = msg.track

        logger.info("Preparing track")
        # TODO #71: Reconsider this in case we distribute drivers. *For now* the driver will only be on a single machine, so we're safe.
        track.prepare_track(current_track, self.config)

        logger.info("Benchmark is about to start.")
        self.quiet = msg.config.opts("system", "quiet.mode", mandatory=False, default_value=False)
        self.es = client.EsClientFactory(msg.config.opts("client", "hosts"), msg.config.opts("client", "options")).create()
        self.metrics_store = metrics.InMemoryMetricsStore(config=self.config, meta_info=msg.metrics_meta_info)
        invocation = self.config.opts("meta", "time.start")
        expected_cluster_health = msg.config.opts("benchmarks", "cluster.health")
        track_name = self.config.opts("benchmarks", "track")
        challenge_name = self.config.opts("benchmarks", "challenge")
        selected_car_name = self.config.opts("benchmarks", "car")
        self.metrics_store.open(invocation, track_name, challenge_name, selected_car_name)

        challenge = select_challenge(self.config, current_track)
        setup_index(self.es, current_track, challenge, expected_cluster_health)
        allocator = Allocator(challenge.schedule)
        self.allocations = allocator.allocations
        self.number_of_steps = len(allocator.join_points) - 1
        self.ops_per_join_point = allocator.operations_per_joinpoint

        logger.info("Benchmark consists of [%d] steps executed by (at most) [%d] clients as specified by the allocation matrix:\n%s" %
                    (self.number_of_steps, len(self.allocations), self.allocations))

        for client_id in range(allocator.clients):
            self.drivers.append(self.createActor(LoadGenerator))
        for client_id, driver in enumerate(self.drivers):
            self.send(driver, StartLoadGenerator(client_id, self.config, current_track.indices, self.allocations[client_id]))

        self.update_progress_message()
        self.wakeupAfter(datetime.timedelta(seconds=Driver.WAKEUP_INTERVAL_SECONDS))

    def joinpoint_reached(self, msg):
        self.currently_completed += 1
        self.clients_completed_current_step[msg.client_id] = (msg.client_local_timestamp, time.perf_counter())
        logger.debug("[%d/%d] drivers reached join point [%d/%d]." %
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
                logger.info("All steps completed. Shutting down")
                # we're done here
                for driver in self.drivers:
                    self.send(driver, thespian.actors.ActorExitRequest())
                self.post_process_samples()
                self.send(self.start_sender, BenchmarkComplete(self.metrics_store.to_externalizable()))
                self.metrics_store.close()
                self.send(self.myAddress, thespian.actors.ActorExitRequest())
            else:
                # start the next task in three seconds (relative to master's timestamp)
                #
                # Assumption: We don't have a lot of clock skew between reaching the join point and sending the next task
                #             (it doesn't matter too much if we're a few ms off).
                start_next_task = time.perf_counter() + 3.0
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
        for sample in self.raw_samples:
            self.metrics_store.put_value_cluster_level(name="latency", value=sample.latency_ms, unit="ms", operation=sample.operation.name,
                                                       operation_type=sample.operation.type, sample_type=sample.sample_type,
                                                       absolute_time=sample.absolute_time, relative_time=sample.relative_time)

            self.metrics_store.put_value_cluster_level(name="service_time", value=sample.service_time_ms, unit="ms",
                                                       operation=sample.operation.name, operation_type=sample.operation.type,
                                                       sample_type=sample.sample_type, absolute_time=sample.absolute_time,
                                                       relative_time=sample.relative_time)

        aggregates = calculate_global_throughput(self.raw_samples)
        for op, samples in aggregates.items():
            for absolute_time, relative_time, sample_type, throughput in moving_average(samples):
                self.metrics_store.put_value_cluster_level(name="throughput", value=throughput, unit=op.granularity_unit,
                                                           operation=op.name, operation_type=op.type, sample_type=sample_type,
                                                           absolute_time=absolute_time, relative_time=relative_time)

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


class LoadGenerator(thespian.actors.Actor):
    """
    The actual driver that applies load against the cluster.

    It will also regularly send measurements to the master node so it can consolidate them.
    """

    WAKEUP_INTERVAL_SECONDS = 5

    def __init__(self):
        super().__init__()
        self.master = None
        self.client_id = None
        self.es = None
        self.config = None
        self.indices = None
        self.tasks = None
        self.current_task = 0
        self.start_timestamp = None
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.executor_future = None
        self.sampler = None
        self.start_driving = False

    def receiveMessage(self, msg, sender):
        try:
            if isinstance(msg, StartLoadGenerator):
                logger.debug("client [%d] is about to start." % msg.client_id)
                self.master = sender
                self.client_id = msg.client_id
                self.es = client.EsClientFactory(msg.config.opts("client", "hosts"), msg.config.opts("client", "options")).create()
                self.config = msg.config
                self.indices = msg.indices
                self.tasks = msg.tasks
                self.current_task = 0
                self.start_timestamp = time.perf_counter()
                self.drive()
            elif isinstance(msg, Drive):
                logger.debug("Client [%d] is continuing its work at task index [%d] on [%f]." %
                             (self.client_id, self.current_task, msg.client_start_timestamp))
                self.master = sender
                self.start_driving = True
                self.wakeupAfter(datetime.timedelta(seconds=time.perf_counter() - msg.client_start_timestamp))
            elif isinstance(msg, thespian.actors.WakeupMessage):
                logger.debug("client [%d] woke up." % self.client_id)
                # it would be better if we could send ourselves a message at a specific time, simulate this with a boolean...
                if self.start_driving:
                    self.start_driving = False
                    self.drive()
                else:
                    self.send_samples()
                    if self.executor_future is not None:
                        if self.executor_future.done():
                            e = self.executor_future.exception(timeout=0)
                            if e:
                                self.send(self.master, BenchmarkFailure("Error in load generator [%d]" % self.client_id, e))
                            else:
                                self.executor_future = None
                                self.drive()
                        else:
                            self.wakeupAfter(datetime.timedelta(seconds=LoadGenerator.WAKEUP_INTERVAL_SECONDS))
            else:
                logger.debug("client [%d] received unknown message [%s] (ignoring)." % (self.client_id, str(msg)))
        except Exception as e:
            self.send(self.master, BenchmarkFailure("Fatal error in load generator [%d]" % self.client_id, e))

    def drive(self):
        task = None
        # skip non-tasks in the task list
        while task is None:
            task = self.tasks[self.current_task]
            self.current_task += 1

        if isinstance(task, JoinPoint):
            logger.info("client [%d] reached join point [%s]." % (self.client_id, task))
            # clients that don't execute tasks don't need to care about waiting
            if self.executor_future is not None:
                self.executor_future.result()
            self.send_samples()
            self.executor_future = None
            self.sampler = None
            self.send(self.master, JoinPointReached(self.client_id, task))
        elif isinstance(task, track.Task):
            logger.info("Client [%d] is executing [%s]." % (self.client_id, task))
            self.sampler = Sampler(self.client_id, task.operation, self.start_timestamp)
            schedule = schedule_for(task, self.client_id, self.indices)
            self.executor_future = self.pool.submit(execute_schedule, schedule, self.es, self.sampler)
            self.wakeupAfter(datetime.timedelta(seconds=LoadGenerator.WAKEUP_INTERVAL_SECONDS))
        else:
            raise exceptions.RallyAssertionError("Unknown task type [%s]" % type(task))

    def send_samples(self):
        if self.sampler:
            samples = self.sampler.samples
            if len(samples) > 0:
                self.send(self.master, UpdateSamples(self.client_id, samples))


class Sampler:
    """
    Encapsulates management of gathered samples.
    """

    def __init__(self, client_id, operation, start_timestamp):
        self.client_id = client_id
        self.operation = operation
        self.start_timestamp = start_timestamp
        self.q = queue.Queue(maxsize=1024)

    def add(self, sample_type, latency_ms, service_time_ms, total_ops, time_period, curr_iteration, total_iterations):
        try:
            self.q.put_nowait(Sample(self.client_id, time.time(), time.perf_counter() - self.start_timestamp, self.operation,
                                     sample_type, latency_ms, service_time_ms, total_ops, time_period, curr_iteration, total_iterations))
        except queue.Full:
            logger.warn("Dropping sample for [%s] due to a full sampling queue." % self.operation.name)

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
    def __init__(self, client_id, absolute_time, relative_time, operation, sample_type, latency_ms, service_time_ms, total_ops,
                 time_period, curr_iteration, total_iterations):
        self.client_id = client_id
        self.absolute_time = absolute_time
        self.relative_time = relative_time
        self.operation = operation
        self.sample_type = sample_type
        self.latency_ms = latency_ms
        self.service_time_ms = service_time_ms
        self.total_ops = total_ops
        self.time_period = time_period
        self.curr_iteration = curr_iteration
        self.total_iterations = total_iterations

    @property
    def percent_completed(self):
        return self.curr_iteration / self.total_iterations


def select_challenge(config, t):
    selected_challenge = config.opts("benchmarks", "challenge")
    for challenge in t.challenges:
        if challenge.name == selected_challenge:
            return challenge
    raise exceptions.SystemSetupError("Unknown challenge [%s] for track [%s]. You can list the available tracks and their "
                                      "challenges with %s list tracks." % (selected_challenge, t.name, PROGRAM_NAME))


def setup_index(es, t, challenge, expected_cluster_health):
    if challenge.index_settings:
        for index in t.indices:
            if es.indices.exists(index=index.name):
                logger.warn("Index [%s] already exists. Deleting it." % index.name)
                es.indices.delete(index=index.name)
            logger.info("Creating index [%s]" % index.name)
            es.indices.create(index=index.name, body=challenge.index_settings)
            for type in index.types:
                mappings = open(type.mapping_file).read()
                logger.info("create mapping for type [%s] in index [%s] with content:\n%s" % (type.name, index.name, mappings))
                logger.debug(mappings)
                es.indices.put_mapping(index=index.name,
                                       doc_type=type.name,
                                       body=json.loads(mappings))
    wait_for_status(es, expected_cluster_health)


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


def _do_wait(es, expected_cluster_status):
    reached_cluster_status = None
    # TODO dm: Remove probing code after release of ES 5.0.
    use_wait_for_relocating_shards = False
    for attempt in range(10):
        try:
            if use_wait_for_relocating_shards:
                result = es.cluster.health(wait_for_status=expected_cluster_status, wait_for_relocating_shards=0, timeout="3s")
            else:
                result = es.cluster.health(wait_for_status=expected_cluster_status, timeout="3s",
                                           params={"wait_for_no_relocating_shards": True})
        except (socket.timeout, elasticsearch.exceptions.ConnectionError):
            pass
        except elasticsearch.exceptions.TransportError as e:
            if 400 <= e.status_code < 500:
                logger.info("Probing cluster health API. Using 'wait_for_relocating_shards=0'.")
                use_wait_for_relocating_shards = True
        else:
            reached_cluster_status = result["status"]
            relocating_shards = result["relocating_shards"]
            logger.info("GOT: %s" % str(result))
            logger.info("ALLOC:\n%s" % es.cat.allocation(v=True))
            logger.info("RECOVERY:\n%s" % es.cat.recovery(v=True))
            logger.info("SHARDS:\n%s" % es.cat.shards(v=True))
            if reached_cluster_status == expected_cluster_status and relocating_shards == 0:
                return reached_cluster_status, relocating_shards
            else:
                time.sleep(0.5)
    msg = "Cluster did not reach status [%s]. Last reached status: [%s]" % (expected_cluster_status, reached_cluster_status)
    logger.error(msg)
    raise exceptions.RallyAssertionError(msg)


def calculate_global_throughput(samples, bucket_interval_secs=1):
    """
    Calculates global throughput based on samples gathered from multiple load generators.

    :param samples: A list containing all samples from all load generators.
    :param bucket_interval_secs: The bucket interval for aggregations.
    :return: A global view of throughput samples.
    """
    samples_per_op = {}
    # first we group all warmup / measurement samples by operation.
    for sample in samples:
        k = sample.operation
        if k not in samples_per_op:
            samples_per_op[k] = []
        samples_per_op[k].append(sample)

    global_throughput = {}
    # with open("raw_samples.csv", "w") as sample_log:
    #    print("client_id,absolute_time,relative_time,operation,sample_type,total_ops,time_period", file=sample_log)
    for k, v in samples_per_op.items():
        op = k
        if op not in global_throughput:
            global_throughput[op] = []
        # sort all samples by time
        current_samples = sorted(v, key=lambda s: s.absolute_time)

        total_count = 0
        interval = 0
        current_bucket = 0
        current_sample_type = current_samples[0].sample_type
        start_time = current_samples[0].absolute_time - current_samples[0].time_period
        skip_buckets = False
        last_throughput = 0

        for sample in current_samples:
            # print("%d,%f,%f,%s,%s,%d,%f" %
            # (sample.client_id, sample.absolute_time, sample.relative_time, sample.operation, sample.sample_type,
            #  sample.total_ops, sample.time_period), file=sample_log)

            # once we have seen a new sample type, we stick to it.
            if current_sample_type < sample.sample_type:
                current_sample_type = sample.sample_type
                total_count = 0
                interval = 0
                current_bucket = 0
                start_time = sample.absolute_time - sample.time_period
                # skip the next few buckets as the system needs time to stabilize again after the sample type has changed
                # TODO dm: Redo
                # skip_buckets = True

            total_count += sample.total_ops
            interval = max(sample.absolute_time - start_time, interval)

            # avoid division by zero
            if interval > 0 and interval >= current_bucket:
                current_bucket = int(interval) + bucket_interval_secs
                throughput = (total_count / interval)
                # skip buckets until throughput catches up. This avoids artifacts introduced by resetting calculation parameters after
                # the sample type has changed
                if not skip_buckets or throughput > last_throughput:
                    skip_buckets = False
                    last_throughput = throughput
                    global_throughput[op].append((sample.absolute_time, sample.relative_time, current_sample_type, throughput))
    return global_throughput


def moving_average(data, window=3):
    average_data = []
    for idx, record in enumerate(data):
        if idx < window:
            average_data.append(record)
        elif idx >= len(data) - window:
            average_data.append(record)
        else:
            sum_in_window = 0
            # also include upper bound
            for offset in range(-window, window + 1):
                _, _, _, v = data[idx + offset]
                sum_in_window += v
            absolute_time, relative_time, sample_type, value = record
            average_data.append((absolute_time, relative_time, sample_type, sum_in_window / (2 * window + 1)))
    return average_data


def execute_schedule(schedule, es, sampler):
    """
    Executes tasks according to the schedule for a given operation.

    :param schedule: The schedule for this operation.
    :param es: Elasticsearch client that will be used to execute the operation.
    :param sampler: A container to store raw samples.
    """
    relative = None
    previous_sample_type = None
    total_start = time.perf_counter()
    curr_total_it = 1
    # noinspection PyBroadException
    try:
        for expected_scheduled_time, sample_type_calculator, curr_iteration, total_it_for_sample_type, total_it_for_task, runner, params in schedule:
            sample_type = sample_type_calculator(total_start)
            # restart the relative time when the sample type changes. This way all warmup samples and measurement samples will start at
            # the relative time zero which simplifies throughput calculation.
            #
            # Assumption: We always get the same operation here, otherwise simply resetting one timer will not work!
            if sample_type != previous_sample_type:
                relative = time.perf_counter()
                previous_sample_type = sample_type
            # expected_scheduled_time is relative to the start of the first iteration
            absolute_expected_schedule_time = relative + expected_scheduled_time
            throughput_throttled = expected_scheduled_time > 0
            if throughput_throttled:
                rest = absolute_expected_schedule_time - time.perf_counter()
                if rest > 0:
                    time.sleep(rest)
            start = time.perf_counter()
            with runner:
                weight = runner(es, params)
            stop = time.perf_counter()

            service_time = stop - start
            # Do not calculate latency separately when we don't throttle throughput. This metric is just confusing then.
            latency = stop - absolute_expected_schedule_time if throughput_throttled else service_time
            sampler.add(sample_type, convert.seconds_to_ms(latency), convert.seconds_to_ms(service_time), weight, (stop - relative),
                        curr_total_it, total_it_for_task)
            curr_total_it += 1
    except BaseException:
        logger.exception("Could not execute schedule")
        raise


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
def schedule_for(task, client_index, indices):
    """
    Calculates a client's schedule for a given task.

    :param task: The task that should be executed.
    :param client_index: The current client index.  Must be in the range [0, `task.clients').
    :param indices: Specification of affected indices.
    :return: A generator for the operations the given client needs to perform for this task.
    """
    op = task.operation
    num_clients = task.clients
    target_throughput = task.target_throughput / num_clients if task.target_throughput else None

    if op.type == track.OperationType.Index:
        runner = BulkIndex()
        return bulk_data_based(target_throughput, task.warmup_time_period, num_clients, client_index, indices, runner,
                               op.params["bulk-size"], op.params["id-conflicts"], op.params["pipeline"])
    else:
        runner = runners[op.type]()
        # will queries always be iteration based?
        return iteration_count_based(target_throughput, task.warmup_iterations // num_clients, task.iterations // num_clients, runner,
                                     op.params)


def iteration_count_based(target_throughput, warmup_iterations, iterations, runner, params=None):
    """

    Calculates the necessary schedule based on a given number of iterations.

    :param target_throughput: The desired target throughput in operations / second or None if throughput should not be limited.
    :param warmup_iterations: The number of warmup iterations to run. 0 if no warmup should be performed.
    :param iterations: The number of measurement iterations to run.
    :param runner: The runner for a given operation.
    :param params: The parameters for a given operation.
    :return: A generator for the corresponding parameters.
    """
    wait_time = 1 / target_throughput if target_throughput else 0
    total_iterations = warmup_iterations + iterations
    if params is None:
        params = []
    for i in range(0, warmup_iterations):
        yield (wait_time * i, lambda start: metrics.SampleType.Warmup, i, warmup_iterations, total_iterations, runner, params)

    for i in range(0, iterations):
        yield (wait_time * i, lambda start: metrics.SampleType.Normal, i, iterations, total_iterations, runner, params)


def build_conflicting_ids(conflicts, docs_to_index, offset):
    if conflicts is None or conflicts == track.IndexIdConflict.NoConflicts:
        return None
    logger.info("build ids with id conflicts of type [%s]" % conflicts)
    all_ids = [0] * docs_to_index
    for i in range(docs_to_index):
        # always consider the offset as each client will index its own range and we don't want uncontrolled conflicts across clients
        if conflicts == track.IndexIdConflict.SequentialConflicts:
            all_ids[i] = "%10d" % (offset + i)
        elif conflicts == track.IndexIdConflict.RandomConflicts:
            all_ids[i] = "%10d" % random.randint(offset, offset + docs_to_index)
        else:
            raise exceptions.SystemSetupError('Unknown id conflict type %s' % conflicts)
    return all_ids


def chain(*iterables):
    """
    Chains the given iterables similar to `itertools.chain` except that it also respects the context manager contract.

    :param iterables: A number of iterable that should be chained.
    :return: An iterable that will delegate to all provided iterables in turn.
    """
    for it in iterables:
        # execute within a context
        with it:
            for element in it:
                yield element


def number_of_bulks(indices, bulk_size, client_index, num_clients):
    """
    Determines the number of bulk operations needed to index all documents in the given indices.

    :param indices: Specification of affected indices.
    :param bulk_size: The size of bulk index operations (number of documents per bulk).
    :param client_index: The current client index.  Must be in the range [0, `num_clients').
    :param num_clients: The total number of clients that will run bulk index operations.
    :return: The number of bulk operations that the given client will issue.
    """
    bulks = 0
    for index in indices:
        for type in index.types:
            offset, num_docs = bounds(type.number_of_documents, client_index, num_clients)
            complete_bulks, rest = (num_docs // bulk_size, num_docs % bulk_size)
            bulks += complete_bulks
            if rest > 0:
                bulks += 1
    return bulks


def create_default_reader(index, type, offset, num_docs, bulk_size, id_conflicts):
    return IndexDataReader(type.document_file, num_docs,
                           build_conflicting_ids(id_conflicts, num_docs, offset), index.name, type.name, bulk_size, offset)


def bounds(total_docs, client_index, num_clients):
    """

    Calculates the start offset and number of documents for each client.

    :param total_docs: The total number of documents to index.
    :param client_index: The current client index.  Must be in the range [0, `num_clients').
    :param num_clients: The total number of clients that will run bulk index operations.
    :return: A tuple containing the start offset for the document corpus and the number documents that the client should index.
    """
    # last client gets one less if the number is uneven
    if client_index + 1 == num_clients and total_docs % num_clients > 0:
        correction = 1
    else:
        correction = 0
    docs_per_client = round(total_docs / num_clients) - correction
    # don't consider the correction for the other clients because it just applies to the last one
    offset = client_index * (docs_per_client + correction)
    return offset, docs_per_client


def bulk_data_based(target_throughput, warmup_time_period, num_clients, client_index, indices, runner, bulk_size, id_conflicts, pipeline,
                    create_reader=create_default_reader):
    """
    Calculates the necessary schedule for bulk operations.


    :param target_throughput: The desired target throughput in operations / second or None if throughput should not be limited.
    :param warmup_time_period: The time period in seconds that is considered for warmup.
    :param num_clients: The total number of clients that will run the bulk operation.
    :param client_index: The current client for which we calculated the schedule. Must be in the range [0, `num_clients').
    :param indices: Specification of affected indices.
    :param runner: The runner for a given operation.
    :param bulk_size: The size of bulk index operations (number of documents per bulk).
    :param id_conflicts: The type of id conflicts to simulate.
    :param pipeline: Name of the ingest pipeline to use. May be None.
    :param create_reader: A function to create the index reader. By default a file based index reader will be created. This parameter is
                          intended for testing only.
    :return: A generator for the bulk operations of the given client.
    """
    logger.info(
        "Creating bulk data based generator for target throughput [%s], client index [%d] of [%d] clients total with bulk size [%d]." %
        (str(target_throughput), client_index, num_clients, bulk_size))

    bulks = number_of_bulks(indices, bulk_size, client_index, num_clients)
    wait_time = 1 / target_throughput if target_throughput else 0
    # determine amount of warmup iterations based on number of records to index. Problem: this assumes we index all data. Depending on the
    # schedule this may not be true (as we may terminate earlier)

    iterations = bulks
    readers = []
    for index in indices:
        for type in index.types:
            offset, num_docs = bounds(type.number_of_documents, client_index, num_clients)
            logger.info("Client [%d] will index [%d] docs starting from offset [%d] for [%s/%s]" %
                        (client_index, num_docs, offset, index, type))
            readers.append(create_reader(index, type, offset, num_docs, bulk_size, id_conflicts))
    reader = chain(*readers)
    it = 0
    for bulk in reader:
        params = {"body": bulk}
        if pipeline:
            params["pipeline"] = pipeline
        yield (wait_time * it,
               lambda start: metrics.SampleType.Warmup if time.perf_counter() - start < warmup_time_period else metrics.SampleType.Normal,
               it, iterations, iterations, runner, params)
        it += 1


class Runner:
    """
    Base class for all operations against Elasticsearch.
    """

    def __enter__(self):
        return self

    def __call__(self, *args):
        """
        Runs the actual method that should be benchmarked.

        :param args: All arguments that are needed to call this method.
        :return: An int indicating the "weight" of this call. This is typically 1 but for bulk operations it should be the actual bulk size.
        """
        raise NotImplementedError("abstract operation")

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class BulkIndex(Runner):
    """
    Bulk indexes the given documents.

    It expects the parameter hash to contain a key "body" containing all documents for the current bulk request.

    """

    def __call__(self, es, params):
        bulk_params = {}
        if "pipeline" in params:
            bulk_params["pipeline"] = params["pipeline"]

        response = es.bulk(body=params["body"], params=bulk_params)
        if response["errors"]:
            for idx, item in enumerate(response["items"]):
                if item["index"]["status"] != 201:
                    msg = "Could not bulk index. "
                    msg += "Error in line [%d]\n" % (idx + 1)
                    msg += "Bulk item: [%s]\n" % item
                    msg += "Buffer size is [%d]\n" % idx
                    raise exceptions.DataError(msg)
        # at this point, the bulk will always contain a separate meta data line
        return len(params["body"]) // 2


class ForceMerge(Runner):
    """
    Runs a force merge operation against Elasticsearch.
    """

    def __call__(self, es, params):
        indices = ",".join(params["indices"])
        logger.info("Force merging indices [%s]." % indices)
        try:
            es.indices.forcemerge(index=indices)
        except elasticsearch.TransportError as e:
            # this is caused by older versions of Elasticsearch (< 2.1), fall back to optimize
            if e.status_code == 400:
                es.indices.optimize(index=indices)
            else:
                raise e
        return 1


class IndicesStats(Runner):
    """
    Gather index stats for all indices.
    """

    def __call__(self, es, params):
        es.indices.stats(metric="_all")
        return 1


class NodeStats(Runner):
    """
    Gather node stats for all nodes.
    """

    def __call__(self, es, params):
        es.nodes.stats(metric="_all")
        return 1


class Query(Runner):
    """
    Runs a request body search against Elasticsearch.

    It expects at least the following keys in the `params` hash:

    * `index`: The index or indices against which to issue the query.
    * `type`: See `index`
    * `use_request_cache`: True iff the request cache should be used.
    * `body`: Query body

    If the following parameters are present in addition, a scroll query will be issued:

    * `pages`: Number of pages to retrieve at most for this scroll. If a scroll query does yield less results than the specified number of
               pages we will terminate earlier.
    * `items_per_page`: Number of items to retrieve per page.

    """

    def __init__(self):
        self.scroll_id = None
        self.es = None

    def __call__(self, es, params):
        if "pages" in params and "items_per_page" in params:
            return self.scroll_query(es, params)
        else:
            return self.request_body_query(es, params)

    def request_body_query(self, es, params):
        es.search(index=params["index"], doc_type=params["type"], request_cache=params["use_request_cache"], body=params["body"])
        return 1

    def scroll_query(self, es, params):
        self.es = es
        r = es.search(
            index=params["index"],
            doc_type=params["type"],
            body=params["body"],
            sort="_doc",
            scroll="10s",
            size=params["items_per_page"],
            request_cache=params["use_request_cache"])
        self.scroll_id = r["_scroll_id"]
        total_pages = params["pages"]
        # Note that starting with ES 2.0, the initial call to search() returns already the first result page
        # so we have to retrieve one page less
        for page in range(total_pages - 1):
            hit_count = len(r["hits"]["hits"])
            if hit_count == 0:
                # We're done prematurely. Even if we are on page index zero, we still made one call.
                return page + 1
            r = es.scroll(scroll_id=self.scroll_id, scroll="10s")
        return total_pages

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.scroll_id and self.es:
            self.es.clear_scroll(scroll_id=self.scroll_id)
            self.scroll_id = None
            self.es = None
        return False


runners = {
    track.OperationType.Search: Query,
    track.OperationType.ForceMerge: ForceMerge,
    track.OperationType.Index: BulkIndex,
    track.OperationType.IndicesStats: IndicesStats,
    track.OperationType.NodesStats: NodeStats,
}


class FileSource:
    @staticmethod
    def open(file_name, mode):
        return FileSource(file_name, mode)

    def __init__(self, file_name, mode):
        self.f = open(file_name, mode)

    def seek(self, offset):
        self.f.seek(offset)

    def readline(self):
        return self.f.readline()

    def close(self):
        self.f.close()
        self.f = None


class IndexDataReader:
    """
    Reads an index file in bulks into an array and also adds the necessary meta-data line before each document.
    """

    def __init__(self, data_file, docs_to_index, conflicting_ids, index_name, type_name, bulk_size, offset=0, file_source=FileSource):
        self.data_file = data_file
        self.docs_to_index = docs_to_index
        self.conflicting_ids = conflicting_ids
        self.index_name = index_name
        self.type_name = type_name
        self.bulk_size = bulk_size
        self.id_up_to = 0
        self.current_bulk = 0
        self.offset = offset
        self.file_source = file_source
        self.f = None

    def __enter__(self):
        self.f = self.file_source.open(self.data_file, 'rt')
        # skip offset number of lines
        logger.info("Skipping %d lines in [%s]." % (self.offset, self.data_file))
        start = time.perf_counter()
        io.skip_lines(self.data_file, self.f, self.offset)
        end = time.perf_counter()
        logger.info("Skipping %d lines took %f s." % (self.offset, end - start))
        return self

    def __iter__(self):
        return self

    def __next__(self):
        """
        Returns lines for one bulk request.
        """
        buffer = []
        try:
            docs_indexed = 0
            docs_left = self.docs_to_index - (self.current_bulk * self.bulk_size)
            if self.f is None or docs_left <= 0:
                raise StopIteration()

            this_bulk_size = min(self.bulk_size, docs_left)
            while docs_indexed < this_bulk_size:
                line = self.f.readline()
                if len(line) == 0:
                    break
                line = line.strip()
                if self.conflicting_ids is not None:
                    # 25% of the time we replace a doc:
                    if self.id_up_to > 0 and random.randint(0, 3) == 3:
                        doc_id = self.conflicting_ids[random.randint(0, self.id_up_to - 1)]
                    else:
                        doc_id = self.conflicting_ids[self.id_up_to]
                        self.id_up_to += 1
                    cmd = '{"index": {"_index": "%s", "_type": "%s", "_id": "%s"}}' % (self.index_name, self.type_name, doc_id)
                else:
                    cmd = '{"index": {"_index": "%s", "_type": "%s"}}' % (self.index_name, self.type_name)

                buffer.append(cmd)
                buffer.append(line)

                docs_indexed += 1

            self.current_bulk += 1
            return buffer
        except IOError:
            logger.exception("Could not read [%s]" % self.data_file)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.f:
            self.f.close()
            self.f = None
        return False
