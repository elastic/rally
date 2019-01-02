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

import concurrent.futures
import threading
import datetime
import logging
import queue
import time

import thespian.actors
from esrally import actor, config, exceptions, metrics, track, client, paths, PROGRAM_NAME
from esrally.driver import runner, scheduler
from esrally.utils import convert, console, net


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


class PrepareTrack:
    """
    Initiates preparation of a track.

    """
    def __init__(self, cfg, track):
        """
        :param cfg: Rally internal configuration object.
        :param track: The track to use.
        """
        self.config = cfg
        self.track = track


class TrackPrepared:
    pass


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


class CompleteCurrentTask:
    """
    Tells a load generator to prematurely complete its current task. This is used to model task dependencies for parallel tasks (i.e. if a
    specific task that is marked accordingly in the track finishes, it will also signal termination of all other tasks in the same parallel
    element).
    """
    pass


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
        # Using perf_counter here is fine even in the distributed case. Although we "leak" this value to other machines, we will only
        # ever interpret this value on the same machine (see `Drive` and the implementation in `Driver#joinpoint_reached()`).
        self.client_local_timestamp = time.perf_counter()
        self.task = task


class BenchmarkComplete:
    """
    Indicates that the benchmark is complete.
    """

    def __init__(self, metrics):
        self.metrics = metrics


class TaskFinished:
    def __init__(self, metrics, next_task_scheduled_in):
        self.metrics = metrics
        self.next_task_scheduled_in = next_task_scheduled_in


class DriverActor(actor.RallyActor):
    RESET_RELATIVE_TIME_MARKER = "reset_relative_time"

    WAKEUP_INTERVAL_SECONDS = 1

    # post-process request metrics every N seconds and send it to the metrics store
    POST_PROCESS_INTERVAL_SECONDS = 30

    """
    Coordinates all worker drivers. This is actually only a thin actor wrapper layer around ``Driver`` which does the actual work.
    """

    def __init__(self):
        super().__init__()
        self.start_sender = None
        self.coordinator = None
        self.status = "init"
        self.post_process_timer = 0

    def receiveMsg_PoisonMessage(self, poisonmsg, sender):
        self.logger.error("Main driver received a fatal indication from a load generator (%s). Shutting down.", poisonmsg.details)
        self.coordinator.close()
        self.send(self.start_sender, actor.BenchmarkFailure("Fatal track or load generator indication", poisonmsg.details))

    def receiveMsg_BenchmarkFailure(self, msg, sender):
        self.logger.error("Main driver received a fatal exception from a load generator. Shutting down.")
        self.coordinator.close()
        self.send(self.start_sender, msg)

    def receiveMsg_BenchmarkCancelled(self, msg, sender):
        self.logger.info("Main driver received a notification that the benchmark has been cancelled.")
        self.coordinator.close()
        self.send(self.start_sender, msg)

    def receiveMsg_ActorExitRequest(self, msg, sender):
        self.logger.info("Main driver received ActorExitRequest and will terminate all load generators.")
        self.status = "exiting"

    def receiveMsg_ChildActorExited(self, msg, sender):
        # is it a driver?
        if msg.childAddress in self.coordinator.drivers:
            driver_index = self.coordinator.drivers.index(msg.childAddress)
            if self.status == "exiting":
                self.logger.info("Load generator [%d] has exited.", driver_index)
            else:
                self.logger.error("Load generator [%d] has exited prematurely. Aborting benchmark.", driver_index)
                self.send(self.start_sender, actor.BenchmarkFailure("Load generator [{}] has exited prematurely.".format(driver_index)))
        else:
            self.logger.info("A track preparator has exited.")

    def receiveUnrecognizedMessage(self, msg, sender):
        self.logger.info("Main driver received unknown message [%s] (ignoring).", str(msg))

    @actor.no_retry("driver")
    def receiveMsg_StartBenchmark(self, msg, sender):
        self.start_sender = sender
        self.coordinator = Driver(self, msg.config)
        self.coordinator.start_benchmark(msg.track, msg.lap, msg.metrics_meta_info)
        self.wakeupAfter(datetime.timedelta(seconds=DriverActor.WAKEUP_INTERVAL_SECONDS))

    @actor.no_retry("driver")
    def receiveMsg_TrackPrepared(self, msg, sender):
        self.transition_when_all_children_responded(sender, msg,
                                                    expected_status=None, new_status=None, transition=self.after_track_prepared)

    @actor.no_retry("driver")
    def receiveMsg_JoinPointReached(self, msg, sender):
        self.coordinator.joinpoint_reached(msg.client_id, msg.client_local_timestamp, msg.task)

    @actor.no_retry("driver")
    def receiveMsg_UpdateSamples(self, msg, sender):
        self.coordinator.update_samples(msg.samples)

    @actor.no_retry("driver")
    def receiveMsg_WakeupMessage(self, msg, sender):
        if msg.payload == DriverActor.RESET_RELATIVE_TIME_MARKER:
            self.coordinator.reset_relative_time()
        elif not self.coordinator.finished():
            self.post_process_timer += DriverActor.WAKEUP_INTERVAL_SECONDS
            if self.post_process_timer >= DriverActor.POST_PROCESS_INTERVAL_SECONDS:
                self.post_process_timer = 0
                self.coordinator.post_process_samples()
            self.coordinator.update_progress_message()
            self.wakeupAfter(datetime.timedelta(seconds=DriverActor.WAKEUP_INTERVAL_SECONDS))

    def create_client(self, client_id, host):
        return self.createActor(LoadGenerator,
                                #globalName="/rally/driver/worker/%s" % str(client_id),
                                targetActorRequirements=self._requirements(host))

    def start_load_generator(self, driver, client_id, cfg, track, allocations):
        self.send(driver, StartLoadGenerator(client_id, cfg, track, allocations))

    def drive_at(self, driver, client_start_timestamp):
        self.send(driver, Drive(client_start_timestamp))

    def complete_current_task(self, driver):
        self.send(driver, CompleteCurrentTask())

    def on_task_finished(self, metrics, next_task_scheduled_in):
        if next_task_scheduled_in > 0:
            self.wakeupAfter(datetime.timedelta(seconds=next_task_scheduled_in), payload=DriverActor.RESET_RELATIVE_TIME_MARKER)
        else:
            self.coordinator.reset_relative_time()
        self.send(self.start_sender, TaskFinished(metrics, next_task_scheduled_in))

    def create_track_preparator(self, host):
        return self.createActor(TrackPreparationActor, targetActorRequirements=self._requirements(host))

    def _requirements(self, host):
        if host == "localhost":
            return {"coordinator": True}
        else:
            return {"ip": host}

    def on_prepare_track(self, preparators, cfg, track):
        self.children = preparators
        msg = PrepareTrack(cfg, track)
        for child in self.children:
            self.send(child, msg)

    def after_track_prepared(self):
        for child in self.children:
            self.send(child, thespian.actors.ActorExitRequest())
        self.children = []
        self.coordinator.after_track_prepared()

    def on_benchmark_complete(self, metrics):
        self.send(self.start_sender, BenchmarkComplete(metrics))


def load_local_config(coordinator_config):
    cfg = config.auto_load_local_config(coordinator_config, additional_sections=[
        # only copy the relevant bits
        "track", "driver", "client",
        # due to distribution version...
        "mechanic"
    ])
    # set root path (normally done by the main entry point)
    cfg.add(config.Scope.application, "node", "rally.root", paths.rally_root())
    return cfg


class TrackPreparationActor(actor.RallyActor):
    def __init__(self):
        super().__init__()

    @actor.no_retry("track preparator")
    def receiveMsg_PrepareTrack(self, msg, sender):
        # load node-specific config to have correct paths available
        cfg = load_local_config(msg.config)
        self.logger.info("Preparing track [%s]", msg.track.name)
        # for "proper" track repositories this will ensure that all state is identical to the coordinator node. For simple tracks
        # the track is usually self-contained but in some cases (plugins are defined) we still need to ensure that the track
        # is present on all machines.
        if msg.track.has_plugins:
            track.track_repo(cfg, fetch=True, update=True)
            # we also need to load track plugins eagerly as the respective parameter sources could require
            track.load_track_plugins(cfg, runner.register_runner, scheduler.register_scheduler)
        # Beware: This is a potentially long-running operation and we're completely blocking our actor here. We should do this
        # maybe in a background thread.
        track.prepare_track(msg.track, cfg)
        self.send(sender, TrackPrepared())


class Driver:
    def __init__(self, target, config):
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
        self.track = None
        self.challenge = None
        self.metrics_store = None
        self.load_driver_hosts = []
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

    def start_benchmark(self, t, lap, metrics_meta_info):
        self.track = t
        self.challenge = select_challenge(self.config, self.track)
        self.quiet = self.config.opts("system", "quiet.mode", mandatory=False, default_value=False)
        self.throughput_calculator = ThroughputCalculator()
        self.metrics_store = metrics.metrics_store(cfg=self.config,
                                                   track=self.track.name,
                                                   challenge=self.challenge.name,
                                                   meta_info=metrics_meta_info,
                                                   lap=lap,
                                                   read_only=False)
        for host in self.config.opts("driver", "load_driver_hosts"):
            if host != "localhost":
                self.load_driver_hosts.append(net.resolve(host))
            else:
                self.load_driver_hosts.append(host)

        preps = [self.target.create_track_preparator(h) for h in self.load_driver_hosts]
        self.target.on_prepare_track(preps, self.config, self.track)

    def after_track_prepared(self):
        self.logger.info("Benchmark is about to start.")
        # ensure relative time starts when the benchmark starts.
        self.reset_relative_time()

        allocator = Allocator(self.challenge.schedule)
        self.allocations = allocator.allocations
        self.number_of_steps = len(allocator.join_points) - 1
        self.tasks_per_join_point = allocator.tasks_per_joinpoint

        self.logger.info("Benchmark consists of [%d] steps executed by (at most) [%d] clients as specified by the allocation matrix:\n%s",
                         self.number_of_steps, len(self.allocations), self.allocations)

        for client_id in range(allocator.clients):
            # allocate clients round-robin to all defined hosts
            host = self.load_driver_hosts[client_id % len(self.load_driver_hosts)]
            self.logger.info("Allocating load generator [%d] on [%s]", client_id, host)
            self.drivers.append(self.target.create_client(client_id, host))
        for client_id, driver in enumerate(self.drivers):
            self.logger.info("Starting load generator [%d].", client_id)
            self.target.start_load_generator(driver, client_id, self.config, self.track, self.allocations[client_id])

        self.update_progress_message()

    def joinpoint_reached(self, client_id, client_local_timestamp, task):
        self.currently_completed += 1
        self.clients_completed_current_step[client_id] = (client_local_timestamp, time.perf_counter())
        self.logger.info("[%d/%d] drivers reached join point [%d/%d].",
                         self.currently_completed, len(self.drivers), self.current_step + 1, self.number_of_steps)
        if self.currently_completed == len(self.drivers):
            self.logger.info("All drivers completed their tasks until join point [%d/%d].", self.current_step + 1, self.number_of_steps)
            # we can go on to the next step
            self.currently_completed = 0
            self.complete_current_task_sent = False
            # make a copy and reset early to avoid any race conditions from clients that reach a join point already while we are sending...
            clients_curr_step = self.clients_completed_current_step
            self.clients_completed_current_step = {}
            self.update_progress_message(task_finished=True)
            # clear per step
            self.most_recent_sample_per_client = {}
            self.current_step += 1

            self.logger.debug("Postprocessing samples...")
            self.post_process_samples()
            m = self.metrics_store.to_externalizable(clear=True)

            if self.finished():
                self.logger.info("All steps completed.")
                self.logger.debug("Closing metrics store...")
                self.metrics_store.close()
                # immediately clear as we don't need it anymore and it can consume a significant amount of memory
                del self.metrics_store
                self.logger.debug("Sending benchmark results...")
                self.target.on_benchmark_complete(m)
            else:
                if self.config.opts("track", "test.mode.enabled"):
                    # don't wait if test mode is enabled and start the next task immediately.
                    waiting_period = 0
                else:
                    # start the next task in one second (relative to master's timestamp)
                    #
                    # Assumption: We don't have a lot of clock skew between reaching the join point and sending the next task
                    #             (it doesn't matter too much if we're a few ms off).
                    waiting_period = 1.0
                self.target.on_task_finished(m, waiting_period)
                # Using a perf_counter here is fine also in the distributed case as we subtract it from `master_received_msg_at` making it
                # a relative instead of an absolute value.
                start_next_task = time.perf_counter() + waiting_period
                for client_id, driver in enumerate(self.drivers):
                    client_ended_task_at, master_received_msg_at = clients_curr_step[client_id]
                    client_start_timestamp = client_ended_task_at + (start_next_task - master_received_msg_at)
                    self.logger.info("Scheduling next task for client id [%d] at their timestamp [%f] (master timestamp [%f])",
                                     client_id, client_start_timestamp, start_next_task)
                    self.target.drive_at(driver, client_start_timestamp)
        else:
            current_join_point = task
            # we need to actively send CompleteCurrentTask messages to all remaining clients.
            if current_join_point.preceding_task_completes_parent and not self.complete_current_task_sent:
                self.logger.info("Tasks before [%s] are able to complete the parent structure. Checking if clients [%s] have finished yet.",
                                 current_join_point, current_join_point.clients_executing_completing_task)
                # are all clients executing said task already done? if so we need to notify the remaining clients
                all_clients_finished = True
                for client_id in current_join_point.clients_executing_completing_task:
                    if client_id not in self.clients_completed_current_step:
                        self.logger.info("Client id [%s] did not yet finish.", client_id)
                        # do not break here so we can see all remaining clients in the log output.
                        all_clients_finished = False
                if all_clients_finished:
                    # As we are waiting for other clients to finish, we would send this message over and over again. Hence we need to
                    # memorize whether we have already sent it for the current step.
                    self.complete_current_task_sent = True
                    self.logger.info("All affected clients have finished. Notifying all clients to complete their current tasks.")
                    for client_id, driver in enumerate(self.drivers):
                        self.target.complete_current_task(driver)

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
        self.raw_samples += samples
        if len(samples) > 0:
            most_recent = samples[-1]
            self.most_recent_sample_per_client[most_recent.client_id] = most_recent

    def update_progress_message(self, task_finished=False):
        if not self.quiet and self.current_step >= 0:
            tasks = ",".join([t.name for t in self.tasks_per_join_point[self.current_step]])

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


class LoadGenerator(actor.RallyActor):
    """
    The actual driver that applies load against the cluster(s).

    It will also regularly send measurements to the master node so it can consolidate them.
    """

    WAKEUP_INTERVAL_SECONDS = 5

    def __init__(self):
        super().__init__()
        self.master = None
        self.client_id = None
        self.es = None
        self.config = None
        self.track = None
        self.tasks = None
        self.current_task_index = 0
        self.current_task = None
        self.abort_on_error = False
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        # cancellation via future does not work, hence we use our own mechanism with a shared variable and polling
        self.cancel = threading.Event()
        # used to indicate that we want to prematurely consider this completed. This is *not* due to cancellation but a regular event in
        # a benchmark and used to model task dependency of parallel tasks.
        self.complete = threading.Event()
        self.executor_future = None
        self.sampler = None
        self.start_driving = False
        self.wakeup_interval = LoadGenerator.WAKEUP_INTERVAL_SECONDS

    @actor.no_retry("load generator")
    def receiveMsg_StartLoadGenerator(self, msg, sender):
        def es_clients(all_hosts, all_client_options):
            es = {}
            for cluster_name, cluster_hosts in all_hosts.items():
                es[cluster_name] = client.EsClientFactory(cluster_hosts, all_client_options[cluster_name]).create()
            return es

        self.logger.info("LoadGenerator[%d] is about to start.", msg.client_id)
        self.master = sender
        self.client_id = msg.client_id
        self.config = load_local_config(msg.config)
        self.abort_on_error = self.config.opts("driver", "on.error") == "abort"
        self.es = es_clients(self.config.opts("client", "hosts").all_hosts, self.config.opts("client", "options").all_client_options)
        self.track = msg.track
        track.set_absolute_data_path(self.config, self.track)
        self.tasks = msg.tasks
        self.current_task_index = 0
        self.cancel.clear()
        self.current_task = None
        # we need to wake up more often in test mode
        if self.config.opts("track", "test.mode.enabled"):
            self.wakeup_interval = 0.5
        runner.register_default_runners()
        if self.track.has_plugins:
            track.load_track_plugins(self.config, runner.register_runner, scheduler.register_scheduler)
        self.drive()

    @actor.no_retry("load generator")
    def receiveMsg_Drive(self, msg, sender):
        sleep_time = datetime.timedelta(seconds=msg.client_start_timestamp - time.perf_counter())
        self.logger.info("LoadGenerator[%d] is continuing its work at task index [%d] on [%f], that is in [%s].",
                         self.client_id, self.current_task_index, msg.client_start_timestamp, sleep_time)
        self.start_driving = True
        self.wakeupAfter(sleep_time)

    @actor.no_retry("load generator")
    def receiveMsg_CompleteCurrentTask(self, msg, sender):
        # finish now ASAP. Remaining samples will be sent with the next WakeupMessage. We will also need to skip to the next
        # JoinPoint. But if we are already at a JoinPoint at the moment, there is nothing to do.
        if self.at_joinpoint():
            self.logger.info("LoadGenerator[%s] has received CompleteCurrentTask but is currently at [%s]. Ignoring.",
                             str(self.client_id), self.current_task)
        else:
            self.logger.info("LoadGenerator[%s] has received CompleteCurrentTask. Completing current task [%s].",
                             str(self.client_id), self.current_task)
            self.complete.set()

    @actor.no_retry("load generator")
    def receiveMsg_WakeupMessage(self, msg, sender):
        # it would be better if we could send ourselves a message at a specific time, simulate this with a boolean...
        if self.start_driving:
            self.logger.info("LoadGenerator[%s] starts driving now.", str(self.client_id))
            self.start_driving = False
            self.drive()
        else:
            current_samples = self.send_samples()
            if self.cancel.is_set():
                self.logger.info("LoadGenerator[%s] has detected that benchmark has been cancelled. Notifying master...",
                                 str(self.client_id))
                self.send(self.master, actor.BenchmarkCancelled())
            elif self.executor_future is not None and self.executor_future.done():
                e = self.executor_future.exception(timeout=0)
                if e:
                    self.logger.info("LoadGenerator[%s] has detected a benchmark failure. Notifying master...", str(self.client_id))
                    # the exception might be user-defined and not be on the load path of the master driver. Hence, it cannot be
                    # deserialized on the receiver so we convert it here to a plain string.
                    self.send(self.master, actor.BenchmarkFailure("Error in load generator [{}]".format(self.client_id), str(e)))
                else:
                    self.logger.info("LoadGenerator[%s] is ready for the next task.", str(self.client_id))
                    self.executor_future = None
                    self.drive()
            else:
                if current_samples and len(current_samples) > 0:
                    most_recent_sample = current_samples[-1]
                    if most_recent_sample.percent_completed is not None:
                        self.logger.debug("LoadGenerator[%s] is executing [%s] (%.2f%% complete).",
                                          str(self.client_id), most_recent_sample.task, most_recent_sample.percent_completed * 100.0)
                    else:
                        self.logger.debug("LoadGenerator[%s] is executing [%s] (dependent eternal task).",
                                          str(self.client_id), most_recent_sample.task)
                else:
                    self.logger.debug("LoadGenerator[%s] is executing (no samples).", str(self.client_id))
                self.wakeupAfter(datetime.timedelta(seconds=self.wakeup_interval))

    def receiveMsg_ActorExitRequest(self, msg, sender):
        self.logger.info("LoadGenerator[%s] is exiting due to ActorExitRequest.", str(self.client_id))
        if self.executor_future is not None and self.executor_future.running():
            self.cancel.set()
            self.pool.shutdown()

    def receiveMsg_BenchmarkFailure(self, msg, sender):
        # sent by our no_retry infrastructure; forward to master
        self.send(self.master, msg)

    def receiveUnrecognizedMessage(self, msg, sender):
        self.logger.info("LoadGenerator[%d] received unknown message [%s] (ignoring).", self.client_id, str(msg))

    def drive(self):
        profiling_enabled = self.config.opts("driver", "profiling")
        task_allocation = self.current_task_and_advance()
        # skip non-tasks in the task list
        while task_allocation is None:
            task_allocation = self.current_task_and_advance()
        self.current_task = task_allocation

        if isinstance(task_allocation, JoinPoint):
            self.logger.info("LoadGenerator[%d] reached join point [%s].", self.client_id, task_allocation)
            # clients that don't execute tasks don't need to care about waiting
            if self.executor_future is not None:
                self.executor_future.result()
            self.send_samples()
            self.cancel.clear()
            self.complete.clear()
            self.executor_future = None
            self.sampler = None
            self.send(self.master, JoinPointReached(self.client_id, task_allocation))
        elif isinstance(task_allocation, TaskAllocation):
            task = task_allocation.task
            # There may be a situation where there are more (parallel) tasks than clients. If we were asked to complete all tasks, we not
            # only need to complete actively running tasks but actually all scheduled tasks until we reach the next join point.
            if self.complete.is_set():
                self.logger.info("LoadGenerator[%d] skips [%s] because it has been asked to complete all tasks until next join point.",
                                 self.client_id, task)
            else:
                self.logger.info("LoadGenerator[%d] is executing [%s].", self.client_id, task)
                self.sampler = Sampler(self.client_id, task, start_timestamp=time.perf_counter())
                # We cannot use the global client index here because we need to support parallel execution of tasks with multiple clients.
                #
                # Consider the following scenario:
                #
                # * Clients 0-3 bulk index into indexA
                # * Clients 4-7 bulk index into indexB
                #
                # Now we need to ensure that we start partitioning parameters correctly in both cases. And that means we need to start
                # from (client) index 0 in both cases instead of 0 for indexA and 4 for indexB.
                schedule = schedule_for(self.track, task_allocation.task, task_allocation.client_index_in_task)

                executor = Executor(task, schedule, self.es, self.sampler, self.cancel, self.complete, self.abort_on_error)
                final_executor = Profiler(executor, self.client_id, task) if profiling_enabled else executor

                self.executor_future = self.pool.submit(final_executor)
                self.wakeupAfter(datetime.timedelta(seconds=self.wakeup_interval))
        else:
            raise exceptions.RallyAssertionError("Unknown task type [%s]" % type(task_allocation))

    def at_joinpoint(self):
        return isinstance(self.current_task, JoinPoint)

    def current_task_and_advance(self):
        current = self.tasks[self.current_task_index]
        self.current_task_index += 1
        return current

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
        self.logger = logging.getLogger(__name__)

    def add(self, sample_type, request_meta_data, latency_ms, service_time_ms, total_ops, total_ops_unit, time_period, percent_completed):
        try:
            self.q.put_nowait(Sample(self.client_id, time.time(), time.perf_counter() - self.start_timestamp, self.task,
                                     sample_type, request_meta_data, latency_ms, service_time_ms, total_ops, total_ops_unit, time_period,
                                     percent_completed))
        except queue.Full:
            self.logger.warning("Dropping sample for [%s] due to a full sampling queue.", self.task.operation.name)

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
        # may be None for eternal tasks!
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

        :param samples: A list containing all samples from all load generators.
        :param bucket_interval_secs: The bucket interval for aggregations.
        :return: A global view of throughput samples.
        """

        import itertools
        samples_per_task = {}
        # first we group all samples by task (operation).
        for sample in samples:
            k = sample.task
            if k not in samples_per_task:
                samples_per_task[k] = []
            samples_per_task[k].append(sample)

        global_throughput = {}
        # with open("raw_samples_new.csv", "a") as sample_log:
        # print("client_id,absolute_time,relative_time,operation,sample_type,total_ops,time_period", file=sample_log)
        for k, v in samples_per_task.items():
            task = k
            if task not in global_throughput:
                global_throughput[task] = []
            # sort all samples by time
            if task in self.task_stats:
                samples = itertools.chain(v, self.task_stats[task].unprocessed)
            else:
                samples = v
            current_samples = sorted(samples, key=lambda s: s.absolute_time)

            if task not in self.task_stats:
                first_sample = current_samples[0]
                self.task_stats[task] = ThroughputCalculator.TaskStats(bucket_interval=bucket_interval_secs,
                                                                       sample_type=first_sample.sample_type,
                                                                       start_time=first_sample.absolute_time - first_sample.time_period)
            current = self.task_stats[task]
            count = current.total_count
            for sample in current_samples:
                # print("%d,%f,%f,%s,%s,%d,%f" %
                #       (sample.client_id, sample.absolute_time, sample.relative_time, sample.operation, sample.sample_type,
                #        sample.total_ops, sample.time_period), file=sample_log)

                # once we have seen a new sample type, we stick to it.
                current.maybe_update_sample_type(sample.sample_type)

                # we need to store the total count separately and cannot update `current.total_count` immediately here because we would
                # count all raw samples in `unprocessed` twice. Hence, we'll only update `current.total_count` when we have calculated a new
                # throughput sample.
                count += sample.total_ops
                current.update_interval(sample.absolute_time)

                if current.can_calculate_throughput():
                    current.finish_bucket(count)
                    global_throughput[task].append(
                        (sample.absolute_time, sample.relative_time, current.sample_type, current.throughput,
                         # we calculate throughput per second
                         "%s/s" % sample.total_ops_unit))
                else:
                    current.unprocessed.append(sample)

            # also include the last sample if we don't have one for the current sample type, even if it is below the bucket interval
            # (mainly needed to ensure we show throughput data in test mode)
            if current.can_add_final_throughput_sample():
                current.finish_bucket(count)
                global_throughput[task].append(
                    (sample.absolute_time, sample.relative_time, current.sample_type, current.throughput, "%s/s" % sample.total_ops_unit))

        return global_throughput


class Profiler:
    def __init__(self, target, client_id, task):
        """
        :param target: The actual executor which should be profiled.
        :param client_id: The id of the client that executes the operation.
        :param task: The task that is executed.
        """
        self.target = target
        self.client_id = client_id
        self.task = task
        self.profile_logger = logging.getLogger("rally.profile")

    def __call__(self, *args, **kwargs):
        import cProfile
        import pstats
        import io as python_io
        profiler = cProfile.Profile()
        profiler.enable()
        try:
            return self.target(*args, **kwargs)
        finally:
            profiler.disable()
            s = python_io.StringIO()
            sortby = 'cumulative'
            ps = pstats.Stats(profiler, stream=s).sort_stats(sortby)
            ps.print_stats()

            profile = "\n=== Profile START for client [%s] and task [%s] ===\n" % (str(self.client_id), str(self.task))
            profile += s.getvalue()
            profile += "=== Profile END for client [%s] and task [%s] ===" % (str(self.client_id), str(self.task))
            self.profile_logger.info(profile)


class Executor:
    def __init__(self, task, schedule, es, sampler, cancel, complete, abort_on_error=False):
        """
        Executes tasks according to the schedule for a given operation.

        :param task: The task that is executed.
        :param schedule: The schedule for this task.
        :param es: Elasticsearch client that will be used to execute the operation.
        :param sampler: A container to store raw samples.
        :param cancel: A shared boolean that indicates we need to cancel execution.
        :param complete: A shared boolean that indicates we need to prematurely complete execution.
        """
        self.task = task
        self.op = task.operation
        self.schedule = schedule
        self.es = es
        self.sampler = sampler
        self.cancel = cancel
        self.complete = complete
        self.abort_on_error = abort_on_error
        self.logger = logging.getLogger(__name__)

    def __call__(self, *args, **kwargs):
        total_start = time.perf_counter()
        # noinspection PyBroadException
        try:
            for expected_scheduled_time, sample_type, percent_completed, runner, params in self.schedule:
                if self.cancel.is_set():
                    self.logger.info("User cancelled execution.")
                    break
                absolute_expected_schedule_time = total_start + expected_scheduled_time
                throughput_throttled = expected_scheduled_time > 0
                if throughput_throttled:
                    rest = absolute_expected_schedule_time - time.perf_counter()
                    if rest > 0:
                        time.sleep(rest)
                start = time.perf_counter()
                total_ops, total_ops_unit, request_meta_data = execute_single(runner, self.es, params, self.abort_on_error)
                stop = time.perf_counter()

                service_time = stop - start
                # Do not calculate latency separately when we don't throttle throughput. This metric is just confusing then.
                latency = stop - absolute_expected_schedule_time if throughput_throttled else service_time
                # last sample should bump progress to 100% if externally completed.
                completed = percent_completed if not self.complete.is_set() else 1.0
                self.sampler.add(sample_type, request_meta_data, convert.seconds_to_ms(latency), convert.seconds_to_ms(service_time),
                                 total_ops, total_ops_unit, (stop - total_start), completed)

                if self.complete.is_set():
                    self.logger.info("Task is considered completed due to external event.")
                    break
        except BaseException:
            self.logger.exception("Could not execute schedule")
            raise
        finally:
            # Actively set it if this task completes its parent
            if self.task.completes_parent:
                self.complete.set()


def execute_single(runner, es, params, abort_on_error=False):
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


class JoinPoint:
    def __init__(self, id, clients_executing_completing_task=None):
        """

        :param id: The join point's id.
        :param clients_executing_completing_task: An array of client indices which execute a task that can prematurely complete its parent
        element. Provide 'None' or an empty array if no task satisfies this predicate.
        """
        if clients_executing_completing_task is None:
            clients_executing_completing_task = []
        self.id = id
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
    def __init__(self, task, client_index_in_task):
        self.task = task
        self.client_index_in_task = client_index_in_task

    def __hash__(self):
        return hash(self.task) ^ hash(self.client_index_in_task)

    def __eq__(self, other):
        return isinstance(other, type(self)) and self.task == other.task and self.client_index_in_task == other.client_index_in_task

    def __repr__(self, *args, **kwargs):
        return "TaskAllocation [%d/%d] for %s" % (self.client_index_in_task, self.task.clients, self.task)


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
            for sub_task in task:
                for client_index in range(start_client_index, start_client_index + sub_task.clients):
                    # this is the actual client that will execute the task. It may differ from the logical one in case we over-commit (i.e.
                    # more tasks than actually available clients)
                    physical_client_index = client_index % max_clients
                    if sub_task.completes_parent:
                        clients_executing_completing_task.append(physical_client_index)
                    allocations[physical_client_index].append(TaskAllocation(sub_task, client_index - start_client_index))
                start_client_index += sub_task.clients

            # uneven distribution between tasks and clients, e.g. there are 5 (parallel) tasks but only 2 clients. Then, one of them
            # executes three tasks, the other one only two. So we need to fill in a `None` for the second one.
            if start_client_index % max_clients > 0:
                # pin the index range to [0, max_clients). This simplifies the code below.
                start_client_index = start_client_index % max_clients
                for client_index in range(start_client_index, max_clients):
                    allocations[client_index].append(None)

            # let all clients join after each task, then we go on
            next_join_point = JoinPoint(join_point_id, clients_executing_completing_task)
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
def schedule_for(current_track, task, client_index):
    """
    Calculates a client's schedule for a given task.

    :param current_track: The current track.
    :param task: The task that should be executed.
    :param client_index: The current client index.  Must be in the range [0, `task.clients').
    :return: A generator for the operations the given client needs to perform for this task.
    """
    logger = logging.getLogger(__name__)
    op = task.operation
    num_clients = task.clients
    sched = scheduler.scheduler_for(task.schedule, task.params)
    logger.info("Choosing [%s] for [%s].", sched, task)
    runner_for_op = runner.runner_for(op.type)
    params_for_op = track.operation_parameters(current_track, op).partition(client_index, num_clients)

    if task.warmup_time_period is not None or task.time_period is not None:
        warmup_time_period = task.warmup_time_period if task.warmup_time_period else 0
        logger.info("Creating time-period based schedule with [%s] distribution for [%s] with a warmup period of [%s] seconds and a "
                    "time period of [%s] seconds.", task.schedule, task, str(warmup_time_period), str(task.time_period))
        return time_period_based(sched, warmup_time_period, task.time_period, runner_for_op, params_for_op)
    else:
        warmup_iterations = task.warmup_iterations if task.warmup_iterations else 0
        if task.iterations:
            iterations = task.iterations
        elif params_for_op.size():
            iterations = params_for_op.size() - warmup_iterations
        else:
            iterations = 1
        logger.info("Creating iteration-count based schedule with [%s] distribution for [%s] with [%d] warmup iterations and "
                    "[%d] iterations." % (task.schedule, op, warmup_iterations, iterations))
        return iteration_count_based(sched, warmup_iterations, iterations, runner_for_op, params_for_op)


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
        if iterations:
            for it in range(0, iterations):
                try:
                    sample_type = metrics.SampleType.Warmup if time.perf_counter() - start < warmup_time_period else metrics.SampleType.Normal
                    percent_completed = (it + 1) / iterations
                    yield (next_scheduled, sample_type, percent_completed, runner, params.params())
                    next_scheduled = sched.next(next_scheduled)
                except StopIteration:
                    return
        else:
            param_source_knows_progress = hasattr(params, "percent_completed")
            while True:
                try:
                    sample_type = metrics.SampleType.Warmup if time.perf_counter() - start < warmup_time_period else metrics.SampleType.Normal
                    # does not contribute at all to completion. Hence, we cannot define completion.
                    percent_completed = params.percent_completed if param_source_knows_progress else None
                    yield (next_scheduled, sample_type, percent_completed, runner, params.params())
                    next_scheduled = sched.next(next_scheduled)
                except StopIteration:
                    return
    else:
        end = start + warmup_time_period + time_period
        it = 0

        while time.perf_counter() < end:
            try:
                now = time.perf_counter()
                sample_type = metrics.SampleType.Warmup if now - start < warmup_time_period else metrics.SampleType.Normal
                percent_completed = (now - start) / (warmup_time_period + time_period)
                yield (next_scheduled, sample_type, percent_completed, runner, params.params())
                next_scheduled = sched.next(next_scheduled)
                it += 1
            except StopIteration:
                return


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
        try:
            sample_type = metrics.SampleType.Warmup if it < warmup_iterations else metrics.SampleType.Normal
            percent_completed = (it + 1) / total_iterations
            yield (next_scheduled, sample_type, percent_completed, runner, params.params())
            next_scheduled = sched.next(next_scheduled)
        except StopIteration:
            return
