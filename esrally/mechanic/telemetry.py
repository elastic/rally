import logging
import os
import re
import signal
import subprocess
import threading

import tabulate
from esrally import metrics, time, exceptions
from esrally.utils import io, sysstats, process, console, versions

logger = logging.getLogger("rally.telemetry")


def list_telemetry():
    console.println("Available telemetry devices:\n")
    devices = [[device.command, device.human_name, device.help] for device in [JitCompiler, Gc, FlightRecorder, PerfStat, NodeStats]]
    console.println(tabulate.tabulate(devices, ["Command", "Name", "Description"]))
    console.println("\nKeep in mind that each telemetry device may incur a runtime overhead which can skew results.")


class Telemetry:
    def __init__(self, enabled_devices=None, devices=None):
        if devices is None:
            devices = []
        if enabled_devices is None:
            enabled_devices = []
        self.enabled_devices = enabled_devices
        self.devices = devices

    def instrument_candidate_env(self, car, candidate_id):
        opts = {}
        for device in self.devices:
            if self._enabled(device):
                additional_opts = device.instrument_env(car, candidate_id)
                # properly merge values with the same key
                for k, v in additional_opts.items():
                    if k in opts:
                        opts[k] = "%s %s" % (opts[k], v)
                    else:
                        opts[k] = v
        return opts

    def attach_to_cluster(self, cluster):
        for device in self.devices:
            if self._enabled(device):
                device.attach_to_cluster(cluster)

    def on_pre_node_start(self, node_name):
        for device in self.devices:
            if self._enabled(device):
                device.on_pre_node_start(node_name)

    def attach_to_node(self, node):
        for device in self.devices:
            if self._enabled(device):
                device.attach_to_node(node)

    def detach_from_node(self, node, running):
        for device in self.devices:
            if self._enabled(device):
                device.detach_from_node(node, running)

    def on_benchmark_start(self):
        for device in self.devices:
            if self._enabled(device):
                device.on_benchmark_start()

    def on_benchmark_stop(self):
        for device in self.devices:
            if self._enabled(device):
                device.on_benchmark_stop()

    def detach_from_cluster(self, cluster):
        for device in self.devices:
            if self._enabled(device):
                device.detach_from_cluster(cluster)

    def _enabled(self, device):
        return device.internal or device.command in self.enabled_devices


########################################################################################
#
# Telemetry devices
#
########################################################################################

class TelemetryDevice:
    def instrument_env(self, car, candidate_id):
        return {}

    def attach_to_cluster(self, cluster):
        pass

    def on_pre_node_start(self, node_name):
        pass

    def attach_to_node(self, node):
        pass

    def detach_from_node(self, node, running):
        pass

    def detach_from_cluster(self, cluster):
        pass

    def on_benchmark_start(self):
        pass

    def on_benchmark_stop(self):
        pass


class InternalTelemetryDevice(TelemetryDevice):
    internal = True


class SamplerThread(threading.Thread):
    def __init__(self, recorder):
        threading.Thread.__init__(self)
        self.stop = False
        self.recorder = recorder

    def finish(self):
        self.stop = True
        self.join()

    def run(self):
        # noinspection PyBroadException
        try:
            while not self.stop:
                self.recorder.record()
                time.sleep(self.recorder.sample_interval)
        except BaseException as e:
            logger.exception("Could not determine {}".format(self.recorder))


class FlightRecorder(TelemetryDevice):
    internal = False
    command = "jfr"
    human_name = "Flight Recorder"
    help = "Enables Java Flight Recorder (requires an Oracle JDK)"

    def __init__(self, telemetry_params, log_root, java_major_version):
        super().__init__()
        self.telemetry_params = telemetry_params
        self.log_root = log_root
        self.java_major_version = java_major_version

    def instrument_env(self, car, candidate_id):
        io.ensure_dir(self.log_root)
        log_file = "%s/%s-%s.jfr" % (self.log_root, car.safe_name, candidate_id)

        console.println("\n***************************************************************************\n")
        console.println("[WARNING] Java flight recorder is a commercial feature of the Oracle JDK.\n")
        console.println("You are using Java flight recorder which requires that you comply with\nthe licensing terms stated in:\n")
        console.println(console.format.link("http://www.oracle.com/technetwork/java/javase/terms/license/index.html"))
        console.println("\nBy using this feature you confirm that you comply with these license terms.\n")
        console.println("Otherwise, please abort and rerun Rally without the \"jfr\" telemetry device.")
        console.println("\n***************************************************************************\n")

        time.sleep(3)

        console.info("%s: Writing flight recording to [%s]" % (self.human_name, log_file), logger=logger)

        java_opts = self.java_opts(log_file)

        logger.info("jfr: Adding JVM arguments: [%s].", java_opts)
        return {"ES_JAVA_OPTS": java_opts}

    def java_opts(self, log_file):
        recording_template = self.telemetry_params.get("recording-template")
        java_opts = "-XX:+UnlockDiagnosticVMOptions -XX:+UnlockCommercialFeatures -XX:+DebugNonSafepoints "

        if self.java_major_version < 9:
            java_opts += "-XX:+FlightRecorder "
            java_opts += "-XX:FlightRecorderOptions=disk=true,maxage=0s,maxsize=0,dumponexit=true,dumponexitpath={} ".format(log_file)
            java_opts += "-XX:StartFlightRecording=defaultrecording=true"
            if recording_template:
                logger.info("jfr: Using recording template [%s].", recording_template)
                java_opts += ",settings={}".format(recording_template)
            else:
                logger.info("jfr: Using default recording template.")
        else:
            java_opts += "-XX:StartFlightRecording=maxsize=0,maxage=0s,disk=true,dumponexit=true,filename={}".format(log_file)
            if recording_template:
                logger.info("jfr: Using recording template [%s].", recording_template)
                java_opts += ",settings={}".format(recording_template)
            else:
                logger.info("jfr: Using default recording template.")
        return java_opts


class JitCompiler(TelemetryDevice):
    internal = False
    command = "jit"
    human_name = "JIT Compiler Profiler"
    help = "Enables JIT compiler logs."

    def __init__(self, log_root):
        super().__init__()
        self.log_root = log_root

    def instrument_env(self, car, candidate_id):
        io.ensure_dir(self.log_root)
        log_file = "%s/%s-%s.jit.log" % (self.log_root, car.safe_name, candidate_id)
        console.info("%s: Writing JIT compiler log to [%s]" % (self.human_name, log_file), logger=logger)
        return {"ES_JAVA_OPTS": "-XX:+UnlockDiagnosticVMOptions -XX:+TraceClassLoading -XX:+LogCompilation "
                                "-XX:LogFile=%s -XX:+PrintAssembly" % log_file}


class Gc(TelemetryDevice):
    internal = False
    command = "gc"
    human_name = "GC log"
    help = "Enables GC logs."

    def __init__(self, log_root, java_major_version):
        super().__init__()
        self.log_root = log_root
        self.java_major_version = java_major_version

    def instrument_env(self, car, candidate_id):
        io.ensure_dir(self.log_root)
        log_file = "%s/%s-%s.gc.log" % (self.log_root, car.safe_name, candidate_id)
        console.info("%s: Writing GC log to [%s]" % (self.human_name, log_file), logger=logger)
        return self.java_opts(log_file)

    def java_opts(self, log_file):
        if self.java_major_version < 9:
            return {"ES_JAVA_OPTS": "-Xloggc:%s -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps "
                                    "-XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime "
                                    "-XX:+PrintTenuringDistribution" % log_file}
        else:
            # see https://docs.oracle.com/javase/9/tools/java.htm#JSWOR-GUID-BE93ABDC-999C-4CB5-A88B-1994AAAC74D5
            return {"ES_JAVA_OPTS": "-Xlog:gc*=info,safepoint=info,age*=trace:file=%s:utctime,uptimemillis,level,tags:filecount=0" % log_file}


class PerfStat(TelemetryDevice):
    internal = False
    command = "perf"
    human_name = "perf stat"
    help = "Reads CPU PMU counters (requires Linux and perf)"

    def __init__(self, log_root):
        super().__init__()
        self.log_root = log_root
        self.process = None
        self.node = None
        self.log = None
        self.attached = False

    def attach_to_node(self, node):
        io.ensure_dir(self.log_root)
        log_file = "%s/%s.perf.log" % (self.log_root, node.node_name)

        console.info("%s: Writing perf logs to [%s]" % (self.human_name, log_file), logger=logger)

        self.log = open(log_file, "wb")

        self.process = subprocess.Popen(["perf", "stat", "-p %s" % node.process.pid],
                                        stdout=self.log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        self.node = node
        self.attached = True

    def detach_from_node(self, node, running):
        if self.attached and running:
            logger.info("Dumping PMU counters for node [%s]" % node.node_name)
            os.kill(self.process.pid, signal.SIGINT)
            try:
                self.process.wait(10.0)
            except subprocess.TimeoutExpired:
                logger.warning("perf stat did not terminate")
            self.log.close()
            self.attached = False


class NodeStats(TelemetryDevice):
    internal = False
    command = "node-stats"
    human_name = "Node Stats"
    help = "Regularly samples node stats"

    """
    Gathers different node stats.
    """
    def __init__(self, telemetry_params, client, metrics_store):
        super().__init__()
        self.telemetry_params = telemetry_params
        self.client = client
        self.metrics_store = metrics_store
        self.sampler = None

    def attach_to_cluster(self, cluster):
        super().attach_to_cluster(cluster)

    def on_benchmark_start(self):
        recorder = NodeStatsRecorder(self.telemetry_params, self.client, self.metrics_store)
        self.sampler = SamplerThread(recorder)
        self.sampler.setDaemon(True)
        self.sampler.start()

    def on_benchmark_stop(self):
        if self.sampler:
            self.sampler.finish()


class NodeStatsRecorder:
    def __init__(self, telemetry_params, client, metrics_store):
        self.sample_interval = telemetry_params.get("node-stats-sample-interval", 1)
        if self.sample_interval <= 0:
            raise exceptions.SystemSetupError(
                "The telemetry parameter 'node-stats-sample-interval' must be greater than zero but was {}.".format(self.sample_interval))

        self.include_indices = telemetry_params.get("node-stats-include-indices", False)
        self.include_thread_pools = telemetry_params.get("node-stats-include-thread-pools", True)
        self.include_buffer_pools = telemetry_params.get("node-stats-include-buffer-pools", True)
        self.include_breakers = telemetry_params.get("node-stats-include-breakers", True)
        self.include_network = telemetry_params.get("node-stats-include-network", True)
        self.client = client
        self.metrics_store = metrics_store

    def __str__(self):
        return "node stats"

    def record(self):
        current_sample = self.sample()
        for node_stats in current_sample:
            node_name = node_stats["name"]
            if self.include_indices:
                self.record_indices_stats(node_name, node_stats,
                                          include=["docs", "store", "indexing", "search", "merges", "query_cache", "fielddata",
                                                   "segments", "translog", "request_cache"])
            if self.include_thread_pools:
                self.record_thread_pool_stats(node_name, node_stats)
            if self.include_breakers:
                self.record_circuit_breaker_stats(node_name, node_stats)
            if self.include_buffer_pools:
                self.record_jvm_buffer_pool_stats(node_name, node_stats)
            if self.include_network:
                self.record_network_stats(node_name, node_stats)

        time.sleep(self.sample_interval)

    def record_indices_stats(self, node_name, node_stats, include):
        indices_stats = node_stats["indices"]
        for section in include:
            if section in indices_stats:
                for metric_name, metric_value in indices_stats[section].items():
                    self.put_value(node_name,
                                   metric_name="indices_{}_{}".format(section, metric_name),
                                   node_stats_metric_name=metric_name,
                                   metric_value=metric_value)

    def record_thread_pool_stats(self, node_name, node_stats):
        thread_pool_stats = node_stats["thread_pool"]
        for pool_name, pool_metrics in thread_pool_stats.items():
            for metric_name, metric_value in pool_metrics.items():
                self.put_value(node_name,
                               metric_name="thread_pool_{}_{}".format(pool_name, metric_name),
                               node_stats_metric_name=metric_name,
                               metric_value=metric_value)

    def record_circuit_breaker_stats(self, node_name, node_stats):
        breaker_stats = node_stats["breakers"]
        for breaker_name, breaker_metrics in breaker_stats.items():
            for metric_name, metric_value in breaker_metrics.items():
                self.put_value(node_name,
                               metric_name="breaker_{}_{}".format(breaker_name, metric_name),
                               node_stats_metric_name=metric_name,
                               metric_value=metric_value)

    def record_jvm_buffer_pool_stats(self, node_name, node_stats):
        buffer_pool_stats = node_stats["jvm"]["buffer_pools"]
        for pool_name, pool_metrics in buffer_pool_stats.items():
            for metric_name, metric_value in pool_metrics.items():
                self.put_value(node_name,
                               metric_name="jvm_buffer_pool_{}_{}".format(pool_name, metric_name),
                               node_stats_metric_name=metric_name,
                               metric_value=metric_value)

    def record_network_stats(self, node_name, node_stats):
        transport_stats = node_stats.get("transport")
        if transport_stats:
            for metric_name, metric_value in transport_stats.items():
                self.put_value(node_name,
                               metric_name="transport_{}".format(metric_name),
                               node_stats_metric_name=metric_name,
                               metric_value=metric_value)

    def put_value(self, node_name, metric_name, node_stats_metric_name, metric_value):
        if isinstance(metric_value, (int, float)) and not isinstance(metric_value, bool):
            # auto-recognize metric keys ending with well-known suffixes
            if node_stats_metric_name.endswith("in_bytes"):
                self.metrics_store.put_value_node_level(node_name=node_name,
                                                        name=metric_name,
                                                        value=metric_value, unit="byte")
            elif node_stats_metric_name.endswith("in_millis"):
                self.metrics_store.put_value_node_level(node_name=node_name,
                                                        name=metric_name,
                                                        value=metric_value, unit="ms")
            else:
                self.metrics_store.put_count_node_level(node_name=node_name,
                                                        name=metric_name,
                                                        count=metric_value)

    def sample(self):
        import elasticsearch
        try:
            stats = self.client.nodes.stats(metric="_all")
        except elasticsearch.TransportError:
            logger.exception("Could not retrieve node stats.")
            return {}
        return stats["nodes"].values()


class StartupTime(InternalTelemetryDevice):
    def __init__(self, metrics_store, stopwatch=time.StopWatch):
        self.metrics_store = metrics_store
        self.timer = stopwatch()

    def on_pre_node_start(self, node_name):
        self.timer.start()

    def attach_to_node(self, node):
        self.timer.stop()
        self.metrics_store.put_value_node_level(node.node_name, "node_startup_time", self.timer.total_time(), "s")


class MergeParts(InternalTelemetryDevice):
    """
    Gathers merge parts time statistics. Note that you need to run a track setup which logs these data.
    """
    MERGE_TIME_LINE = re.compile(r": (\d+) msec to merge ([a-z ]+) \[(\d+) docs\]")

    def __init__(self, metrics_store, node_log_dir):
        super().__init__()
        self.node_log_dir = node_log_dir
        self.metrics_store = metrics_store
        self._t = None
        self.node = None

    def attach_to_node(self, node):
        self.node = node

    def on_benchmark_stop(self):
        logger.info("Analyzing merge times.")
        # first decompress all logs. They have unique names so it's safe to do that. It's easier to first decompress everything
        for log_file in os.listdir(self.node_log_dir):
            log_path = "%s/%s" % (self.node_log_dir, log_file)
            if io.is_archive(log_path):
                logger.info("Decompressing [%s] to analyze merge times..." % log_path)
                io.decompress(log_path, self.node_log_dir)

        # we need to add up times from all files
        merge_times = {}
        for log_file in os.listdir(self.node_log_dir):
            log_path = "%s/%s" % (self.node_log_dir, log_file)
            if not io.is_archive(log_file):
                logger.debug("Analyzing merge times in [%s]" % log_path)
                with open(log_path, mode="rt", encoding="utf-8") as f:
                    self._extract_merge_times(f, merge_times)
            else:
                logger.debug("Skipping archived logs in [%s]." % log_path)
        if merge_times:
            self._store_merge_times(merge_times)
        logger.info("Finished analyzing merge times. Extracted [%s] different merge time components." % len(merge_times))

    def _extract_merge_times(self, file, merge_times):
        for line in file.readlines():
            match = MergeParts.MERGE_TIME_LINE.search(line)
            if match is not None:
                duration_ms, part, num_docs = match.groups()
                if part not in merge_times:
                    merge_times[part] = [0, 0]
                l = merge_times[part]
                l[0] += int(duration_ms)
                l[1] += int(num_docs)

    def _store_merge_times(self, merge_times):
        for k, v in merge_times.items():
            metric_suffix = k.replace(" ", "_")
            self.metrics_store.put_value_node_level(self.node.node_name, "merge_parts_total_time_%s" % metric_suffix, v[0], "ms")
            self.metrics_store.put_count_node_level(self.node.node_name, "merge_parts_total_docs_%s" % metric_suffix, v[1])


class DiskIo(InternalTelemetryDevice):
    """
    Gathers disk I/O stats.
    """
    def __init__(self, metrics_store, node_count_on_host):
        super().__init__()
        self.metrics_store = metrics_store
        self.node_count_on_host = node_count_on_host
        self.node = None
        self.process = None
        self.disk_start = None
        self.process_start = None

    def attach_to_node(self, node):
        self.node = node
        self.process = sysstats.setup_process_stats(node.process.pid)

    def on_benchmark_start(self):
        if self.process is not None:
            self.process_start = sysstats.process_io_counters(self.process)
            if self.process_start:
                logger.info("Using more accurate process-based I/O counters.")
            else:
                try:
                    self.disk_start = sysstats.disk_io_counters()
                    logger.warning("Process I/O counters are unsupported on this platform. Falling back to less accurate disk I/O counters.")
                except RuntimeError:
                    logger.exception("Could not determine I/O stats at benchmark start.")

    def on_benchmark_stop(self):
        if self.process is not None:
            # Be aware the semantics of write counts etc. are different for disk and process statistics.
            # Thus we're conservative and only report I/O bytes now.
            # noinspection PyBroadException
            try:
                # we have process-based disk counters, no need to worry how many nodes are on this host
                if self.process_start:
                    process_end = sysstats.process_io_counters(self.process)
                    read_bytes = process_end.read_bytes - self.process_start.read_bytes
                    write_bytes = process_end.write_bytes - self.process_start.write_bytes
                elif self.disk_start:
                    if self.node_count_on_host > 1:
                        logger.info("There are [%d] nodes on this host and Rally fell back to disk I/O counters. "
                                    "Attributing [1/%d] of total I/O to [%s]." %
                                    (self.node_count_on_host, self.node_count_on_host, self.node.node_name))

                    disk_end = sysstats.disk_io_counters()
                    read_bytes = (disk_end.read_bytes - self.disk_start.read_bytes) // self.node_count_on_host
                    write_bytes = (disk_end.write_bytes - self.disk_start.write_bytes) // self.node_count_on_host
                else:
                    raise RuntimeError("Neither process nor disk I/O counters are available")

                self.metrics_store.put_count_node_level(self.node.node_name, "disk_io_write_bytes", write_bytes, "byte")
                self.metrics_store.put_count_node_level(self.node.node_name, "disk_io_read_bytes", read_bytes, "byte")
            # Catching RuntimeException is not sufficient as psutil might raise AccessDenied et.al. which is derived from Exception
            except BaseException:
                logger.exception("Could not determine I/O stats at benchmark end.")


class CpuUsage(InternalTelemetryDevice):
    """
    Gathers CPU usage statistics.
    """
    def __init__(self, metrics_store):
        super().__init__()
        self.metrics_store = metrics_store
        self.sampler = None
        self.node = None

    def attach_to_node(self, node):
        self.node = node

    def on_benchmark_start(self):
        if self.node:
            recorder = CpuUsageRecorder(self.node, self.metrics_store)
            self.sampler = SamplerThread(recorder)
            self.sampler.setDaemon(True)
            self.sampler.start()

    def on_benchmark_stop(self):
        if self.sampler:
            self.sampler.finish()


class CpuUsageRecorder:
    def __init__(self, node, metrics_store):
        self.node = node
        self.process = sysstats.setup_process_stats(node.process.pid)
        self.metrics_store = metrics_store
        # the call is blocking already; there is no need for additional waiting in the sampler thread.
        self.sample_interval = 0

    def record(self):
        import psutil
        try:
            self.metrics_store.put_value_node_level(node_name=self.node.node_name, name="cpu_utilization_1s",
                                                    value=sysstats.cpu_utilization(self.process), unit="%")
        # this can happen when the Elasticsearch process has been terminated already and we were not quick enough to stop.
        except psutil.NoSuchProcess:
            pass

    def __str__(self):
        return "cpu utilization"


def store_node_attribute_metadata(metrics_store, nodes_info):
    # push up all node level attributes to cluster level iff the values are identical for all nodes
    pseudo_cluster_attributes = {}
    for node in nodes_info:
        if "attributes" in node:
            for k, v in node["attributes"].items():
                attribute_key = "attribute_%s" % str(k)
                metrics_store.add_meta_info(metrics.MetaInfoScope.node, node["name"], attribute_key, v)
                if attribute_key not in pseudo_cluster_attributes:
                    pseudo_cluster_attributes[attribute_key] = set()
                pseudo_cluster_attributes[attribute_key].add(v)

    for k, v in pseudo_cluster_attributes.items():
        if len(v) == 1:
            metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, k, next(iter(v)))


def store_plugin_metadata(metrics_store, nodes_info):
    # push up all plugins to cluster level iff all nodes have the same ones
    all_nodes_plugins = []
    all_same = False

    for node in nodes_info:
        plugins = [p["name"] for p in extract_value(node, ["plugins"], fallback=[]) if "name" in p]
        if not all_nodes_plugins:
            all_nodes_plugins = plugins.copy()
            all_same = True
        else:
            # order does not matter so we do a set comparison
            all_same = all_same and set(all_nodes_plugins) == set(plugins)

        if plugins:
            metrics_store.add_meta_info(metrics.MetaInfoScope.node, node["name"], "plugins", plugins)

    if all_same and all_nodes_plugins:
        metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, "plugins", all_nodes_plugins)


def extract_value(node, path, fallback="unknown"):
    value = node
    try:
        for k in path:
            value = value[k]
    except KeyError:
        logger.warning("Could not determine meta-data at path [%s]." % ",".join(path))
        value = fallback
    return value


class ClusterEnvironmentInfo(InternalTelemetryDevice):
    """
    Gathers static environment information on a cluster level (e.g. version numbers).
    """
    def __init__(self, client, metrics_store):
        super().__init__()
        self.metrics_store = metrics_store
        self.client = client

    def attach_to_cluster(self, cluster):
        client_info = self.client.info()
        revision = client_info["version"]["build_hash"]
        distribution_version = client_info["version"]["number"]
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, "source_revision", revision)
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, "distribution_version", distribution_version)

        info = self.client.nodes.info(node_id="_all")
        nodes_info = info["nodes"].values()
        for node in nodes_info:
            node_name = node["name"]
            # while we could determine this for bare-metal nodes that are provisioned by Rally, there are other cases (Docker, externally
            # provisioned clusters) where it's not that easy.
            self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, "jvm_vendor", extract_value(node, ["jvm", "vm_vendor"]))
            self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, "jvm_version", extract_value(node, ["jvm", "version"]))

        store_plugin_metadata(self.metrics_store, nodes_info)
        store_node_attribute_metadata(self.metrics_store, nodes_info)


class NodeEnvironmentInfo(InternalTelemetryDevice):
    """
    Gathers static environment information like OS or CPU details for Rally-provisioned nodes.
    """
    def __init__(self, metrics_store):
        super().__init__()
        self.metrics_store = metrics_store

    def attach_to_node(self, node):
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node.node_name, "os_name", sysstats.os_name())
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node.node_name, "os_version", sysstats.os_version())
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node.node_name, "cpu_logical_cores", sysstats.logical_cpu_cores())
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node.node_name, "cpu_physical_cores", sysstats.physical_cpu_cores())
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node.node_name, "cpu_model", sysstats.cpu_model())
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node.node_name, "node_name", node.node_name)
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node.node_name, "host_name", node.host_name)


class ExternalEnvironmentInfo(InternalTelemetryDevice):
    """
    Gathers static environment information for externally provisioned clusters.
    """
    def __init__(self, client, metrics_store):
        super().__init__()
        self.metrics_store = metrics_store
        self.client = client
        self._t = None

    def attach_to_cluster(self, cluster):
        stats = self.client.nodes.stats(metric="_all")
        nodes = stats["nodes"]
        for node in nodes.values():
            node_name = node["name"]
            host = node.get("host", "unknown")
            self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, "node_name", node_name)
            self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, "host_name", host)

        info = self.client.nodes.info(node_id="_all")
        nodes_info = info["nodes"].values()
        for node in nodes_info:
            node_name = node["name"]
            self.store_node_info(node_name, "os_name", node, ["os", "name"])
            self.store_node_info(node_name, "os_version", node, ["os", "version"])
            self.store_node_info(node_name, "cpu_logical_cores", node, ["os", "available_processors"])
            self.store_node_info(node_name, "jvm_vendor", node, ["jvm", "vm_vendor"])
            self.store_node_info(node_name, "jvm_version", node, ["jvm", "version"])

        store_plugin_metadata(self.metrics_store, nodes_info)
        store_node_attribute_metadata(self.metrics_store, nodes_info)

    def store_node_info(self, node_name, metric_key, node, path):
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, metric_key, extract_value(node, path))


class ClusterMetaDataInfo(InternalTelemetryDevice):
    """
    Enriches the cluster with meta-data about it and its nodes.
    """
    def __init__(self, client):
        super().__init__()
        self.client = client

    def attach_to_cluster(self, cluster):
        client_info = self.client.info()
        revision = client_info["version"]["build_hash"]
        distribution_version = client_info["version"]["number"]

        cluster.distribution_version = distribution_version
        cluster.source_revision = revision

        for node_stats in self.client.nodes.stats(metric="_all")["nodes"].values():
            node_name = node_stats["name"]
            if cluster.has_node(node_name):
                cluster_node = cluster.node(node_name)
            else:
                host = node_stats.get("host", "unknown")
                cluster_node = cluster.add_node(host, node_name)
            self.add_node_stats(cluster, cluster_node, node_stats)

        for node_info in self.client.nodes.info(node_id="_all")["nodes"].values():
            self.add_node_info(cluster, node_info)

    def add_node_info(self, cluster, node_info):
        node_name = node_info["name"]
        cluster_node = cluster.node(node_name)
        if cluster_node:
            cluster_node.ip = extract_value(node_info, ["ip"])
            cluster_node.os = {
                "name": extract_value(node_info, ["os", "name"]),
                "version": extract_value(node_info, ["os", "version"])
            }
            cluster_node.jvm = {
                "vendor": extract_value(node_info, ["jvm", "vm_vendor"]),
                "version": extract_value(node_info, ["jvm", "version"])
            }
            cluster_node.cpu = {
                "available_processors": extract_value(node_info, ["os", "available_processors"]),
                "allocated_processors": extract_value(node_info, ["os", "allocated_processors"], fallback=None),
            }
            for plugin in extract_value(node_info, ["plugins"], fallback=[]):
                if "name" in plugin:
                    cluster_node.plugins.append(plugin["name"])

            if versions.major_version(cluster.distribution_version) == 1:
                cluster_node.memory = {
                    "total_bytes": extract_value(node_info, ["os", "mem", "total_in_bytes"], fallback=None)
                }

    def add_node_stats(self, cluster, cluster_node, stats):
        if cluster_node:
            data_dirs = extract_value(stats, ["fs", "data"], fallback=[])
            for data_dir in data_dirs:
                fs_meta_data = {
                    "mount": data_dir.get("mount", "unknown"),
                    "type": data_dir.get("type", "unknown"),
                    "spins": data_dir.get("spins", "unknown")
                }
                cluster_node.fs.append(fs_meta_data)
            if versions.major_version(cluster.distribution_version) > 1:
                cluster_node.memory = {
                    "total_bytes": extract_value(stats, ["os", "mem", "total_in_bytes"], fallback=None)
                }


class GcTimesSummary(InternalTelemetryDevice):
    """
    Gathers a summary of the total young gen/old gen GC runtime during the whole race.
    """
    def __init__(self, client, metrics_store):
        super().__init__()
        self.metrics_store = metrics_store
        self.client = client
        self.gc_times_per_node = {}

    def on_benchmark_start(self):
        self.gc_times_per_node = self.gc_times()

    def on_benchmark_stop(self):
        gc_times_at_end = self.gc_times()
        total_old_gen_collection_time = 0
        total_young_gen_collection_time = 0

        for node_name, gc_times_end in gc_times_at_end.items():
            if node_name in self.gc_times_per_node:
                gc_times_start = self.gc_times_per_node[node_name]
                young_gc_time = max(gc_times_end[0] - gc_times_start[0], 0)
                old_gc_time = max(gc_times_end[1] - gc_times_start[1], 0)

                total_young_gen_collection_time += young_gc_time
                total_old_gen_collection_time += old_gc_time

                self.metrics_store.put_value_node_level(node_name, "node_young_gen_gc_time", young_gc_time, "ms")
                self.metrics_store.put_value_node_level(node_name, "node_old_gen_gc_time", old_gc_time, "ms")
            else:
                logger.warning("Cannot determine GC times for node [%s]. It was not part of the cluster at the start of the benchmark.")

        self.metrics_store.put_value_cluster_level("node_total_young_gen_gc_time", total_young_gen_collection_time, "ms")
        self.metrics_store.put_value_cluster_level("node_total_old_gen_gc_time", total_old_gen_collection_time, "ms")

        self.gc_times_per_node = None

    def gc_times(self):
        logger.debug("Gathering GC times")
        gc_times = {}
        import elasticsearch
        try:
            stats = self.client.nodes.stats(metric="_all")
        except elasticsearch.TransportError:
            logger.exception("Could not retrieve GC times.")
            return gc_times
        nodes = stats["nodes"]
        for node in nodes.values():
            node_name = node["name"]
            gc = node["jvm"]["gc"]["collectors"]
            old_gen_collection_time = gc["old"]["collection_time_in_millis"]
            young_gen_collection_time = gc["young"]["collection_time_in_millis"]
            gc_times[node_name] = (young_gen_collection_time, old_gen_collection_time)
        return gc_times


class IndexStats(InternalTelemetryDevice):
    """
    Gathers statistics via the Elasticsearch index stats API
    """
    def __init__(self, client, metrics_store):
        super().__init__()
        self.client = client
        self.metrics_store = metrics_store
        self.first_time = True

    def on_benchmark_start(self):
        # we only determine this value at the start of the benchmark (in the first lap). This is actually only useful for
        # the pipeline "benchmark-only" where we don't have control over the cluster and the user might not have restarted
        # the cluster so we can at least tell them.
        if self.first_time:
            index_times = self.index_times(self.index_stats()["primaries"])
            for k, v in index_times.items():
                if v > 0:
                    console.warn("%s is %d ms indicating that the cluster is not in a defined clean state. Recorded index time "
                                 "metrics may be misleading." % (k, v), logger=logger)
            self.first_time = False

    def on_benchmark_stop(self):
        import json
        logger.info("Gathering indices stats for all primaries on benchmark stop.")
        index_stats = self.index_stats()
        logger.info("Returned indices stats:\n%s" % json.dumps(index_stats, indent=2))
        if "primaries" not in index_stats:
            return
        p = index_stats["primaries"]
        # actually this is add_count
        self.add_metrics(self.extract_value(p, ["segments", "count"]), "segments_count")
        self.add_metrics(self.extract_value(p, ["segments", "memory_in_bytes"]), "segments_memory_in_bytes", "byte")

        for metric_key, value in self.index_times(p).items():
            logger.info("Adding [%s] = [%s] to metrics store." % (str(metric_key), str(value)))
            self.add_metrics(value, metric_key, "ms")

        self.add_metrics(self.extract_value(p, ["segments", "doc_values_memory_in_bytes"]), "segments_doc_values_memory_in_bytes", "byte")
        self.add_metrics(self.extract_value(p, ["segments", "stored_fields_memory_in_bytes"]), "segments_stored_fields_memory_in_bytes", "byte")
        self.add_metrics(self.extract_value(p, ["segments", "terms_memory_in_bytes"]), "segments_terms_memory_in_bytes", "byte")
        self.add_metrics(self.extract_value(p, ["segments", "norms_memory_in_bytes"]), "segments_norms_memory_in_bytes", "byte")
        self.add_metrics(self.extract_value(p, ["segments", "points_memory_in_bytes"]), "segments_points_memory_in_bytes", "byte")
        self.add_metrics(self.extract_value(index_stats, ["total", "store", "size_in_bytes"]), "store_size_in_bytes", "byte")
        self.add_metrics(self.extract_value(index_stats, ["total", "translog", "size_in_bytes"]), "translog_size_in_bytes", "byte")

    def index_stats(self):
        # noinspection PyBroadException
        try:
            stats = self.client.indices.stats(metric="_all", level="shards")
            return stats["_all"]
        except BaseException:
            logger.exception("Could not retrieve index stats.")
            return {}

    def index_times(self, p):
        return {
            "merges_total_time": self.extract_value(p, ["merges", "total_time_in_millis"], default_value=0),
            "merges_total_throttled_time": self.extract_value(p, ["merges", "total_throttled_time_in_millis"], default_value=0),
            "indexing_total_time": self.extract_value(p, ["indexing", "index_time_in_millis"], default_value=0),
            "indexing_throttle_time": self.extract_value(p, ["indexing", "throttle_time_in_millis"], default_value=0),
            "refresh_total_time": self.extract_value(p, ["refresh", "total_time_in_millis"], default_value=0),
            "flush_total_time": self.extract_value(p, ["flush", "total_time_in_millis"], default_value=0)
        }

    def add_metrics(self, value, metric_key, unit=None):
        if value is not None:
            if unit:
                self.metrics_store.put_value_cluster_level(metric_key, value, unit)
            else:
                self.metrics_store.put_count_cluster_level(metric_key, value)

    def extract_value(self, primaries, path, default_value=None):
        value = primaries
        try:
            for k in path:
                value = value[k]
            return value
        except KeyError:
            logger.warning("Could not determine value at path [%s]. Returning default value [%s]" % (",".join(path), str(default_value)))
            return default_value


class MlBucketProcessingTime(InternalTelemetryDevice):
    def __init__(self, client, metrics_store):
        super().__init__()
        self.client = client
        self.metrics_store = metrics_store

    def detach_from_cluster(self, cluster):
        import elasticsearch
        try:
            results = self.client.search(index=".ml-anomalies-*", body={
                "size": 0,
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"result_type": "bucket"}},
                            # TODO: We could restrict this by job id if we need to measure multiple jobs...
                            # {"term": {"job_id": "job_id"}}
                        ]
                    }
                },
                "aggs": {
                    "max_bucket_processing_time": {
                        "max": {"field": "processing_time_ms"}
                    }
                }
            })
        except elasticsearch.TransportError:
            logger.exception("Could not retrieve ML bucket processing time.")
            return
        try:
            value = results["aggregations"]["max_bucket_processing_time"]["value"]
            if value:
                self.metrics_store.put_value_cluster_level("ml_max_processing_time_millis", value, "ms")
        except KeyError:
            # no ML running
            pass


class IndexSize(InternalTelemetryDevice):
    """
    Measures the final size of the index
    """
    def __init__(self, data_paths, metrics_store):
        super().__init__()
        self.data_paths = data_paths
        self.metrics_store = metrics_store
        self.attached = False

    def attach_to_node(self, node):
        self.attached = True

    def detach_from_node(self, node, running):
        # we need to gather the file size after the node has terminated so we can be sure that it has written all its buffers.
        if not running and self.attached and self.data_paths:
            self.attached = False
            index_size_bytes = 0
            for data_path in self.data_paths:
                index_size_bytes += io.get_size(data_path)
                process.run_subprocess_with_logging("find %s -ls" % data_path, header="index files:")
            self.metrics_store.put_count_node_level(node.node_name, "final_index_size_bytes", index_size_bytes, "byte")
