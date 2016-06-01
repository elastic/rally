import logging
import os
import re
import signal
import subprocess
import threading

from esrally import metrics, track
from esrally.utils import io, sysstats, process

logger = logging.getLogger("rally.telemetry")


class Telemetry:
    def __init__(self, config, metrics_store=None, devices=None):
        self._config = config
        if devices is None:
            self._devices = [
                FlightRecorder(config, metrics_store),
                JitCompiler(config, metrics_store),
                PerfStat(config, metrics_store),
                Ps(config, metrics_store),
                MergeParts(config, metrics_store),
                EnvironmentInfo(config, metrics_store),
                NodeStats(config, metrics_store),
                IndexStats(config, metrics_store),
                IndexSize(config, metrics_store)
                # We do not include the ExternalEnvironmentInfo here by intention as it should only be used for externally launched clusters
            ]
        else:
            self._devices = devices
        self._enabled_devices = self._config.opts("telemetry", "devices")

    def list(self):
        external_devices = []
        for device in self._devices:
            if not device.internal:
                external_devices.append([device.command, device.human_name, device.help])
        return external_devices

    def instrument_candidate_env(self, car, candidate_id):
        opts = {}
        for device in self._devices:
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
        for device in self._devices:
            if self._enabled(device):
                device.attach_to_cluster(cluster)

    def attach_to_node(self, node):
        for device in self._devices:
            if self._enabled(device):
                device.attach_to_node(node)

    def detach_from_node(self, node):
        for device in self._devices:
            if self._enabled(device):
                device.detach_from_node(node)

    def on_benchmark_start(self, phase):
        for device in self._devices:
            if self._enabled(device):
                device.on_benchmark_start(phase)

    def on_benchmark_stop(self, phase):
        for device in self._devices:
            if self._enabled(device):
                device.on_benchmark_stop(phase)

    def detach_from_cluster(self, cluster):
        for device in self._devices:
            if self._enabled(device):
                device.detach_from_cluster(cluster)

    def _enabled(self, device):
        return device.internal or device.command in self._enabled_devices


########################################################################################
#
# Telemetry devices
#
########################################################################################

class TelemetryDevice:
    def __init__(self, config, metrics_store):
        self._config = config
        self._metrics_store = metrics_store

    @property
    def metrics_store(self):
        return self._metrics_store

    @property
    def config(self):
        return self._config

    @property
    def internal(self):
        raise NotImplementedError("abstract method")

    @property
    def command(self):
        raise NotImplementedError("abstract method")

    @property
    def human_name(self):
        raise NotImplementedError("abstract method")

    @property
    def help(self):
        raise NotImplementedError("abstract method")

    def instrument_env(self, car, candidate_id):
        return {}

    def attach_to_cluster(self, cluster):
        pass

    def attach_to_node(self, node):
        pass

    def detach_from_node(self, node):
        pass

    def detach_from_cluster(self, cluster):
        pass

    def on_benchmark_start(self, phase):
        pass

    def on_benchmark_stop(self, phase):
        pass


class InternalTelemetryDevice(TelemetryDevice):
    def __init__(self, config, metrics_store):
        super().__init__(config, metrics_store)

    @property
    def internal(self):
        return True

    @property
    def command(self):
        return "internal"

    @property
    def human_name(self):
        return ""

    @property
    def help(self):
        return ""


class FlightRecorder(TelemetryDevice):
    def __init__(self, config, metrics_store):
        super().__init__(config, metrics_store)

    @property
    def internal(self):
        return False

    @property
    def command(self):
        return "jfr"

    @property
    def human_name(self):
        return "Flight Recorder"

    @property
    def help(self):
        return "Enables Java Flight Recorder on the benchmark candidate (will only work on Oracle JDK)"

    def instrument_env(self, car, candidate_id):
        log_root = "%s/%s" % (self._config.opts("system", "challenge.root.dir"), self._config.opts("benchmarks", "metrics.log.dir"))
        io.ensure_dir(log_root)
        log_file = "%s/%s-%s.jfr" % (log_root, car.name, candidate_id)

        logger.info("%s profiler: Writing telemetry data to [%s]." % (self.human_name, log_file))
        print("%s: Writing flight recording to %s" % (self.human_name, log_file))
        # this is more robust in case we want to use custom settings
        # see http://stackoverflow.com/questions/34882035/how-to-record-allocations-with-jfr-on-command-line
        #
        # in that case change to: -XX:StartFlightRecording=defaultrecording=true,settings=es-memory-profiling
        return {"ES_JAVA_OPTS": "-XX:+UnlockDiagnosticVMOptions -XX:+UnlockCommercialFeatures -XX:+DebugNonSafepoints -XX:+FlightRecorder "
                                "-XX:FlightRecorderOptions=disk=true,dumponexit=true,dumponexitpath=%s "
                                "-XX:StartFlightRecording=defaultrecording=true" % log_file}


class JitCompiler(TelemetryDevice):
    def __init__(self, config, metrics_store):
        super().__init__(config, metrics_store)

    @property
    def internal(self):
        return False

    @property
    def command(self):
        return "jit"

    @property
    def human_name(self):
        return "JIT Compiler Profiler"

    @property
    def help(self):
        return "Enables JIT compiler logs."

    def instrument_env(self, car, candidate_id):
        log_root = "%s/%s" % (self._config.opts("system", "challenge.root.dir"), self._config.opts("benchmarks", "metrics.log.dir"))
        io.ensure_dir(log_root)
        log_file = "%s/%s-%s.jit.log" % (log_root, car.name, candidate_id)

        logger.info("%s: Writing JIT compiler logs to [%s]." % (self.human_name, log_file))
        print("%s: Writing JIT compiler log to %s" % (self.human_name, log_file))
        return {"ES_JAVA_OPTS": "-XX:+UnlockDiagnosticVMOptions -XX:+TraceClassLoading -XX:+LogCompilation "
                                "-XX:LogFile=%s -XX:+PrintAssembly" % log_file}


class PerfStat(TelemetryDevice):
    def __init__(self, config, metrics_store):
        super().__init__(config, metrics_store)
        self.process = None
        self.node = None
        self.log = None

    @property
    def internal(self):
        return False

    @property
    def command(self):
        return "perf"

    @property
    def human_name(self):
        return "perf stat"

    @property
    def help(self):
        return "Reads CPU PMU counters (beta, only on Linux, requires perf)"

    def attach_to_node(self, node):
        log_root = "%s/%s" % (self._config.opts("system", "challenge.root.dir"), self._config.opts("benchmarks", "metrics.log.dir"))
        io.ensure_dir(log_root)
        log_file = "%s/%s.perf.log" % (log_root, node.node_name)

        logger.info("%s: Writing perf logs to [%s]." % (self.human_name, log_file))
        print("%s: Writing perf logs to %s" % (self.human_name, log_file))

        self.log = open(log_file, "wb")

        self.process = subprocess.Popen(["perf", "stat", "-p %s" % node.process.pid],
                                        stdout=self.log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        self.node = node

    def detach_from_node(self, node):
        logger.info("Dumping PMU counters for node [%s]" % node.node_name)
        os.kill(self.process.pid, signal.SIGINT)
        try:
            self.process.wait(10.0)
        except subprocess.TimeoutExpired:
            logger.warn("perf stat did not terminate")
        self.log.close()


class MergeParts(InternalTelemetryDevice):
    """
    Gathers merge parts time statistics. Note that you need to run a track setup which logs these data.
    """
    MERGE_TIME_LINE = re.compile(r": (\d+) msec to merge ([a-z ]+) \[(\d+) docs\]")

    def __init__(self, config, metrics_store):
        super().__init__(config, metrics_store)
        self._t = None

    def on_benchmark_stop(self, phase):
        # TODO: This works currently only by coincidence (as this method is called once per node instead of once per cluster.
        # But as we only use it in a single node benchmark, it has the same effect. As we need to rethink this API anyway, let's leave
        # it for now but we need to change this later).
        # only gather metrics when the whole benchmark is done
        if phase is None:
            server_log_dir = self._config.opts("launcher", "candidate.log.dir")
            for log_file in os.listdir(server_log_dir):
                log_path = "%s/%s" % (server_log_dir, log_file)
                logger.debug("Analyzing merge parts in [%s]" % log_path)
                with open(log_path) as f:
                    merge_times = self._extract_merge_times(f)
                    if merge_times:
                        self._store_merge_times(merge_times)

    def _extract_merge_times(self, file):
        merge_times = {}
        for line in file.readlines():
            match = MergeParts.MERGE_TIME_LINE.search(line)
            if match is not None:
                duration_ms, part, num_docs = match.groups()
                if part not in merge_times:
                    merge_times[part] = [0, 0]
                l = merge_times[part]
                l[0] += int(duration_ms)
                l[1] += int(num_docs)
        return merge_times

    def _store_merge_times(self, merge_times):
        for k, v in merge_times.items():
            metric_suffix = k.replace(" ", "_")
            self._metrics_store.put_value_cluster_level("merge_parts_total_time_%s" % metric_suffix, v[0], "ms")
            self._metrics_store.put_count_cluster_level("merge_parts_total_docs_%s" % metric_suffix, v[1])


class Ps(InternalTelemetryDevice):
    """
    Gathers process statistics like CPU usage or disk I/O.
    """
    def __init__(self, config, metrics_store):
        super().__init__(config, metrics_store)
        self._t = None

    def attach_to_node(self, node):
        self._t = {}
        for phase in track.BenchmarkPhase:
            self._t[phase] = GatherProcessStats(node, self._metrics_store, phase)

    def on_benchmark_start(self, phase):
        if self._t and phase:
            self._t[phase].start()

    def on_benchmark_stop(self, phase):
        if self._t and phase:
            self._t[phase].finish()


class GatherProcessStats(threading.Thread):
    def __init__(self, node, metrics_store, phase):
        threading.Thread.__init__(self)
        self.stop = False
        self.node = node
        self.process = sysstats.setup_process_stats(node.process.pid)
        self.metrics_store = metrics_store
        self.phase = phase
        self.disk_start = None
        self.process_start = None

    def finish(self):
        self.stop = True
        self.join()
        # Be aware the semantics of write counts etc. are different for disk and process statistics.
        # Thus we're conservative and only report I/O bytes now.
        disk_end = sysstats.disk_io_counters()
        process_end = sysstats.process_io_counters(self.process)

        self.metrics_store.put_count_node_level(self.node.node_name, "disk_io_write_bytes_%s" % self.phase.name,
                                                self.write_bytes(process_end, disk_end), "byte")
        self.metrics_store.put_count_node_level(self.node.node_name, "disk_io_read_bytes_%s" % self.phase.name,
                                                self.read_bytes(process_end, disk_end), "byte")

    def read_bytes(self, process_end, disk_end):
        if self.process_start and process_end:
            return process_end.read_bytes - self.process_start.read_bytes
        else:
            return disk_end.read_bytes - self.disk_start.read_bytes

    def write_bytes(self, process_end, disk_end):
        if self.process_start and process_end:
            return process_end.write_bytes - self.process_start.write_bytes
        else:
            return disk_end.write_bytes - self.disk_start.write_bytes

    def run(self):
        self.disk_start = sysstats.disk_io_counters()
        self.process_start = sysstats.process_io_counters(self.process)
        if self.process_start:
            logger.info("Using more accurate process-based I/O counters.")
        else:
            logger.warn("Process I/O counters are unsupported on this platform. Falling back to less accurate disk I/O counters.")

        while not self.stop:
            self.metrics_store.put_value_node_level(self.node.node_name, "cpu_utilization_1s_%s" % self.phase.name,
                                                    sysstats.cpu_utilization(self.process), "%")


class EnvironmentInfo(InternalTelemetryDevice):
    """
    Gathers static environment information like OS or CPU details for Rally-provisioned clusters.
    """
    def __init__(self, config, metrics_store):
        super().__init__(config, metrics_store)
        self._t = None

    def attach_to_cluster(self, cluster):
        revision = cluster.info()["version"]["build_hash"]
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, "source_revision", revision)
        info = cluster.nodes_info()
        for node in info["nodes"].values():
            node_name = node["name"]
            self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, "jvm_vendor", node["jvm"]["vm_vendor"])
            self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, "jvm_version", node["jvm"]["version"])

    def attach_to_node(self, node):
        # we gather also host level metrics here although they will just be overridden for multiple nodes on the same node (which is no
        # problem as the values are identical anyway).
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node.node_name, "os_name", sysstats.os_name())
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node.node_name, "os_version", sysstats.os_version())
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node.node_name, "cpu_logical_cores", sysstats.logical_cpu_cores())
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node.node_name, "cpu_physical_cores", sysstats.physical_cpu_cores())
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node.node_name, "cpu_model", sysstats.cpu_model())
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node.node_name, "node_name", node.node_name)
        # This is actually the only node level metric, but it is easier to implement this way
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node.node_name, "host_name", node.host_name)


class ExternalEnvironmentInfo(InternalTelemetryDevice):
    """
    Gathers static environment information for externally provisioned clusters.
    """
    def __init__(self, config, metrics_store):
        super().__init__(config, metrics_store)
        self._t = None

    def attach_to_cluster(self, cluster):
        revision = cluster.info()["version"]["build_hash"]
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, "source_revision", revision)

        stats = cluster.nodes_stats(metric="_all", level="shards")
        nodes = stats["nodes"]
        for node in nodes.values():
            node_name = node["name"]
            self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, "node_name", node_name)
            self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, "host_name", node["host"])

        info = cluster.nodes_info()
        for node in info["nodes"].values():
            self.try_store_node_info(node, "os_name", ["os", "name"])
            self.try_store_node_info(node, "os_version", ["os", "version"])
            self.try_store_node_info(node, "cpu_logical_cores", ["os", "available_processors"])
            self.try_store_node_info(node, "jvm_vendor", ["jvm", "vm_vendor"])
            self.try_store_node_info(node, "jvm_version", ["jvm", "version"])

    def try_store_node_info(self, node, metric_key, path):
        node_name = node["name"]
        value = node
        try:
            for k in path:
                value = value[k]
        except KeyError:
            logger.warn("Could not determine metric [%s] for node [%s] at path [%s]." % (metric_key, node_name, ",".join(path)))
            value = "unknown"
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, node_name, metric_key, value)


class NodeStats(InternalTelemetryDevice):
    """
    Gathers statistics via the Elasticsearch nodes stats API
    """
    def __init__(self, config, metrics_store):
        super().__init__(config, metrics_store)
        self.cluster = None

    def attach_to_cluster(self, cluster):
        self.cluster = cluster

    def on_benchmark_stop(self, phase):
        # only gather metrics when the whole benchmark is done
        if self.cluster and phase is None:
            logger.info("Gathering nodes stats")
            stats = self.cluster.nodes_stats(metric="_all", level="shards")
            total_old_gen_collection_time = 0
            total_young_gen_collection_time = 0
            nodes = stats["nodes"]
            for node in nodes.values():
                node_name = node["name"]
                gc = node["jvm"]["gc"]["collectors"]
                old_gen_collection_time = gc["old"]["collection_time_in_millis"]
                young_gen_collection_time = gc["young"]["collection_time_in_millis"]
                self.metrics_store.put_value_node_level(node_name, "node_old_gen_gc_time", old_gen_collection_time, "ms")
                self.metrics_store.put_value_node_level(node_name, "node_young_gen_gc_time", young_gen_collection_time, "ms")
                total_old_gen_collection_time += old_gen_collection_time
                total_young_gen_collection_time += young_gen_collection_time

            self.metrics_store.put_value_cluster_level("node_total_old_gen_gc_time", total_old_gen_collection_time, "ms")
            self.metrics_store.put_value_cluster_level("node_total_young_gen_gc_time", total_young_gen_collection_time, "ms")


class IndexStats(InternalTelemetryDevice):
    """
    Gathers statistics via the Elasticsearch index stats API
    """
    def __init__(self, config, metrics_store):
        super().__init__(config, metrics_store)
        self.cluster = None

    def attach_to_cluster(self, cluster):
        self.cluster = cluster

    def on_benchmark_stop(self, phase):
        if self.cluster and phase is track.BenchmarkPhase.index:
            logger.info("Gathering indices stats")
            stats = self.cluster.indices_stats(metric="_all", level="shards")
            primaries = stats["_all"]["primaries"]

            self.try_store_cluster_metrics(primaries, "segments_count", None, ["segments", "count"])
            self.try_store_cluster_metrics(primaries, "segments_memory_in_bytes", "byte", ["segments", "memory_in_bytes"])
            self.try_store_cluster_metrics(primaries, "segments_doc_values_memory_in_bytes", "byte", ["segments", "doc_values_memory_in_bytes"])
            self.try_store_cluster_metrics(primaries, "segments_stored_fields_memory_in_bytes", "byte", ["segments", "stored_fields_memory_in_bytes"])
            self.try_store_cluster_metrics(primaries, "segments_terms_memory_in_bytes", "byte", ["segments", "terms_memory_in_bytes"])
            self.try_store_cluster_metrics(primaries, "segments_norms_memory_in_bytes", "byte", ["segments", "norms_memory_in_bytes"])
            self.try_store_cluster_metrics(primaries, "segments_points_memory_in_bytes", "byte", ["segments", "points_memory_in_bytes"])

            self.try_store_cluster_metrics(primaries, "merges_total_time", "ms", ["merges", "total_time_in_millis"])
            self.try_store_cluster_metrics(primaries, "merges_total_throttled_time", "ms", ["merges", "total_throttled_time_in_millis"])
            self.try_store_cluster_metrics(primaries, "indexing_total_time", "ms", ["indexing", "index_time_in_millis"])
            self.try_store_cluster_metrics(primaries, "refresh_total_time", "ms", ["refresh", "total_time_in_millis"])
            self.try_store_cluster_metrics(primaries, "flush_total_time", "ms", ["flush", "total_time_in_millis"])

    def try_store_cluster_metrics(self, primaries, metric_key, unit, path):
        value = primaries
        try:
            for k in path:
                value = value[k]
            self.metrics_store.put_value_cluster_level(metric_key, value, unit)
        except KeyError:
            logger.warn("Could not determine metric [%s] at path [%s]." % (metric_key, ",".join(path)))


class IndexSize(InternalTelemetryDevice):
    """
    Measures the final size of the index
    """
    def __init__(self, config, metrics_store):
        super().__init__(config, metrics_store)

    def detach_from_cluster(self, cluster):
        data_paths = self.config.opts("provisioning", "local.data.paths", mandatory=False)
        if data_paths is not None:
            data_path = data_paths[0]
            index_size_bytes = io.get_size(data_path)
            self.metrics_store.put_count_cluster_level("final_index_size_bytes", index_size_bytes, "byte")
            process.run_subprocess_with_logging("find %s -ls" % data_path, header="index files:")
