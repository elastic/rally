import logging
import threading
import re
import os

from rally.utils import io, sysstats
from rally.track import track
from rally import metrics

logger = logging.getLogger("rally.telemetry")


class Telemetry:
    def __init__(self, config, metrics_store=None, devices=None):
        self._config = config
        if devices is None:
            self._devices = [
                FlightRecorder(config, metrics_store),
                JitCompiler(config, metrics_store),
                Ps(config, metrics_store),
                MergeParts(config, metrics_store),
                EnvironmentInfo(config, metrics_store)
            ]
        else:
            self._devices = devices
        self._enabled_devices = self._config.opts("telemetry", "devices")

    def list(self):
        print("Available telemetry devices:\n")
        for device in self._devices:
            print("* %s (%s): %s (Always enabled: %s)" % (device.command, device.human_name, device.help, device.mandatory))
        print("\nKeep in mind that each telemetry device may incur a runtime overhead which can skew results.")

    def instrument_candidate_env(self, setup, candidate_id):
        opts = {}
        for device in self._devices:
            if self._enabled(device):
                additional_opts = device.instrument_env(setup, candidate_id)
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

    def _enabled(self, device):
        return device.mandatory or device.command in self._enabled_devices


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
    def mandatory(self):
        raise NotImplementedError("abstract method")

    @property
    def command(self):
        raise NotImplementedError("abstract method")

    @property
    def human_name(self):
        raise NotImplementedError("abstract method")

    @property
    def help(self):
        return ""

    def instrument_env(self, setup, candidate_id):
        return {}

    def attach_to_cluster(self, cluster):
        pass

    def attach_to_node(self, node):
        pass

    def detach_from_node(self, node):
        pass

    def on_benchmark_start(self, phase):
        pass

    def on_benchmark_stop(self, phase):
        pass


class FlightRecorder(TelemetryDevice):
    def __init__(self, config, metrics_store):
        super().__init__(config, metrics_store)

    @property
    def mandatory(self):
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

    def instrument_env(self, setup, candidate_id):
        log_root = "%s/%s" % (self._config.opts("system", "track.setup.root.dir"), self._config.opts("benchmarks", "metrics.log.dir"))
        io.ensure_dir(log_root)
        log_file = "%s/%s-%s.jfr" % (log_root, setup.name, candidate_id)

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
    def mandatory(self):
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

    def instrument_env(self, setup, candidate_id):
        log_root = "%s/%s" % (self._config.opts("system", "track.setup.root.dir"), self._config.opts("benchmarks", "metrics.log.dir"))
        io.ensure_dir(log_root)
        log_file = "%s/%s-%s.jit.log" % (log_root, setup.name, candidate_id)

        logger.info("%s: Writing JIT compiler logs to [%s]." % (self.human_name, log_file))
        print("%s: Writing JIT compiler log to %s" % (self.human_name, log_file))
        return {"ES_JAVA_OPTS": "-XX:+UnlockDiagnosticVMOptions -XX:+TraceClassLoading -XX:+LogCompilation "
                                "-XX:LogFile=%s -XX:+PrintAssembly" % log_file}


class MergeParts(TelemetryDevice):
    MERGE_TIME_LINE = re.compile(r": (\d+) msec to merge ([a-z ]+) \[(\d+) docs\]")

    def __init__(self, config, metrics_store):
        super().__init__(config, metrics_store)
        self._t = None

    @property
    def mandatory(self):
        return True

    @property
    def command(self):
        return "merge-parts"

    @property
    def human_name(self):
        return "Merge Parts Statistics"

    @property
    def help(self):
        return "Gathers merge parts time statistics. Note that you need to run a track setup which logs these data."

    def on_benchmark_stop(self, phase):
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


class Ps(TelemetryDevice):
    def __init__(self, config, metrics_store):
        super().__init__(config, metrics_store)
        self._t = None

    @property
    def mandatory(self):
        return True

    @property
    def command(self):
        return "ps"

    @property
    def human_name(self):
        return "Process Statistics"

    @property
    def help(self):
        return "Gathers process statistics like CPU usage or disk I/O."

    def attach_to_node(self, node):
        disk = self._config.opts("benchmarks", "metrics.stats.disk.device", mandatory=False)
        logger.info("Gathering disk device statistics for disk [%s]" % disk)
        self._t = {}
        for phase in track.BenchmarkPhase:
            self._t[phase] = GatherProcessStats(node, disk, self._metrics_store, phase)

    def on_benchmark_start(self, phase):
        if phase:
            self._t[phase].start()

    def on_benchmark_stop(self, phase):
        if phase:
            self._t[phase].finish()


class GatherProcessStats(threading.Thread):
    def __init__(self, node, disk_name, metrics_store, phase):
        threading.Thread.__init__(self)
        self.stop = False
        self.node = node
        self.process = sysstats.setup_process_stats(node.process.pid)
        self.disk_name = disk_name
        self.metrics_store = metrics_store
        self.phase = phase
        self.disk_start = None

    def finish(self):
        self.stop = True
        self.join()
        disk_end = sysstats.disk_io_counters(self.disk_name)

        self.metrics_store.put_count_node_level(self.node.node_name, "disk_io_write_bytes_%s" % self.phase.name,
                                                disk_end.write_bytes - self.disk_start.write_bytes, "byte")
        self.metrics_store.put_count_node_level(self.node.node_name, "disk_io_write_count_%s" % self.phase.name,
                                                disk_end.write_count - self.disk_start.write_count)
        # may be wrong on OS X: https://github.com/giampaolo/psutil/issues/700
        self.metrics_store.put_value_node_level(self.node.node_name, "disk_io_write_time_%s" % self.phase.name,
                                                disk_end.write_time - self.disk_start.write_time, "ms")

        self.metrics_store.put_count_node_level(self.node.node_name, "disk_io_read_bytes_%s" % self.phase.name,
                                                disk_end.read_bytes - self.disk_start.read_bytes, "byte")
        self.metrics_store.put_count_node_level(self.node.node_name, "disk_io_read_count_%s" % self.phase.name,
                                                disk_end.read_count - self.disk_start.read_count)
        # may be wrong on OS X: https://github.com/giampaolo/psutil/issues/700
        self.metrics_store.put_value_node_level(self.node.node_name, "disk_io_read_time_%s" % self.phase.name,
                                                disk_end.read_time - self.disk_start.read_time, "ms")

    def run(self):
        self.disk_start = sysstats.disk_io_counters(self.disk_name)
        while not self.stop:
            self.metrics_store.put_value_node_level(self.node.node_name, "cpu_utilization_1s_%s" % self.phase.name,
                                                    sysstats.cpu_utilization(self.process), "%")


class EnvironmentInfo(TelemetryDevice):
    def __init__(self, config, metrics_store):
        super().__init__(config, metrics_store)
        self._t = None

    @property
    def mandatory(self):
        return True

    @property
    def command(self):
        return "env"

    @property
    def human_name(self):
        return "Environment Info"

    @property
    def help(self):
        return "Gathers static environment information like OS or CPU details."

    def attach_to_cluster(self, cluster):
        revision = cluster.info()["version"]["build_hash"]
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, "source_revision", revision)

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
