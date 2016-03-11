import logging
import threading
import psutil
import re
import os

from rally.utils import io

logger = logging.getLogger("rally.telemetry")


class Telemetry:
    def __init__(self, config, metrics_store=None, devices=None):
        self._config = config
        if devices is None:
            self._devices = [
                FlightRecorder(config, metrics_store),
                JitCompiler(config, metrics_store),
                Ps(config, metrics_store),
                MergeParts(config, metrics_store)
            ]
        else:
            self._devices = devices
        self._enabled_devices = self._config.opts("telemetry", "devices")

    def list(self):
        print("Available telemetry devices:\n")
        for device in self._devices:
            print("* %s (%s): %s (Always enabled: %s)" % (device.command, device.human_name, device.help, device.mandatory))
        print("\nKeep in mind that each telemetry device may incur a runtime overhead which can skew results.")

    def instrument_candidate_env(self, setup):
        opts = {}
        for device in self._devices:
            if self._enabled(device):
                additional_opts = device.instrument_env(setup)
                # properly merge values with the same key
                for k, v in additional_opts.items():
                    if k in opts:
                        opts[k] = "%s %s" % (opts[k], v)
                    else:
                        opts[k] = v
        return opts

    def attach_to_process(self, process):
        for device in self._devices:
            if self._enabled(device):
                device.attach_to_process(process)

    def detach_from_process(self, process):
        for device in self._devices:
            if self._enabled(device):
                device.detach_from_process(process)

    def on_benchmark_start(self):
        for device in self._devices:
            if self._enabled(device):
                device.on_benchmark_start()

    def on_benchmark_stop(self):
        for device in self._devices:
            if self._enabled(device):
                device.on_benchmark_stop()

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

    def instrument_env(self, setup):
        return {}

    def attach_to_process(self, process):
        pass

    def detach_from_process(self, process):
        pass

    def on_benchmark_start(self):
        pass

    def on_benchmark_stop(self):
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

    def instrument_env(self, setup):
        log_root = "%s/%s" % (self._config.opts("system", "track.setup.root.dir"), self._config.opts("benchmarks", "metrics.log.dir"))
        io.ensure_dir(log_root)
        log_file = "%s/%s.jfr" % (log_root, setup.name)

        logger.info("%s profiler: Writing telemetry data to [%s]." % (self.human_name, log_file))
        print("%s: Writing flight recording to %s" % (self.human_name, log_file))
        return {"ES_JAVA_OPTS": "-XX:+UnlockDiagnosticVMOptions -XX:+UnlockCommercialFeatures -XX:+DebugNonSafepoints -XX:+FlightRecorder "
                                "-XX:FlightRecorderOptions=defaultrecording=true,disk=true,dumponexit=true,dumponexitpath=%s" % log_file}


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

    def instrument_env(self, setup):
        log_root = "%s/%s" % (self._config.opts("system", "track.setup.root.dir"), self._config.opts("benchmarks", "metrics.log.dir"))
        io.ensure_dir(log_root)
        log_file = "%s/%s.jit.log" % (log_root, setup.name)

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

    def on_benchmark_stop(self):
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
            self._metrics_store.put_value("merge_parts_total_time_%s" % metric_suffix, v[0], "ms")
            self._metrics_store.put_count("merge_parts_total_docs_%s" % metric_suffix, v[1])


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

    def attach_to_process(self, process):
        disk = self._config.opts("benchmarks", "metrics.stats.disk.device", mandatory=False)
        self._t = GatherProcessStats(process.pid, disk, self._metrics_store)

    def on_benchmark_start(self):
        self._t.start()

    def on_benchmark_stop(self):
        self._t.finish()


class GatherProcessStats(threading.Thread):
    def __init__(self, pid, disk_name, metrics_store):
        threading.Thread.__init__(self)
        self.stop = False
        self.process = psutil.Process(pid)
        self.disk_name = disk_name
        self.metrics_store = metrics_store
        self.disk_start = self._disk_io_counters()

    def finish(self):
        self.stop = True
        self.join()
        disk_end = self._disk_io_counters()

        self.metrics_store.put_count("disk_io_write_bytes", disk_end.write_bytes - self.disk_start.write_bytes, "byte")
        self.metrics_store.put_count("disk_io_write_count", disk_end.write_count - self.disk_start.write_count)
        self.metrics_store.put_value("disk_io_write_time", disk_end.write_time - self.disk_start.write_time, "ms")

        self.metrics_store.put_count("disk_io_read_bytes", disk_end.read_bytes - self.disk_start.read_bytes, "byte")
        self.metrics_store.put_count("disk_io_read_count", disk_end.read_count - self.disk_start.read_count)
        self.metrics_store.put_value("disk_io_read_time", disk_end.read_time - self.disk_start.read_time, "ms")

    def _disk_io_counters(self):
        if self._use_specific_disk():
            return psutil.disk_io_counters(perdisk=True)[self.disk_name]
        else:
            return psutil.disk_io_counters(perdisk=False)

    def _use_specific_disk(self):
        return self.disk_name is not None and self.disk_name != ""

    def run(self):
        while not self.stop:
            self.metrics_store.put_value("cpu_utilization_1s", self.process.cpu_percent(interval=1.0), "%")
