import logging
import threading
import psutil

import rally.utils.io
import rally.metrics

logger = logging.getLogger("rally.telemetry")


class Telemetry:
  def __init__(self, config, metrics_store):
    self._config = config
    self._devices = [
      FlightRecorder(config),
      Ps(config, metrics_store)
    ]
    self._enabled_devices = self._config.opts("telemetry", "devices")

  def list(self):
    print("Available telemetry devices:")
    for device in self._devices:
      print("\t%s (%s): %s (Always enabled: %s)" % (device.command, device.human_name, device.help, device.mandatory))
    print("\nKeep in mind that each telemetry devices may incur a runtime overhead which can skew results.")

  def instrument_candidate_env(self, setup):
    opts = {}
    for device in self._devices:
      if self._enabled(device):
        # TODO dm: The maps need to be properly merged, otherwise the overwrite each other when we really have multiple devices...
        opts.update(device.instrument_env(setup))
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

class FlightRecorder:
  def __init__(self, config):
    self._config = config

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
    rally.utils.io.ensure_dir(log_root)
    log_file = "%s/%s.jfr" % (log_root, setup.name)

    logger.info("%s profiler: Writing telemetry data to [%s]." % (self.human_name, log_file))
    print("%s: writing flight recording to %s" % (self.human_name, log_file))
    # TODO dm: Consider using also a delay, starting flight recording only after a specific trigger (e.g. warmup is done), etc., etc.
    # TODO dm: We should probably put the file name in quotes or escape it properly somehow (if there are spaces in the path...)
    return {"ES_JAVA_OPTS": "-XX:+UnlockCommercialFeatures -XX:+FlightRecorder "
                            "-XX:FlightRecorderOptions=defaultrecording=true,dumponexit=true,dumponexitpath=%s" % (log_file)}

  def attach_to_process(self, process):
    pass

  def detach_from_process(self, process):
    pass

  def on_benchmark_start(self):
    pass

  def on_benchmark_stop(self):
    pass


class Ps:
  def __init__(self, config, metrics_store):
    self._config = config
    self._metrics_store = metrics_store
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

  def instrument_env(self, setup):
    # nothing to instrument
    return {}

  def attach_to_process(self, process):
    disk = self._config.opts("benchmarks", "metrics.stats.disk.device", mandatory=False)
    self._t = GatherProcessStats(process.pid, disk, self._metrics_store)

  def detach_from_process(self, process):
    pass
    #writeCount, writeBytes, writeCount, writeTime, readCount, readBytes, readTime = self._t.finish()
    # TODO dm: Now put these into the metrics store
    #self.collect('WRITES: %s bytes, %s time, %s count' % (writeBytes, writeTime, writeCount))
    #self.collect('READS: %s bytes, %s time, %s count' % (readBytes, readTime, readCount))
    # We write raw metrics into the metrics store... (not the median)
    #cpuPercents.sort()
    #self.collect('CPU median: %s' % cpuPercents[int(len(cpuPercents) / 2)])
    #for pct in cpuPercents:
    #  self.collect('  %s' % pct)

  def on_benchmark_start(self):
    self._t.start()

  def on_benchmark_stop(self):
    self._t.finish()






class GatherProcessStats(threading.Thread):
  def __init__(self, pid, disk_name, metrics_store):
    threading.Thread.__init__(self)
    #self.cpuPercents = []
    self.stop = False
    self.process = psutil.Process(pid)
    self.disk_name = disk_name
    self.metrics_store = metrics_store
    if self._use_specific_disk():
      self.diskStart = psutil.disk_io_counters(perdisk=True)[self.disk_name]
    else:
      self.diskStart = psutil.disk_io_counters(perdisk=False)

  def finish(self):
    self.stop = True
    self.join()
    # TODO dm: Write also these metrics to the metrics store
    if self._use_specific_disk():
      diskEnd = psutil.disk_io_counters(perdisk=True)[self.disk_name]
    else:
      diskEnd = psutil.disk_io_counters(perdisk=False)
    writeBytes = diskEnd.write_bytes - self.diskStart.write_bytes
    writeCount = diskEnd.write_count - self.diskStart.write_count
    writeTime = diskEnd.write_time - self.diskStart.write_time
    readBytes = diskEnd.read_bytes - self.diskStart.read_bytes
    readCount = diskEnd.read_count - self.diskStart.read_count
    readTime = diskEnd.read_time - self.diskStart.read_time
    return writeCount, writeBytes, writeCount, writeTime, readCount, readBytes, readTime

  def _use_specific_disk(self):
    return self.disk_name is not None and self.disk_name != ""

  def run(self):

    # TODO: disk counters too

    while not self.stop:
      self.metrics_store.put("cpu_utilization_1s", self.process.cpu_percent(interval=1.0))
      #logger.debug('CPU: %s' % self.cpuPercents[-1])
