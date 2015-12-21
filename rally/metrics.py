import threading
import logging
import psutil

import rally.utils.io
import rally.config

logger = logging.getLogger("rally.metrics")


#TODO dm [Refactoring]: Convert to the new telemetry infrastructure (see https://github.com/elastic/rally/issues/21)

# For now we just support dumping to a log file (as is). This will change *significantly* later. We just want to tear apart normal logs
# from metrics output
class MetricsCollector:
  def __init__(self, config):
    self._config = config
    self._print_lock = threading.Lock()
    self._stats = None
    telemetry_root = "%s/%s" % (self._config.opts("system", "track.setup.root.dir"), self._config.opts("benchmarks", "metrics.log.dir"))
    rally.utils.io.ensure_dir(telemetry_root)

    self._log_file = open("%s/telemetry.txt" % telemetry_root, "w")

  def expose_print_lock_dirty_hack_remove_me_asap(self):
    return self._print_lock

  def collect(self, message):
    # TODO dm: Get rid of the print lock
    with self._print_lock:
      self._log_file.write(message)
      self._log_file.write("\n")

  def start_collection(self, cluster):
    disk = self._config.opts("benchmarks", "metrics.stats.disk.device", mandatory=False)
    # TODO dm: This ties metrics collection completely to a locally running server -> refactor (later)
    self._stats = self._gather_process_stats(cluster.servers[0].pid, disk)

  def collect_total_stats(self):
    if self._stats is not None:
      cpuPercents, writeCount, writeBytes, writeCount, writeTime, readCount, readBytes, readTime = self._stats.finish()
      self.collect('WRITES: %s bytes, %s time, %s count' % (writeBytes, writeTime, writeCount))
      self.collect('READS: %s bytes, %s time, %s count' % (readBytes, readTime, readCount))
      cpuPercents.sort()
      self.collect('CPU median: %s' % cpuPercents[int(len(cpuPercents) / 2)])
      for pct in cpuPercents:
        self.collect('  %s' % pct)

  def stop_collection(self):
    self._log_file.close()

  def _gather_process_stats(self, pid, diskName):
    t = GatherProcessStats(pid, diskName)
    t.start()
    return t


class GatherProcessStats(threading.Thread):
  def __init__(self, pid, disk_name):
    threading.Thread.__init__(self)
    self.cpuPercents = []
    self.stop = False
    self.process = psutil.Process(pid)
    self.diskName = disk_name
    if self._use_specific_disk():
      self.diskStart = psutil.disk_io_counters(perdisk=True)[self.diskName]
    else:
      self.diskStart = psutil.disk_io_counters(perdisk=False)

  def finish(self):
    self.stop = True
    self.join()
    if self._use_specific_disk():
      diskEnd = psutil.disk_io_counters(perdisk=True)[self.diskName]
    else:
      diskEnd = psutil.disk_io_counters(perdisk=False)
    writeBytes = diskEnd.write_bytes - self.diskStart.write_bytes
    writeCount = diskEnd.write_count - self.diskStart.write_count
    writeTime = diskEnd.write_time - self.diskStart.write_time
    readBytes = diskEnd.read_bytes - self.diskStart.read_bytes
    readCount = diskEnd.read_count - self.diskStart.read_count
    readTime = diskEnd.read_time - self.diskStart.read_time
    return self.cpuPercents, writeCount, writeBytes, writeCount, writeTime, readCount, readBytes, readTime

  def _use_specific_disk(self):
    return self.diskName is not None and self.diskName != ""

  def run(self):

    # TODO: disk counters too

    while not self.stop:
      self.cpuPercents.append(self.process.cpu_percent(interval=1.0))
      logger.debug('CPU: %s' % self.cpuPercents[-1])
