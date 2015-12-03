import threading
import logging

logger = logging.getLogger("rally.metrics")

try:
  import psutil
except ImportError:
  logger.warn('psutil not installed; no system level cpu/memory stats will be recorded')
  psutil = None

import rally.utils.io
import rally.config


# For now we just support dumping to a log file (as is). This will change *significantly* later. We just want to tear apart normal logs
# from metrics output
class MetricsCollector:
  def __init__(self, config, bucket_name):
    self._config = config
    self._bucket_name = bucket_name
    self._print_lock = threading.Lock()
    self._stats = None
    # This is a compromise between the new and old folder structure.
    # Ideally, we'd just have: $LOG_ROOT/$BENCHMARK_TIMESTAMP/metrics/$(BUCKET_NAME).txt
    #TODO dm: Unify folder structure but also consider backtesting (i.e. migrate the existing folder structure on the benchmark server)
    log_root = "%s/%s" % (self._config.opts("system", "log.dir"), self._config.opts("benchmarks", "metrics.log.dir"))
    d = self._config.opts("meta", "time.start")
    ts = '%04d-%02d-%02d-%02d-%02d-%02d' % (d.year, d.month, d.day, d.hour, d.minute, d.second)
    metrics_log_dir = "%s/%s" % (log_root, self._bucket_name)
    rally.utils.io.ensure_dir(metrics_log_dir)
    # TODO dm: As we're not block-bound we don't have the convenience of "with"... - ensure we reliably close the file anyway
    self._log_file = open("%s/%s.txt" % (metrics_log_dir, ts), "w")

  def expose_print_lock_dirty_hack_remove_me_asap(self):
    return self._print_lock

  def collect(self, message):
    # TODO dm: Get rid of the print lock
    with self._print_lock:
      self._log_file.write(message)
      self._log_file.write("\n")
      # just for testing
      # self._log_file.flush()

  def start_collection(self, cluster):
    disk = self._config.opts("benchmarks", "metrics.stats.disk.device", mandatory=False)
    # TODO dm: This ties metrics collection completely to a locally running server -> refactor (later)
    self._stats = self._gather_process_stats(cluster.servers()[0].pid, disk)

  def stop_collection(self):
    if self._stats is not None:
      cpuPercents, writeCount, writeBytes, writeCount, writeTime, readCount, readBytes, readTime = self._stats.finish()
      self.collect('WRITES: %s bytes, %s time, %s count' % (writeBytes, writeTime, writeCount))
      self.collect('READS: %s bytes, %s time, %s count' % (readBytes, readTime, readCount))
      cpuPercents.sort()
      self.collect('CPU median: %s' % cpuPercents[int(len(cpuPercents) / 2)])
      for pct in cpuPercents:
        self.collect('  %s' % pct)
    self._log_file.close()

  def _gather_process_stats(self, pid, diskName):
    if psutil is None:
      return None

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
