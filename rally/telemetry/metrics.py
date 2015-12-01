import threading

try:
  import psutil
except ImportError:
  # TODO dm: Use a logger for that
  print('WARNING: psutil not installed; no system level cpu/memory stats will be recorded')
  psutil = None


# For now we just support dumping to a log file (as is). This will change *significantly* later. We just want to tear apart normal logs
# from metrics output
class MetricsCollector:
  def __init__(self):
    self._print_lock = threading.Lock()
    self._stats = None

  def expose_print_lock_dirty_hack_remove_me_asap(self):
    return self._print_lock

  def collect(self, message):
    # TODO dm: Get rid of the print lock
    with self._print_lock:
      print(message)

  def startCollection(self, cluster):
    # TODO dm: This ties metrics collection completely to a locally running server -> refactor (later)
    # TODO dm: The second parameter was previously constants.STATS_DISK_DEVICE instead of None -> reintroduced in config as constant 'metrics.stats.disk.device' (-> use later)
    self._stats = self._gather_process_stats(cluster.servers()[0].pid, None)

  def stopCollection(self):
    if self._stats is not None:
      cpuPercents, writeCount, writeBytes, writeCount, writeTime, readCount, readBytes, readTime = self._stats.finish()
      print('WRITES: %s bytes, %s time, %s count' % (writeBytes, writeTime, writeCount))
      print('READS: %s bytes, %s time, %s count' % (readBytes, readTime, readCount))
      cpuPercents.sort()
      print('CPU median: %s' % cpuPercents[int(len(cpuPercents) / 2)])
      for pct in cpuPercents:
        print('  %s' % pct)

  def _gather_process_stats(self, pid, diskName):
    if psutil is None:
      return None

    t = GatherProcessStats(pid, diskName)
    t.start()
    return t


class GatherProcessStats(threading.Thread):
  def __init__(self, pid, diskName):
    threading.Thread.__init__(self)
    self.cpuPercents = []
    self.stop = False
    self.process = psutil.Process(pid)
    self.diskName = diskName
    if self.diskName is not None:
      self.diskStart = psutil.disk_io_counters(perdisk=True)[self.diskName]
    else:
      self.diskStart = psutil.disk_io_counters(perdisk=False)

  def finish(self):
    self.stop = True
    self.join()
    if self.diskName is not None:
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

  def run(self):

    # TODO: disk counters too

    while not self.stop:
      self.cpuPercents.append(self.process.cpu_percent(interval=1.0))
      print('CPU: %s' % self.cpuPercents[-1])
