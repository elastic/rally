import statistics
import threading
import logging
import psutil
import os
from collections import defaultdict

import rally.config
import rally.utils.io
import rally.utils.paths

logger = logging.getLogger("rally.metrics")


# Encapsulates path name
class MetricsFile:
  def __init__(self, config, invocation, track, track_setup_name):
    # We could use invocation root dir from config directly as long as we're only interested in the *current* run, but that's not
    # always the case (for reporting)
    invocation_root = rally.utils.paths.invocation_root_dir(config.opts("system", "root.dir"), invocation)
    track_root = rally.utils.paths.track_root_dir(invocation_root, track.name)
    track_setup_root = rally.utils.paths.track_setup_root_dir(track_root, track_setup_name)
    self._telemetry_root = "%s/%s" % (track_setup_root, config.opts("benchmarks", "metrics.log.dir"))
    self._log_file = "%s/metrics_store.txt" % self._telemetry_root

  @property
  def parent_directory(self):
    return self._telemetry_root

  @property
  def path(self):
    return self._log_file


# TODO dm: Rather expose these two different interfaces to the user...
class MetricsReader:
  def __init__(self, metrics_file):
    self._metrics_file = metrics_file

  def metrics(self):
    metrics = defaultdict(list)
    with open(self._metrics_file.path, "r") as f:
      for line in f:
        line = line.strip()
        k, v = line.split(";")
        metrics[k].append(v)
    return metrics


class MetricsWriter:
  def __init__(self, metrics_file):
    self._metrics_file = metrics_file

  def write(self, metrics):
    rally.utils.io.ensure_dir(self._metrics_file.parent_directory)
    with open(self._metrics_file.path, "w") as f:
      for k, v in metrics.items():
        # v is a list...
        for item in v:
          # TODO dm: Consider using the CSV library...
          f.write("{0};{1}\n".format(k, item))


class IllegalUsageError(Exception):
  pass


# TODO dm: Not sure how to call that...
class MetricsStoreLocator:
  def __init__(self, config):
    self._config = config

  def invocations(self):
    root = rally.utils.paths.all_invocations_root_dir(self._config.opts("system", "root.dir"))
    invocations = self._find_child_dirs(root)
    return [rally.utils.paths.to_timestamp(ts) for ts in invocations]

  def get_tracks(self, invocation):
    invocation_root = rally.utils.paths.invocation_root_dir(self._config.opts("system", "root.dir"), invocation)
    return self._find_child_dirs(rally.utils.paths.all_tracks_root_root_dir(invocation_root))

  def get_track_setups(self, invocation, track):
    invocation_root = rally.utils.paths.invocation_root_dir(self._config.opts("system", "root.dir"), invocation)
    track_root = rally.utils.paths.track_root_dir(invocation_root, track.name)
    return self._find_child_dirs(track_root)

  def _find_child_dirs(self, root):
    return [c for c in os.listdir(root) if os.path.isdir(os.path.join(root, c))]


class MetricsStore:
  """
  A simple metrics store based on a hash-map which gets written to disk once the store is closed.

  It is intended as a first step towards a "proper" metrics store (like Elasticsearch itself) without sacrificing the text based format
  """

  def __init__(self, config, invocation, track, track_setup_name):
    self._config = config
    self._metrics_file = MetricsFile(config, invocation, track, track_setup_name)
    invocation_ts = '%04d-%02d-%02d-%02d-%02d-%02d' % \
                    (invocation.year, invocation.month, invocation.day, invocation.hour, invocation.minute, invocation.second)
    self._key_prefix = "%s:%s:%s" % (invocation_ts, track.name.lower(), track_setup_name.lower())
    self._metrics = None
    self._writer = None
    self._closed = True

  def open_for_write(self):
    self._metrics = defaultdict(list)
    self._writer = MetricsWriter(self._metrics_file)
    self._closed = False

  def open_for_read(self):
    reader = MetricsReader(self._metrics_file)
    self._metrics = reader.metrics()

  def close(self):
    # TODO dm: Guard against wrong usage... (we could open for read and fail then on close as the write is None)
    self._writer.write(self._metrics)
    self._closed = True

  # we have to store also multiple dimensions here (like track, track-setup, invocation-timestamp(already encoded in the path?)
  def put(self, key, value, additional_dimensions=None):
    self._ensure_open()
    # TODO dm: Consider adding also a timestamp here... (what's the overhead of calling time.time()??)
    self._metrics[self._key(key, additional_dimensions)].append(value)

  def get_one(self, key, additional_dimensions=None, value_converter=str):
    v = self.get(key, additional_dimensions, value_converter)
    if v:
      return v[0]
    else:
      return v

  # note that we may get multiple values here, e.g. if we store multiple statistics... => k -> [v]
  def get(self, key, additional_dimensions=None, value_converter=str):
    v = self._metrics[self._key(key, additional_dimensions)]
    if v:
      return [value_converter(e) for e in v]
    else:
      return v

  def _key(self, key, additional_dimensions):
    # This is really just bolted on
    if additional_dimensions:
      dimension_key = "::".join(additional_dimensions) + "-"
    else:
      dimension_key = ""
    return "%s%s::%s" % (dimension_key, self._key_prefix, key)

  def _ensure_open(self):
    if self._closed:
      raise IllegalUsageError("Attempted to use a closed metrics store")


# TODO dm [Refactoring]: Convert to the new telemetry infrastructure (see https://github.com/elastic/rally/issues/21)
# TODO dm #21: Remove
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

  def collect(self, message):
    # TODO dm: Get rid of the print lock
    with self._print_lock:
      self._log_file.write(message)
      self._log_file.write("\n")

  def start_collection(self, cluster):
    disk = self._config.opts("benchmarks", "metrics.stats.disk.device", mandatory=False)
    # TODO dm: This ties metrics collection completely to a locally running server -> refactor (later)
    self._stats = self._gather_process_stats(cluster.servers[0].process.pid, disk)

  def collect_total_stats(self):
    if self._stats is not None:
      cpuPercents, writeCount, writeBytes, writeCount, writeTime, readCount, readBytes, readTime = self._stats.finish()
      self.collect('WRITES: %s bytes, %s time, %s count' % (writeBytes, writeTime, writeCount))
      self.collect('READS: %s bytes, %s time, %s count' % (readBytes, readTime, readCount))
      self.collect('CPU median: %s' % statistics.median(cpuPercents))
      for pct in cpuPercents:
        self.collect('  %s' % pct)

  def stop_collection(self):
    self._log_file.close()

  def _gather_process_stats(self, pid, diskName):
    t = GatherProcessStats(pid, diskName)
    t.start()
    return t


# TODO dm #21: Remove
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
