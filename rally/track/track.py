import os
import logging
import urllib.request
import shutil
from enum import Enum

from rally import config
from rally.utils import io, sysstats, convert, process

logger = logging.getLogger("rally.track")

# Ensure cluster status green even for single nodes. Please don't add anything else here except to get the cluster to status
# 'green' even with a single node.
greenNodeSettings = '''
index.number_of_replicas: 0
'''

mergePartsSettings = '''
index.number_of_replicas: 0
index.merge.scheduler.auto_throttle: false
'''

benchmarkFastSettings = '''
index.refresh_interval: 30s

index.number_of_shards: 6
index.number_of_replicas: 0

index.translog.flush_threshold_size: 4g
index.translog.flush_threshold_ops: 500000
'''

mergePartsLogConfig = '''
es.logger.level: INFO
rootLogger: ${es.logger.level}, console, file
logger:
  action: DEBUG
  com.amazonaws: WARN
  com.amazonaws.jmx.SdkMBeanRegistrySupport: ERROR
  com.amazonaws.metrics.AwsSdkMetrics: ERROR
  org.apache.http: INFO
  index.search.slowlog: TRACE, index_search_slow_log_file
  index.indexing.slowlog: TRACE, index_indexing_slow_log_file
  index.engine.lucene.iw: TRACE

additivity:
  index.search.slowlog: false
  index.indexing.slowlog: false
  deprecation: false

appender:
  console:
    type: console
    layout:
      type: consolePattern
      conversionPattern: "[%d{ISO8601}][%-5p][%-25c] %m%n"

  file:
    type: dailyRollingFile
    file: ${path.logs}/${cluster.name}.log
    datePattern: "'.'yyyy-MM-dd"
    layout:
      type: pattern
      conversionPattern: "[%d{ISO8601}][%-5p][%-25c] %.10000m%n"

  index_search_slow_log_file:
    type: dailyRollingFile
    file: ${path.logs}/${cluster.name}_index_search_slowlog.log
    datePattern: "'.'yyyy-MM-dd"
    layout:
      type: pattern
      conversionPattern: "[%d{ISO8601}][%-5p][%-25c] %m%n"

  index_indexing_slow_log_file:
    type: dailyRollingFile
    file: ${path.logs}/${cluster.name}_index_indexing_slowlog.log
    datePattern: "'.'yyyy-MM-dd"
    layout:
      type: pattern
      conversionPattern: "[%d{ISO8601}][%-5p][%-25c] %m%n"
'''

# Specific tracks add themselves to this dictionary.
#
# key = Track name, value = Track instance
tracks = {}


class Track:
    """
    A track defines the data set that is used. It corresponds loosely to a use case (e.g. logging, event processing, analytics, ...)
    """

    def __init__(self, name, description, source_url, mapping_url, index_name, type_name, number_of_documents, compressed_size_in_bytes,
                 uncompressed_size_in_bytes,
                 local_file_name, local_mapping_name, estimated_benchmark_time_in_minutes, track_setups, queries):
        self.name = name
        self.description = description
        self.source_url = source_url
        self.mapping_url = mapping_url
        self.index_name = index_name
        self.type_name = type_name
        self.number_of_documents = number_of_documents
        self.compressed_size_in_bytes = compressed_size_in_bytes
        self.uncompressed_size_in_bytes = uncompressed_size_in_bytes
        self.local_file_name = local_file_name
        self.local_mapping_name = local_mapping_name
        self.estimated_benchmark_time_in_minutes = estimated_benchmark_time_in_minutes
        self.track_setups = track_setups
        self.queries = queries
        # self-register
        tracks[name] = self


class CandidateSettings:
    def __init__(self, config_snippet=None, logging_config=None, nodes=1, processors=1, heap=None, java_opts=None, gc_opts=None):
        """
        Creates new settings for a benchmark candidate.

        :param config_snippet: A string snippet that will be appended as is to elasticsearch.yml of the benchmark candidate.
        :param logging_config: A string representing the entire contents of logging.yml. If not set, the factory default will be used.
        :param nodes: The number of nodes to start. Defaults to 1 node. All nodes are started on the same machine.
        :param processors: The number of processors to use (per node).
        :param heap: A string defining the maximum amount of Java heap to use. For allows values, see the documentation on -Xmx
        in `<http://docs.oracle.com/javase/8/docs/technotes/tools/unix/java.html#BABHDABI>`
        :param java_opts: Additional Java options to pass to each node on startup.
        :param gc_opts: Additional garbage collector options to pass to each node on startup.
        """
        self.custom_config_snippet = config_snippet
        self.custom_logging_config = logging_config
        self.nodes = nodes
        self.processors = processors
        self.heap = heap
        self.java_opts = java_opts
        self.gc_opts = gc_opts


class IndexIdConflict(Enum):
    """
    Determines which id conflicts to simulate during indexing.

    * NoConflicts: Produce no id conflicts
    * SequentialConflicts: A document id is replaced with a document id with a sequentially increasing id
    * RandomConflicts: A document id is replaced with a document id with a random other id

    Note that this assumes that each document in the benchmark corpus has an id between [1, size_of(corpus)]
    """
    NoConflicts = 0,
    SequentialConflicts = 1,
    RandomConflicts = 2


class BenchmarkSettings:
    def __init__(self, benchmark_search=False, benchmark_indexing=True, id_conflicts=IndexIdConflict.NoConflicts):
        self.benchmark_search = benchmark_search
        self.benchmark_indexing = benchmark_indexing
        self.id_conflicts = id_conflicts


class TrackSetup:
    """
    A track setup defines the concrete operations that will be done and also influences system configuration
    """

    def __init__(self,
                 name,
                 description,
                 candidate_settings=CandidateSettings(),
                 benchmark_settings=BenchmarkSettings()):
        self.name = name
        self.description = description
        self.candidate_settings = candidate_settings
        self.test_settings = benchmark_settings


class Query:
    def __init__(self, name, normalization_factor=1):
        self.name = name
        self.normalization_factor = normalization_factor

    def run(self, es):
        raise NotImplementedError("abstract method")

    def __str__(self):
        return self.name


class Marshal:
    def __init__(self, config):
        self._config = config

    """
    A marshal set up the track for the race
    """

    def setup(self, track):
        data_set_root = "%s/%s" % (self._config.opts("benchmarks", "local.dataset.cache"), track.name.lower())
        data_set_path = "%s/%s" % (data_set_root, track.local_file_name)
        if not os.path.isfile(data_set_path):
            self._download_benchmark_data(track, data_set_root, data_set_path)
        unzipped_data_set_path = self._unzip(data_set_path)
        # global per benchmark (we never run benchmarks in parallel)
        self._config.add(config.Scope.benchmark, "benchmarks", "dataset.path", unzipped_data_set_path)

        mapping_path = "%s/%s" % (data_set_root, track.local_mapping_name)
        if not os.path.isfile(mapping_path):
            self._download_mapping_data(track, data_set_root, mapping_path)
        self._config.add(config.Scope.benchmark, "benchmarks", "mapping.path", mapping_path)

    def _unzip(self, data_set_path):
        # we assume that track data are always compressed and try to unzip them before running the benchmark
        basename, extension = io.splitext(data_set_path)
        if not os.path.isfile(basename):
            logger.info("Unzipping track data from [%s] to [%s]." % (data_set_path, basename))
            io.unzip(data_set_path, io.dirname(data_set_path))
        return basename

    def _download_benchmark_data(self, track, data_set_root, data_set_path):
        io.ensure_dir(data_set_root)
        logger.info("Benchmark data for %s not available in '%s'" % (track.name, data_set_path))
        url = track.source_url
        size = round(convert.bytes_to_mb(track.compressed_size_in_bytes))
        # ensure output appears immediately
        print("Downloading benchmark data from %s (%s MB) ... " % (url, size), end='', flush=True)
        if url.startswith("http"):
            self._do_download_via_http(url, data_set_path)
        elif url.startswith("s3"):
            self._do_download_via_s3(track, url, data_set_path)
        else:
            raise RuntimeError("Cannot download benchmark data. No protocol handler for [%s] available." % url)
        print("done")

    def _download_mapping_data(self, track, data_set_root, mapping_path):
        io.ensure_dir(data_set_root)
        logger.info("Mappings for %s not available in '%s'" % (track.name, mapping_path))
        url = track.mapping_url
        # for now, we just allow HTTP downloads for mappings (S3 support is probably remove anyway...)
        if url.startswith("http"):
            self._do_download_via_http(url, mapping_path)
        else:
            raise RuntimeError("Cannot download mappings. No protocol handler for [%s] available." % url)

    def _do_download_via_http(self, url, data_set_path):
        tmp_data_set_path = data_set_path + ".tmp"
        try:
            with urllib.request.urlopen(url) as response, open(tmp_data_set_path, "wb") as out_file:
                shutil.copyfileobj(response, out_file)
        except:
            logger.info("Removing temp file %s" % tmp_data_set_path)
            os.remove(tmp_data_set_path)
            raise
        else:
            os.rename(tmp_data_set_path, data_set_path)

    def _do_download_via_s3(self, track, url, data_set_path):
        tmp_data_set_path = data_set_path + ".tmp"
        s3cmd = "s3cmd -v get %s %s" % (url, tmp_data_set_path)
        try:
            success = process.run_subprocess_with_logging(s3cmd)
            # Exit code for s3cmd does not seem to be reliable so we also check the file size although this is rather fragile...
            if not success or os.path.getsize(tmp_data_set_path) != track.compressed_size_in_bytes:
                # cleanup probably corrupt data file...
                if os.path.isfile(tmp_data_set_path):
                    os.remove(tmp_data_set_path)
                raise RuntimeError("Could not get benchmark data from S3: '%s'. Is s3cmd installed and set up properly?" % s3cmd)
        except:
            logger.info("Removing temp file %s" % tmp_data_set_path)
            os.remove(tmp_data_set_path)
            raise
        else:
            os.rename(tmp_data_set_path, data_set_path)


# Be very wary of the order here!!! reporter.py assumes this order - see similar comment there
track_setups = [
    TrackSetup(
        name="defaults",
        description="append-only, using all default settings.",
        candidate_settings=CandidateSettings(config_snippet=greenNodeSettings),
        benchmark_settings=BenchmarkSettings(benchmark_search=True)
    ),
    TrackSetup(
        name="4gheap",
        description="same as Defaults except using a 4 GB heap (ES_HEAP_SIZE), because the ES default (-Xmx1g) sometimes hits OOMEs.",
        candidate_settings=CandidateSettings(config_snippet=greenNodeSettings, heap="4g"),
        benchmark_settings=BenchmarkSettings()
    ),

    TrackSetup(
        name="fastsettings",
        description="append-only, using 4 GB heap, and these settings: <pre>%s</pre>" % benchmarkFastSettings,
        candidate_settings=CandidateSettings(config_snippet=benchmarkFastSettings, heap="4g"),
        benchmark_settings=BenchmarkSettings()
    ),

    TrackSetup(
        name="fastupdates",
        description="the same as fast, except we pass in an ID (worst case random UUID) for each document and 25% of the time the ID "
                    "already exists in the index.",
        candidate_settings=CandidateSettings(config_snippet=benchmarkFastSettings, heap="4g"),
        benchmark_settings=BenchmarkSettings(id_conflicts=IndexIdConflict.SequentialConflicts)
    ),

    TrackSetup(
        name="two_nodes_defaults",
        description="append-only, using all default settings, but runs 2 nodes on 1 box (5 shards, 1 replica).",
        # integer divide!
        candidate_settings=CandidateSettings(config_snippet=greenNodeSettings, nodes=2,
                                             processors=sysstats.number_of_cpu_cores() // 2),
        benchmark_settings=BenchmarkSettings()
    ),

    TrackSetup(
        name="defaults_verbose_iw",
        description="Based on defaults but specifically set up to gather merge part times.",
        # integer divide!
        candidate_settings=CandidateSettings(config_snippet=greenNodeSettings,
                                             logging_config=mergePartsLogConfig),
        benchmark_settings=BenchmarkSettings()
    ),
]
