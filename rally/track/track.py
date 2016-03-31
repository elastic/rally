import os
import logging
import urllib.error
from enum import Enum

from rally import config
from rally.utils import io, sysstats, convert, process, net

logger = logging.getLogger("rally.track")

# Ensure cluster status green even for single nodes. Please don't add anything else here except to get the cluster to status
# 'green' even with a single node.
greenNodeSettings = {
    "index.number_of_replicas": 0
}

mergePartsSettings = {
    "index.number_of_replicas": 0,
    "index.merge.scheduler.auto_throttle": False
}

benchmarkFastSettings = {
    "index.refresh_interval": "30s",
    "index.number_of_shards": 6,
    "index.number_of_replicas": 0,
    "index.translog.flush_threshold_size": "4g"
}

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

    def __init__(self, name, description, source_root_url, index_name, type_name, number_of_documents, compressed_size_in_bytes,
                 uncompressed_size_in_bytes, document_file_name, mapping_file_name, estimated_benchmark_time_in_minutes,
                 track_setups, queries):
        """

        Creates a new track.

        :param name: A short, descriptive name for this track. As per convention, this name should be in lower-case without spaces.
        :param description: A longer description for this track.
        :param source_root_url: The publicly reachable http URL of the root folder for this track (without a trailing slash). Directly
        below this URL, three files should be located: the benchmark document file (see document_file_name), the mapping
        file (see mapping_file_name) and a readme (goes with the name "README.txt" as per convention).
        :param index_name: The name of the index to create.
        :param type_name: The type name for this index.
        :param number_of_documents: The number of documents in the benchmark document. Needed for proper progress reporting.
        :param compressed_size_in_bytes: The compressed size in bytes of the benchmark document. Needed for verification of the download and
         user reporting.
        :param uncompressed_size_in_bytes: The size in bytes of the benchmark document after decompressing it.
        :param document_file_name: The file name of benchmark document name on the remote server.
        :param mapping_file_name: The file name of the mapping file on the remote server.
        :param estimated_benchmark_time_in_minutes: A ballpark estimation how long the benchmark will run (in minutes). This is just needed
        to give the user a very rough idea on the duration of the benchmark.
        :param track_setups: A list of one or more track setups to use. If in doubt, reuse the predefined list "track.track_setups". Rally's
        default configuration assumes that each track defines at least one track setup with the name "defaults". This is not required but
        simplifies usage.
        :param queries: A list of queries to run in case searching should be benchmarked.
        """
        self.name = name
        self.description = description
        self.source_root_url = source_root_url
        self.index_name = index_name
        self.type_name = type_name
        self.number_of_documents = number_of_documents
        self.compressed_size_in_bytes = compressed_size_in_bytes
        self.uncompressed_size_in_bytes = uncompressed_size_in_bytes
        self.document_file_name = document_file_name
        self.mapping_file_name = mapping_file_name
        self.readme_file_name = "README.txt"
        self.estimated_benchmark_time_in_minutes = estimated_benchmark_time_in_minutes
        self.track_setups = track_setups
        self.queries = queries
        # self-register
        tracks[name] = self


class CandidateSettings:
    def __init__(self, config_snippet=None, logging_config=None, index_settings=None, nodes=1, processors=1, heap=None,
                 java_opts=None, gc_opts=None):
        """
        Creates new settings for a benchmark candidate.

        :param config_snippet: A string snippet that will be appended as is to elasticsearch.yml of the benchmark candidate.
        :param logging_config: A string representing the entire contents of logging.yml. If not set, the factory defaults will be used.
        :param index_settings: A hash of index-level settings that will be set when the index is created.
        :param nodes: The number of nodes to start. Defaults to 1 node. All nodes are started on the same machine.
        :param processors: The number of processors to use (per node).
        :param heap: A string defining the maximum amount of Java heap to use. For allows values, see the documentation on -Xmx
        in `<http://docs.oracle.com/javase/8/docs/technotes/tools/unix/java.html#BABHDABI>`
        :param java_opts: Additional Java options to pass to each node on startup.
        :param gc_opts: Additional garbage collector options to pass to each node on startup.
        """
        if index_settings is None:
            index_settings = {}
        self.custom_config_snippet = config_snippet
        self.custom_logging_config = logging_config
        self.index_settings = index_settings
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
    def __init__(self, benchmark_search=False, benchmark_indexing=True, id_conflicts=IndexIdConflict.NoConflicts, force_merge=True):
        self.benchmark_search = benchmark_search
        self.benchmark_indexing = benchmark_indexing
        self.id_conflicts = id_conflicts
        self.force_merge = force_merge


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
        self.benchmark_settings = benchmark_settings


class Query:
    def __init__(self, name, normalization_factor=1):
        self.name = name
        self.normalization_factor = normalization_factor

    def run(self, es):
        """
        Runs a query.

        :param es: Elasticsearch client object
        """
        raise NotImplementedError("abstract method")

    def close(self, es):
        """
        Subclasses can override this method for any custom cleanup logic

        :param es: Elasticsearch client object
        """
        pass

    def __str__(self):
        return self.name


class Marshal:
    def __init__(self, cfg):
        self._config = cfg

    """
    A marshal sets up the track for the race
    """

    def setup(self, track):
        offline = self._config.opts("system", "offline.mode")
        data_set_root = "%s/%s" % (self._config.opts("benchmarks", "local.dataset.cache"), track.name.lower())
        data_set_path = "%s/%s" % (data_set_root, track.document_file_name)
        if not offline and not os.path.isfile(data_set_path):
            self._download_benchmark_data(track, data_set_root, data_set_path)
        unzipped_data_set_path = self._unzip(data_set_path)
        # global per benchmark (we never run benchmarks in parallel)
        self._config.add(config.Scope.benchmark, "benchmarks", "dataset.path", unzipped_data_set_path)

        mapping_path = "%s/%s" % (data_set_root, track.mapping_file_name)
        # Try to always download the mapping file, there might be an updated version
        if not offline:
            try:
                self._download_mapping_data(track, data_set_root, mapping_path)
            except urllib.error.URLError:
                # Just retry with the old version if there is one
                if os.path.isfile(mapping_path):
                    logger.info("Could not download mapping file (probably offline). Skipping...")
                else:
                    raise

        self._config.add(config.Scope.benchmark, "benchmarks", "mapping.path", mapping_path)

        readme_path = "%s/%s" % (data_set_root, track.readme_file_name)
        if not offline and not os.path.isfile(readme_path):
            self._download_readme(track, data_set_root, readme_path)


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
        url = "%s/%s" % (track.source_root_url, track.document_file_name)
        size = round(convert.bytes_to_mb(track.compressed_size_in_bytes))
        # ensure output appears immediately
        print("Downloading benchmark data from %s (%s MB) ... " % (url, size), end='', flush=True)
        if url.startswith("http"):
            net.download(url, data_set_path)
        elif url.startswith("s3"):
            self._do_download_via_s3(track, url, data_set_path)
        else:
            raise RuntimeError("Cannot download benchmark data. No protocol handler for [%s] available." % url)
        print("done")

    def _download_mapping_data(self, track, data_set_root, mapping_path):
        io.ensure_dir(data_set_root)
        logger.info("Mappings for %s not available in '%s'" % (track.name, mapping_path))
        url = "%s/%s" % (track.source_root_url, track.mapping_file_name)
        # for now, we just allow HTTP downloads for mappings (S3 support is probably remove anyway...)
        if url.startswith("http"):
            net.download(url, mapping_path)
        else:
            raise RuntimeError("Cannot download mappings. No protocol handler for [%s] available." % url)

    def _download_readme(self, track, data_set_root, readme_path):
        io.ensure_dir(data_set_root)
        logger.info("Readme for %s not available in '%s'" % (track.name, readme_path))
        url = "%s/%s" % (track.source_root_url, track.readme_file_name)
        if url.startswith("http"):
            net.download(url, readme_path)
        else:
            raise RuntimeError("Cannot download Readme. No protocol handler for [%s] available." % url)

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
        candidate_settings=CandidateSettings(index_settings=greenNodeSettings),
        benchmark_settings=BenchmarkSettings(benchmark_search=True)
    ),
    TrackSetup(
        name="4gheap",
        description="same as Defaults except using a 4 GB heap (ES_HEAP_SIZE), because the ES default (-Xmx1g) sometimes hits OOMEs.",
        candidate_settings=CandidateSettings(index_settings=greenNodeSettings, heap="4g"),
        benchmark_settings=BenchmarkSettings()
    ),

    TrackSetup(
        name="fastsettings",
        description="append-only, using 4 GB heap, and these settings: <pre>%s</pre>" % benchmarkFastSettings,
        candidate_settings=CandidateSettings(index_settings=benchmarkFastSettings, heap="4g"),
        benchmark_settings=BenchmarkSettings()
    ),

    TrackSetup(
        name="fastupdates",
        description="the same as fast, except we pass in an ID (worst case random UUID) for each document and 25% of the time the ID "
                    "already exists in the index.",
        candidate_settings=CandidateSettings(index_settings=benchmarkFastSettings, heap="4g"),
        benchmark_settings=BenchmarkSettings(id_conflicts=IndexIdConflict.SequentialConflicts)
    ),

    TrackSetup(
        name="two_nodes_defaults",
        description="append-only, using all default settings, but runs 2 nodes on 1 box (5 shards, 1 replica).",
        # integer divide!
        candidate_settings=CandidateSettings(index_settings=greenNodeSettings, nodes=2,
                                             processors=sysstats.number_of_cpu_cores() // 2),
        benchmark_settings=BenchmarkSettings()
    ),

    TrackSetup(
        name="defaults_verbose_iw",
        description="Based on defaults but specifically set up to gather merge part times.",
        # integer divide!
        candidate_settings=CandidateSettings(index_settings=greenNodeSettings,
                                             logging_config=mergePartsLogConfig),
        benchmark_settings=BenchmarkSettings()
    ),
]
