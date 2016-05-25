import os
import logging
import collections
import json
import urllib.error
from enum import Enum

from esrally import config, exceptions
from esrally.utils import io, sysstats, convert, process, net

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
tracks = collections.OrderedDict()


class Index:
    """
    Defines an index in Elasticsearch.
    """

    def __init__(self, name, types):
        """

        Creates a new index.

        :param name: The index name. Mandatory.
        :param types: A list of types. Should contain at least one type.
        """
        self.name = name
        self.types = types

    @property
    def number_of_documents(self):
        num_docs = 0
        for t in self.types:
            num_docs += t.number_of_documents
        return num_docs


class Type:
    """
    Defines a type in Elasticsearch.
    """

    def __init__(self, name, mapping_file_name, document_file_name=None, number_of_documents=0, compressed_size_in_bytes=0,
                 uncompressed_size_in_bytes=0):
        """

        Creates a new type. Mappings are mandatory but the document_file_name (and associated properties) are optional.

        :param name: The name of this type. Mandatory.
        :param mapping_file_name: The file name of the mapping file on the remote server. Mandatory.
        :param document_file_name: The file name of benchmark document name on the remote server. Optional (e.g. for percolation we
        just need a mapping but no documents)
        :param number_of_documents: The number of documents in the benchmark document. Needed for proper progress reporting. Only needed if
         a document_file_name is given.
        :param compressed_size_in_bytes: The compressed size in bytes of the benchmark document. Needed for verification of the download and
         user reporting. Only needed if a document_file_name is given.
        :param uncompressed_size_in_bytes: The size in bytes of the benchmark document after decompressing it. Only needed if a
        document_file_name is given.
        """
        self.name = name
        self.mapping_file_name = mapping_file_name
        self.document_file_name = document_file_name
        self.number_of_documents = number_of_documents
        self.compressed_size_in_bytes = compressed_size_in_bytes
        self.uncompressed_size_in_bytes = uncompressed_size_in_bytes


class Track:
    """
    A track defines the data set that is used. It corresponds loosely to a use case (e.g. logging, event processing, analytics, ...)
    """

    def __init__(self, name, short_description, description, source_root_url, challenges, index_name=None, type_name=None,
                 number_of_documents=0, compressed_size_in_bytes=0, uncompressed_size_in_bytes=0, document_file_name=None,
                 mapping_file_name=None, indices=None):
        """

        Creates a new track.

        Tracks that use only a single index and type can specify all relevant values directly using the parameters index_name, type_name,
        number_of_documents, compressed_size_in_bytes, uncompressed_size_in_bytes, document_file_name and mapping_file_name.

        Tracks that need multiple indices or types have to create Index instances themselves and pass all indices in an indices list.

        :param name: A short, descriptive name for this track. As per convention, this name should be in lower-case without spaces.
        :param short_description: A short description for this track (should be less than 80 characters).
        :param description: A longer description for this track.
        :param source_root_url: The publicly reachable http URL of the root folder for this track (without a trailing slash). Directly
        below this URL, three files should be located: the benchmark document file (see document_file_name), the mapping
        file (see mapping_file_name) and a readme (goes with the name "README.txt" as per convention).
        :param challenges: A list of one or more challenges to use. If in doubt, reuse the predefined list "track.challenges". Rally's
        default configuration assumes that each track defines at least one challenge with the name "append-no-conflicts".
        This is not required but simplifies usage.
        :param index_name: The name of the index to create.
        :param type_name: The type name for this index.
        :param number_of_documents: The number of documents in the benchmark document. Needed for proper progress reporting.
        :param compressed_size_in_bytes: The compressed size in bytes of the benchmark document. Needed for verification of the download and
         user reporting.
        :param uncompressed_size_in_bytes: The size in bytes of the benchmark document after decompressing it.
        :param document_file_name: The file name of benchmark document name on the remote server.
        :param mapping_file_name: The file name of the mapping file on the remote server.
        :param indices: A list of indices for this track. Set this parameter when using multiple indices or types.
        """
        self.name = name
        self.short_description = short_description
        self.description = description
        self.source_root_url = source_root_url
        self.challenges = challenges
        self.readme_file_name = "README.txt"
        # multiple indices
        if index_name is None:
            if not indices:
                raise RuntimeError("You have to pass either indices or an index_name")
            self.indices = indices
        else:
            self.indices = [Index(index_name, [
                Type(type_name, mapping_file_name, document_file_name, number_of_documents, compressed_size_in_bytes,
                     uncompressed_size_in_bytes)
            ])]
        # self-register
        tracks[name] = self

    @property
    def number_of_documents(self):
        num_docs = 0
        for index in self.indices:
            num_docs += index.number_of_documents
        return num_docs


# This will eventually move out of track
class Car:
    def __init__(self, name, config_snippet=None, logging_config=None, nodes=1, processors=1, heap=None,
                 java_opts=None, gc_opts=None):
        """
        Creates new settings for a benchmark candidate.

        :param name: A descriptive name for this car.
        :param config_snippet: A string snippet that will be appended as is to elasticsearch.yml of the benchmark candidate.
        :param logging_config: A string representing the entire contents of logging.yml. If not set, the factory defaults will be used.
        :param nodes: The number of nodes to start. Defaults to 1 node. All nodes are started on the same machine.
        :param processors: The number of processors to use (per node).
        :param heap: A string defining the maximum amount of Java heap to use. For allows values, see the documentation on -Xmx
        in `<http://docs.oracle.com/javase/8/docs/technotes/tools/unix/java.html#BABHDABI>`
        :param java_opts: Additional Java options to pass to each node on startup.
        :param gc_opts: Additional garbage collector options to pass to each node on startup.
        """
        self.name = name
        self.custom_config_snippet = config_snippet
        self.custom_logging_config = logging_config
        self.nodes = nodes
        self.processors = processors
        self.heap = heap
        self.java_opts = java_opts
        self.gc_opts = gc_opts

    def __str__(self):
        return self.name


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


class BenchmarkPhase(Enum):
    index = 0,
    stats = 1
    search = 2,


class LatencyBenchmarkSettings:
    def __init__(self, queries=None, warmup_iteration_count=1000, iteration_count=1000):
        """
        Creates new LatencyBenchmarkSettings.

        :param queries: A list of queries to run. Optional.
        :param warmup_iteration_count: The number of times each query should be run for warmup. Defaults to 1000 iterations.
        :param iteration_count: The number of times each query should be run. Defaults to 1000 iterations.
        """
        if queries is None:
            queries = []
        self.queries = queries
        self.warmup_iteration_count = warmup_iteration_count
        self.iteration_count = iteration_count


class IndexBenchmarkSettings:
    def __init__(self, index_settings=None, clients=8, bulk_size=5000, id_conflicts=IndexIdConflict.NoConflicts, force_merge=True):
        """
        :param index_settings: A hash of index-level settings that will be set when the index is created.
        :param clients: Number of concurrent clients that should index data.
        :param bulk_size: The number of documents to submit in a single bulk (Default: 5000).
        :param id_conflicts: Whether to produce index id conflicts during indexing (Default: NoConflicts).
        :param force_merge: Whether to do a force merge after the index benchmark (Default: True).
        """
        if index_settings is None:
            index_settings = {}
        self.index_settings = index_settings
        self.clients = clients
        self.bulk_size = bulk_size
        self.id_conflicts = id_conflicts
        self.force_merge = force_merge


class Challenge:
    """
    A challenge defines the concrete operations that will be done.
    """

    def __init__(self,
                 name,
                 description,
                 benchmark=None):
        if benchmark is None:
            benchmark = {}
        self.name = name
        self.description = description
        self.benchmark = benchmark

    def __str__(self):
        return self.name


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
        data_set_root = "%s/%s" % (self._config.opts("benchmarks", "local.dataset.cache"), track.name.lower())
        data_set_paths = {}
        mapping_paths = {}
        for index in track.indices:
            for type in index.types:
                mapping_file_name = self.mapping_file_name(type)
                mapping_path = "%s/%s" % (data_set_root, mapping_file_name)
                mapping_url = "%s/%s" % (track.source_root_url, mapping_file_name)
                # Try to always download the mapping file, there might be an updated version
                try:
                    self._download(mapping_url, mapping_path, force_download=True, raise_url_error=True)
                except urllib.error.URLError:
                    distribution_version = self._config.opts("source", "distribution.version", mandatory=False)
                    if distribution_version and len(distribution_version.strip()) > 0:
                        msg = "Could not download mapping file [%s] for Elasticsearch distribution [%s] from [%s]. Please note that only " \
                              "versions starting from 5.0.0-alpha1 are supported." % (mapping_file_name, distribution_version, mapping_url)
                    else:
                        msg = "Could not download mapping file [%s] from [%s]. Please check that the data are available." % \
                              (mapping_file_name, mapping_url)
                    logger.error(msg)
                    raise exceptions.SystemSetupError(msg)

                mapping_paths[type] = mapping_path

                if type.document_file_name:
                    data_set_path = "%s/%s" % (data_set_root, type.document_file_name)
                    data_url = "%s/%s" % (track.source_root_url, type.document_file_name)
                    self._download(data_url, data_set_path, size_in_bytes=type.compressed_size_in_bytes)
                    decompressed_data_set_path = self._decompress(data_set_path)
                    data_set_paths[type] = decompressed_data_set_path

        self._config.add(config.Scope.benchmark, "benchmarks", "dataset.path", data_set_paths)
        self._config.add(config.Scope.benchmark, "benchmarks", "mapping.path", mapping_paths)

        readme_path = "%s/%s" % (data_set_root, track.readme_file_name)
        readme_url = "%s/%s" % (track.source_root_url, track.readme_file_name)
        self._download(readme_url, readme_path)

    def mapping_file_name(self, type):
        distribution_version = self._config.opts("source", "distribution.version", mandatory=False)
        if distribution_version and len(distribution_version.strip()) > 0:
            path, extension = io.splitext(type.mapping_file_name)
            return "%s-%s%s" % (path, distribution_version, extension)
        else:
            return type.mapping_file_name

    def _download(self, url, local_path, size_in_bytes=None, force_download=False, raise_url_error=False):
        offline = self._config.opts("system", "offline.mode")
        file_exists = os.path.isfile(local_path)

        if file_exists and not force_download:
            logger.info("[%s] already exists locally. Skipping download." % local_path)
            return

        if not offline:
            logger.info("Downloading from [%s] to [%s]." % (url, local_path))
            try:
                io.ensure_dir(os.path.dirname(local_path))
                if size_in_bytes:
                    size_in_mb = round(convert.bytes_to_mb(size_in_bytes))
                    # ensure output appears immediately
                    print("Downloading data from %s (%s MB) ... " % (url, size_in_mb), end='', flush=True)
                if url.startswith("http"):
                    net.download(url, local_path, size_in_bytes)
                elif url.startswith("s3"):
                    self._do_download_via_s3(url, local_path, size_in_bytes)
                else:
                    raise exceptions.SystemSetupError("Cannot download benchmark data from [%s]. Only http(s) and s3 are supported." % url)
                if size_in_bytes:
                    print("Done")
            except urllib.error.URLError:
                logger.exception("Could not download [%s] to [%s]." % (url, local_path))
                if raise_url_error:
                    raise

        # file must exist at this point -> verify
        if not os.path.isfile(local_path):
            if offline:
                raise exceptions.SystemSetupError("Cannot find %s. Please disable offline mode and retry again." % local_path)
            else:
                raise exceptions.SystemSetupError("Could not download from %s to %s. Please verify that data are available at %s and "
                                                  "check your internet connection." % (url, local_path, url))

    def _decompress(self, data_set_path):
        # we assume that track data are always compressed and try to decompress them before running the benchmark
        basename, extension = io.splitext(data_set_path)
        if not os.path.isfile(basename):
            logger.info("Unzipping track data from [%s] to [%s]." % (data_set_path, basename))
            io.decompress(data_set_path, io.dirname(data_set_path))
        return basename

    def _do_download_via_s3(self, url, data_set_path, size_in_bytes):
        tmp_data_set_path = data_set_path + ".tmp"
        s3cmd = "s3cmd -v get %s %s" % (url, tmp_data_set_path)
        try:
            success = process.run_subprocess_with_logging(s3cmd)
            # Exit code for s3cmd does not seem to be reliable so we also check the file size although this is rather fragile...
            if not success or (size_in_bytes is not None and os.path.getsize(tmp_data_set_path) != size_in_bytes):
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


# This will eventually move out of track
cars = [
    Car(name="defaults"),
    Car(name="4gheap", heap="4g"),
    Car(name="two_nodes", nodes=2, processors=sysstats.logical_cpu_cores() // 2),
    Car(name="verbose_iw", logging_config=mergePartsLogConfig)
]


class TrackSyntaxError(Exception):
    """
    Raised whenever a syntax problem is encountered when loading the track specification.
    """
    pass


class TrackFileReader:
    def __init__(self, cfg):
        self.cfg = cfg

    def _all_track_names(self):
        # TODO dm: This will do for now. Resolve this on the file system later (#69).
        return ["geonames", "tiny"]

    def all_tracks(self):
        return [self.read(track_name) for track_name in self._all_track_names()]

    def read(self, track_name):
        # TODO dm: This will change with #69
        track_file = "%s/track/%s.json" % (self.cfg.opts("system", "rally.root"), track_name)
        track_spec = json.loads(open(track_file).read())
        return TrackReader().read(track_spec)


class TrackReader:
    def __init__(self):
        pass

    def read(self, track_specification):
        name = self._r(track_specification, ["meta", "name"], expected_type=str)
        short_description = self._r(track_specification, ["meta", "short-description"], expected_type=str)
        description = self._r(track_specification, ["meta", "description"], expected_type=str)
        source_root_url = self._r(track_specification, ["meta", "data-url"], expected_type=str)
        indices = [self._create_index(index) for index in self._r(track_specification, "indices")]
        challenges = self._create_challenges(track_specification, indices)

        return Track(name=name, short_description=short_description, description=description, source_root_url=source_root_url,
                     challenges=challenges, indices=indices)

    def _check(self, value, msg, validator):
        if not validator(value):
            raise TrackSyntaxError(msg)
        else:
            return value

    def _check_is_positive(self, name, value, zero_allowed=False):
        if zero_allowed:
            validator = lambda v: v >= 0
        else:
            validator = lambda v: v > 0
        return self._check(value, "'%s' must be positive but is %d" % (name, value), validator)

    def _check_non_empty(self, name, value, error_ctx=None):
        if error_ctx:
            msg = "'%s' must not be empty in '%s'." % (name, error_ctx)
        else:
            msg = "'%s' must not be empty." % name
        return self._check(value, msg, lambda v: v and len(v) > 0)

    def _r(self, root, path, error_ctx=None, expected_type=None, mandatory=True, default_value=None):
        if isinstance(path, str):
            path = [path]

        structure = root
        try:
            for k in path:
                structure = structure[k]
            if expected_type:
                # either we cannot convert the data to this type or it is already the wrong type
                try:
                    converted = expected_type(structure)
                except BaseException:
                    raise TrackSyntaxError(
                        "Value '%s' of element '%s' is not of expected type '%s'" % (structure, ".".join(path), expected_type))
                if converted != structure:
                    raise TrackSyntaxError(
                        "Value '%s' of element '%s' is not of expected type '%s'" % (structure, ".".join(path), expected_type))
            return structure
        except KeyError:
            if mandatory:
                if error_ctx:
                    raise TrackSyntaxError("Mandatory element '%s' is missing in '%s'." % (".".join(path), error_ctx))
                else:
                    raise TrackSyntaxError("Mandatory element '%s' is missing." % ".".join(path))
            else:
                return default_value

    def _create_index(self, index_spec):
        return Index(name=self._r(index_spec, "name", expected_type=str),
                     types=[self._create_type(type_spec) for type_spec in self._r(index_spec, "types")]
                     )

    def _create_type(self, type_spec):
        return Type(name=self._r(type_spec, "name", expected_type=str),
                    mapping_file_name=self._r(type_spec, "mapping", expected_type=str),
                    document_file_name=self._r(type_spec, "documents", expected_type=str),
                    number_of_documents=self._r(type_spec, "document-count", expected_type=int),
                    compressed_size_in_bytes=self._r(type_spec, "compressed-bytes", expected_type=int),
                    uncompressed_size_in_bytes=self._r(type_spec, "uncompressed-bytes", expected_type=int)
                    )

    def _create_challenges(self, track_spec, indices):
        ops = self._parse_operations(self._r(track_spec, "operations"), indices)
        challenges = []
        for challenge in self._check_non_empty("challenges", self._r(track_spec, "challenges")):
            challenge_name = self._r(challenge, "name", expected_type=str, error_ctx="challenges")
            challenge_description = self._r(challenge, "description", expected_type=str, error_ctx=challenge_name)
            benchmarks = {}

            operations_per_type = {}
            for op in self._check_non_empty("schedule", self._r(challenge, "schedule", error_ctx=challenge_name), error_ctx=challenge_name):
                if op not in ops:
                    raise TrackSyntaxError("'schedule' for challenge '%s' contains a non-existing operation '%s'. "
                                           "Please add an operation '%s' to the 'operations' block." % (challenge_name, op, op))

                benchmark_type, benchmark_spec = ops[op]
                if benchmark_type in benchmarks:
                    new_op_name = op
                    old_op_name = operations_per_type[benchmark_type]
                    raise TrackSyntaxError("'schedule' for challenge '%s' contains multiple operations of type '%s' which is currently "
                                           "unsupported. Please remove one of these operations: '%s', '%s'" %
                                           (challenge_name, benchmark_type, old_op_name, new_op_name))
                benchmarks[benchmark_type] = benchmark_spec
                operations_per_type[benchmark_type] = op
            challenges.append(Challenge(name=challenge_name,
                                        description=challenge_description,
                                        benchmark=benchmarks))

        return challenges

    def _parse_operations(self, ops_specs, indices):
        # key = name, value = (BenchmarkPhase instance, BenchmarkSettings instance)
        ops = {}
        for ops_spec_entry in ops_specs:
            try:
                ops_spec_name, ops_spec = next(iter(ops_spec_entry.items()))
            except AttributeError:
                raise TrackSyntaxError("Cannot parse '%s'. The ‘operations' block must contain only operation objects." % ops_spec_entry)
            ops[ops_spec_name] = self._create_op(ops_spec_name, ops_spec, indices)

        return ops

    def _create_op(self, ops_spec_name, ops_spec, indices):
        benchmark_type = self._r(ops_spec, "type", expected_type=str)
        if benchmark_type == "index":
            id_conflicts = self._r(ops_spec, "conflicts", expected_type=str, mandatory=False)
            if not id_conflicts:
                id_conflicts = IndexIdConflict.NoConflicts
            elif id_conflicts == "sequential":
                id_conflicts = IndexIdConflict.SequentialConflicts
            elif id_conflicts == "random":
                id_conflicts = IndexIdConflict.RandomConflicts
            else:
                raise TrackSyntaxError("Unknown conflict type '%s' for operation '%s'" % (id_conflicts, ops_spec))

            return (BenchmarkPhase.index,
                    IndexBenchmarkSettings(index_settings=self._r(ops_spec, "index-settings", error_ctx=ops_spec_name),
                                           clients=self._check_is_positive("count",
                                                                           self._r(ops_spec, ["clients", "count"],
                                                                                   expected_type=int, error_ctx=ops_spec_name)),
                                           bulk_size=self._check_is_positive("bulk-size",
                                                                             self._r(ops_spec, "bulk-size",
                                                                                     expected_type=int, error_ctx=ops_spec_name)),
                                           force_merge=self._r(ops_spec, "force-merge", expected_type=bool, error_ctx=ops_spec_name),
                                           id_conflicts=id_conflicts))
        elif benchmark_type == "search":
            # TODO: Honor clients settings
            return (BenchmarkPhase.search,
                    LatencyBenchmarkSettings(queries=self._create_queries(self._r(ops_spec, "queries", error_ctx=ops_spec_name), indices),
                                             warmup_iteration_count=self._check_is_positive("warmup-iterations",
                                                                                            self._r(ops_spec, "warmup-iterations",
                                                                                                    expected_type=int,
                                                                                                    error_ctx=ops_spec_name)
                                                                                            , zero_allowed=True),
                                             iteration_count=self._check_is_positive("iterations",
                                                                                     self._r(ops_spec, "iterations", expected_type=int))))
        elif benchmark_type == "stats":
            # TODO: Honor clients settings
            return (BenchmarkPhase.stats,
                    LatencyBenchmarkSettings(
                       warmup_iteration_count=self._check_is_positive("warmup-iterations",
                                                                      self._r(ops_spec, "warmup-iterations", expected_type=int,
                                                                              error_ctx=ops_spec_name),
                                                                      zero_allowed=True),
                       iteration_count=self._check_is_positive("iterations", self._r(ops_spec, "iterations", expected_type=int,
                                                                                     error_ctx=ops_spec_name))))
        else:
            raise TrackSyntaxError("Unknown benchmark type '%s' for operation '%s'" % (benchmark_type, ops_spec_name))

    def _create_queries(self, queries_spec, indices):
        if len(indices) == 1 and len(indices[0].types) == 1:
            default_index = indices[0].name
            default_type = indices[0].types[0].name
        else:
            default_index = None
            default_type = None
        queries = []
        for query_spec_entry in queries_spec:
            query_name, query_spec = next(iter(query_spec_entry.items()))
            query_type = self._r(query_spec, "type", expected_type=str, mandatory=False, default_value="default", error_ctx=query_name)

            index_name = self._r(query_spec, "index", expected_type=str, mandatory=False, default_value=default_index, error_ctx=query_name)
            type_name = self._r(query_spec, "type", expected_type=str, mandatory=False, default_value=default_type, error_ctx=query_name)

            if not index_name or not type_name:
                raise TrackSyntaxError("Query '%s' requires an index and a type." % query_name)
            request_cache = self._r(query_spec, "cache", expected_type=bool, mandatory=False, default_value=False, error_ctx=query_name)
            if query_type == "default":
                query_body = self._r(query_spec, "body", error_ctx=query_name)
                queries.append(DefaultQuery(index=index_name, type=type_name, name=query_name,
                                            body=query_body, use_request_cache=request_cache))
            elif query_type == "scroll":
                query_body = self._r(query_spec, "body", mandatory=False, error_ctx=query_name)
                pages = self._check_is_positive("pages", self._r(query_spec, "pages", expected_type=int, error_ctx=query_name))
                items_per_page = self._check_is_positive("results-per-page",
                                                         self._r(query_spec, "results-per-page", expected_type=int, error_ctx=query_name))
                queries.append(ScrollQuery(index=index_name, type=type_name, name=query_name, body=query_body,
                                           use_request_cache=request_cache, pages=pages, items_per_page=items_per_page))
            else:
                raise TrackSyntaxError("Unknown query type '%s' in query '%s'" % (query_type, query_name))
        return queries


class DefaultQuery(Query):
    def __init__(self, index, type, name, body, use_request_cache=False):
        Query.__init__(self, name)
        self.index = index
        self.type = type
        self.body = body
        self.use_request_cache = use_request_cache

    def run(self, es):
        return es.search(index=self.index, doc_type=self.type, request_cache=self.use_request_cache, body=self.body)


class ScrollQuery(Query):
    def __init__(self, index, type, name, body, use_request_cache, pages, items_per_page):
        Query.__init__(self, name, normalization_factor=pages)
        self.index = index
        self.type = type
        self.pages = pages
        self.items_per_page = items_per_page
        self.body = body
        self.use_request_cache = use_request_cache
        self.scroll_id = None

    def run(self, es):
        r = es.search(
            index=self.index,
            doc_type=self.type,
            body=self.body,
            sort="_doc",
            scroll="10s",
            size=self.items_per_page,
            request_cache=self.use_request_cache)
        self.scroll_id = r["_scroll_id"]
        # Note that starting with ES 2.0, the initial call to search() returns already the first result page
        # so we have to retrieve one page less
        for i in range(self.pages - 1):
            hit_count = len(r["hits"]["hits"])
            if hit_count == 0:
                # done
                break
            r = es.scroll(scroll_id=self.scroll_id, scroll="10s")

    def close(self, es):
        if self.scroll_id:
            es.clear_scroll(scroll_id=self.scroll_id)
            self.scroll_id = None
