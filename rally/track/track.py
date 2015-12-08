##########################################################################################
#
#
# BEWARE: Unfortunately, this is one of the weakest spots of the domain model. Expect a
#         lot of shuffling around! This is just here to get going.
#
#
##########################################################################################

# Abstract representation for a track (corresponds to a concrete benchmark)
#
# A track defines the data set that is used. It corresponds loosely to a use case (e.g. logging, event processing, analytics, ...)
#
# Contains of 1 .. n track setups

# TODO dm [Refactoring: Prio high]: move track.py one level up when we're done (import rally.track.track is just weird)

import os
import logging
import urllib.request
import shutil
from enum import Enum

import rally.config as cfg
import rally.utils.io
import rally.utils.format

import rally.cluster

logger = logging.getLogger("rally.track")


class CandidateSettings:
  # TODO dm: Allow also other settings such as custom JVM options
  def __init__(self, custom_config_snippet=None, nodes=1, processors=1, heap=None):
    self._custom_config_snippet = custom_config_snippet
    self._nodes = nodes
    self._processors = processors
    self._heap = heap

  @property
  def custom_config_snippet(self):
    return self._custom_config_snippet

  @property
  def nodes(self):
    return self._nodes

  @property
  def processors(self):
    return self._processors

  @property
  def heap(self):
    return self._heap


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


# TODO dm [Refactoring]: Is this name good enough? Maybe benchmarkSettings?
class TestSettings:
  def __init__(self, benchmark_search=False, benchmark_indexing=True, id_conflicts=IndexIdConflict.NoConflicts):
    self._benchmark_search = benchmark_search
    self._benchmark_indexing = benchmark_indexing
    self._id_conflicts = id_conflicts

  @property
  def benchmark_search(self):
    return self._benchmark_search

  @property
  def benchmark_indexing(self):
    return self._benchmark_indexing

  @property
  def id_conflicts(self):
    return self._id_conflicts


class TrackSetupSpecification:
  # TODO dm [Refactoring]: Specifying the required cluster status is just a workaround...
  def __init__(self, name, description, candidate_settings, test_settings, required_cluster_status=rally.cluster.ClusterStatus.yellow):
    self._name = name
    self._description = description
    self._candidate_settings = candidate_settings
    self._test_settings = test_settings
    self._required_cluster_status = required_cluster_status

  # TODO dm [Refactoring]: This class is supposed to be just a specification and should not play an active role - move this code
  def setup(self, config):
    # Provide runtime configuration
    if self.candidate_settings.heap:
      config.add(cfg.Scope.trackSetupScope, "provisioning", "es.heap", self.candidate_settings.heap)
    if self.candidate_settings.custom_config_snippet:
      config.add(cfg.Scope.trackSetupScope, "provisioning", "es.config", self.candidate_settings.custom_config_snippet)

    config.add(cfg.Scope.trackSetupScope, "provisioning", "es.processors", self.candidate_settings.processors)
    config.add(cfg.Scope.trackSetupScope, "provisioning", "es.nodes", self.candidate_settings.nodes)

  @property
  def name(self):
    return self._name

  @property
  def description(self):
    return self._description

  @property
  def candidate_settings(self):
    return self._candidate_settings

  @property
  def test_settings(self):
    return self._test_settings

  @property
  def required_cluster_status(self):
    return self._required_cluster_status


class TrackSpecification:
  # TODO dm: For now we allow just one index with one type. Change this later
  def __init__(self, name, description, source_url, mapping_url, index_name, type_name, number_of_documents, compressed_size_in_bytes,
               uncompressed_size_in_bytes,
               local_file_name, estimated_benchmark_time_in_minutes, track_setups, queries):
    self._name = name
    self._description = description
    self._source_url = source_url
    self._mapping_url = mapping_url
    self._index_name = index_name
    self._type_name = type_name
    self._number_of_documents = number_of_documents
    self._compressed_size_in_bytes = compressed_size_in_bytes
    self._uncompressed_size_in_bytes = uncompressed_size_in_bytes
    self._local_file_name = local_file_name
    self._estimated_benchmark_time_in_minutes = estimated_benchmark_time_in_minutes
    self._track_setups = track_setups
    self._queries = queries

  @property
  def name(self):
    return self._name

  @property
  def description(self):
    return self._description

  @property
  def source_url(self):
    return self._source_url

  @property
  def mapping_url(self):
    return self._mapping_url

  @property
  def index_name(self):
    return self._index_name

  @property
  def type_name(self):
    return self._type_name

  @property
  def number_of_documents(self):
    return self._number_of_documents

  @property
  def compressed_size_in_bytes(self):
    return self._compressed_size_in_bytes

  @property
  def uncompressed_size_in_bytes(self):
    return self._uncompressed_size_in_bytes

  @property
  def local_file_name(self):
    return self._local_file_name

  @property
  def estimated_benchmark_time_in_minutes(self):
    return self._estimated_benchmark_time_in_minutes

  @property
  def track_setups(self):
    return self._track_setups

  @property
  def queries(self):
    return self._queries


class Query:
  def __init__(self, name, normalization_factor=1):
    self._name = name
    self._normalization_factor = normalization_factor

  def run(self, es):
    raise NotImplementedError("abstract method")

  @property
  def name(self):
    return self._name

  @property
  def normalization_factor(self):
    return self._normalization_factor


class Marshal:
  def __init__(self, config):
    self._config = config

  """
  A marshal set up the track for the race
  """

  def setup(self, track_spec):
    data_set_root = "%s/%s" % (self._config.opts("benchmarks", "local.dataset.cache"), track_spec.name.lower())
    data_set_path = "%s/%s" % (data_set_root, track_spec.local_file_name)
    if not os.path.isfile(data_set_path):
      self._download_benchmark_data(track_spec, data_set_root, data_set_path)
    # global per benchmark (we never run benchmarks in parallel)
    self._config.add(cfg.Scope.benchmarkScope, "benchmarks", "dataset.path", data_set_path)
    # TODO dm: Also download the mapping file. For now we deliver the mapping file with Rally as a workaround and rely on the convention
    # that the mapping file is called resources/datasets/$track_spec.lower()/mappings.json
    mapping_path = "%s/mappings.json" % data_set_root
    if not os.path.isfile(mapping_path):
      self._download_mapping_data(mapping_path)
    self._config.add(cfg.Scope.benchmarkScope, "benchmarks", "mapping.path", mapping_path)

  def _download_benchmark_data(self, track_spec, data_set_root, data_set_path):
    rally.utils.io.ensure_dir(data_set_root)
    logger.info("Benchmark data for %s not available in '%s'" % (track_spec.name, data_set_path))
    url = track_spec.source_url
    size = round(rally.utils.format.bytes_to_mb(track_spec.compressed_size_in_bytes))
    # ensure output appears immediately
    print("Downloading benchmark data from %s (%s MB) ... " % (url, size), end='', flush=True)
    with urllib.request.urlopen(url) as response, open(data_set_path, 'wb') as out_file:
      shutil.copyfileobj(response, out_file)
    print("done")

  def _download_mapping_data(self, mapping_path):
    root_path = self._config.opts("system", "rally.root")
    mapping_source = "%s/resources/datasets/countries/mappings.json" % root_path
    shutil.copyfile(mapping_source, mapping_path)


# TODO dm: Do we need this at all? It is just needed for one time setup -> maybe move this just to another class?
class Track:
  def __init__(self, name, description, track_setups):
    self._name = name
    self._description = description
    self._track_setups = track_setups

  def name(self):
    return self._name

  def description(self):
    return self._description

  def track_setups(self):
    return self._track_setups

  def estimated_disk_usage_in_bytes(self):
    # TODO dm: Implement a best-effort guess and provide a proper info the user at startup (+ startup option to suppress this for CI builds)
    return 0

  def estimated_runtime_in_minutes(self):
    return 60

  # TODO dm: Consider using abc (http://stackoverflow.com/questions/4382945/abstract-methods-in-python)
  def setup(self, config):
    raise NotImplementedError("abstract method")

  # TODO dm: We should provide the track specification in the constructor later!!
  def setup2(self, track_spec):
    data_set_root = "%s%s" % (self._config.opts("benchmarks", "local.dataset.cache"), track_spec.name.lower())
    data_set_path = "%s/%s" % (data_set_root, track_spec.local_file_name)
    if not os.path.isfile(data_set_path):
      self._download_benchmark_data(track_spec, data_set_root, data_set_path)
    # global per benchmark (we never run benchmarks in parallel)
    self._config.add(cfg.Scope.benchmarkScope, "benchmarks", "dataset.path", data_set_path)
    # TODO dm: Also download the mapping file. For now we deliver the mapping file with Rally as a workaround and rely on the convention
    # that the mapping file is called resources/datasets/$track_spec.lower()/mappings.json
    mapping_path = "%s/mappings.json" % data_set_root
    if not os.path.isfile(mapping_path):
      self._download_mapping_data(mapping_path)
    self._config.add(cfg.Scope.benchmarkScope, "benchmarks", "mapping.path", mapping_path)

  def _download_benchmark_data(self, track_spec, data_set_root, data_set_path):
    rally.utils.io.ensure_dir(data_set_root)
    logger.info("Benchmark data for %s not available in '%s'" % (track_spec.name(), data_set_path))
    url = track_spec.source_url()
    size = rally.utils.format.bytes_to_mb(track_spec.compressed_size_in_bytes())
    # ensure output appears immediately
    print("Downloading benchmark data from %s... (%s MB) " % url, size, end='', flush=True)
    with urllib.request.urlopen(url) as response, open(data_set_path, 'wb') as out_file:
      shutil.copyfileobj(response, out_file)
    print("done")

  def _download_mapping_data(self, mapping_path):
    root_path = self._config.opts("system", "rally.root")
    mapping_source = "%s/resources/datasets/countries/mappings.json" % root_path
    shutil.copyfile(mapping_source, )


# Abstract representation for track setups
#
# A track setup defines the concrete operations that will be done and also influences system configuration
class TrackSetup:
  def __init__(self, name, description):
    self._name = name
    self._description = description

  def name(self):
    return self._name

  def description(self):
    return self._description

  def spec(self):
    # FIXME dm: This has to return the associated spec
    return None

  # TODO dm: Consider using abc (http://stackoverflow.com/questions/4382945/abstract-methods-in-python)
  def required_cluster_status(self):
    raise NotImplementedError("abstract method")

  # TODO dm: This is just here to ease the migration, consider gathering metrics for everything later
  def requires_metrics(self):
    raise NotImplementedError("abstract method")

  # TODO dm: rename to configure_cluster? prepare_cluster_configuration?
  def setup(self, config):
    raise NotImplementedError("abstract method")

  def setup_benchmark(self, cluster):
    raise NotImplementedError("abstract method")

  def benchmark_indexing(self, cluster, metrics):
    raise NotImplementedError("abstract method")

  def benchmark_searching(self):
    raise NotImplementedError("abstract method")
