import os
import logging
import urllib.request
import shutil
from enum import Enum

import rally.config as cfg
import rally.utils.io
import rally.utils.convert
import rally.utils.process

import rally.cluster

logger = logging.getLogger("rally.track")


class Track:
  """
  A track defines the data set that is used. It corresponds loosely to a use case (e.g. logging, event processing, analytics, ...)
  """

  # TODO dm: For now we allow just one index with one type. Change this later
  def __init__(self, name, description, source_url, mapping_url, index_name, type_name, number_of_documents, compressed_size_in_bytes,
               uncompressed_size_in_bytes,
               local_file_name, local_mapping_name, estimated_benchmark_time_in_minutes, track_setups, queries):
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
    self._local_mapping_name = local_mapping_name
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
  def local_mapping_name(self):
    return self._local_mapping_name

  @property
  def estimated_benchmark_time_in_minutes(self):
    return self._estimated_benchmark_time_in_minutes

  @property
  def track_setups(self):
    return self._track_setups

  @property
  def queries(self):
    return self._queries


class TrackSetup:
  """
  A track setup defines the concrete operations that will be done and also influences system configuration
  """

  # TODO dm [Refactoring]: Specifying the required cluster status is just a workaround...
  def __init__(self, name, description, candidate_settings, benchmark_settings, required_cluster_status=rally.cluster.ClusterStatus.yellow):
    self._name = name
    self._description = description
    self._candidate_settings = candidate_settings
    self._test_settings = benchmark_settings
    self._required_cluster_status = required_cluster_status

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


class CandidateSettings:
  def __init__(self, custom_config_snippet=None, nodes=1, processors=1, heap=None, java_opts=None, gc_opts=None):
    self._custom_config_snippet = custom_config_snippet
    self._nodes = nodes
    self._processors = processors
    self._heap = heap
    self._java_opts = java_opts
    self._gc_opts = gc_opts

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

  @property
  def java_opts(self):
    return self._java_opts

  @property
  def gc_opts(self):
    return self._gc_opts


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

  def __str__(self):
    return self._name


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
    # global per benchmark (we never run benchmarks in parallel)
    self._config.add(cfg.Scope.benchmarkScope, "benchmarks", "dataset.path", data_set_path)

    mapping_path = "%s/%s" % (data_set_root, track.local_mapping_name)
    if not os.path.isfile(mapping_path):
      self._download_mapping_data(track, data_set_root, mapping_path)
    self._config.add(cfg.Scope.benchmarkScope, "benchmarks", "mapping.path", mapping_path)

  def _download_benchmark_data(self, track, data_set_root, data_set_path):
    rally.utils.io.ensure_dir(data_set_root)
    logger.info("Benchmark data for %s not available in '%s'" % (track.name, data_set_path))
    url = track.source_url
    size = round(rally.utils.convert.bytes_to_mb(track.compressed_size_in_bytes))
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
    rally.utils.io.ensure_dir(data_set_root)
    logger.info("Mappings for %s not available in '%s'" % (track.name, mapping_path))
    url = track.mapping_url
    # for now, we just allow HTTP downloads for mappings (S3 support is probably remove anyway...)
    if url.startswith("http"):
      self._do_download_via_http(url, mapping_path)
    else:
      raise RuntimeError("Cannot download mappings. No protocol handler for [%s] available." % url)

  def _do_download_via_http(self, url, data_set_path):
    tmp_data_set_path = data_set_path + '.tmp'
    try:
      with urllib.request.urlopen(url) as response, open(tmp_data_set_path, 'wb') as out_file:
        shutil.copyfileobj(response, out_file)
    except:
      logger.info('Removing temp file %s' % tmp_data_set_path)
      os.remove(tmp_data_set_path)
      raise
    else:
      os.rename(tmp_data_set_path, data_set_path)

  def _do_download_via_s3(self, track, url, data_set_path):
    tmp_data_set_path = data_set_path + '.tmp'
    s3cmd = "s3cmd -v get %s %s" % (url, tmp_data_set_path)
    try:
      success = rally.utils.process.run_subprocess(s3cmd)
      # Exit code for s3cmd does not seem to be reliable so we also check the file size although this is rather fragile...
      if not success or os.path.getsize(tmp_data_set_path) != track.compressed_size_in_bytes:
        # cleanup probably corrupt data file...
        if os.path.isfile(tmp_data_set_path):
          os.remove(tmp_data_set_path)
        raise RuntimeError("Could not get benchmark data from S3: '%s'. Is s3cmd installed and set up properly?" % s3cmd)
    except:
      logger.info('Removing temp file %s' % tmp_data_set_path)
      os.remove(tmp_data_set_path)
      raise
    else:
      os.rename(tmp_data_set_path, data_set_path)
