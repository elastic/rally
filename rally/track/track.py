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


class CandidateSettings:
    def __init__(self, custom_config_snippet=None, nodes=1, processors=1, heap=None, java_opts=None, gc_opts=None):
        self.custom_config_snippet = custom_config_snippet
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
                 benchmark_settings=BenchmarkSettings(),
                 # TODO dm [Refactoring]: Specifying the required cluster status is just a workaround...
                 required_cluster_status=rally.cluster.ClusterStatus.yellow):
        self.name = name
        self.description = description
        self.candidate_settings = candidate_settings
        self.test_settings = benchmark_settings
        self.required_cluster_status = required_cluster_status


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
        self._config.add(cfg.Scope.benchmark, "benchmarks", "dataset.path", unzipped_data_set_path)

        mapping_path = "%s/%s" % (data_set_root, track.local_mapping_name)
        if not os.path.isfile(mapping_path):
            self._download_mapping_data(track, data_set_root, mapping_path)
        self._config.add(cfg.Scope.benchmark, "benchmarks", "mapping.path", mapping_path)

    def _unzip(self, data_set_path):
        # we assume that track data are always compressed and try to unzip them before running the benchmark
        basename, extension = rally.utils.io.splitext(data_set_path)
        if not os.path.isfile(basename):
            logger.info("Unzipping track data from [%s] to [%s]." % (data_set_path, basename))
            rally.utils.io.unzip(data_set_path, rally.utils.io.dirname(data_set_path))
        return basename

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
            success = rally.utils.process.run_subprocess_with_logging(s3cmd)
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
