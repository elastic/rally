import time
import json
import random
import bz2
import gzip
import os
import logging

from elasticsearch.helpers import parallel_bulk, streaming_bulk

import rally.metrics as m
import rally.track.track
import rally.utils.process
import rally.utils.convert
import rally.utils.progress

logger = logging.getLogger("rally.driver")


class Driver:
  """
  Driver runs the benchmark.
  """

  def __init__(self, config):
    self._config = config
    self._metrics = None
    self._index_benchmark = None

  def setup(self, cluster, track, track_setup):
    # TODO dm: Should we encapsulate this in the index benchmark class?
    mapping_path = self._config.opts("benchmarks", "mapping.path")
    mappings = open(mapping_path).read()
    logger.debug('create index w/ mappings')
    logger.debug(mappings)
    # TODO dm: retrieve from cluster
    cluster.client().indices.create(index=track.index_name)
    cluster.client().indices.put_mapping(index=track.index_name,
                                         doc_type=track.type_name,
                                         body=json.loads(mappings))
    cluster.wait_for_status(track_setup.required_cluster_status)

  def go(self, cluster, track, track_setup):
    self._metrics = m.MetricsCollector(self._config)
    # TODO dm [High Priority]: This is just here to ease the migration, consider gathering metrics for *all* track setups later
    if track_setup.name == 'defaults':
      self._metrics.start_collection(cluster)
    cluster.on_benchmark_start()
    self._index_benchmark = IndexBenchmark(self._config, track, track_setup, cluster, self._metrics)
    if track_setup.test_settings.benchmark_indexing:
      self._index_benchmark.run()
    # TODO dm: *Might* be interesting to gather metrics also for searching (esp. memory consumption) -> later
    if track_setup.test_settings.benchmark_search:
      search_benchmark = SearchBenchmark(self._config, track, track_setup, cluster, self._metrics)
      search_benchmark.run()
    cluster.on_benchmark_stop()
    self._metrics.collect_total_stats()

  def tear_down(self, track, track_setup):
    # This is also just a hack for now (should be in track for first step and metrics for second one)
    data_paths = self._config.opts("provisioning", "local.data.paths")
    if track_setup.test_settings.benchmark_indexing:
      self._index_benchmark.printIndexStats(data_paths[0])
    self._metrics.stop_collection()


class TimedOperation:
  def timed(self, target, args, repeat=1):
    start = time.time()
    for i in range(repeat):
      result = target(args)
    stop = time.time()
    return (stop - start) / repeat, result


class SearchBenchmark(TimedOperation):
  def __init__(self, config, track, track_setup, cluster, metrics):
    self._config = config
    self._track = track
    self._track_setup = track_setup
    self._cluster = cluster
    self._metrics = metrics
    self._progress = rally.utils.progress.CmdLineProgressReporter()

  # TODO dm: This is just a workaround to get us started. Metrics gathering must move to metrics.py. It is also somewhat brutal to treat
  #         everything as metrics (which is not true -> but later...)
  def print_metrics(self, message):
    self._metrics.collect(message)

  # TODO dm: Ensure we properly warmup before running metrics (what about ES internal caches? Ensure we don't do bogus benchmarks!)
  def run(self):
    es = self._cluster.client()
    logger.info("Running search benchmark")
    # Run a few (untimed) warmup iterations before the actual benchmark
    self._run_benchmark(es, "  Benchmarking search (warmup iteration %d/%d)")
    times = self._run_benchmark(es, "  Benchmarking search (iteration %d/%d)")

    for q in self._track.queries:
      l = [x[q.name] for x in times]
      l.sort()
      # TODO dm: (Conceptual) We are measuring a latency here. -> Provide percentiles (later)
      # HINT dm: Reporting relevant
      self.print_metrics('SEARCH %s (median): %.6f sec' % (q.name, l[int(len(l) / 2)]))

  def _run_benchmark(self, es, message, repetitions=10):
    times = []
    for i in range(repetitions):
      self._progress.print(
        message % ((i + 1), repetitions),
        "[%3d%% done]" % (round(100 * (i + 1) / repetitions))
      )
      times.append(self._run_one_round(es))
    self._progress.finish()
    return times

  def _run_one_round(self, es):
    d = {}
    for query in self._track.queries:
      # repeat multiple times within one run to guard against timer resolution problems
      duration, result = self.timed(query.run, es, repeat=10)
      d[query.name] = duration / query.normalization_factor
    return d


class IndexBenchmark(TimedOperation):
  def __init__(self, config, track, track_setup, cluster, metrics):
    self._config = config
    self._track = track
    self._track_setup = track_setup
    self._cluster = cluster
    self._metrics = metrics
    # TODO dm: Just needed for print output - can we simplify this?
    self._nextPrint = 0
    self._numDocsIndexed = 0
    self._totBytesIndexed = 0
    self._progress = rally.utils.progress.CmdLineProgressReporter()
    self._rand = random.Random(17)
    self._bulk_size = 5000
    logger.info('Use %d docs per bulk request' % self._bulk_size)

  def run(self):
    docs_to_index = self._track.number_of_documents
    data_set_path = self._config.opts("benchmarks", "dataset.path")

    # We cannot know how many docs have been updated if we produce id conflicts
    if self._track_setup.test_settings.id_conflicts == rally.track.track.IndexIdConflict.NoConflicts:
      expected_doc_count = docs_to_index
    else:
      expected_doc_count = None

    finished = False

    try:
      self.index(data_set_path, expected_doc_count)
      finished = True
    finally:
      self._progress.finish()
      logger.info('IndexBenchmark finished successfully: %s' % finished)

  def generate_ids(self, conflicts):
    docs_to_index = self._track.number_of_documents
    logger.info('build ids with id conflicts of type %s' % conflicts)

    if conflicts == rally.track.track.IndexIdConflict.SequentialConflicts:
      yield from (
        '%10d' % (
          self._rand.randint(0, i)
          # pick already returned id in 25% of cases
          if i > 0 and self._rand.randint(0, 3) == 3
          else i
        ) for i in range(docs_to_index)
      )
    elif conflicts == rally.track.track.IndexIdConflict.RandomConflicts:
      ids = []
      for _ in range(docs_to_index):
        if ids and self._rand.randint(0, 3) == 3:
          # pick already returned id in 25%
          id = self._rand.choice(ids)
          #FIXME dm: Shouldn't we put the conflicting id also into this set?
        else:
          id = '%10i' % self._rand.randint(0, docs_to_index)
          ids.append(id)
      yield id
    else:
      raise RuntimeError('Unknown id conflict type %s' % conflicts)

  def get_expand_action(self):
    conflicts = self._track_setup.test_settings.id_conflicts
    if conflicts is not rally.track.track.IndexIdConflict.NoConflicts:
      id_generator = self.generate_ids(conflicts)
    else:
      id_generator = None

    def expand_action(data):
      if id_generator:
        action = '{"index": {"_id": %d}}' % next(id_generator)
      else:
        action = '{"index": {}}'
      self._sent_bytes += len(data)
      return action, data.strip()

    return expand_action

  def _open_file(self, docsFile):
    if docsFile.endswith('.bz2'):
      return bz2.open(docsFile, 'rt')
    elif docsFile.endswith('.gz'):
      return gzip.open(docsFile, 'rt')
    else:
      return open(docsFile, 'rt')

  def _read_records(self, documents):
    with self._open_file(documents) as f:
      yield from f

  def index(self, docsFile, expected_doc_count, doFlush=True, doStats=True):
    numClientThreads = int(self._config.opts("benchmarks", "index.client.threads"))
    logger.info('Indexing JSON docs file: [%s]' % docsFile)
    logger.info('Launching %d client bulk indexing threads' % numClientThreads)

    indexName = self._track.index_name
    typeName = self._track.type_name
    es = self._cluster.client()

    self.startTime = time.time()
    self._sent_bytes = 0

    processed = 0
    self._print_progress(processed)
    try:
      for _ in parallel_bulk(es,
                             self._read_records(docsFile),
                             thread_count=numClientThreads,
                             index=indexName,
                             doc_type=typeName,
                             chunk_size=self._bulk_size,
                             expand_action_callback=self.get_expand_action(),
                             ):
        if processed % 10000 == 0:
          self._print_progress(processed)
        processed += 1
    except KeyboardInterrupt:
      logger.info('Received SIGINT: IndexBenchmark will be stopped prematurely.')

    self._print_progress(processed)

    end_time = time.time()
    # HINT dm: Reporting relevant
    self.print_metrics('Total docs/sec: %.1f' % (processed / (end_time - self.startTime)))

    if doFlush:
      logger.info("Force flushing index [%s]." % indexName)
      es.indices.flush(index=indexName, params={'request_timeout': 600})
      logger.info("Force flush has finished successfully.")

    if doStats:
      logger.info("Gathering indices stats")
      # TODO dm: This should be a profiler
      t0 = time.time()
      stats = es.indices.stats(index=indexName, metric='_all', level='shards')
      t1 = time.time()
      # HINT dm: Reporting relevant
      self.print_metrics('Indices stats took %.3f msec' % (1000 * (t1 - t0)))
      self.print_metrics('INDICES STATS: %s' % json.dumps(stats, sort_keys=True,
                                                          indent=4, separators=(',', ': ')))
      actual_doc_count = stats['_all']['primaries']['docs']['count']

      logger.info("Gathering nodes stats")
      t0 = time.time()
      stats = es.nodes.stats(metric='_all', level='shards')
      t1 = time.time()
      # HINT dm: Reporting relevant
      self.print_metrics('Node stats took %.3f msec' % (1000 * (t1 - t0)))
      self.print_metrics('NODES STATS: %s' % json.dumps(stats, sort_keys=True,
                                                        indent=4, separators=(',', ': ')))

      logger.info("Gathering segments stats")
      t0 = time.time()
      stats = es.indices.segments(params={'verbose': True})
      t1 = time.time()
      self.print_metrics('Segments stats took %.3f msec' % (1000 * (t1 - t0)))
      self.print_metrics('SEGMENTS STATS: %s' % json.dumps(stats, sort_keys=True,
                                                           indent=4, separators=(',', ': ')))
      if expected_doc_count is not None and expected_doc_count != actual_doc_count:
          msg = "wrong number of documents indexed: expected %s but got %s" % (expected_doc_count, actual_doc_count)
          logger.error(msg)
          raise RuntimeError(msg)

  # TODO dm: This is just a workaround to get us started. Metrics gathering must move to metrics.py. It is also somewhat brutal to treat
  #         everything as metrics (which is not true -> but later...)
  def print_metrics(self, message):
    self._metrics.collect(message)

  def _print_progress(self, docs_processed):
    # TODO dm: Think about silencing this on "non" dev machines, it introduces another unnecessary pause and I/O
    # stack-confine asap to keep the reporting error low (this number may be updated by other threads), we don't need to be entirely
    # accurate here as this is "just" a progress report for the user
    sent = self._sent_bytes
    elapsed = time.time() - self.startTime
    docs_per_second = docs_processed / elapsed
    mb_per_second = rally.utils.convert.bytes_to_mb(sent) / elapsed

    self._progress.print(
      "  Benchmarking indexing at %.1f docs/s, %.1f MB/sec" % (docs_per_second, mb_per_second),
      "[%3d%% done]" % round(100 * docs_processed / self._track.number_of_documents)
    )
    logger.info('Indexer: %d docs: %.2f sec [%.1f dps, %.1f MB/sec]' % (docs_processed, elapsed, docs_per_second, mb_per_second))

  # TODO dm: This should probably be a profiler
  def printIndexStats(self, dataDir):
    indexSizeKB = os.popen('du -s %s' % dataDir).readline().split()[0]
    # HINT dm: Reporting relevant
    self.print_metrics('index size %s KB' % indexSizeKB)
    rally.utils.process.run_subprocess_with_logging("find %s -ls" % dataDir, header="index files:")
