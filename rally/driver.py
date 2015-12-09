import time
import json
import threading
import random
import bz2
import gzip
import os
import logging

import rally.metrics as m
import rally.track.track
import rally.utils.process
import rally.utils.format
import rally.utils.progress

# TODO dm: Remove / encapsulate after porting
# From the original code:
DOCS_IN_BLOCK = 5000
rand = random.Random(17)

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
    self._metrics = m.MetricsCollector(self._config, track_setup.name)
    # TODO dm [High Priority]: This is just here to ease the migration, consider gathering metrics for *all* track setups later
    if track_setup.name == 'defaults':
      self._metrics.start_collection(cluster)
    self._index_benchmark = IndexBenchmark(self._config, track, track_setup, cluster, self._metrics)
    if track_setup.test_settings.benchmark_indexing:
      self._index_benchmark.run()
    # TODO dm: *Might* be interesting to gather metrics also for searching (esp. memory consumption) -> later
    if track_setup.test_settings.benchmark_search:
      search_benchmark = SearchBenchmark(self._config, track, track_setup, cluster, self._metrics)
      search_benchmark.run()
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
    print("")
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

    docs_to_index = track.number_of_documents
    data_set_path = self._config.opts("benchmarks", "dataset.path")

    conflicts = track_setup.test_settings.id_conflicts
    if conflicts is not rally.track.track.IndexIdConflict.NoConflicts:
      ids = self.buildIDs(conflicts)
    else:
      ids = None

    logger.info('Use %d docs per bulk request' % DOCS_IN_BLOCK)
    self._bulk_docs = BulkDocs(track.index_name,
                               track.type_name,
                               data_set_path,
                               ids,
                               docs_to_index,
                               rand,
                               DOCS_IN_BLOCK)

  def buildIDs(self, conflicts):
    docs_to_index = self._track.number_of_documents
    logger.info('build ids with id conflicts of type %s' % conflicts)

    all_ids = [0] * docs_to_index
    for i in range(docs_to_index):
      if conflicts == rally.track.track.IndexIdConflict.SequentialConflicts:
        all_ids[i] = '%10d' % i
      elif conflicts == rally.track.track.IndexIdConflict.RandomConflicts:
        all_ids[i] = '%10d' % rand.randint(0, docs_to_index)
      else:
        raise RuntimeError('Unknown id conflict type %s' % conflicts)
    return all_ids

  def run(self):
    docs_to_index = self._track.number_of_documents
    data_set_path = self._config.opts("benchmarks", "dataset.path")

    # We cannot know how many docs have been updated if we produce id conflicts
    if self._track_setup.test_settings.id_conflicts == rally.track.track.IndexIdConflict.NoConflicts:
      expectedDocCount = docs_to_index
    else:
      expectedDocCount = None

    finished = False

    try:
      # TODO dm: Reduce number of parameters...
      self.indexAllDocs(data_set_path, self._track.index_name, self._cluster.client(), self._bulk_docs, expectedDocCount)
      # just for ending the progress output
      print("")
      finished = True

    finally:
      # HINT dm: Not reporting relevant
      self.print_metrics('Finished?: %s' % finished)
      self._bulk_docs.close()

  def indexBulkDocs(self, es, startingGun, myID, bulkDocs, failedEvent, stopEvent, pauseSec=None):

    """
    Runs one (client) bulk index thread.
    """

    startingGun.await()

    while not stopEvent.isSet():
      # t0 = time.time()
      buffer = bulkDocs.nextNDocs()
      if len(buffer) == 0:
        break
      # t1 = time.time()
      # self.print_metrics('IndexerThread%d: get took %.1f msec' % (myID, 1000*(t1-t0)))

      count = int(len(buffer) / 2)
      data = '\n'.join(buffer)
      del buffer[:]

      try:
        result = es.bulk(body=data, params={'request_timeout': 60000})
      except BaseException as e:
        logger.error("Indexing failed: %s" % e)
        failedEvent.set()
        stopEvent.set()
        raise
      else:
        if result['errors'] != False or len(result['items']) != count:
          self.print_metrics('bulk failed (count=%s):' % count)
          self.print_metrics('%s' % json.dumps(result, sort_keys=True,
                                               indent=4, separators=(',', ': ')))
          failedEvent.set()
          stopEvent.set()
          raise RuntimeError('bulk failed')

      # t2 = time.time()
      # logger.info('IndexerThread%d: index took %.1f msec' % (myID, 1000*(t2-t1)))
      self.printStatus(count, len(data))
      if pauseSec is not None:
        time.sleep(pauseSec)

  def indexAllDocs(self, docsFile, indexName, es, bulkDocs, expectedDocCount, doFlush=True, doStats=True):
    numClientThreads = int(self._config.opts("benchmarks", "index.client.threads"))
    starting_gun = CountDownLatch(1)
    self.print_metrics('json docs file: %s' % docsFile)

    stopEvent = threading.Event()
    failedEvent = threading.Event()

    try:
      # Launch all threads
      self.print_metrics('Launching %d client bulk indexing threads' % numClientThreads)
      threads = []
      for i in range(numClientThreads):
        t = threading.Thread(target=self.indexBulkDocs, args=(es, starting_gun, i, bulkDocs, failedEvent, stopEvent))
        t.setDaemon(True)
        t.start()
        threads.append(t)

      # Tell all threads to start:
      starting_gun.countDown()
      self.startTime = time.time()

      for t in threads:
        t.join()

    except KeyboardInterrupt:
      self.print_metrics('\nSIGINT: now stop')
      stopEvent.set()
      for t in threads:
        t.join()

    if failedEvent.isSet():
      raise RuntimeError('some indexing threads failed')

    end_time = time.time()
    # HINT dm: Reporting relevant
    self.print_metrics('Total docs/sec: %.1f' % (bulkDocs.indexedDocCount / (end_time - self.startTime)))

    self.printStatus(0, 0)

    if doFlush:
      self.print_metrics('now force flush')
      es.indices.flush(index=indexName, params={'request_timeout': 600})

    if doStats:
      self.print_metrics('get stats')
      t0 = time.time()
      stats = es.indices.stats(index=indexName, metric='_all', level='shards')
      t1 = time.time()
      # HINT dm: Reporting relevant
      self.print_metrics('Indices stats took %.3f msec' % (1000 * (t1 - t0)))
      self.print_metrics('INDICES STATS: %s' % json.dumps(stats, sort_keys=True,
                                                          indent=4, separators=(',', ': ')))

      actualDocCount = stats['_all']['primaries']['docs']['count']

      t0 = time.time()
      stats = es.nodes.stats(metric='_all', level='shards')
      t1 = time.time()
      # HINT dm: Reporting relevant
      self.print_metrics('Node stats took %.3f msec' % (1000 * (t1 - t0)))
      self.print_metrics('NODES STATS: %s' % json.dumps(stats, sort_keys=True,
                                                        indent=4, separators=(',', ': ')))
      t0 = time.time()
      stats = es.indices.segments(params={'verbose': True})
      t1 = time.time()
      self.print_metrics('Segments stats took %.3f msec' % (1000 * (t1 - t0)))
      self.print_metrics('SEGMENTS STATS: %s' % json.dumps(stats, sort_keys=True,
                                                           indent=4, separators=(',', ': ')))

      # TODO dm: Enable debug mode later on
      # if DEBUG == False and expectedDocCount is not None and expectedDocCount != actualDocCount:
      #  raise RuntimeError('wrong number of documents indexed: expected %s but got %s' % (expectedDocCount, actualDocCount))

  # TODO dm: This is just a workaround to get us started. Metrics gathering must move to metrics.py. It is also somewhat brutal to treat
  #         everything as metrics (which is not true -> but later...)
  def print_metrics(self, message):
    self._metrics.collect(message)

  def printStatus(self, incDocs, incBytes):
    # TODO dm: Move (somehow) to metrics collector
    # FIXME dm: Well, the method name says it all
    with self._metrics.expose_print_lock_dirty_hack_remove_me_asap():
      self._numDocsIndexed += incDocs
      self._totBytesIndexed += incBytes
      if self._numDocsIndexed >= self._nextPrint or incDocs == 0:
        t = time.time()
        docs_per_second = self._numDocsIndexed / (t - self.startTime)
        mb_per_second = rally.utils.format.bytes_to_mb(self._totBytesIndexed) / (t - self.startTime)
        self._progress.print(
          "  Benchmarking indexing at %.1f docs/s, %.1f MB/sec" % (docs_per_second, mb_per_second),
          # "docs: %d / %d [%3d%%]" % (self._numDocsIndexed, self._track.number_of_documents, round(100 * self._numDocsIndexed / self._track.number_of_documents))
          "[%3d%% done]" % round(100 * self._numDocsIndexed / self._track.number_of_documents)
        )
        logger.info(
          'Indexer: %d docs: %.2f sec [%.1f dps, %.1f MB/sec]' % (self._numDocsIndexed, t - self.startTime, docs_per_second, mb_per_second))
        self._nextPrint += 10000

  def printIndexStats(self, dataDir):
    indexSizeKB = os.popen('du -s %s' % dataDir).readline().split()[0]
    # HINT dm: Reporting relevant
    self.print_metrics('index size %s KB' % indexSizeKB)
    # TODO dm: The output of this should probably be logged (remove from metrics)
    self.print_metrics('index files:')
    # TODO dm: The output of this should probably be logged (not necessary in metrics)
    rally.utils.process.run_subprocess('find %s -ls' % dataDir)


class CountDownLatch:
  def __init__(self, count=1):
    self.count = count
    self.lock = threading.Condition()

  def countDown(self):
    with self.lock:
      self.count -= 1

      if self.count <= 0:
        self.lock.notifyAll()

  def await(self):
    with self.lock:
      while self.count > 0:
        self.lock.wait()


class BulkDocs:
  """
  Pulls docs out of a one-json-line-per-doc file and makes bulk requests.
  """

  def __init__(self, indexName, typeName, docsFile, ids, docsToIndex, rand, docsInBlock):
    if docsFile.endswith('.bz2'):
      self.f = bz2.BZ2File(docsFile)
    elif docsFile.endswith('.gz'):
      self.f = gzip.open(docsFile)
    else:
      self.f = open(docsFile, 'rb')
    self.indexName = indexName
    self.typeName = typeName
    self.blockCount = 0
    self.idUpto = 0
    self.ids = ids
    self.fileLock = threading.Lock()
    self.docsToIndex = docsToIndex
    self.rand = rand
    self.docsInBlock = docsInBlock
    self.indexedDocCount = 0

  def close(self):
    if self.f is None:
      self.f.close()
      self.f = None

  def nextNDocs(self):

    """
    Returns lines for one bulk request.
    """

    buffer = []

    with self.fileLock:
      docsLeft = self.docsToIndex - (self.blockCount * self.docsInBlock)
      if self.f is None or docsLeft <= 0:
        return []

      self.blockCount += 1
      limit = 2 * min(self.docsInBlock, docsLeft)

      while True:
        line = self.f.readline()
        if len(line) == 0:
          self.close()
          break
        line = line.decode('utf-8')
        line = line.rstrip()

        if self.ids is not None:
          # 25% of the time we replace a doc:
          if self.idUpto > 0 and self.rand.randint(0, 3) == 3:
            id = self.ids[self.rand.randint(0, self.idUpto - 1)]
          else:
            id = self.ids[self.idUpto]
            self.idUpto += 1
          # TODO: can't we set default index & type in the bulk request, instead of per doc here?
          cmd = '{"index": {"_index": "%s", "_type": "%s", "_id": "%s"}}' % (self.indexName, self.typeName, id)
        else:
          # TODO: can't we set default index & type in the bulk request, instead of per doc here?
          cmd = '{"index": {"_index": "%s", "_type": "%s"}}' % (self.indexName, self.typeName)

        buffer.append(cmd)
        buffer.append(line)

        if len(buffer) >= limit:
          break

    self.indexedDocCount += len(buffer) / 2

    return buffer
