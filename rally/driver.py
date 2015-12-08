import time
import json
import threading
import random
import bz2
import gzip
import os
import logging

import rally.metrics as m
import rally.track as track
import rally.utils.process

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

  def setup(self, cluster, track_setup):
    track_setup.setup_benchmark(cluster)
    cluster.wait_for_status(track_setup.required_cluster_status())

  def setup2(self, cluster, track_setup):
    # TODO dm: Should we encapsulate this in the index benchmark class?
    mapping_path = self._config.opts("benchmarks", "mapping.path")
    mappings = open(mapping_path).read()
    logger.debug('create index w/ mappings')
    logger.debug(mappings)
    # TODO dm: retrieve from cluster
    # FIXME dm: Hardcoded index name!!!
    cluster.client().indices.create(index='countries')
    cluster.client().indices.put_mapping(index='countries',
                                          doc_type='type',
                                          body=json.loads(mappings))
    cluster.wait_for_status(track_setup.required_cluster_status())


  def go(self, cluster, track_setup):
    metrics = m.MetricsCollector(self._config, track_setup.name())
    # TODO dm: This is just here to ease the migration, consider gathering metrics for *all* track setups later
    if track_setup.requires_metrics():
      metrics.start_collection(cluster)
    # TODO dm: I sense this is too concrete for a driver -> abstract this a bit later (should move to trac setupk, they all need a unified interface)
    track_setup.benchmark_indexing(cluster, metrics)
    # TODO dm: *Might* be interesting to gather metrics also for searching (esp. memory consumption) -> later
    track_setup.benchmark_searching(cluster, metrics)
    # TODO dm: Check with Mike if it is ok to include this here (original code shut down the server first)
    # This is also just a hack for now (should be in track for first step and metrics for second one)
    data_paths = self._config.opts("provisioning", "local.data.paths")
    track_setup.printIndexStats(data_paths[0])
    metrics.stop_collection()

  # TODO dm: Implement the new benchmarking approach here
  def go2(self, cluster, track, track_setup):
    metrics = m.MetricsCollector(self._config, track_setup.name())
    # TODO dm: This is just here to ease the migration, consider gathering metrics for *all* track setups later
    if track_setup.requires_metrics():
      metrics.start_collection(cluster)

    # TODO dm: provide track
    index_benchmark = IndexBenchmark(self._config, track, track_setup)
    index_benchmark.run()
    # TODO dm: *Might* be interesting to gather metrics also for searching (esp. memory consumption) -> later
    track_setup.benchmark_searching(cluster, metrics)
    # TODO dm: Check with Mike if it is ok to include this here (original code shut down the server first)
    # This is also just a hack for now (should be in track for first step and metrics for second one)
    data_paths = self._config.opts("provisioning", "local.data.paths")
    track_setup.printIndexStats(data_paths[0])


    metrics.stop_collection()



class TimedOperation:
  def timed(self, target, args):
    start = time.time()
    result = target(args)
    stop = time.time()
    return (stop - start), result


class SearchBenchmark(TimedOperation):
  pass
  # TODO dm: Ensure we properly warmup before running metrics (what about ES internal caches? Ensure we don't do bogus benchmarks!)

  def run(self):
    pass
    # TODO dm: Execute queries timed and provide a proper data structure (e.g. a hash of results)


class IndexBenchmark(TimedOperation):
  def __init__(self, config, track, track_setup):
    self._config = config
    # TODO dm: read docs_to_index probably from the spec itself... this one will not work anyway
    docs_to_index = self._config.opts("benchmarks", "docs.number")
    data_set_path = self._config.opts("benchmarks", "dataset.path")

    conflicts = track_setup.spec().test_settings.id_conflicts
    if conflicts is not track.IndexIdConflict.NoConflicts:
      ids = self.buildIDs(conflicts)
    else:
      ids = None

    logger.info('Use %d docs per bulk request' % DOCS_IN_BLOCK)
    # TODO dm: How do we know which index to use?!?
    self._bulk_docs = BulkDocs('countries',
                               data_set_path,
                               ids,
                               docs_to_index,
                               rand,
                               DOCS_IN_BLOCK)

  def buildIDs(self, conflicts):
    # TODO dm: read docs_to_index probably from the spec itself... this one will not work anyway
    docs_to_index = self._config.opts("benchmarks", "docs.number")
    logger.info('build ids with id conflicts of type %s' % conflicts)

    all_ids = [0] * docs_to_index
    for i in range(docs_to_index):
      if conflicts == track.IndexIdConflict.SequentialConflicts:
        all_ids[i] = '%10d' % i
      elif conflicts == track.IndexIdConflict.RandomConflicts:
        all_ids[i] = '%10d' % rand.randint(0, docs_to_index)
      else:
        raise RuntimeError('Unknown id conflict type %s' % conflicts)
    return all_ids


  def run(self):
    # TODO dm: read docs_to_index probably from the spec itself... this one will not work anyway
    docs_to_index = self._config.opts("benchmark.countries", "docs.number")
    data_set_path = self._config.opts("benchmark.countries", "dataset.path")

    # TODO dm: This is also one of the many workarounds to get us going. Set up metrics properly
    self._metrics = metrics

    # We cannot know how many docs have been updated if we produce id conflicts
    if self._track_setup.spec().test_settings.id_conflicts == track.IndexIdConflict.NoConflicts:
      expectedDocCount = docs_to_index
    else:
      expectedDocCount = None

    finished = False

    try:
      # TODO dm: Reduce number of parameters...
      self.indexAllDocs(data_set_path, 'countries', cluster.client(), self._bulk_docs, expectedDocCount)
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
    global startTime
    numClientThreads = int(self._config.opts("benchmarks.countries", "index.client.threads"))
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
      startTime = time.time()

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
    self.print_metrics('Total docs/sec: %.1f' % (bulkDocs.indexedDocCount / (end_time - startTime)))

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
        # FIXME dm: Don't use print_metrics here. Not needed for metrics output, we already hold the print lock and it seems to be non-reentrant
        logger.info('Indexer: %d docs: %.2f sec [%.1f dps, %.1f MB/sec]' % (
          self._numDocsIndexed, t - startTime, self._numDocsIndexed / (t - startTime),
          (self._totBytesIndexed / 1024 / 1024.) / (t - startTime)))
        self._nextPrint += 10000

  def printIndexStats(self, dataDir):
    indexSizeKB = os.popen('du -s %s' % dataDir).readline().split()[0]
    # HINT dm: Reporting relevant
    self.print_metrics('index size %s KB' % indexSizeKB)
    # TODO dm: The output of this should probably be logged (remove from metrics)
    self.print_metrics('index files:')
    # TODO dm: The output of this should probably be logged (not necessary in metrics)
    rally.utils.process.run_subprocess('find %s -ls' % dataDir)



# TODO dm: Move to a more appropriate place
class CountDownLatch(object):
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

  def __init__(self, indexName, docsFile, ids, docsToIndex, rand, docsInBlock):
    if docsFile.endswith('.bz2'):
      self.f = bz2.BZ2File(docsFile)
    elif docsFile.endswith('.gz'):
      self.f = gzip.open(docsFile)
    else:
      self.f = open(docsFile, 'rb')
    self.indexName = indexName
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
          cmd = '{"index": {"_index": "%s", "_type": "type", "_id": "%s"}}' % (self.indexName, id)
        else:
          # TODO: can't we set default index & type in the bulk request, instead of per doc here?
          cmd = '{"index": {"_index": "%s", "_type": "type"}}' % self.indexName

        buffer.append(cmd)
        buffer.append(line)

        if len(buffer) >= limit:
          break

    self.indexedDocCount += len(buffer) / 2

    return buffer
