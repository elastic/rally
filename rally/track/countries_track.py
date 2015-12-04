import time
import json
import threading
import os
import random
import bz2
import gzip
import logging

import rally.utils.sysstats as sysstats
import rally.utils.process

import rally.cluster
import rally.track.track as track
import rally.config as cfg

logger = logging.getLogger("rally.track.countries")

# TODO dm: Remove / encapsulate after porting
# From the original code:
DOCS_IN_BLOCK = 5000
# VERBOSE_IW = False
DO_IDS = False
ID_TYPE = 'random'
rand = random.Random(17)


# TODO dm: This is a *complete* copy of the logging benchmark (which is by itself already rather ugly -> this code is just there to get us started quickly and will change *completely* in the coming weeks
# Concrete benchmarking scenario for countries
class CountriesTrack(track.Track):
  def __init__(self, name, track_setups):
    track.Track.__init__(self, name, track_setups)
    self._config = None

  def setup(self, config):
    self._config = config
    # Download necessary data etc.
    self._config.add(cfg.Scope.benchmarkScope, "benchmark.countries", "docs.number", 8647880)
    #data_set_path = "%s/%s" % (self._config.opts("benchmarks", "local.dataset.cache"), "documents.json.bz2")
    data_set_path = "%s/%s" % (self._config.opts("benchmarks", "local.dataset.cache"), "documents-2k.json.bz2")
    if not os.path.isfile(data_set_path):
      self._download_benchmark_data(data_set_path)
    self._config.add(cfg.Scope.benchmarkScope, "benchmark.countries", "dataset.path", data_set_path)

  def _download_benchmark_data(self, data_set_path):
    logger.info("Benchmark data for %s not available in '%s'" % (self.name(), data_set_path))
    # A 2 GB download justifies user feedback ...
    print("Could  not find benchmark data. Trying to download (around 200 MB) ...")
    # TODO dm: Download me!!!
    s3cmd = "http://benchmarks.elastic.co/corpora/geonames/documents.json.bz2 %s" % data_set_path
    success = rally.utils.process.run_subprocess(s3cmd)
    # Exit code for s3cmd does not seem to be reliable so we also check the file size although this is rather fragile...
    if not success or os.path.getsize(data_set_path) != 1843865288:
      # cleanup probably corrupt data file...
      if os.path.isfile(data_set_path):
        os.remove(data_set_path)
      raise RuntimeError("Could not get benchmark data from S3: '%s'. Is s3cmd installed and set up properly?" % s3cmd)



class CountriesTrackSetup(track.TrackSetup):
  def __init__(self, name, elasticsearch_settings=None, build_ids=False, nodes=1, processors=1, heap=None, requires_metrics=False):
    track.TrackSetup.__init__(self, name)
    self._elasticsearch_settings = elasticsearch_settings
    self._build_ids = build_ids
    self._nodes = nodes
    self._processors = processors
    self._heap = heap
    self._config = None
    self._requires_metrics = requires_metrics
    self._bulk_docs = None
    # TODO dm: Were previously globals - can we reduce their scope even further?
    self._nextPrint = 0
    self._numDocsIndexed = 0
    self._totBytesIndexed = 0
    self._metrics = None

  def setup(self, config):
    self._config = config
    # Provide runtime configuration
    if self._heap:
      self._config.add(cfg.Scope.trackSetupScope, "provisioning", "es.heap", self._heap)
    if self._elasticsearch_settings:
      self._config.add(cfg.Scope.trackSetupScope, "provisioning", "es.config", self._elasticsearch_settings)

    self._config.add(cfg.Scope.trackSetupScope, "provisioning", "es.processors", self._processors)
    self._config.add(cfg.Scope.trackSetupScope, "provisioning", "es.nodes", self._nodes)

  # Set up required for running the benchmark
  def setup_benchmark(self, cluster):
    root_path = self._config.opts("system", "rally.root")
    mappings = open("%s/resources/datasets/countries/mappings.json" % root_path).read()

    docs_to_index = self._config.opts("benchmark.countries", "docs.number")
    data_set_path = self._config.opts("benchmark.countries", "dataset.path")

    if DO_IDS:
      ids = self.buildIDs()
    else:
      ids = None

    logger.info('Use %d docs per bulk request' % DOCS_IN_BLOCK)
    # TODO dm: extract more constants to benchmark settings
    self._bulk_docs = BulkDocs('countries',
                               data_set_path,
                               ids,
                               docs_to_index,
                               rand,
                               DOCS_IN_BLOCK)

    logger.debug('create index w/ mappings')
    logger.debug(mappings)
    # TODO dm: retrieve from cluster
    cluster.client().indices.create(index='countries')
    cluster.client().indices.put_mapping(index='countries',
                                         doc_type='type',
                                         body=json.loads(mappings))

  def requires_metrics(self):
    return self._requires_metrics

  def required_cluster_status(self):
    #FIXME dm: This depends also on the replica count so we better have a way to derive that, for now we assume that we need green
    # when we have special settings
    if self._nodes == 1 and self._elasticsearch_settings is None:
      return rally.cluster.ClusterStatus.yellow
    else:
      return rally.cluster.ClusterStatus.green

  def benchmark_indexing(self, cluster, metrics):
    docs_to_index = self._config.opts("benchmark.countries", "docs.number")
    data_set_path = self._config.opts("benchmark.countries", "dataset.path")

    #TODO dm: This is also one of the many workarounds to get us going. Set up metrics properly
    self._metrics = metrics

    # TODO dm: Check properly -> flag!
    if self._name != 'fastupdates':
      expectedDocCount = docs_to_index
    else:
      expectedDocCount = None

    finished = False

    try:
      # TODO dm: Reduce number of parameters...
      self.indexAllDocs(data_set_path, 'countries', cluster.client(), self._bulk_docs, expectedDocCount)
      finished = True

    finally:
      #HINT dm: Not reporting relevant
      self.print_metrics('Finished?: %s' % finished)
      self._bulk_docs.close()

  def benchmark_searching(self, cluster, metrics):
    #TODO dm: This is also one of the many workarounds to get us going. Set up metrics properly
    self._metrics = metrics
    # TODO dm: (a) configure this properly (use a flag, not a name check), (b) check with Mike if we want to perform search tests in all configurations
    if self._name == 'defaults':
      self.doBasicSearchTests(cluster.client())

  # TODO dm: This is a 1:1 copy of the original code, adapt me later
  def buildIDs(self):
    if not self._build_ids:
      return

    docs_to_index = self._config.opts("benchmark.countries", "docs.number")

    self.print_metrics('build IDs: %s' % ID_TYPE)

    allIDs = [0] * docs_to_index

    for i in range(docs_to_index):
      if ID_TYPE == 'sequential':
        allIDs[i] = '%10d' % i
      elif ID_TYPE == 'random':
        allIDs[i] = '%10d' % rand.randint(0, docs_to_index)
      else:
        raise RuntimeError('unknown ID_TYPE %s' % ID_TYPE)
    self.print_metrics('  done')
    return allIDs

  # TODO dm: (Conceptual) Introduce warmup iterations!!
  def doBasicSearchTests(self, es):
    # NOTE: there are real problems here, e.g. we only suddenly do searching after
    # indexing is done (so hotspot will be baffled), merges are likely still running
    # at this point, etc. ... it's a start:
    self.print_metrics('\nRun simple search tests...')
    times = []
    for i in range(120):
      time.sleep(0.5)
      d = {}

      t0 = time.time()
      resp = es.search(index='countries')
      t1 = time.time()
      d['default'] = t1 - t0

      t0 = time.time()
      # FIXME dm: Obviously we should search for something else here
      resp = es.search(index='countries', doc_type='type', q='message:en')
      t1 = time.time()
      d['term'] = t1 - t0

      t0 = time.time()
      resp = es.search(index='countries', doc_type='type', q='"ab_international.languages.ja+off"')
      t1 = time.time()
      d['phrase'] = t1 - t0

      t0 = time.time()
      resp = es.search(index='countries', doc_type='type', body='''
  {
      "size": 0,
      "aggregations": {
    "by_hour": {
        "date_histogram": {
      "field": "@timestamp",
      "interval": "hour"
        }
    }
      }
  }''')

      t1 = time.time()
      d['hourly_agg'] = t1 - t0

      # Scroll, 1K docs at a time, 25 times:
      t0 = time.time()
      r = es.search(index='countries', doc_type='type', sort='_doc', scroll='10m', size=1000)
      count = 1
      for i in range(24):
        numHits = len(r['hits']['hits'])
        if numHits == 0:
          # done
          break
        r = es.scroll(scroll_id=r['_scroll_id'], scroll='10m')
        count += 1
      t1 = time.time()
      d['scroll_all'] = (t1 - t0) / count

      if False and i == 0:
        self.print_metrics('SEARCH:\n%s' % json.dumps(resp, sort_keys=True,
                                         indent=4, separators=(',', ': ')))
      self.print_metrics('%.2f msec' % (1000*(t1-t0)))
      times.append(d)

    for q in ('default', 'term', 'phrase', 'hourly_agg', 'scroll_all'):
      l = [x[q] for x in times]
      l.sort()
      # TODO dm: (Conceptual) We are measuring a latency here. -> Provide percentiles (later)
      #HINT dm: Reporting relevant
      self.print_metrics('SEARCH %s (median): %.6f sec' % (q, l[int(len(l) / 2)]))

  def indexBulkDocs(self, es, startingGun, myID, bulkDocs, failedEvent, stopEvent, pauseSec=None):

    """
    Runs one (client) bulk index thread.
    """

    startingGun.await()

    while not stopEvent.isSet():
      #t0 = time.time()
      buffer = bulkDocs.nextNDocs()
      if len(buffer) == 0:
        break
      #t1 = time.time()
      #self.print_metrics('IndexerThread%d: get took %.1f msec' % (myID, 1000*(t1-t0)))

      count = int(len(buffer)/2)
      data = '\n'.join(buffer)
      del buffer[:]

      try:
        result = es.bulk(body=data, params={'request_timeout': 60000})
      except:
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

      #t2 = time.time()
      #logger.info('IndexerThread%d: index took %.1f msec' % (myID, 1000*(t2-t1)))
      self.printStatus(count, len(data))
      if pauseSec is not None:
        time.sleep(pauseSec)

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
          self._numDocsIndexed, t - startTime, self._numDocsIndexed / (t - startTime), (self._totBytesIndexed / 1024 / 1024.) / (t - startTime)))
        self._nextPrint += 10000

  def printIndexStats(self, dataDir):
    indexSizeKB = os.popen('du -s %s' % dataDir).readline().split()[0]
    #HINT dm: Reporting relevant
    self.print_metrics('index size %s KB' % indexSizeKB)
    # TODO dm: The output of this should probably be logged (remove from metrics)
    self.print_metrics('index files:')
    # TODO dm: The output of this should probably be logged (not necessary in metrics)
    rally.utils.process.run_subprocess('find %s -ls' % dataDir)

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
    #HINT dm: Reporting relevant
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
      #HINT dm: Reporting relevant
      self.print_metrics('Indices stats took %.3f msec' % (1000 * (t1 - t0)))
      self.print_metrics('INDICES STATS: %s' % json.dumps(stats, sort_keys=True,
                                             indent=4, separators=(',', ': ')))

      #TODO dm: Unused?
      actualDocCount = stats['_all']['primaries']['docs']['count']

      t0 = time.time()
      stats = es.nodes.stats(metric='_all', level='shards')
      t1 = time.time()
      #HINT dm: Reporting relevant
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

  #TODO dm: This is just a workaround to get us started. Metrics gathering must move to metrics.py. It is also somewhat brutal to treat
  #         everything as metrics (which is not true -> but later...)
  def print_metrics(self, message):
    self._metrics.collect(message)


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

      # TODO dm: Maybe reenable debugging later
      # if DEBUG and self.blockCount >= 50:
      #  return []

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

    self.indexedDocCount += len(buffer)/2

    return buffer


countriesBenchmarkFastSettings = '''
index.refresh_interval: 30s

index.number_of_shards: 6
index.number_of_replicas: 0

index.translog.flush_threshold_size: 4g
index.translog.flush_threshold_ops: 500000
'''

countriesTrack = CountriesTrack("Countries", [
  # TODO dm: Be very wary of the order here!!! reporter.py assumes this order - see similar comment there
  CountriesTrackSetup("defaults", requires_metrics=True),
  CountriesTrackSetup("4gheap", heap='4g'),
  CountriesTrackSetup("fastsettings", elasticsearch_settings=countriesBenchmarkFastSettings, heap='4g'),
  CountriesTrackSetup("fastupdates", elasticsearch_settings=countriesBenchmarkFastSettings, heap='4g', build_ids=True),
  # integer divide!
  CountriesTrackSetup("two_nodes_defaults", processors=sysstats.number_of_cpu_cores() // 2, nodes=2),
])
