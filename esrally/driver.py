import json
import random
import logging
import threading

from esrally import time, exceptions
from esrally.track import track
from esrally.utils import io, convert, process, progress

logger = logging.getLogger("rally.driver")


class Driver:
    """
    Driver runs the benchmark.
    """

    def __init__(self, config, clock=time.Clock):
        self.cfg = config
        self.clock = clock
        self.index_benchmark = None

    def setup(self, cluster, track, track_setup):
        # does not make sense to add any mappings if we don't benchmark indexing
        if track_setup.benchmark.benchmark_indexing:
            mapping_path = self.cfg.opts("benchmarks", "mapping.path")
            for index in track.indices:
                logger.debug("Creating index [%s]" % index.name)
                settings = track_setup.candidate.index_settings
                # Workaround to support multiple versions (this is not how this will be handled in the future..)
                if "master" in settings:
                    # check whether we do a binary benchmark
                    distribution_version = self.cfg.opts("source", "distribution.version", mandatory=False)
                    if distribution_version and len(distribution_version.strip()) > 0:
                        if distribution_version in settings:
                            index_settings = settings[distribution_version]
                        else:
                            raise exceptions.SystemSetupError("Could not find index settings for Elasticsearch version [%s]" %
                                                              distribution_version)
                    else:
                        index_settings = settings["master"]
                else:
                    index_settings = settings
                cluster.client.indices.create(index=index.name, body=index_settings)
                for type in index.types:
                    mappings = open(mapping_path[type]).read()
                    logger.debug("create mapping for type [%s] in index [%s]" % (type.name, index.name))
                    logger.debug(mappings)
                    cluster.client.indices.put_mapping(index=index.name,
                                                       doc_type=type.name,
                                                       body=json.loads(mappings))
        cluster.wait_for_status_green()

    def go(self, cluster, current_track, track_setup):
        cluster.on_benchmark_start()
        self.index(cluster, current_track, track_setup)
        self.search(cluster, current_track, track_setup)
        cluster.on_benchmark_stop()

    def index(self, cluster, current_track, track_setup):
        if track_setup.benchmark.benchmark_indexing:
            logger.info("Starting index benchmark for track [%s] and track setup [%s]" % (current_track.name, track_setup.name))
            cluster.on_benchmark_start(track.BenchmarkPhase.index)
            self.index_benchmark = ThreadedIndexBenchmark(self.cfg, self.clock, current_track, track_setup, cluster)
            self.index_benchmark.run()
            cluster.on_benchmark_stop(track.BenchmarkPhase.index)
            logger.info("Stopped index benchmark for track [%s] and track setup [%s]" % (current_track.name, track_setup.name))

    def search(self, cluster, current_track, track_setup):
        if track_setup.benchmark.benchmark_search:
            logger.info("Starting search benchmark for track [%s] and track setup [%s]" % (current_track.name, track_setup.name))
            cluster.on_benchmark_start(track.BenchmarkPhase.search)
            search_benchmark = SearchBenchmark(self.cfg, self.clock, current_track, track_setup, cluster)
            search_benchmark.run()
            cluster.on_benchmark_stop(track.BenchmarkPhase.search)
            logger.info("Stopped search benchmark for track [%s] and track setup [%s]" % (current_track.name, track_setup.name))

    def tear_down(self, track, track_setup):
        if track_setup.benchmark.benchmark_indexing:
            # This is also just a hack for now (should be in track for first step and metrics for second one)
            data_paths = self.cfg.opts("provisioning", "local.data.paths", mandatory=False)
            if data_paths is not None:
                self.index_benchmark.print_index_stats(data_paths[0])


class TimedOperation:
    def __init__(self, clock):
        self.clock = clock

    def timed(self, target, repeat=1, *args, **kwargs):
        stop_watch = self.clock.stop_watch()
        stop_watch.start()
        result = None
        for i in range(repeat):
            result = target(*args, **kwargs)
        stop_watch.stop()
        return stop_watch.total_time() / repeat, result


class SearchBenchmark(TimedOperation):
    def __init__(self, cfg, clock, track, track_setup, cluster):
        TimedOperation.__init__(self, clock)
        self.cfg = cfg
        self.track = track
        self.track_setup = track_setup
        self.cluster = cluster
        self.metrics_store = cluster.metrics_store
        self.progress = progress.CmdLineProgressReporter()
        self.quiet_mode = self.cfg.opts("system", "quiet.mode")

    def run(self):
        es = self.cluster.client
        logger.info("Running search benchmark (warmup)")
        self._run_benchmark(es, "  Benchmarking search (warmup iteration %d/%d)", repetitions=1000)
        logger.info("Running search benchmark")
        times = self._run_benchmark(es, "  Benchmarking search (iteration %d/%d)", repetitions=1000)
        logger.info("Search benchmark has finished")

        for q in self.track.queries:
            latencies = [t[q.name] for t in times]
            for latency in latencies:
                self.metrics_store.put_value_cluster_level("query_latency_%s" % q.name, convert.seconds_to_ms(latency), "ms")

    def _run_benchmark(self, es, message, repetitions=10):
        times = []
        quiet = self.quiet_mode
        for iteration in range(1, repetitions + 1):
            if not quiet and (iteration % 50 == 0 or iteration == 1):
                self.progress.print(
                    message % (iteration, repetitions),
                    "[%3d%% done]" % (round(100 * iteration / repetitions))
                )
            times.append(self._run_one_round(es))
        self.progress.finish()
        return times

    def _run_one_round(self, es):
        d = {}
        for query in self.track.queries:
            duration, result = self.timed(query.run, 1, es)
            d[query.name] = duration / query.normalization_factor
            query.close(es)
        return d


class IndexBenchmark(TimedOperation):
    def __init__(self, config, clock, track, track_setup, cluster):
        TimedOperation.__init__(self, clock)
        self.cfg = config
        self.stop_watch = clock.stop_watch()
        self.track = track
        self.track_setup = track_setup
        self.cluster = cluster
        self.metrics_store = cluster.metrics_store
        self.quiet_mode = self.cfg.opts("system", "quiet.mode")
        self.progress = progress.CmdLineProgressReporter()
        self.bulk_size = 5000
        self.processed = 0
        logger.info("Use %d docs per bulk request" % self.bulk_size)

    def run(self):
        finished = False
        try:
            self.index()
            finished = True
        finally:
            self.progress.finish()
            logger.info("IndexBenchmark finished successfully: %s" % finished)

    def index(self):
        force_merge = self.track_setup.benchmark.force_merge
        docs_to_index = self.track.number_of_documents

        # We cannot know how many docs have been updated if we produce id conflicts
        conflicts = self.track_setup.benchmark.id_conflicts
        if conflicts == track.IndexIdConflict.NoConflicts:
            expected_doc_count = docs_to_index
            ids = None
        else:
            expected_doc_count = None
            ids = self.build_conflicting_ids(conflicts)

        self.index_documents(ids)

        logger.info("Force flushing all indices.")
        es = self.cluster.client
        es.indices.flush(params={"request_timeout": 600})
        logger.info("Force flush has finished successfully.")

        if force_merge:
            logger.info("Force merging all indices.")
            es.indices.forcemerge()

        self._index_stats(expected_doc_count)
        self._node_stats()

    def build_conflicting_ids(self, conflicts):
        docs_to_index = self.track.number_of_documents
        logger.info('build ids with id conflicts of type %s' % conflicts)

        all_ids = [0] * docs_to_index
        for i in range(docs_to_index):
            if conflicts == track.IndexIdConflict.SequentialConflicts:
                all_ids[i] = '%10d' % i
            elif conflicts == track.IndexIdConflict.RandomConflicts:
                all_ids[i] = '%10d' % random.randint(0, docs_to_index)
            else:
                raise RuntimeError('Unknown id conflict type %s' % conflicts)
        return all_ids

    def _index_stats(self, expected_doc_count):
        # warmup
        self.repeat(self.cluster.client.indices.stats, metric="_all", level="shards")
        durations, stats = self.repeat(self.cluster.client.indices.stats, metric="_all", level="shards")
        for duration in durations:
            self.metrics_store.put_value_cluster_level("indices_stats_latency", convert.seconds_to_ms(duration), "ms")
        primaries = stats["_all"]["primaries"]
        # TODO: Consider introducing "probes" for verifications
        actual_doc_count = primaries["docs"]["count"]
        if expected_doc_count is not None and expected_doc_count != actual_doc_count:
            msg = "Wrong number of documents indexed: expected %s but got %s. If you benchmark against an external cluster be sure to " \
                  "start with all indices empty." % (expected_doc_count, actual_doc_count)
            logger.error(msg)
            raise AssertionError(msg)

    def _node_stats(self):
        # warmup
        self.repeat(self.cluster.client.nodes.stats, metric="_all", level="shards")
        durations, stats = self.repeat(self.cluster.client.nodes.stats, metric="_all", level="shards")
        for duration in durations:
            self.metrics_store.put_value_cluster_level("node_stats_latency", convert.seconds_to_ms(duration), "ms")

    # we don't want to gather to many samples as we're actually in the middle of the index benchmark and would skew other metrics like
    # CPU usage too much (TODO #27: split this step from actual indexing and report proper percentiles not just median.)
    def repeat(self, api_call, repetitions=10, *args, **kwargs):
        times = []
        result = None
        for iteration in range(0, repetitions):
            duration, result = self.timed(api_call, 1, *args, **kwargs)
            times.append(duration)
        return times, result

    def _print_progress(self, docs_processed):
        if not self.quiet_mode:
            # stack-confine asap to keep the reporting error low (this number may be updated by other threads), we don't need to be entirely
            # accurate here as this is "just" a progress report for the user
            docs_total = self.track.number_of_documents
            elapsed = self.stop_watch.split_time()
            docs_per_second = docs_processed / elapsed
            self.progress.print(
                "  Benchmarking indexing at %.1f docs/s" % docs_per_second,
                "[%3d%% done]" % round(100 * docs_processed / docs_total)
            )
            logger.info("Indexer: %d docs: [%.1f docs/s]" % (docs_processed, docs_per_second))

    def print_index_stats(self, data_dir):
        index_size_bytes = io.get_size(data_dir)
        self.metrics_store.put_count_cluster_level("final_index_size_bytes", index_size_bytes, "byte")
        process.run_subprocess_with_logging("find %s -ls" % data_dir, header="index files:")

    def index_documents(self, ids):
        raise NotImplementedError("abstract method")


class ThreadedIndexBenchmark(IndexBenchmark):
    def __init__(self, config, clock, track, track_setup, cluster):
        super().__init__(config, clock, track, track_setup, cluster)

    def index_documents(self, ids):
        num_client_threads = int(self.cfg.opts("benchmarks", "index.clients"))
        logger.info("Launching %d client bulk indexing threads" % num_client_threads)

        self.stop_watch.start()
        self._print_progress(self.processed)

        for index in self.track.indices:
            for type in index.types:
                if type.document_file_name:
                    documents = self.cfg.opts("benchmarks", "dataset.path")
                    docs_file = documents[type]
                    logger.info("Indexing JSON docs file: [%s]" % docs_file)

                    start_signal = CountDownLatch(1)
                    stop_event = threading.Event()
                    failed_event = threading.Event()
                    docs_to_index = self.track.number_of_documents

                    iterator = BulkIterator(index.name, type.name, docs_file, ids, docs_to_index, self.bulk_size)

                    threads = []
                    try:
                        for i in range(num_client_threads):
                            t = threading.Thread(target=self.bulk_index, args=(start_signal, iterator, failed_event, stop_event))
                            t.setDaemon(True)
                            t.start()
                            threads.append(t)

                        start_signal.count_down()
                        for t in threads:
                            t.join()

                    except KeyboardInterrupt:
                        stop_event.set()
                        for t in threads:
                            t.join()

                    if failed_event.isSet():
                        raise RuntimeError("some indexing threads failed")

        self.stop_watch.stop()
        self._print_progress(self.processed)
        docs_per_sec = (self.processed / self.stop_watch.total_time())
        self.metrics_store.put_value_cluster_level("indexing_throughput", round(docs_per_sec), "docs/s")

    def bulk_index(self, start_signal, bulk_iterator, failed_event, stop_event):
        """
        Runs one (client) bulk index thread.
        """
        start_signal.await()

        while not stop_event.isSet():
            buffer, document_count = bulk_iterator.next_bulk()
            if document_count == 0:
                break
            data = "\n".join(buffer)
            del buffer[:]

            try:
                self.cluster.client.bulk(body=data, params={'request_timeout': 60000})
            except BaseException as e:
                logger.error("Indexing failed: %s" % e)
                failed_event.set()
                stop_event.set()
                raise
            self.update_bulk_status(document_count)

    def update_bulk_status(self, count):
        self.processed += count
        if self.processed % 10000 == 0:

            self._print_progress(self.processed)
            # use 10% of all documents for warmup before gathering metrics
            if self.processed > self.track.number_of_documents / 10:
                elapsed = self.stop_watch.split_time()
                docs_per_sec = (self.processed / elapsed)
                self.metrics_store.put_value_cluster_level("indexing_throughput", round(docs_per_sec), "docs/s")


class CountDownLatch:
    def __init__(self, count=1):
        self.count = count
        self.lock = threading.Condition()

    def count_down(self):
        with self.lock:
            self.count -= 1

            if self.count <= 0:
                self.lock.notifyAll()

    def await(self):
        with self.lock:
            while self.count > 0:
                self.lock.wait()


class BulkIterator:
    def __init__(self, index_name, type_name, documents, ids, docs_to_index, bulk_size):
        self.f = open(documents, 'rt')
        self.index_name = index_name
        self.type_name = type_name
        self.current_bulk = 0
        self.id_up_to = 0
        self.ids = ids
        self.file_lock = threading.Lock()
        self.docs_to_index = docs_to_index
        self.rand = random.Random(17)
        self.bulk_size = bulk_size
        self.indexed_document_count = 0

    def close(self):
        if self.f:
            self.f.close()
            self.f = None

    def next_bulk(self):

        """
        Returns lines for one bulk request.
        """

        buffer = []
        try:
            with self.file_lock:
                docs_left = self.docs_to_index - (self.current_bulk * self.bulk_size)
                if self.f is None or docs_left <= 0:
                    return [], 0

                self.current_bulk += 1
                limit = 2 * min(self.bulk_size, docs_left)

                while True:
                    line = self.f.readline()
                    if len(line) == 0:
                        self.close()
                        break
                    line = line.strip()

                    if self.ids is not None:
                        # 25% of the time we replace a doc:
                        if self.id_up_to > 0 and self.rand.randint(0, 3) == 3:
                            id = self.ids[self.rand.randint(0, self.id_up_to - 1)]
                        else:
                            id = self.ids[self.id_up_to]
                            self.id_up_to += 1
                        cmd = '{"index": {"_index": "%s", "_type": "%s", "_id": "%s"}}' % (self.index_name, self.type_name, id)
                    else:
                        cmd = '{"index": {"_index": "%s", "_type": "%s"}}' % (self.index_name, self.type_name)

                    buffer.append(cmd)
                    buffer.append(line)

                    if len(buffer) >= limit:
                        break

            # contains index commands and data (in bulk format), hence 2 lines per record
            document_count = len(buffer) / 2
            self.indexed_document_count += document_count
            return buffer, document_count
        except BaseException as e:
            logger.exception("Could not read")
