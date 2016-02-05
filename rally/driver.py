import json
import random
import logging

from elasticsearch.helpers import parallel_bulk

from rally import time
from rally.track import track
from rally.utils import io, convert, process, progress

logger = logging.getLogger("rally.driver")


class Driver:
    """
    Driver runs the benchmark.
    """

    def __init__(self, config, clock=time.Clock):
        self._config = config
        self._clock = clock
        self._metrics = None
        self._index_benchmark = None

    def setup(self, cluster, track, track_setup):
        mapping_path = self._config.opts("benchmarks", "mapping.path")
        mappings = open(mapping_path).read()
        logger.debug("create index w/ mappings")
        logger.debug(mappings)
        cluster.client.indices.create(index=track.index_name)
        cluster.client.indices.put_mapping(index=track.index_name,
                                           doc_type=track.type_name,
                                           body=json.loads(mappings))
        cluster.wait_for_status_green()

    def go(self, cluster, track, track_setup):
        cluster.on_benchmark_start()
        self._index_benchmark = IndexBenchmark(self._config, self._clock, track, track_setup, cluster, self._metrics)
        if track_setup.test_settings.benchmark_indexing:
            self._index_benchmark.run()
        if track_setup.test_settings.benchmark_search:
            search_benchmark = SearchBenchmark(self._config, self._clock, track, track_setup, cluster, self._metrics)
            search_benchmark.run()
        cluster.on_benchmark_stop()

    def tear_down(self, track, track_setup):
        # This is also just a hack for now (should be in track for first step and metrics for second one)
        data_paths = self._config.opts("provisioning", "local.data.paths")
        if track_setup.test_settings.benchmark_indexing:
            self._index_benchmark.print_index_stats(data_paths[0])


class TimedOperation:
    def __init__(self, clock):
        self._clock = clock

    def timed(self, target, repeat=1, *args, **kwargs):
        stop_watch = self._clock.stop_watch()
        stop_watch.start()
        for i in range(repeat):
            result = target(*args, **kwargs)
        stop_watch.stop()
        return stop_watch.total_time() / repeat, result


class SearchBenchmark(TimedOperation):
    def __init__(self, config, clock, track, track_setup, cluster, metrics):
        TimedOperation.__init__(self, clock)
        self._config = config
        self._clock = clock
        self._track = track
        self._track_setup = track_setup
        self._cluster = cluster
        self._metrics_store = cluster.metrics_store
        self._progress = progress.CmdLineProgressReporter()
        self._quiet_mode = self._config.opts("system", "quiet.mode")

    # TODO dm: Ensure we properly warmup before running metrics (what about ES internal caches? Ensure we don't do bogus benchmarks!)
    def run(self):
        es = self._cluster.client
        logger.info("Running search benchmark")
        # Run a few (untimed) warmup iterations before the actual benchmark
        self._run_benchmark(es, "  Benchmarking search (warmup iteration %d/%d)")
        times = self._run_benchmark(es, "  Benchmarking search (iteration %d/%d)")

        for q in self._track.queries:
            l = [x[q.name] for x in times]
            l.sort()
            for latency in l:
                self._metrics_store.put_value("query_latency_%s" % q.name, convert.seconds_to_ms(latency), "ms")

    def _run_benchmark(self, es, message, repetitions=10):
        times = []
        quiet = self._quiet_mode
        for i in range(repetitions):
            if not quiet:
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
            duration, result = self.timed(query.run, 10, es)
            d[query.name] = duration / query.normalization_factor
        return d


class IndexBenchmark(TimedOperation):
    def __init__(self, config, clock, track, track_setup, cluster, metrics):
        TimedOperation.__init__(self, clock)
        self._config = config
        self._stop_watch = clock.stop_watch()
        self._track = track
        self._track_setup = track_setup
        self._cluster = cluster
        self._metrics_store = cluster.metrics_store
        self._metrics = metrics
        self._quiet_mode = self._config.opts("system", "quiet.mode")
        self._sent_bytes = 0
        self._progress = progress.CmdLineProgressReporter()
        self._rand = random.Random(17)
        self._bulk_size = 5000
        logger.info("Use %d docs per bulk request" % self._bulk_size)

    def run(self):
        docs_to_index = self._track.number_of_documents
        data_set_path = self._config.opts("benchmarks", "dataset.path")

        # We cannot know how many docs have been updated if we produce id conflicts
        if self._track_setup.test_settings.id_conflicts == track.IndexIdConflict.NoConflicts:
            expected_doc_count = docs_to_index
        else:
            expected_doc_count = None

        finished = False

        try:
            self.index(data_set_path, expected_doc_count)
            finished = True
        finally:
            self._progress.finish()
            logger.info("IndexBenchmark finished successfully: %s" % finished)

    def generate_ids(self, conflicts):
        docs_to_index = self._track.number_of_documents
        logger.info("build ids with id conflicts of type %s" % conflicts)

        if conflicts == track.IndexIdConflict.SequentialConflicts:
            yield from (
                "%10d" % (
                    self._rand.randint(0, i)
                    # pick already returned id in 25% of cases
                    if i > 0 and self._rand.randint(0, 3) == 3
                    else i
                ) for i in range(docs_to_index)
            )
        elif conflicts == track.IndexIdConflict.RandomConflicts:
            ids = []
            for _ in range(docs_to_index):
                if ids and self._rand.randint(0, 3) == 3:
                    # pick already returned id in 25%
                    id = self._rand.choice(ids)
                else:
                    id = "%10i" % self._rand.randint(0, docs_to_index)
                    ids.append(id)
            yield id
        else:
            raise RuntimeError("Unknown id conflict type %s" % conflicts)

    def get_expand_action(self):
        conflicts = self._track_setup.test_settings.id_conflicts
        if conflicts is not track.IndexIdConflict.NoConflicts:
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

    def _read_records(self, documents):
        with open(documents, "rt") as f:
            yield from f

    def index(self, documents, expected_doc_count, flush=True, stats=True):
        num_client_threads = int(self._config.opts("benchmarks", "index.client.threads"))
        logger.info("Indexing JSON docs file: [%s]" % documents)
        logger.info("Launching %d client bulk indexing threads" % num_client_threads)

        index = self._track.index_name
        type = self._track.type_name
        es = self._cluster.client

        processed = 0
        self._stop_watch.start()
        self._print_progress(processed)
        try:
            for _ in parallel_bulk(es,
                                   self._read_records(documents),
                                   thread_count=num_client_threads,
                                   index=index,
                                   doc_type=type,
                                   chunk_size=self._bulk_size,
                                   expand_action_callback=self.get_expand_action(),
                                   ):
                if not self._quiet_mode and processed % 10000 == 0:
                    self._print_progress(processed)
                processed += 1
        except KeyboardInterrupt:
            logger.info("Received SIGINT: IndexBenchmark will be stopped prematurely.")

        self._stop_watch.stop()
        self._print_progress(processed)
        docs_per_sec = (processed / self._stop_watch.total_time())
        self._metrics_store.put_value("indexing_throughput", round(docs_per_sec), "docs/s")

        if flush:
            logger.info("Force flushing index [%s]." % index)
            es.indices.flush(index=index, params={"request_timeout": 600})
            logger.info("Force flush has finished successfully.")

        if stats:
            self._index_stats(expected_doc_count)
            self._node_stats()

    def _index_stats(self, expected_doc_count):
        index = self._track.index_name
        logger.info("Gathering indices stats")
        duration, stats = self.timed(self._cluster.client.indices.stats, index=index, metric="_all", level="shards")
        self._metrics_store.put_value("indices_stats_latency", convert.seconds_to_ms(duration), "ms")
        primaries = stats["_all"]["primaries"]
        self._metrics_store.put_count("segments_count", primaries["segments"]["count"])
        self._metrics_store.put_count("segments_memory_in_bytes", primaries["segments"]["memory_in_bytes"], "byte")
        self._metrics_store.put_count("segments_doc_values_memory_in_bytes", primaries["segments"]["doc_values_memory_in_bytes"], "byte")
        self._metrics_store.put_count("segments_stored_fields_memory_in_bytes", primaries["segments"]["stored_fields_memory_in_bytes"],
                                      "byte")
        self._metrics_store.put_count("segments_terms_memory_in_bytes", primaries["segments"]["terms_memory_in_bytes"], "byte")
        self._metrics_store.put_count("segments_norms_memory_in_bytes", primaries["segments"]["norms_memory_in_bytes"], "byte")
        self._metrics_store.put_value("merges_total_time", primaries["merges"]["total_time_in_millis"], "ms")
        self._metrics_store.put_value("merges_total_throttled_time", primaries["merges"]["total_throttled_time_in_millis"], "ms")
        self._metrics_store.put_value("indexing_total_time", primaries["indexing"]["index_time_in_millis"], "ms")
        self._metrics_store.put_value("refresh_total_time", primaries["refresh"]["total_time_in_millis"], "ms")
        self._metrics_store.put_value("flush_total_time", primaries["flush"]["total_time_in_millis"], "ms")

        actual_doc_count = primaries["docs"]["count"]
        if expected_doc_count is not None and expected_doc_count != actual_doc_count:
            msg = "wrong number of documents indexed: expected %s but got %s" % (expected_doc_count, actual_doc_count)
            logger.error(msg)
            raise RuntimeError(msg)

    def _node_stats(self):
        logger.info("Gathering nodes stats")
        duration, stats = self.timed(self._cluster.client.nodes.stats, metric="_all", level="shards")
        self._metrics_store.put_value("node_stats_latency", convert.seconds_to_ms(duration), "ms")
        # for now we only put data for one node in here but we should really put all of them into the metrics store
        nodes = stats["nodes"]
        node = list(nodes.keys())[0]
        node = nodes[node]
        gc = node["jvm"]["gc"]["collectors"]
        self._metrics_store.put_value("node_total_old_gen_gc_time", gc["old"]["collection_time_in_millis"], "ms")
        self._metrics_store.put_value("node_total_young_gen_gc_time", gc["young"]["collection_time_in_millis"], "ms")

    def _print_progress(self, docs_processed):
        if not self._quiet_mode:
            # stack-confine asap to keep the reporting error low (this number may be updated by other threads), we don't need to be entirely
            # accurate here as this is "just" a progress report for the user
            sent = self._sent_bytes
            elapsed = self._stop_watch.split_time()
            docs_per_second = docs_processed / elapsed
            mb_per_second = convert.bytes_to_mb(sent) / elapsed
            self._progress.print(
                "  Benchmarking indexing at %.1f docs/s, %.1f MB/sec" % (docs_per_second, mb_per_second),
                "[%3d%% done]" % round(100 * docs_processed / self._track.number_of_documents)
            )
            logger.info("Indexer: %d docs: %.2f sec [%.1f dps, %.1f MB/sec]" % (docs_processed, elapsed, docs_per_second, mb_per_second))

    def print_index_stats(self, data_dir):
        index_size_bytes = io.get_size(data_dir)
        self._metrics_store.put_count("final_index_size_bytes", index_size_bytes, "byte")
        process.run_subprocess_with_logging("find %s -ls" % data_dir, header="index files:")
