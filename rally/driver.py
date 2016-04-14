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
        # does not make sense to add any mappings if we don't benchmark indexing
        if track_setup.benchmark_settings.benchmark_indexing:
            mapping_path = self._config.opts("benchmarks", "mapping.path")
            for index in track.indices:
                logger.debug("Creating index [%s]" % index.name)
                cluster.client.indices.create(index=index.name, body=track_setup.candidate_settings.index_settings)
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
        if track_setup.benchmark_settings.benchmark_indexing:
            logger.info("Starting index benchmark for track [%s] and track setup [%s]" % (current_track.name, track_setup.name))
            cluster.on_benchmark_start(track.BenchmarkPhase.index)
            self._index_benchmark = IndexBenchmark(self._config, self._clock, current_track, track_setup, cluster, self._metrics)
            self._index_benchmark.run()
            cluster.on_benchmark_stop(track.BenchmarkPhase.index)
            logger.info("Stopped index benchmark for track [%s] and track setup [%s]" % (current_track.name, track_setup.name))
        if track_setup.benchmark_settings.benchmark_search:
            logger.info("Starting search benchmark for track [%s] and track setup [%s]" % (current_track.name, track_setup.name))
            cluster.on_benchmark_start(track.BenchmarkPhase.search)
            search_benchmark = SearchBenchmark(self._config, self._clock, current_track, track_setup, cluster, self._metrics)
            search_benchmark.run()
            cluster.on_benchmark_stop(track.BenchmarkPhase.search)
            logger.info("Stopped search benchmark for track [%s] and track setup [%s]" % (current_track.name, track_setup.name))
        cluster.on_benchmark_stop()

    def tear_down(self, track, track_setup):
        if track_setup.benchmark_settings.benchmark_indexing:
            # This is also just a hack for now (should be in track for first step and metrics for second one)
            data_paths = self._config.opts("provisioning", "local.data.paths", mandatory=False)
            if data_paths is not None:
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

    def run(self):
        es = self._cluster.client
        logger.info("Running search benchmark (warmup)")
        # Run a few (untimed) warmup iterations before the actual benchmark
        self._run_benchmark(es, "  Benchmarking search (warmup iteration %d/%d)", repetitions=1000)
        logger.info("Running search benchmark")
        times = self._run_benchmark(es, "  Benchmarking search (iteration %d/%d)", repetitions=1000)
        logger.info("Search benchmark has finished")

        for q in self._track.queries:
            l = [x[q.name] for x in times]
            l.sort()
            for latency in l:
                self._metrics_store.put_value_cluster_level("query_latency_%s" % q.name, convert.seconds_to_ms(latency), "ms")

    def _run_benchmark(self, es, message, repetitions=10):
        times = []
        quiet = self._quiet_mode
        for iteration in range(1, repetitions + 1):
            if not quiet and (iteration % 50 == 0 or iteration == 1):
                self._progress.print(
                    message % (iteration, repetitions),
                    "[%3d%% done]" % (round(100 * iteration / repetitions))
                )
            times.append(self._run_one_round(es))
        self._progress.finish()
        return times

    def _run_one_round(self, es):
        d = {}
        for query in self._track.queries:
            duration, result = self.timed(query.run, 1, es)
            d[query.name] = duration / query.normalization_factor
            query.close(es)
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
        finished = False
        try:
            self.index()
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
                    id = "%10d" % self._rand.randint(0, docs_to_index)
                    ids.append(id)
                yield id
        else:
            raise RuntimeError("Unknown id conflict type %s" % conflicts)

    def get_expand_action(self):
        conflicts = self._track_setup.benchmark_settings.id_conflicts
        if conflicts is not track.IndexIdConflict.NoConflicts:
            id_generator = self.generate_ids(conflicts)
        else:
            id_generator = None

        def expand_action(data):
            if id_generator:
                action = '{"index": {"_id": %s}}' % next(id_generator)
            else:
                action = '{"index": {}}'
            self._sent_bytes += len(data)
            return action, data.strip()

        return expand_action

    def _read_records(self, documents):
        logger.info("Indexing JSON docs file: [%s]" % documents)
        with open(documents, "rt") as f:
            yield from f

    def index(self):
        force_merge = self._track_setup.benchmark_settings.force_merge
        docs_to_index = self._track.number_of_documents
        documents = self._config.opts("benchmarks", "dataset.path")

        # We cannot know how many docs have been updated if we produce id conflicts
        if self._track_setup.benchmark_settings.id_conflicts == track.IndexIdConflict.NoConflicts:
            expected_doc_count = docs_to_index
        else:
            expected_doc_count = None
        num_client_threads = int(self._config.opts("benchmarks", "index.client.threads"))
        logger.info("Launching %d client bulk indexing threads" % num_client_threads)

        es = self._cluster.client
        processed = 0
        total = self._track.number_of_documents
        self._stop_watch.start()
        self._print_progress(processed, total)

        for index in self._track.indices:
            for type in index.types:
                if type.document_file_name:
                    try:
                        for _ in parallel_bulk(es,
                                               self._read_records(documents[type]),
                                               thread_count=num_client_threads,
                                               index=index.name,
                                               doc_type=type.name,
                                               chunk_size=self._bulk_size,
                                               expand_action_callback=self.get_expand_action(),
                                               ):
                            if processed % 10000 == 0:
                                self._print_progress(processed, total)
                                # use 10% of all documents for warmup before gathering metrics
                                if processed > total / 10:
                                    elapsed = self._stop_watch.split_time()
                                    docs_per_sec = (processed / elapsed)
                                    self._metrics_store.put_value_cluster_level("indexing_throughput", round(docs_per_sec), "docs/s")
                            processed += 1
                    except KeyboardInterrupt:
                        logger.info("Received SIGINT: IndexBenchmark will be stopped prematurely.")

        self._stop_watch.stop()
        self._print_progress(processed, total)
        docs_per_sec = (processed / self._stop_watch.total_time())
        self._metrics_store.put_value_cluster_level("indexing_throughput", round(docs_per_sec), "docs/s")

        logger.info("Force flushing all indices.")
        es.indices.flush(params={"request_timeout": 600})
        logger.info("Force flush has finished successfully.")

        if force_merge:
            logger.info("Force merging all indices.")
            es.indices.forcemerge()

        self._index_stats(expected_doc_count)
        self._node_stats()

    def _index_stats(self, expected_doc_count):
        logger.info("Gathering indices stats")
        duration, stats = self.timed(self._cluster.client.indices.stats, metric="_all", level="shards")
        self._metrics_store.put_value_cluster_level("indices_stats_latency", convert.seconds_to_ms(duration), "ms")
        primaries = stats["_all"]["primaries"]
        self._metrics_store.put_count_cluster_level("segments_count", primaries["segments"]["count"])
        self._metrics_store.put_count_cluster_level("segments_memory_in_bytes", primaries["segments"]["memory_in_bytes"], "byte")
        self._metrics_store.put_count_cluster_level("segments_doc_values_memory_in_bytes", primaries["segments"]["doc_values_memory_in_bytes"], "byte")
        self._metrics_store.put_count_cluster_level("segments_stored_fields_memory_in_bytes", primaries["segments"]["stored_fields_memory_in_bytes"],
                                      "byte")
        self._metrics_store.put_count_cluster_level("segments_terms_memory_in_bytes", primaries["segments"]["terms_memory_in_bytes"], "byte")
        self._metrics_store.put_count_cluster_level("segments_norms_memory_in_bytes", primaries["segments"]["norms_memory_in_bytes"], "byte")
        self._metrics_store.put_value_cluster_level("merges_total_time", primaries["merges"]["total_time_in_millis"], "ms")
        self._metrics_store.put_value_cluster_level("merges_total_throttled_time", primaries["merges"]["total_throttled_time_in_millis"], "ms")
        self._metrics_store.put_value_cluster_level("indexing_total_time", primaries["indexing"]["index_time_in_millis"], "ms")
        self._metrics_store.put_value_cluster_level("refresh_total_time", primaries["refresh"]["total_time_in_millis"], "ms")
        self._metrics_store.put_value_cluster_level("flush_total_time", primaries["flush"]["total_time_in_millis"], "ms")

        actual_doc_count = primaries["docs"]["count"]
        if expected_doc_count is not None and expected_doc_count != actual_doc_count:
            msg = "wrong number of documents indexed: expected %s but got %s" % (expected_doc_count, actual_doc_count)
            logger.error(msg)
            raise RuntimeError(msg)

    def _node_stats(self):
        logger.info("Gathering nodes stats")
        duration, stats = self.timed(self._cluster.client.nodes.stats, metric="_all", level="shards")
        self._metrics_store.put_value_cluster_level("node_stats_latency", convert.seconds_to_ms(duration), "ms")
        total_old_gen_collection_time = 0
        total_young_gen_collection_time = 0
        nodes = stats["nodes"]
        for node in nodes.values():
            node_name = node["name"]
            gc = node["jvm"]["gc"]["collectors"]
            old_gen_collection_time = gc["old"]["collection_time_in_millis"]
            young_gen_collection_time = gc["young"]["collection_time_in_millis"]
            self._metrics_store.put_value_node_level(node_name, "node_old_gen_gc_time", old_gen_collection_time, "ms")
            self._metrics_store.put_value_node_level(node_name, "node_young_gen_gc_time", young_gen_collection_time, "ms")
            total_old_gen_collection_time += old_gen_collection_time
            total_young_gen_collection_time += young_gen_collection_time

        self._metrics_store.put_value_cluster_level("node_total_old_gen_gc_time", total_old_gen_collection_time, "ms")
        self._metrics_store.put_value_cluster_level("node_total_young_gen_gc_time", total_young_gen_collection_time, "ms")

    def _print_progress(self, docs_processed, docs_total):
        if not self._quiet_mode:
            # stack-confine asap to keep the reporting error low (this number may be updated by other threads), we don't need to be entirely
            # accurate here as this is "just" a progress report for the user
            sent = self._sent_bytes
            elapsed = self._stop_watch.split_time()
            docs_per_second = docs_processed / elapsed
            self._progress.print(
                "  Benchmarking indexing at %.1f docs/s" % docs_per_second,
                "[%3d%% done]" % round(100 * docs_processed / docs_total)
            )
            logger.info("Indexer: %d docs: %.2f sec [%.1f docs/s]" % (docs_processed, elapsed, docs_per_second))

    def print_index_stats(self, data_dir):
        index_size_bytes = io.get_size(data_dir)
        self._metrics_store.put_count_cluster_level("final_index_size_bytes", index_size_bytes, "byte")
        process.run_subprocess_with_logging("find %s -ls" % data_dir, header="index files:")
