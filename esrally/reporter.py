import logging
import tabulate

from esrally import metrics, exceptions
from esrally.track import track
from esrally.utils import convert

logger = logging.getLogger("rally.reporting")

MEDIAN = "50.0"


def print_internal(message):
    print(message)
    logger.info(message)


def print_header(message):
    print_internal("\033[1m%s\033[0m" % message)


def red(message):
    return "\033[31;1m%s\033[0m" % message


def green(message):
    return "\033[32;1m%s\033[0m" % message


def neutral(message):
    return "\033[39;1m%s\033[0m" % message


class Stats:
    def __init__(self, store, stats_sample_size, queries, search_sample_size):
        self.indexing_throughput = self.summary_stats(store, "indexing_throughput")
        self.total_time = store.get_one("indexing_total_time")
        self.merge_time = store.get_one("merges_total_time")
        self.refresh_time = store.get_one("refresh_total_time")
        self.flush_time = store.get_one("flush_total_time")
        self.merge_throttle_time = store.get_one("merges_total_throttled_time")

        self.merge_part_time_postings = store.get_one("merge_parts_total_time_postings")
        self.merge_part_time_stored_fields = store.get_one("merge_parts_total_time_stored_fields")
        self.merge_part_time_doc_values = store.get_one("merge_parts_total_time_doc_values")
        self.merge_part_time_norms = store.get_one("merge_parts_total_time_norms")
        self.merge_part_time_vectors = store.get_one("merge_parts_total_time_vectors")
        self.query_latencies = {}
        if search_sample_size and search_sample_size > 0:
            for query in queries:
                self.query_latencies[query] = self.single_latency(store, query, search_sample_size)
        self.median_cpu_usage = {}
        for phase in track.BenchmarkPhase:
            m = self.median(store, "cpu_utilization_1s_%s" % phase.name)
            if m:
                self.median_cpu_usage[phase.name] = m
        self.young_gc_time = store.get_one("node_total_young_gen_gc_time")
        self.old_gc_time = store.get_one("node_total_old_gen_gc_time")
        self.index_size = store.get_one("final_index_size_bytes")
        self.bytes_written = store.get_one("disk_io_write_bytes_%s" % track.BenchmarkPhase.index.name)

        self.memory_segments = store.get_one("segments_memory_in_bytes")
        self.memory_doc_values = store.get_one("segments_doc_values_memory_in_bytes")
        self.memory_terms = store.get_one("segments_terms_memory_in_bytes")
        self.memory_norms = store.get_one("segments_norms_memory_in_bytes")
        self.memory_points = store.get_one("segments_points_memory_in_bytes")
        self.memory_stored_fields = store.get_one("segments_stored_fields_memory_in_bytes")

        self.index_size = store.get_one("final_index_size_bytes")
        self.bytes_written = store.get_one("disk_io_write_bytes_%s" % track.BenchmarkPhase.index.name)

        self.segment_count = store.get_one("segments_count")
        if stats_sample_size and stats_sample_size > 0:
            self.indices_stats_latency = self.single_latency(store, "indices_stats", stats_sample_size)
            self.node_stats_latency = self.single_latency(store, "node_stats", stats_sample_size)
        else:
            self.indices_stats_latency = None
            self.node_stats_latency = None

    def summary_stats(self, store, metric_name):
        percentiles = store.get_percentiles(metric_name, percentiles=[MEDIAN])
        stats = store.get_stats(metric_name)
        if percentiles and stats:
            return stats["min"], percentiles[MEDIAN], stats["max"]
        else:
            return None

    def has_indexing_times(self):
        return self.total_time and self.merge_time and self.refresh_time and self.flush_time and self.merge_throttle_time

    def has_merge_part_stats(self):
        return self.merge_part_time_postings and \
               self.merge_part_time_stored_fields and \
               self.memory_doc_values and \
               self.merge_part_time_norms and \
               self.merge_part_time_vectors

    def has_memory_stats(self):
        return self.memory_segments and \
               self.memory_doc_values and \
               self.memory_terms and \
               self.memory_norms and \
               self.memory_points and \
               self.memory_stored_fields

    def has_disk_usage_stats(self):
        return self.index_size and self.bytes_written

    def median(self, store, metric_name):
        percentiles = store.get_percentiles(metric_name, percentiles=[MEDIAN])
        if percentiles:
            return percentiles[MEDIAN]
        else:
            return None

    def single_latency(self, store, q, sample_size):
        return store.get_percentiles("query_latency_%s" % q, percentiles=self.percentiles_for_sample_size(sample_size))

    def percentiles_for_sample_size(self, sample_size):
        # if needed we can come up with something smarter but it'll do for now
        if sample_size <= 1:
            raise AssertionError("Percentiles require at least one sample")
        elif sample_size == 1:
            return [100]
        elif 1 < sample_size < 10:
            return [50.0, 100]
        elif 10 <= sample_size < 100:
            return [50.0, 90.0, 100]
        elif 100 <= sample_size < 1000:
            return [90.0, 99.0, 100]
        else:
            return [90.0, 99.0, 99.9, 100]


class SummaryReporter:
    def __init__(self, config):
        self._config = config

    def report(self, t):
        print_header("------------------------------------------------------")
        print_header("    _______             __   _____                    ")
        print_header("   / ____(_)___  ____ _/ /  / ___/_________  ________ ")
        print_header("  / /_  / / __ \/ __ `/ /   \__ \/ ___/ __ \/ ___/ _ \\")
        print_header(" / __/ / / / / / /_/ / /   ___/ / /__/ /_/ / /  /  __/")
        print_header("/_/   /_/_/ /_/\__,_/_/   /____/\___/\____/_/   \___/ ")
        print_header("------------------------------------------------------")

        selected_setups = self._config.opts("benchmarks", "tracksetups.selected")
        invocation = self._config.opts("meta", "time.start")
        logger.info("Generating summary report for invocation=[%s], track=[%s], track setups=%s" % (invocation, t.name, selected_setups))
        for track_setup in t.track_setups:
            if track_setup.name in selected_setups:
                if len(selected_setups) > 1:
                    print_header("*** Track setup %s ***\n" % track_setup.name)
                store = metrics.EsMetricsStore(self._config)
                store.open(invocation, t.name, track_setup.name)

                stats = Stats(store,
                              self.sample_size(track_setup, track.BenchmarkPhase.stats),
                              t.queries,
                              self.sample_size(track_setup, track.BenchmarkPhase.search))

                metrics_table = []
                if track.BenchmarkPhase.index in track_setup.benchmark:
                    metrics_table += self.report_index_throughput(stats)
                    metrics_table += self.report_total_times(stats)
                    metrics_table += self.report_merge_part_times(stats)

                if track.BenchmarkPhase.search in track_setup.benchmark:
                    metrics_table += self.report_search_latency(stats)

                metrics_table += self.report_cpu_usage(stats)
                metrics_table += self.report_gc_times(stats)

                metrics_table += self.report_disk_usage(stats)
                metrics_table += self.report_segment_memory(stats)
                metrics_table += self.report_segment_counts(stats)

                if track.BenchmarkPhase.stats in track_setup.benchmark:
                    metrics_table += self.report_stats_latency(stats)

                print_internal(tabulate.tabulate(metrics_table, headers=["Metric", "Value"], numalign="right", stralign="right"))

    def sample_size(self, track_setup, benchmark_phase):
        if track.BenchmarkPhase.stats in track_setup.benchmark:
            return track_setup.benchmark[benchmark_phase].iteration_count
        else:
            return None

    def report_index_throughput(self, stats):
        if stats.indexing_throughput:
            min, median, max = stats.indexing_throughput
            return [
                ["Min Indexing Throughput [docs/s]", min],
                ["Median Indexing Throughput [docs/s]", median],
                ["Max Indexing Throughput [docs/s]", max]
            ]
        else:
            return []

    def report_search_latency(self, stats):
        lines = []
        for query, latency in stats.query_latencies.items():
            for percentile, value in latency.items():
                lines.append(["Query latency %s (%s percentile) [ms]" % (query, percentile), value])
        return lines

    def report_total_times(self, stats):
        return [
            ["Indexing time [min]", convert.ms_to_minutes(stats.total_time)],
            ["Merge time [min]", convert.ms_to_minutes(stats.merge_time)],
            ["Refresh time [min]", convert.ms_to_minutes(stats.refresh_time)],
            ["Flush time [min]", convert.ms_to_minutes(stats.flush_time)],
            ["Merge throttle time [min]", convert.ms_to_minutes(stats.merge_throttle_time)]
        ]

    def report_merge_part_times(self, stats):
        # note that these times are not(!) wall clock time results but total times summed up over multiple threads
        if stats.has_merge_part_stats():
            return [
                ["Merge time (postings) [min]", convert.ms_to_minutes(stats.merge_part_time_postings)],
                ["Merge time (stored fields) [min]", convert.ms_to_minutes(stats.merge_part_time_stored_fields)],
                ["Merge time (doc values) [min]", convert.ms_to_minutes(stats.merge_part_time_doc_values)],
                ["Merge time (norms) [min]", convert.ms_to_minutes(stats.merge_part_time_norms)],
                ["Merge time (vectors) [min]", convert.ms_to_minutes(stats.merge_part_time_vectors)]
            ]
        else:
            return []

    def report_cpu_usage(self, stats):
        cpu_usage = []
        for phase in track.BenchmarkPhase:
            if phase.name in stats.median_cpu_usage:
                cpu_usage.append(["Median CPU usage (%s) [%%]" % phase.name, stats.median_cpu_usage[phase.name]])
        return cpu_usage

    def report_gc_times(self, stats):
        return [
            ["Total Young Gen GC [s]", convert.ms_to_seconds(stats.young_gc_time)],
            ["Total Old Gen GC [s]", convert.ms_to_seconds(stats.old_gc_time)]
        ]

    def report_disk_usage(self, stats):
        if stats.has_disk_usage_stats():
            return [
                ["Index size [GB]", convert.bytes_to_gb(stats.index_size)],
                ["Totally written [GB]", convert.bytes_to_gb(stats.bytes_written)]
            ]
        else:
            return []

    def report_segment_memory(self, stats):
        if stats.has_memory_stats():
            return [
                ["Heap used for segments [MB]", convert.bytes_to_mb(stats.memory_segments)],
                ["Heap used for doc values [MB]", convert.bytes_to_mb(stats.memory_doc_values)],
                ["Heap used for terms [MB]", convert.bytes_to_mb(stats.memory_terms)],
                ["Heap used for norms [MB]", convert.bytes_to_mb(stats.memory_norms)],
                ["Heap used for points [MB]", convert.bytes_to_mb(stats.memory_points)],
                ["Heap used for points [MB]", convert.bytes_to_mb(stats.memory_stored_fields)],
            ]
        else:
            return []

    def report_segment_counts(self, stats):
        if stats.segment_count:
            return [["Segment count", stats.segment_count]]
        else:
            return []

    def report_stats_latency(self, stats):
        lines = []
        if stats.indices_stats_latency:
            for percentile, value in stats.indices_stats_latency.items():
                lines.append(["Indices Stats(%s percentile) [ms]" % percentile, value])

        if stats.node_stats_latency:
            for percentile, value in stats.node_stats_latency.items():
                lines.append(["Nodes Stats(%s percentile) [ms]" % percentile, value])
        return lines


class ComparisonReporter:
    def __init__(self, config):
        self._config = config

    def report(self, r1, r1_track_setup, r2, r2_track_setup):
        selected_baseline_track_setup = self.track_setup(r1, "baseline", r1_track_setup)
        selected_contender_track_setup = self.track_setup(r2, "contender", r2_track_setup)

        logger.info("Generating comparison report for baseline (invocation=[%s], track=[%s], track setup=[%s]) and "
                    "contender (invocation=[%s], track=[%s], track setup=[%s])" %
                    (r1.trial_timestamp, r1.track, selected_baseline_track_setup,
                     r2.trial_timestamp, r2.track, selected_contender_track_setup))
        # we don't verify anything about the races as it is possible that the user benchmarks two different tracks intentionally
        baseline_store = metrics.EsMetricsStore(self._config)
        baseline_store.open(r1.trial_timestamp, r1.track, selected_baseline_track_setup.name)
        baseline_stats = Stats(baseline_store,
                               stats_sample_size=selected_baseline_track_setup.stats_sample_size,
                               queries=selected_baseline_track_setup.queries,
                               search_sample_size=selected_baseline_track_setup.search_sample_size)

        contender_store = metrics.EsMetricsStore(self._config)
        contender_store.open(r2.trial_timestamp, r2.track, selected_contender_track_setup.name)
        contender_stats = Stats(contender_store,
                                stats_sample_size=selected_contender_track_setup.stats_sample_size,
                                queries=selected_contender_track_setup.queries,
                                search_sample_size=selected_contender_track_setup.search_sample_size)

        print_internal("")
        print_internal("Comparing baseline")
        print_internal("  Race timestamp: %s" % r1.trial_timestamp)
        print_internal("  Track setup: %s" % selected_baseline_track_setup.name)
        print_internal("")
        print_internal("with contender")
        print_internal("  Race timestamp: %s" % r2.trial_timestamp)
        print_internal("  Track setup: %s" % selected_contender_track_setup.name)
        print_internal("")
        print_header("------------------------------------------------------")
        print_header("    _______             __   _____                    ")
        print_header("   / ____(_)___  ____ _/ /  / ___/_________  ________ ")
        print_header("  / /_  / / __ \/ __ `/ /   \__ \/ ___/ __ \/ ___/ _ \\")
        print_header(" / __/ / / / / / /_/ / /   ___/ / /__/ /_/ / /  /  __/")
        print_header("/_/   /_/_/ /_/\__,_/_/   /____/\___/\____/_/   \___/ ")
        print_header("------------------------------------------------------")

        metrics_table = []

        metrics_table += self.report_index_throughput(baseline_stats, contender_stats)
        metrics_table += self.report_merge_part_times(baseline_stats, contender_stats)
        metrics_table += self.report_total_times(baseline_stats, contender_stats)
        metrics_table += self.report_search_latency(baseline_stats, contender_stats)
        metrics_table += self.report_cpu_usage(baseline_stats, contender_stats)
        metrics_table += self.report_gc_times(baseline_stats, contender_stats)
        metrics_table += self.report_disk_usage(baseline_stats, contender_stats)
        metrics_table += self.report_segment_memory(baseline_stats, contender_stats)
        metrics_table += self.report_segment_counts(baseline_stats, contender_stats)
        metrics_table += self.report_stats_latency(baseline_stats, contender_stats)

        print_internal(tabulate.tabulate(metrics_table, headers=["Metric", "Baseline", "Contender", "Diff"], numalign="right", stralign="right"))

    def report_index_throughput(self, baseline_stats, contender_stats):
        if baseline_stats.indexing_throughput and contender_stats.indexing_throughput:
            b_min, b_median, b_max = baseline_stats.indexing_throughput
            c_min, c_median, c_max = contender_stats.indexing_throughput
            return [
                self.line("Min Indexing Throughput [docs/s]", b_min, c_min, treat_increase_as_improvement=True),
                self.line("Median Indexing Throughput [docs/s]", b_median, c_median, treat_increase_as_improvement=True),
                self.line("Max Indexing Throughput [docs/s]", b_max, c_max, treat_increase_as_improvement=True)
            ]
        else:
            return []

    def report_merge_part_times(self, baseline_stats, contender_stats):
        if baseline_stats.has_merge_part_stats() and contender_stats.has_merge_part_stats():
            return [
                self.line("Merge time (postings) [min]", baseline_stats.merge_part_time_postings, contender_stats.merge_part_time_postings,
                          treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
                self.line("Merge time (stored fields) [min]", baseline_stats.merge_part_time_stored_fields,
                          contender_stats.merge_part_time_stored_fields,
                          treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
                self.line("Merge time (doc values) [min]", baseline_stats.merge_part_time_doc_values,
                          contender_stats.merge_part_time_doc_values,
                          treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
                self.line("Merge time (norms) [min]", baseline_stats.merge_part_time_norms,
                          contender_stats.merge_part_time_norms,
                          treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
                self.line("Merge time (vectors) [min]", baseline_stats.merge_part_time_vectors,
                          contender_stats.merge_part_time_vectors,
                          treat_increase_as_improvement=False, formatter=convert.ms_to_minutes)
            ]
        else:
            return []

    def report_total_times(self, baseline_stats, contender_stats):
        return [
            self.line("Indexing time [min]", baseline_stats.total_time, contender_stats.total_time,
                      treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
            self.line("Merge time [min]", baseline_stats.merge_time, contender_stats.merge_time,
                      treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
            self.line("Refresh time [min]", baseline_stats.refresh_time, contender_stats.refresh_time,
                      treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
            self.line("Flush time [min]", baseline_stats.flush_time, contender_stats.flush_time,
                      treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
            self.line("Merge throttle time [min]", baseline_stats.merge_throttle_time, contender_stats.merge_throttle_time,
                      treat_increase_as_improvement=False, formatter=convert.ms_to_minutes)
        ]

    def report_search_latency(self, baseline_stats, contender_stats):
        lines = []
        for query, latency in baseline_stats.query_latencies.items():
            if query in contender_stats.query_latencies:
                for percentile, baseline_value in latency.items():
                    if percentile in contender_stats.query_latencies[query]:
                        contender_value = contender_stats.query_latencies[query][percentile]
                        lines.append(self.line("Query latency %s (%s percentile) [ms]" % (query, percentile),
                                               baseline_value, contender_value, treat_increase_as_improvement=False))
        return lines

    def report_cpu_usage(self, baseline_stats, contender_stats):
        cpu_usage = []
        for phase in track.BenchmarkPhase:
            if phase.name in baseline_stats.median_cpu_usage and phase.name in contender_stats.median_cpu_usage:
                cpu_usage.append(self.line("Median CPU usage (%s) [%%]" % phase.name,
                                           baseline_stats.median_cpu_usage[phase.name], contender_stats.median_cpu_usage[phase.name],
                                           treat_increase_as_improvement=True))
        return cpu_usage

    def report_gc_times(self, baseline_stats, contender_stats):
        return [
            self.line("Total Young Gen GC [s]", baseline_stats.young_gc_time, contender_stats.young_gc_time,
                      treat_increase_as_improvement=False, formatter=convert.ms_to_seconds),
            self.line("Total Old Gen GC [s]", baseline_stats.old_gc_time, contender_stats.old_gc_time,
                      treat_increase_as_improvement=False, formatter=convert.ms_to_seconds)
        ]

    def report_disk_usage(self, baseline_stats, contender_stats):
        if baseline_stats.has_disk_usage_stats() and contender_stats.has_disk_usage_stats():
            return [
                self.line("Index size [GB]", baseline_stats.index_size, contender_stats.index_size,
                          treat_increase_as_improvement=False, formatter=convert.bytes_to_gb),
                self.line("Totally written [GB]", baseline_stats.bytes_written, contender_stats.bytes_written,
                          treat_increase_as_improvement=False, formatter=convert.bytes_to_gb),
            ]
        else:
            return []

    def report_segment_memory(self, baseline_stats, contender_stats):
        if baseline_stats.has_memory_stats() and contender_stats.has_memory_stats():
            return [
                self.line("Heap used for segments [MB]", baseline_stats.memory_segments, contender_stats.memory_segments,
                          treat_increase_as_improvement=False, formatter=convert.bytes_to_mb),
                self.line("Heap used for doc values [MB]", baseline_stats.memory_doc_values, contender_stats.memory_doc_values,
                          treat_increase_as_improvement=False, formatter=convert.bytes_to_mb),
                self.line("Heap used for terms [MB]", baseline_stats.memory_terms, contender_stats.memory_terms,
                          treat_increase_as_improvement=False, formatter=convert.bytes_to_mb),
                self.line("Heap used for norms [MB]", baseline_stats.memory_norms, contender_stats.memory_norms,
                          treat_increase_as_improvement=False, formatter=convert.bytes_to_mb),
                self.line("Heap used for points [MB]", baseline_stats.memory_points, contender_stats.memory_points,
                          treat_increase_as_improvement=False, formatter=convert.bytes_to_mb),
                self.line("Heap used for points [MB]", baseline_stats.memory_stored_fields, contender_stats.memory_stored_fields,
                          treat_increase_as_improvement=False, formatter=convert.bytes_to_mb)
            ]
        else:
            return []

    def report_segment_counts(self, baseline_stats, contender_stats):
        if baseline_stats.segment_count and contender_stats.segment_count:
            return [
                self.line("Segment count", baseline_stats.segment_count, contender_stats.segment_count, treat_increase_as_improvement=False)
            ]
        else:
            return []

    def report_stats_latency(self, baseline_stats, contender_stats):
        lines = []
        if baseline_stats.indices_stats_latency and contender_stats.indices_stats_latency:
            for percentile, baseline_value in baseline_stats.indices_stats_latency.items():
                if percentile in contender_stats.indices_stats_latency:
                    contender_value = contender_stats.indices_stats_latency[percentile]
                    lines.append(self.line("Indices Stats(%s percentile) [ms]" % percentile, baseline_value, contender_value,
                                           treat_increase_as_improvement=False))

        if baseline_stats.node_stats_latency and contender_stats.node_stats_latency:
            for percentile, baseline_value in baseline_stats.node_stats_latency.items():
                if percentile in contender_stats.node_stats_latency:
                    contender_value = contender_stats.node_stats_latency[percentile]
                    lines.append(self.line("Nodes Stats(%s percentile) [ms]" % percentile, baseline_value, contender_value,
                                           treat_increase_as_improvement=False))
        return lines

    def track_setup(self, race, comparison_type, user_defined_track_setup):
        if user_defined_track_setup:
            for track_setup in race.track_setups:
                if user_defined_track_setup == track_setup.name:
                    return track_setup
            raise exceptions.ImproperlyConfigured("Specified track setup [%s] for %s is not among the available track setups %s."
                                                  % (user_defined_track_setup, comparison_type, race.track_setups))
        else:
            if len(race.track_setups) == 1:
                return race.track_setups[0]
            else:
                raise exceptions.ImproperlyConfigured("Cannot autodetect correct track setup for %s. Please specify one on the command "
                                                      "line with the parameter %s-track-setup. Possible values are: %s" %
                                                      (comparison_type, comparison_type, race.track_setups))

    def line(self, metric, baseline, contender, treat_increase_as_improvement, formatter=lambda x: x):
        if baseline is not None and contender is not None:
            return [metric, formatter(baseline), formatter(contender),
                    self.diff(baseline, contender, treat_increase_as_improvement, formatter)]
        else:
            return []

    def diff(self, baseline, contender, treat_increase_as_improvement, formatter=lambda x: x):
        diff = formatter(contender - baseline)
        if treat_increase_as_improvement:
            color_greater = green
            color_smaller = red
        else:
            color_greater = red
            color_smaller = green

        if diff > 0:
            return color_greater("+%.5f" % diff)
        elif diff < 0:
            return color_smaller("%.5f" % diff)
        else:
            # tabulate needs this to align all values correctly
            return neutral("%.5f" % diff)
