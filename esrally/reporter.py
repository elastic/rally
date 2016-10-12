import collections
import csv
import io
import logging

import tabulate

from esrally import metrics, exceptions
from esrally.utils import convert, io as rio, console

logger = logging.getLogger("rally.reporting")

MEDIAN = "50.0"


def summarize(cfg, track):
    SummaryReporter(cfg).report(track)


def compare(cfg):
    baseline_ts = cfg.opts("report", "comparison.baseline.timestamp")
    contender_ts = cfg.opts("report", "comparison.contender.timestamp")

    if not baseline_ts or not contender_ts:
        raise exceptions.SystemSetupError("compare needs baseline and a contender")
    race_store = metrics.race_store(cfg)
    ComparisonReporter(cfg).report(
        race_store.find_by_timestamp(baseline_ts),
        race_store.find_by_timestamp(contender_ts))


def print_internal(message):
    console.println(message, logger=logger.info)


def print_header(message):
    print_internal(console.format.bold(message))


class Stats:
    def __init__(self, store, challenge):
        self.op_metrics = collections.OrderedDict()
        for tasks in challenge.schedule:
            for task in tasks:
                op = task.operation.name
                self.op_metrics[op] = {}
                self.op_metrics[op]["throughput"] = self.summary_stats(store, "throughput", op)
                self.op_metrics[op]["latency"] = self.single_latency(store, op)
                self.op_metrics[op]["service_time"] = self.single_latency(store, op, metric_name="service_time")

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
        self.merge_part_time_points = store.get_one("merge_parts_total_time_points")
        self.query_latencies = collections.OrderedDict()

        self.median_cpu_usage = self.median(store, "cpu_utilization_1s", sample_type=metrics.SampleType.Normal)
        self.young_gc_time = store.get_one("node_total_young_gen_gc_time")
        self.old_gc_time = store.get_one("node_total_old_gen_gc_time")

        self.memory_segments = store.get_one("segments_memory_in_bytes")
        self.memory_doc_values = store.get_one("segments_doc_values_memory_in_bytes")
        self.memory_terms = store.get_one("segments_terms_memory_in_bytes")
        self.memory_norms = store.get_one("segments_norms_memory_in_bytes")
        self.memory_points = store.get_one("segments_points_memory_in_bytes")
        self.memory_stored_fields = store.get_one("segments_stored_fields_memory_in_bytes")

        self.index_size = store.get_one("final_index_size_bytes")
        self.bytes_written = store.get_one("disk_io_write_bytes")

        self.segment_count = store.get_one("segments_count")

    def summary_stats(self, store, metric_name, operation_name):
        percentiles = store.get_percentiles(metric_name,
                                            operation=operation_name,
                                            sample_type=metrics.SampleType.Normal,
                                            percentiles=[MEDIAN])
        unit = store.get_unit(metric_name, operation=operation_name)
        stats = store.get_stats(metric_name, operation=operation_name, sample_type=metrics.SampleType.Normal)
        if percentiles and stats:
            return stats["min"], percentiles[MEDIAN], stats["max"], unit
        else:
            return None, None, None, unit

    def has_merge_part_stats(self):
        return self.merge_part_time_postings or \
               self.merge_part_time_stored_fields or \
               self.memory_doc_values or \
               self.merge_part_time_norms or \
               self.merge_part_time_vectors or \
               self.merge_part_time_points

    def has_memory_stats(self):
        return self.memory_segments is not None and \
               self.memory_doc_values is not None and \
               self.memory_terms is not None and \
               self.memory_norms is not None and \
               self.memory_points is not None and \
               self.memory_stored_fields is not None

    def has_disk_usage_stats(self):
        return self.index_size and self.bytes_written

    def median(self, store, metric_name, operation_name=None, operation_type=None, sample_type=None):
        percentiles = store.get_percentiles(metric_name,
                                            operation=operation_name,
                                            operation_type=operation_type,
                                            sample_type=sample_type,
                                            percentiles=[MEDIAN])
        if percentiles:
            return percentiles[MEDIAN]
        else:
            return None

    def single_latency(self, store, operation, metric_name="latency"):
        sample_type = metrics.SampleType.Normal
        sample_size = store.get_count(metric_name, operation=operation, sample_type=sample_type)
        if sample_size > 0:
            return store.get_percentiles(metric_name,
                                         operation=operation,
                                         sample_type=sample_type,
                                         percentiles=self.percentiles_for_sample_size(sample_size))
        else:
            return {}

    def percentiles_for_sample_size(self, sample_size):
        # if needed we can come up with something smarter but it'll do for now
        if sample_size < 1:
            raise AssertionError("Percentiles require at least one sample")
        elif sample_size == 1:
            return [100]
        elif 1 < sample_size < 10:
            return [50.0, 100]
        elif 10 <= sample_size < 100:
            return [50.0, 90.0, 100]
        elif 100 <= sample_size < 1000:
            return [50.0, 90.0, 99.0, 100]
        elif 1000 <= sample_size < 10000:
            return [50.0, 90.0, 99.0, 99.9, 100]
        else:
            return [50.0, 90.0, 99.0, 99.9, 99.99, 100]


class SummaryReporter:
    def __init__(self, config):
        self._config = config

    def report(self, t):
        print_internal("")
        print_header("------------------------------------------------------")
        print_header("    _______             __   _____                    ")
        print_header("   / ____(_)___  ____ _/ /  / ___/_________  ________ ")
        print_header("  / /_  / / __ \/ __ `/ /   \__ \/ ___/ __ \/ ___/ _ \\")
        print_header(" / __/ / / / / / /_/ / /   ___/ / /__/ /_/ / /  /  __/")
        print_header("/_/   /_/_/ /_/\__,_/_/   /____/\___/\____/_/   \___/ ")
        print_header("------------------------------------------------------")
        print_internal("")

        selected_challenge = self._config.opts("benchmarks", "challenge")
        for challenge in t.challenges:
            if challenge.name == selected_challenge:
                store = metrics.metrics_store(self._config)
                stats = Stats(store, challenge)

                metrics_table = []
                meta_info_table = []
                metrics_table += self.report_total_times(stats)
                metrics_table += self.report_merge_part_times(stats)

                metrics_table += self.report_cpu_usage(stats)
                metrics_table += self.report_gc_times(stats)

                metrics_table += self.report_disk_usage(stats)
                metrics_table += self.report_segment_memory(stats)
                metrics_table += self.report_segment_counts(stats)

                for tasks in challenge.schedule:
                    for task in tasks:
                        metrics_table += self.report_throughput(stats, task.operation)
                        metrics_table += self.report_latency(stats, task.operation)
                        metrics_table += self.report_service_time(stats, task.operation)

                meta_info_table += self.report_meta_info()

                self.write_report(metrics_table, meta_info_table)

    def write_report(self, metrics_table, meta_info_table):
        report_format = self._config.opts("report", "reportformat")
        report_file = self._config.opts("report", "reportfile")

        if len(report_file) > 0:
            meta_info_file = "%s.meta" % report_file
        else:
            meta_info_file = report_file

        self.write_single_report(report_format, report_file, headers=["Metric", "Operation", "Value", "Unit"], data=metrics_table)
        self.write_single_report(report_format, meta_info_file, headers=["Name", "Value"], data=meta_info_table,
                                 force_cmd_line_output=False)

    def write_single_report(self, report_format, report_file, headers, data, force_cmd_line_output=True):
        if report_format == "markdown":
            report = tabulate.tabulate(data, headers=headers, tablefmt="pipe", numalign="right", stralign="right")
        elif report_format == "csv":
            with io.StringIO() as out:
                writer = csv.writer(out)
                writer.writerow(headers)
                for metric_record in data:
                    writer.writerow(metric_record)
                report = out.getvalue()
        else:
            raise exceptions.SystemSetupError("Unknown report format '%s'" % report_format)

        if force_cmd_line_output:
            print_internal(report)
        if len(report_file) > 0:
            normalized_report_file = rio.normalize_path(report_file)
            logger.info("Writing report to [%s] (user specified: [%s]) in format [%s]" %
                        (normalized_report_file, report_file, report_format))
            if force_cmd_line_output:
                print("\nWriting report also to '%s'" % normalized_report_file)
            # ensure that the parent folder already exists when we try to write the file...
            rio.ensure_dir(rio.dirname(normalized_report_file))
            with open(normalized_report_file, mode="w", encoding="UTF-8") as f:
                f.writelines(report)

    def report_throughput(self, stats, operation):
        min, median, max, unit = stats.op_metrics[operation.name]["throughput"]
        return [
            ["Min Throughput", operation.name, min, unit],
            ["Median Throughput", operation.name, median, unit],
            ["Max Throughput", operation.name, max, unit]
        ]

    def report_latency(self, stats, operation):
        lines = []
        latency = stats.op_metrics[operation.name]["latency"]
        for percentile, value in latency.items():
            lines.append(["%sth percentile latency" % percentile, operation.name, value, "ms"])
        return lines

    def report_service_time(self, stats, operation):
        lines = []
        service_time = stats.op_metrics[operation.name]["service_time"]
        for percentile, value in service_time.items():
            lines.append(["%sth percentile service time" % percentile, operation.name, value, "ms"])
        return lines

    def report_total_times(self, stats):
        total_times = []
        unit = "min"
        self.append_if_present(total_times, "Indexing time", "", stats.total_time, unit, convert.ms_to_minutes)
        self.append_if_present(total_times, "Merge time", "", stats.merge_time, unit, convert.ms_to_minutes)
        self.append_if_present(total_times, "Refresh time", "", stats.refresh_time, unit, convert.ms_to_minutes)
        self.append_if_present(total_times, "Flush time", "", stats.flush_time, unit, convert.ms_to_minutes)
        self.append_if_present(total_times, "Merge throttle time", "", stats.merge_throttle_time, unit, convert.ms_to_minutes)

        return total_times

    def append_if_present(self, l, k, operation, v, unit, converter=lambda x: x):
        if v:
            l.append([k, operation, converter(v), unit])

    def report_merge_part_times(self, stats):
        # note that these times are not(!) wall clock time results but total times summed up over multiple threads
        merge_part_times = []
        unit = "min"
        self.append_if_present(merge_part_times, "Merge time (postings)", "", stats.merge_part_time_postings, unit, convert.ms_to_minutes)
        self.append_if_present(merge_part_times, "Merge time (stored fields)", "", stats.merge_part_time_stored_fields, unit,
                               convert.ms_to_minutes)
        self.append_if_present(merge_part_times, "Merge time (doc values)", "", stats.merge_part_time_doc_values, unit,
                               convert.ms_to_minutes)
        self.append_if_present(merge_part_times, "Merge time (norms)", "", stats.merge_part_time_norms, unit, convert.ms_to_minutes)
        self.append_if_present(merge_part_times, "Merge time (vectors)", "", stats.merge_part_time_vectors, unit, convert.ms_to_minutes)
        self.append_if_present(merge_part_times, "Merge time (points)", "", stats.merge_part_time_points, unit, convert.ms_to_minutes)
        return merge_part_times

    def report_cpu_usage(self, stats):
        cpu_usage = []
        self.append_if_present(cpu_usage, "Median CPU usage", "", stats.median_cpu_usage, "%")
        return cpu_usage

    def report_gc_times(self, stats):
        return [
            ["Total Young Gen GC", "", convert.ms_to_seconds(stats.young_gc_time), "s"],
            ["Total Old Gen GC", "", convert.ms_to_seconds(stats.old_gc_time), "s"]
        ]

    def report_disk_usage(self, stats):
        if stats.has_disk_usage_stats():
            return [
                ["Index size", "", convert.bytes_to_gb(stats.index_size), "GB"],
                ["Totally written", "", convert.bytes_to_gb(stats.bytes_written), "GB"]
            ]
        else:
            return []

    def report_segment_memory(self, stats):
        memory_stats = []
        unit = "MB"
        self.append_if_present(memory_stats, "Heap used for segments", "", stats.memory_segments, unit, convert.bytes_to_mb)
        self.append_if_present(memory_stats, "Heap used for doc values", "", stats.memory_doc_values, unit, convert.bytes_to_mb)
        self.append_if_present(memory_stats, "Heap used for terms", "", stats.memory_terms, unit, convert.bytes_to_mb)
        self.append_if_present(memory_stats, "Heap used for norms", "", stats.memory_norms, unit, convert.bytes_to_mb)
        self.append_if_present(memory_stats, "Heap used for points", "", stats.memory_points, unit, convert.bytes_to_mb)
        self.append_if_present(memory_stats, "Heap used for stored fields", "", stats.memory_stored_fields, unit, convert.bytes_to_mb)
        return memory_stats

    def report_segment_counts(self, stats):
        if stats.segment_count:
            return [["Segment count", "", stats.segment_count, ""]]
        else:
            return []

    def report_meta_info(self):
        return [
            ["Elasticsearch source revision", self._config.opts("meta", "source.revision", mandatory=False, default_value="unknown")]
        ]


class ComparisonReporter:
    def __init__(self, config):
        self._config = config

    def report(self, r1, r2):
        logger.info("Generating comparison report for baseline (invocation=[%s], track=[%s], challenge=[%s], car=[%s]) and "
                    "contender (invocation=[%s], track=[%s], challenge=[%s], car=[%s])" %
                    (r1.trial_timestamp, r1.track, r1.challenge, r1.car,
                     r2.trial_timestamp, r2.track, r2.challenge, r2.car))
        # we don't verify anything about the races as it is possible that the user benchmarks two different tracks intentionally
        baseline_store = metrics.metrics_store(self._config,
                                               invocation=r1.trial_timestamp, track=r1.track, challenge=r1.challenge.name, car=r1.car)
        baseline_stats = Stats(baseline_store, r1.challenge)

        contender_store = metrics.metrics_store(self._config,
                                                invocation=r2.trial_timestamp, track=r2.track, challenge=r2.challenge.name, car=r2.car)
        contender_stats = Stats(contender_store, r2.challenge)

        print_internal("")
        print_internal("Comparing baseline")
        print_internal("  Race timestamp: %s" % r1.trial_timestamp)
        print_internal("  Challenge: %s" % r1.challenge.name)
        print_internal("  Car: %s" % r1.car)
        print_internal("")
        print_internal("with contender")
        print_internal("  Race timestamp: %s" % r2.trial_timestamp)
        print_internal("  Challenge: %s" % r2.challenge.name)
        print_internal("  Car: %s" % r2.car)
        print_internal("")
        print_header("------------------------------------------------------")
        print_header("    _______             __   _____                    ")
        print_header("   / ____(_)___  ____ _/ /  / ___/_________  ________ ")
        print_header("  / /_  / / __ \/ __ `/ /   \__ \/ ___/ __ \/ ___/ _ \\")
        print_header(" / __/ / / / / / /_/ / /   ___/ / /__/ /_/ / /  /  __/")
        print_header("/_/   /_/_/ /_/\__,_/_/   /____/\___/\____/_/   \___/ ")
        print_header("------------------------------------------------------")
        print_internal("")

        metrics_table = []
        metrics_table += self.report_total_times(baseline_stats, contender_stats)
        metrics_table += self.report_merge_part_times(baseline_stats, contender_stats)

        # metrics_table += self.report_cpu_usage(baseline_stats, contender_stats)
        metrics_table += self.report_gc_times(baseline_stats, contender_stats)

        metrics_table += self.report_disk_usage(baseline_stats, contender_stats)
        metrics_table += self.report_segment_memory(baseline_stats, contender_stats)
        metrics_table += self.report_segment_counts(baseline_stats, contender_stats)

        for t1 in r1.challenge.schedule:
            for t2 in r2.challenge.schedule:
                # only report matching metrics
                if t1.operation.name == t2.operation.name:
                    metrics_table += self.report_throughput(baseline_stats, contender_stats, t1.operation)
                    metrics_table += self.report_latency(baseline_stats, contender_stats, t1.operation)
                    metrics_table += self.report_service_time(baseline_stats, contender_stats, t1.operation)

        print_internal(tabulate.tabulate(metrics_table,
                                         headers=["Metric", "Operation", "Baseline", "Contender", "Diff", "Unit"],
                                         numalign="right", stralign="right"))

    def report_throughput(self, baseline_stats, contender_stats, operation):
        b_min, b_median, b_max, b_unit = baseline_stats.op_metrics[operation.name]["throughput"]
        c_min, c_median, c_max, c_unit = contender_stats.op_metrics[operation.name]["throughput"]
        return [
            self.line("Min Throughput", b_min, c_min, operation, b_unit, treat_increase_as_improvement=True),
            self.line("Median Throughput", b_median, c_median, operation, b_unit, treat_increase_as_improvement=True),
            self.line("Max Throughput", b_max, c_max, operation, b_unit, treat_increase_as_improvement=True)
        ]

    def report_latency(self, baseline_stats, contender_stats, operation):
        lines = []

        baseline_latency = baseline_stats.op_metrics[operation.name]["latency"]
        contender_latency = contender_stats.op_metrics[operation.name]["latency"]

        for percentile, baseline_value in baseline_latency.items():
            if percentile in contender_latency:
                contender_value = contender_latency[percentile]
                lines.append(self.line("%sth percentile latency" % percentile, baseline_value, contender_value,
                                       operation, "ms", treat_increase_as_improvement=False))
        return lines

    def report_service_time(self, baseline_stats, contender_stats, operation):
        lines = []

        baseline_service_time = baseline_stats.op_metrics[operation.name]["service_time"]
        contender_service_time = contender_stats.op_metrics[operation.name]["service_time"]

        for percentile, baseline_value in baseline_service_time.items():
            if percentile in contender_service_time:
                contender_value = contender_service_time[percentile]
                lines.append(self.line("%sth percentile service time" % percentile, baseline_value, contender_value,
                                       operation, "ms", treat_increase_as_improvement=False))
        return lines

    def report_merge_part_times(self, baseline_stats, contender_stats):
        lines = []
        if baseline_stats.has_merge_part_stats() and contender_stats.has_merge_part_stats():
            self.append_if_present(lines,
                                   self.line("Merge time (postings)", baseline_stats.merge_part_time_postings,
                                             contender_stats.merge_part_time_postings,
                                             "", "min", treat_increase_as_improvement=False, formatter=convert.ms_to_minutes))
            self.append_if_present(lines,
                                   self.line("Merge time (stored fields)", baseline_stats.merge_part_time_stored_fields,
                                             contender_stats.merge_part_time_stored_fields,
                                             "", "min", treat_increase_as_improvement=False, formatter=convert.ms_to_minutes))
            self.append_if_present(lines,
                                   self.line("Merge time (doc values)", baseline_stats.merge_part_time_doc_values,
                                             contender_stats.merge_part_time_doc_values,
                                             "", "min", treat_increase_as_improvement=False, formatter=convert.ms_to_minutes))
            self.append_if_present(lines,
                                   self.line("Merge time (norms)", baseline_stats.merge_part_time_norms,
                                             contender_stats.merge_part_time_norms,
                                             "", "min", treat_increase_as_improvement=False, formatter=convert.ms_to_minutes))
            self.append_if_present(lines,
                                   self.line("Merge time (vectors)", baseline_stats.merge_part_time_vectors,
                                             contender_stats.merge_part_time_vectors,
                                             "", "min", treat_increase_as_improvement=False, formatter=convert.ms_to_minutes))
        return lines

    def append_if_present(self, l, v):
        if v and len(v) > 0:
            l.append(v)

    def report_total_times(self, baseline_stats, contender_stats):
        return [
            self.line("Indexing time", baseline_stats.total_time, contender_stats.total_time, "", "min",
                      treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
            self.line("Merge time", baseline_stats.merge_time, contender_stats.merge_time, "", "min",
                      treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
            self.line("Refresh time", baseline_stats.refresh_time, contender_stats.refresh_time, "", "min",
                      treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
            self.line("Flush time", baseline_stats.flush_time, contender_stats.flush_time, "", "min",
                      treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
            self.line("Merge throttle time", baseline_stats.merge_throttle_time, contender_stats.merge_throttle_time, "", "min",
                      treat_increase_as_improvement=False, formatter=convert.ms_to_minutes)
        ]

    # def report_cpu_usage(self, baseline_stats, contender_stats):
    #     cpu_usage = []
    #     for op, v in baseline_stats.median_cpu_usage.items():
    #         if op in contender_stats.median_cpu_usage:
    #             cpu_usage.append(self.line("Median CPU usage", baseline_stats.median_cpu_usage[op], contender_stats.median_cpu_usage[op],
    #                                        op, "%", treat_increase_as_improvement=True))
    #     return cpu_usage

    def report_gc_times(self, baseline_stats, contender_stats):
        return [
            self.line("Total Young Gen GC", baseline_stats.young_gc_time, contender_stats.young_gc_time, "", "s",
                      treat_increase_as_improvement=False, formatter=convert.ms_to_seconds),
            self.line("Total Old Gen GC", baseline_stats.old_gc_time, contender_stats.old_gc_time, "", "s",
                      treat_increase_as_improvement=False, formatter=convert.ms_to_seconds)
        ]

    def report_disk_usage(self, baseline_stats, contender_stats):
        if baseline_stats.has_disk_usage_stats() and contender_stats.has_disk_usage_stats():
            return [
                self.line("Index size", baseline_stats.index_size, contender_stats.index_size, "", "GB",
                          treat_increase_as_improvement=False, formatter=convert.bytes_to_gb),
                self.line("Totally written", baseline_stats.bytes_written, contender_stats.bytes_written, "", "GB",
                          treat_increase_as_improvement=False, formatter=convert.bytes_to_gb),
            ]
        else:
            return []

    def report_segment_memory(self, baseline_stats, contender_stats):
        if baseline_stats.has_memory_stats() and contender_stats.has_memory_stats():
            return [
                self.line("Heap used for segments", baseline_stats.memory_segments, contender_stats.memory_segments, "", "MB",
                          treat_increase_as_improvement=False, formatter=convert.bytes_to_mb),
                self.line("Heap used for doc values", baseline_stats.memory_doc_values, contender_stats.memory_doc_values, "", "MB",
                          treat_increase_as_improvement=False, formatter=convert.bytes_to_mb),
                self.line("Heap used for terms", baseline_stats.memory_terms, contender_stats.memory_terms, "", "MB",
                          treat_increase_as_improvement=False, formatter=convert.bytes_to_mb),
                self.line("Heap used for norms", baseline_stats.memory_norms, contender_stats.memory_norms, "", "MB",
                          treat_increase_as_improvement=False, formatter=convert.bytes_to_mb),
                self.line("Heap used for points", baseline_stats.memory_points, contender_stats.memory_points, "", "MB",
                          treat_increase_as_improvement=False, formatter=convert.bytes_to_mb),
                self.line("Heap used for points", baseline_stats.memory_stored_fields, contender_stats.memory_stored_fields, "", "MB",
                          treat_increase_as_improvement=False, formatter=convert.bytes_to_mb)
            ]
        else:
            return []

    def report_segment_counts(self, baseline_stats, contender_stats):
        if baseline_stats.segment_count and contender_stats.segment_count:
            return [
                self.line("Segment count", baseline_stats.segment_count, contender_stats.segment_count,
                          "", "", treat_increase_as_improvement=False)
            ]
        else:
            return []

    def line(self, metric, baseline, contender, operation, unit, treat_increase_as_improvement, formatter=lambda x: x):
        if baseline is not None and contender is not None:
            return [metric, str(operation), formatter(baseline), formatter(contender),
                    self.diff(baseline, contender, treat_increase_as_improvement, formatter), unit]
        else:
            return []

    def diff(self, baseline, contender, treat_increase_as_improvement, formatter=lambda x: x):
        diff = formatter(contender - baseline)
        if treat_increase_as_improvement:
            color_greater = console.format.green
            color_smaller = console.format.red
        else:
            color_greater = console.format.red
            color_smaller = console.format.green

        if diff > 0:
            return color_greater("+%.5f" % diff)
        elif diff < 0:
            return color_smaller("%.5f" % diff)
        else:
            # tabulate needs this to align all values correctly
            return console.format.neutral("%.5f" % diff)
