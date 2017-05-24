import collections
import csv
import io
import logging

import tabulate
from esrally import metrics, exceptions
from esrally.utils import convert, io as rio, console

logger = logging.getLogger("rally.reporting")


def calculate_results(metrics_store, race, lap=None):
    calc = StatsCalculator(metrics_store, race.challenge, lap)
    return calc()


def summarize(race, cfg, lap=None):
    logger.info("Summarizing results.")
    results = race.results_of_lap_number(lap) if lap else race.results
    SummaryReporter(results, cfg, race.revision, lap, race.total_laps).report()


def compare(cfg):
    baseline_ts = cfg.opts("reporting", "baseline.timestamp")
    contender_ts = cfg.opts("reporting", "contender.timestamp")

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


def write_single_report(report_file, report_format, cwd, headers, data_plain, data_rich, write_header=True, show_also_in_console=True):
    if report_format == "markdown":
        formatter = format_as_markdown
    elif report_format == "csv":
        formatter = format_as_csv
    else:
        raise exceptions.SystemSetupError("Unknown report format '%s'" % report_format)

    if show_also_in_console:
        print_internal(formatter(headers, data_rich))
    if len(report_file) > 0:
        normalized_report_file = rio.normalize_path(report_file, cwd)
        logger.info("Writing report to [%s] (user specified: [%s]) in format [%s]" %
                    (normalized_report_file, report_file, report_format))
        # ensure that the parent folder already exists when we try to write the file...
        rio.ensure_dir(rio.dirname(normalized_report_file))
        with open(normalized_report_file, mode="a+", encoding="UTF-8") as f:
            f.writelines(formatter(headers, data_plain, write_header))


def format_as_markdown(headers, data, write_header=True):
    rendered = tabulate.tabulate(data, headers=headers, tablefmt="pipe", numalign="right", stralign="right")
    if write_header:
        return rendered + "\n"
    else:
        # remove all header data (it's not possible to do so entirely with tabulate directly...)
        return "\n".join(rendered.splitlines()[2:]) + "\n"


def format_as_csv(headers, data, write_header=True):
    with io.StringIO() as out:
        writer = csv.writer(out)
        if write_header:
            writer.writerow(headers)
        for metric_record in data:
            writer.writerow(metric_record)
        return out.getvalue()


class StatsCalculator:
    def __init__(self, store, challenge, lap=None):
        self.store = store
        self.challenge = challenge
        self.lap = lap

    def __call__(self):
        result = Stats()

        for tasks in self.challenge.schedule:
            for task in tasks:
                op = task.operation.name
                logger.debug("Gathering request metrics for [%s]." % op)
                result.add_op_metrics(
                    op,
                    self.summary_stats("throughput", op),
                    self.single_latency(op),
                    self.single_latency(op, metric_name="service_time"),
                    self.error_rate(op)
                )

        logger.debug("Gathering indexing metrics.")
        result.total_time = self.sum("indexing_total_time")
        result.merge_time = self.sum("merges_total_time")
        result.refresh_time = self.sum("refresh_total_time")
        result.flush_time = self.sum("flush_total_time")
        result.merge_throttle_time = self.sum("merges_total_throttled_time")

        logger.debug("Gathering merge part metrics.")
        result.merge_part_time_postings = self.sum("merge_parts_total_time_postings")
        result.merge_part_time_stored_fields = self.sum("merge_parts_total_time_stored_fields")
        result.merge_part_time_doc_values = self.sum("merge_parts_total_time_doc_values")
        result.merge_part_time_norms = self.sum("merge_parts_total_time_norms")
        result.merge_part_time_vectors = self.sum("merge_parts_total_time_vectors")
        result.merge_part_time_points = self.sum("merge_parts_total_time_points")

        logger.debug("Gathering CPU usage metrics.")
        result.median_cpu_usage = self.median("cpu_utilization_1s", sample_type=metrics.SampleType.Normal)

        logger.debug("Gathering garbage collection metrics.")
        result.young_gc_time = self.sum("node_total_young_gen_gc_time")
        result.old_gc_time = self.sum("node_total_old_gen_gc_time")

        logger.debug("Gathering segment memory metrics.")
        result.memory_segments = self.median("segments_memory_in_bytes")
        result.memory_doc_values = self.median("segments_doc_values_memory_in_bytes")
        result.memory_terms = self.median("segments_terms_memory_in_bytes")
        result.memory_norms = self.median("segments_norms_memory_in_bytes")
        result.memory_points = self.median("segments_points_memory_in_bytes")
        result.memory_stored_fields = self.median("segments_stored_fields_memory_in_bytes")

        # This metric will only be written for the last iteration (as it can only be determined after the cluster has been shut down)
        logger.debug("Gathering disk metrics.")
        result.index_size = self.one("final_index_size_bytes")
        result.bytes_written = self.sum("disk_io_write_bytes")

        # convert to int, fraction counts are senseless
        median_segment_count = self.median("segments_count")
        result.segment_count = int(median_segment_count) if median_segment_count is not None else median_segment_count
        return result

    def sum(self, metric_name):
        values = self.store.get(metric_name, lap=self.lap)
        if values:
            return sum(values)
        else:
            return None

    def one(self, metric_name):
        return self.store.get_one(metric_name, lap=self.lap)

    def summary_stats(self, metric_name, operation_name):
        median = self.store.get_median(metric_name, operation=operation_name, sample_type=metrics.SampleType.Normal, lap=self.lap)
        unit = self.store.get_unit(metric_name, operation=operation_name)
        stats = self.store.get_stats(metric_name, operation=operation_name, sample_type=metrics.SampleType.Normal, lap=self.lap)
        if median and stats:
            return {
                "min": stats["min"],
                "median": median,
                "max": stats["max"],
                "unit": unit
            }
        else:
            return {
                "min": None,
                "median": None,
                "max": None,
                "unit": unit
            }

    def error_rate(self, operation_name):
        return self.store.get_error_rate(operation=operation_name, sample_type=metrics.SampleType.Normal, lap=self.lap)

    def median(self, metric_name, operation_name=None, operation_type=None, sample_type=None):
        return self.store.get_median(metric_name, operation=operation_name, operation_type=operation_type, sample_type=sample_type,
                                     lap=self.lap)

    def single_latency(self, operation, metric_name="latency"):
        sample_type = metrics.SampleType.Normal
        sample_size = self.store.get_count(metric_name, operation=operation, sample_type=sample_type, lap=self.lap)
        if sample_size > 0:
            percentiles = self.store.get_percentiles(metric_name,
                                                     operation=operation,
                                                     sample_type=sample_type,
                                                     percentiles=self.percentiles_for_sample_size(sample_size),
                                                     lap=self.lap)
            # safely encode so we don't have any dots in field names
            safe_percentiles = collections.OrderedDict()
            for k, v in percentiles.items():
                safe_percentiles[self.safe_float_key(k)] = v
            return safe_percentiles
        else:
            return {}

    def safe_float_key(self, k):
        return str(k).replace(".", "_")

    def percentiles_for_sample_size(self, sample_size):
        # if needed we can come up with something smarter but it'll do for now
        if sample_size < 1:
            raise AssertionError("Percentiles require at least one sample")
        elif sample_size == 1:
            return [100]
        elif 1 < sample_size < 10:
            return [50, 100]
        elif 10 <= sample_size < 100:
            return [50, 90, 100]
        elif 100 <= sample_size < 1000:
            return [50, 90, 99, 100]
        elif 1000 <= sample_size < 10000:
            return [50, 90, 99, 99.9, 100]
        else:
            return [50, 90, 99, 99.9, 99.99, 100]


class Stats:
    def __init__(self, d=None):
        self.op_metrics = self.v(d, "op_metrics", default=[])
        self.total_time = self.v(d, "total_time")
        self.merge_time = self.v(d, "merge_time")
        self.refresh_time = self.v(d, "refresh_time")
        self.flush_time = self.v(d, "flush_time")
        self.merge_throttle_time = self.v(d, "merge_throttle_time")

        self.merge_part_time_postings = self.v(d, "merge_part_time_postings")
        self.merge_part_time_stored_fields = self.v(d, "merge_part_time_stored_fields")
        self.merge_part_time_doc_values = self.v(d, "merge_part_time_doc_values")
        self.merge_part_time_norms = self.v(d, "merge_part_time_norms")
        self.merge_part_time_vectors = self.v(d, "merge_part_time_vectors")
        self.merge_part_time_points = self.v(d, "merge_part_time_points")

        self.median_cpu_usage = self.v(d, "median_cpu_usage")

        self.young_gc_time = self.v(d, "young_gc_time")
        self.old_gc_time = self.v(d, "old_gc_time")

        self.memory_segments = self.v(d, "memory_segments")
        self.memory_doc_values = self.v(d, "memory_doc_values")
        self.memory_terms = self.v(d, "memory_terms")
        self.memory_norms = self.v(d, "memory_norms")
        self.memory_points = self.v(d, "memory_points")
        self.memory_stored_fields = self.v(d, "memory_stored_fields")

        self.index_size = self.v(d, "index_size")
        self.bytes_written = self.v(d, "bytes_written")

        self.segment_count = self.v(d, "segment_count")

    def as_dict(self):
        return self.__dict__

    def as_flat_list(self):
        all_results = []
        for metric, value in self.as_dict().items():
            if metric == "op_metrics":
                for item in value:
                    if "throughput" in item:
                        all_results.append({"operation": item["operation"], "name": "throughput", "value": item["throughput"]})
                    if "latency" in item:
                        all_results.append({"operation": item["operation"], "name": "latency", "value": item["latency"]})
                    if "service_time" in item:
                        all_results.append({"operation": item["operation"], "name": "service_time", "value": item["service_time"]})
                    if "error_rate" in item:
                        all_results.append({"operation": item["operation"], "name": "error_rate", "value": {"single": item["error_rate"]}})
            elif value is not None:
                result = {
                    "name": metric,
                    "value": {
                        "single": value
                    }
                }
                all_results.append(result)
        # sorting is just necessary to have a stable order for tests. As we just have a small number of metrics, the overhead is neglible.
        return sorted(all_results, key=lambda m: m["name"])

    def v(self, d, k, default=None):
        return d.get(k, default) if d else default

    def add_op_metrics(self, operation, throughput, latency, service_time, error_rate):
        self.op_metrics.append({
            "operation": operation,
            "throughput": throughput,
            "latency": latency,
            "service_time": service_time,
            "error_rate": error_rate
        })

    def operations(self):
        return [v["operation"] for v in self.op_metrics]

    def metrics(self, operation):
        for r in self.op_metrics:
            if r["operation"] == operation:
                return r
        return None

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


class SummaryReporter:
    def __init__(self, results, config, revision, current_lap, total_laps):
        self.results = results
        self._config = config
        self.revision = revision
        self.current_lap = current_lap
        self.total_laps = total_laps

    def is_final_report(self):
        return self.current_lap is None

    def needs_header(self):
        return self.total_laps == 1 or self.current_lap == 1

    @property
    def lap(self):
        return "All" if self.is_final_report() else str(self.current_lap)

    def report(self):
        if self.is_final_report():
            print_internal("")
            print_header("------------------------------------------------------")
            print_header("    _______             __   _____                    ")
            print_header("   / ____(_)___  ____ _/ /  / ___/_________  ________ ")
            print_header("  / /_  / / __ \/ __ `/ /   \__ \/ ___/ __ \/ ___/ _ \\")
            print_header(" / __/ / / / / / /_/ / /   ___/ / /__/ /_/ / /  /  __/")
            print_header("/_/   /_/_/ /_/\__,_/_/   /____/\___/\____/_/   \___/ ")
            print_header("------------------------------------------------------")
            print_internal("")
        else:
            print_internal("")
            print_header("--------------------------------------------------")
            print_header("    __                   _____                    ")
            print_header("   / /   ____ _____     / ___/_________  ________ ")
            print_header("  / /   / __ `/ __ \    \__ \/ ___/ __ \/ ___/ _ \\")
            print_header(" / /___/ /_/ / /_/ /   ___/ / /__/ /_/ / /  /  __/")
            print_header("/_____/\__,_/ .___/   /____/\___/\____/_/   \___/ ")
            print_header("           /_/                                    ")
            print_header("--------------------------------------------------")
            print_internal("")

        stats = self.results

        warnings = []
        metrics_table = []
        meta_info_table = []
        metrics_table += self.report_total_times(stats)
        metrics_table += self.report_merge_part_times(stats)

        metrics_table += self.report_cpu_usage(stats)
        metrics_table += self.report_gc_times(stats)

        metrics_table += self.report_disk_usage(stats)
        metrics_table += self.report_segment_memory(stats)
        metrics_table += self.report_segment_counts(stats)

        for record in stats.op_metrics:
            operation = record["operation"]
            metrics_table += self.report_throughput(record, operation)
            metrics_table += self.report_latency(record, operation)
            metrics_table += self.report_service_time(record, operation)
            metrics_table += self.report_error_rate(record, operation)
            self.add_warnings(warnings, record, operation)

        meta_info_table += self.report_meta_info()

        self.write_report(metrics_table, meta_info_table)

        if warnings:
            for warning in warnings:
                console.warn(warning, logger=logger)

    def add_warnings(self, warnings, values, op):
        if values["throughput"]["median"] is None:
            error_rate = values["error_rate"]
            if error_rate:
                warnings.append("No throughput metrics available for [%s]. Likely cause: Error rate is %.1f%%. Please check the logs."
                                % (op, error_rate * 100))
            else:
                warnings.append("No throughput metrics available for [%s]. Likely cause: The benchmark ended already during warmup." % op)

    def write_report(self, metrics_table, meta_info_table):
        report_file = self._config.opts("reporting", "output.path")
        report_format = self._config.opts("reporting", "format")
        cwd = self._config.opts("node", "rally.cwd")
        write_single_report(report_file, report_format, cwd, headers=["Lap", "Metric", "Operation", "Value", "Unit"],
                            data_plain=metrics_table,
                            data_rich=metrics_table, write_header=self.needs_header())
        if self.is_final_report() and len(report_file) > 0:
            write_single_report("%s.meta" % report_file, report_format, cwd, headers=["Name", "Value"], data_plain=meta_info_table,
                                data_rich=meta_info_table, show_also_in_console=False)

    def report_throughput(self, values, operation):
        min = values["throughput"]["min"]
        median = values["throughput"]["median"]
        max = values["throughput"]["max"]
        unit = values["throughput"]["unit"]
        return [
            [self.lap, "Min Throughput", operation, min, unit],
            [self.lap, "Median Throughput", operation, median, unit],
            [self.lap, "Max Throughput", operation, max, unit]
        ]

    def report_latency(self, values, operation):
        lines = []
        latency = values["latency"]
        if latency:
            for percentile, value in latency.items():
                lines.append([self.lap, "%sth percentile latency" % self.decode_percentile_key(percentile), operation, value, "ms"])
        return lines

    def report_service_time(self, values, operation):
        lines = []
        service_time = values["service_time"]
        if service_time:
            for percentile, value in service_time.items():
                lines.append([self.lap, "%sth percentile service time" % self.decode_percentile_key(percentile), operation, value, "ms"])
        return lines

    def decode_percentile_key(self, k):
        return k.replace("_", ".")

    def report_error_rate(self, values, operation):
        lines = []
        error_rate = values["error_rate"]
        if error_rate is not None:
            lines.append([self.lap, "error rate", operation, "%.2f" % (error_rate * 100.0), "%"])
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
            l.append([self.lap, k, operation, converter(v), unit])

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
            [self.lap, "Total Young Gen GC", "", convert.ms_to_seconds(stats.young_gc_time), "s"],
            [self.lap, "Total Old Gen GC", "", convert.ms_to_seconds(stats.old_gc_time), "s"]
        ]

    def report_disk_usage(self, stats):
        if stats.has_disk_usage_stats():
            return [
                [self.lap, "Index size", "", convert.bytes_to_gb(stats.index_size), "GB"],
                [self.lap, "Totally written", "", convert.bytes_to_gb(stats.bytes_written), "GB"]
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
            return [[self.lap, "Segment count", "", stats.segment_count, ""]]
        else:
            return []

    def report_meta_info(self):
        return [
            ["Elasticsearch source revision", self.revision]
        ]


class ComparisonReporter:
    def __init__(self, config):
        self._config = config
        self.plain = False

    def report(self, r1, r2):
        logger.info("Generating comparison report for baseline (invocation=[%s], track=[%s], challenge=[%s], car=[%s]) and "
                    "contender (invocation=[%s], track=[%s], challenge=[%s], car=[%s])" %
                    (r1.trial_timestamp, r1.track, r1.challenge, r1.car,
                     r2.trial_timestamp, r2.track, r2.challenge, r2.car))
        # we don't verify anything about the races as it is possible that the user benchmarks two different tracks intentionally
        baseline_stats = Stats(r1.results)
        contender_stats = Stats(r2.results)

        print_internal("")
        print_internal("Comparing baseline")
        print_internal("  Race timestamp: %s" % r1.trial_timestamp)
        print_internal("  Challenge: %s" % r1.challenge_name)
        print_internal("  Car: %s" % r1.car)
        print_internal("")
        print_internal("with contender")
        print_internal("  Race timestamp: %s" % r2.trial_timestamp)
        print_internal("  Challenge: %s" % r2.challenge_name)
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

        metric_table_plain = self.metrics_table(baseline_stats, contender_stats, plain=True)
        metric_table_rich = self.metrics_table(baseline_stats, contender_stats, plain=False)
        # Writes metric_table_rich to console, writes metric_table_plain to file
        self.write_report(metric_table_plain, metric_table_rich)

    def metrics_table(self, baseline_stats, contender_stats, plain):
        self.plain = plain
        metrics_table = []
        metrics_table += self.report_total_times(baseline_stats, contender_stats)
        metrics_table += self.report_merge_part_times(baseline_stats, contender_stats)
        # metrics_table += self.report_cpu_usage(baseline_stats, contender_stats)
        metrics_table += self.report_gc_times(baseline_stats, contender_stats)
        metrics_table += self.report_disk_usage(baseline_stats, contender_stats)
        metrics_table += self.report_segment_memory(baseline_stats, contender_stats)
        metrics_table += self.report_segment_counts(baseline_stats, contender_stats)

        for op in baseline_stats.operations():
            if op in contender_stats.operations():
                metrics_table += self.report_throughput(baseline_stats, contender_stats, op)
                metrics_table += self.report_latency(baseline_stats, contender_stats, op)
                metrics_table += self.report_service_time(baseline_stats, contender_stats, op)
                metrics_table += self.report_error_rate(baseline_stats, contender_stats, op)
        return metrics_table

    def format_as_table(self, table):
        return tabulate.tabulate(table,
                                 headers=["Metric", "Operation", "Baseline", "Contender", "Diff", "Unit"],
                                 tablefmt="pipe", numalign="right", stralign="right")

    def write_report(self, metrics_table, metrics_table_console):
        report_file = self._config.opts("reporting", "output.path")
        report_format = self._config.opts("reporting", "format")
        cwd = self._config.opts("node", "rally.cwd")
        write_single_report(report_file, report_format, cwd, headers=["Metric", "Operation", "Baseline", "Contender", "Diff", "Unit"],
                            data_plain=metrics_table, data_rich=metrics_table_console, write_header=True)

    def report_throughput(self, baseline_stats, contender_stats, operation):
        b_min = baseline_stats.metrics(operation)["throughput"]["min"]
        b_median = baseline_stats.metrics(operation)["throughput"]["median"]
        b_max = baseline_stats.metrics(operation)["throughput"]["max"]
        b_unit = baseline_stats.metrics(operation)["throughput"]["unit"]

        c_min = contender_stats.metrics(operation)["throughput"]["min"]
        c_median = contender_stats.metrics(operation)["throughput"]["median"]
        c_max = contender_stats.metrics(operation)["throughput"]["max"]

        return self.join(
            self.line("Min Throughput", b_min, c_min, operation, b_unit, treat_increase_as_improvement=True),
            self.line("Median Throughput", b_median, c_median, operation, b_unit, treat_increase_as_improvement=True),
            self.line("Max Throughput", b_max, c_max, operation, b_unit, treat_increase_as_improvement=True)
        )

    def report_latency(self, baseline_stats, contender_stats, operation):
        lines = []

        baseline_latency = baseline_stats.metrics(operation)["latency"]
        contender_latency = contender_stats.metrics(operation)["latency"]

        for percentile, baseline_value in baseline_latency.items():
            if percentile in contender_latency:
                contender_value = contender_latency[percentile]
                lines.append(self.line("%sth percentile latency" % self.decode_percentile_key(percentile), baseline_value, contender_value,
                                       operation, "ms", treat_increase_as_improvement=False))
        return lines

    def report_service_time(self, baseline_stats, contender_stats, operation):
        lines = []

        baseline_service_time = baseline_stats.metrics(operation)["service_time"]
        contender_service_time = contender_stats.metrics(operation)["service_time"]

        for percentile, baseline_value in baseline_service_time.items():
            if percentile in contender_service_time:
                contender_value = contender_service_time[percentile]
                self.append_if_present(lines, self.line("%sth percentile service time" %
                                                        self.decode_percentile_key(percentile), baseline_value, contender_value,
                                                        operation, "ms", treat_increase_as_improvement=False))
        return lines

    def decode_percentile_key(self, k):
        return k.replace("_", ".")

    def report_error_rate(self, baseline_stats, contender_stats, operation):
        baseline_error_rate = baseline_stats.metrics(operation)["error_rate"]
        contender_error_rate = contender_stats.metrics(operation)["error_rate"]
        return self.join(
            self.line("error rate", baseline_error_rate, contender_error_rate, operation, "%",
                      treat_increase_as_improvement=False, formatter=convert.factor(100.0))
        )

    def report_merge_part_times(self, baseline_stats, contender_stats):
        if baseline_stats.has_merge_part_stats() and contender_stats.has_merge_part_stats():
            return self.join(
                self.line("Merge time (postings)", baseline_stats.merge_part_time_postings,
                          contender_stats.merge_part_time_postings,
                          "", "min", treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
                self.line("Merge time (stored fields)", baseline_stats.merge_part_time_stored_fields,
                          contender_stats.merge_part_time_stored_fields,
                          "", "min", treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
                self.line("Merge time (doc values)", baseline_stats.merge_part_time_doc_values,
                          contender_stats.merge_part_time_doc_values,
                          "", "min", treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
                self.line("Merge time (norms)", baseline_stats.merge_part_time_norms,
                          contender_stats.merge_part_time_norms,
                          "", "min", treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
                self.line("Merge time (vectors)", baseline_stats.merge_part_time_vectors,
                          contender_stats.merge_part_time_vectors,
                          "", "min", treat_increase_as_improvement=False, formatter=convert.ms_to_minutes)
            )
        else:
            return []

    def append_if_present(self, l, v):
        if v and len(v) > 0:
            l.append(v)

    def join(self, *args):
        lines = []
        for arg in args:
            if arg and len(arg) > 0:
                lines.append(arg)

        return lines

    def report_total_times(self, baseline_stats, contender_stats):
        return self.join(
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
        )

    def report_gc_times(self, baseline_stats, contender_stats):
        return self.join(
            self.line("Total Young Gen GC", baseline_stats.young_gc_time, contender_stats.young_gc_time, "", "s",
                      treat_increase_as_improvement=False, formatter=convert.ms_to_seconds),
            self.line("Total Old Gen GC", baseline_stats.old_gc_time, contender_stats.old_gc_time, "", "s",
                      treat_increase_as_improvement=False, formatter=convert.ms_to_seconds)
        )

    def report_disk_usage(self, baseline_stats, contender_stats):
        if baseline_stats.has_disk_usage_stats() and contender_stats.has_disk_usage_stats():
            return self.join(
                self.line("Index size", baseline_stats.index_size, contender_stats.index_size, "", "GB",
                          treat_increase_as_improvement=False, formatter=convert.bytes_to_gb),
                self.line("Totally written", baseline_stats.bytes_written, contender_stats.bytes_written, "", "GB",
                          treat_increase_as_improvement=False, formatter=convert.bytes_to_gb)
            )
        else:
            return []

    def report_segment_memory(self, baseline_stats, contender_stats):
        if baseline_stats.has_memory_stats() and contender_stats.has_memory_stats():
            return self.join(
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
                self.line("Heap used for stored fields", baseline_stats.memory_stored_fields, contender_stats.memory_stored_fields, "",
                          "MB",
                          treat_increase_as_improvement=False, formatter=convert.bytes_to_mb)
            )
        else:
            return []

    def report_segment_counts(self, baseline_stats, contender_stats):
        if baseline_stats.segment_count and contender_stats.segment_count:
            return self.join(
                self.line("Segment count", baseline_stats.segment_count, contender_stats.segment_count,
                          "", "", treat_increase_as_improvement=False)
            )
        else:
            return []

    def line(self, metric, baseline, contender, operation, unit, treat_increase_as_improvement, formatter=lambda x: x):
        if baseline is not None and contender is not None:
            return [metric, str(operation), formatter(baseline), formatter(contender),
                    self.diff(baseline, contender, treat_increase_as_improvement, formatter), unit]
        else:
            return []

    def diff(self, baseline, contender, treat_increase_as_improvement, formatter=lambda x: x):
        def identity(x):
            return x

        diff = formatter(contender - baseline)
        if self.plain:
            color_greater = identity
            color_smaller = identity
            color_neutral = identity
        elif treat_increase_as_improvement:
            color_greater = console.format.green
            color_smaller = console.format.red
            color_neutral = console.format.neutral
        else:
            color_greater = console.format.red
            color_smaller = console.format.green
            color_neutral = console.format.neutral

        if diff > 0:
            return color_greater("+%.5f" % diff)
        elif diff < 0:
            return color_smaller("%.5f" % diff)
        else:
            # tabulate needs this to align all values correctly
            return color_neutral("%.5f" % diff)
