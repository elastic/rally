# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import collections
import csv
import io
import sys
import logging
import statistics

import tabulate
from esrally import metrics, exceptions
from esrally.utils import convert, io as rio, console


FINAL_SCORE = r"""
------------------------------------------------------
    _______             __   _____
   / ____(_)___  ____ _/ /  / ___/_________  ________
  / /_  / / __ \/ __ `/ /   \__ \/ ___/ __ \/ ___/ _ \
 / __/ / / / / / /_/ / /   ___/ / /__/ /_/ / /  /  __/
/_/   /_/_/ /_/\__,_/_/   /____/\___/\____/_/   \___/
------------------------------------------------------
            """

LAP_SCORE = r"""
--------------------------------------------------
    __                   _____
   / /   ____ _____     / ___/_________  ________
  / /   / __ `/ __ \    \__ \/ ___/ __ \/ ___/ _ \
 / /___/ /_/ / /_/ /   ___/ / /__/ /_/ / /  /  __/
/_____/\__,_/ .___/   /____/\___/\____/_/   \___/
           /_/
--------------------------------------------------
"""


def calculate_results(metrics_store, race, lap=None):
    calc = StatsCalculator(metrics_store, race.challenge, lap)
    return calc()


def summarize(race, cfg, lap=None):
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
    console.println(message, logger=logging.getLogger(__name__).info)


def print_header(message):
    print_internal(console.format.bold(message))


def write_single_report(report_file, report_format, cwd, headers, data_plain, data_rich, write_header=True):
    if report_format == "markdown":
        formatter = format_as_markdown
    elif report_format == "csv":
        formatter = format_as_csv
    else:
        raise exceptions.SystemSetupError("Unknown report format '%s'" % report_format)

    print_internal(formatter(headers, data_rich))
    if len(report_file) > 0:
        normalized_report_file = rio.normalize_path(report_file, cwd)
        # ensure that the parent folder already exists when we try to write the file...
        rio.ensure_dir(rio.dirname(normalized_report_file))
        with open(normalized_report_file, mode="a+", encoding="utf-8") as f:
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


# helper function for encoding and decoding float keys so that the Elasticsearch metrics store can safe it.
def encode_float_key(k):
    # ensure that the key is indeed a float to unify the representation (e.g. 50 should be represented as "50_0")
    return str(float(k)).replace(".", "_")


def percentiles_for_sample_size(sample_size):
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


class StatsCalculator:
    def __init__(self, store, challenge, lap=None):
        self.store = store
        self.challenge = challenge
        self.lap = lap
        self.logger = logging.getLogger(__name__)

    def __call__(self):
        result = Stats()

        for tasks in self.challenge.schedule:
            for task in tasks:
                if task.operation.include_in_reporting:
                    t = task.name
                    self.logger.debug("Gathering request metrics for [%s].", t)
                    result.add_op_metrics(
                        t,
                        task.operation.name,
                        self.summary_stats("throughput", t),
                        self.single_latency(t),
                        self.single_latency(t, metric_name="service_time"),
                        self.error_rate(t)
                    )
        self.logger.debug("Gathering node startup time metrics.")
        startup_times = self.store.get_raw("node_startup_time")
        for startup_time in startup_times:
            if "meta" in startup_time and "node_name" in startup_time["meta"]:
                result.add_node_metrics(startup_time["meta"]["node_name"], startup_time["value"])
            else:
                self.logger.debug("Skipping incomplete startup time record [%s].", str(startup_time))

        self.logger.debug("Gathering indexing metrics.")
        result.total_time = self.sum("indexing_total_time")
        result.total_time_per_shard = self.shard_stats("indexing_total_time")
        result.indexing_throttle_time = self.sum("indexing_throttle_time")
        result.indexing_throttle_time_per_shard = self.shard_stats("indexing_throttle_time")
        result.merge_time = self.sum("merges_total_time")
        result.merge_time_per_shard = self.shard_stats("merges_total_time")
        result.merge_count = self.sum("merges_total_count")
        result.refresh_time = self.sum("refresh_total_time")
        result.refresh_time_per_shard = self.shard_stats("refresh_total_time")
        result.refresh_count = self.sum("refresh_total_count")
        result.flush_time = self.sum("flush_total_time")
        result.flush_time_per_shard = self.shard_stats("flush_total_time")
        result.flush_count = self.sum("flush_total_count")
        result.merge_throttle_time = self.sum("merges_total_throttled_time")
        result.merge_throttle_time_per_shard = self.shard_stats("merges_total_throttled_time")

        self.logger.debug("Gathering merge part metrics.")
        result.merge_part_time_postings = self.sum("merge_parts_total_time_postings")
        result.merge_part_time_stored_fields = self.sum("merge_parts_total_time_stored_fields")
        result.merge_part_time_doc_values = self.sum("merge_parts_total_time_doc_values")
        result.merge_part_time_norms = self.sum("merge_parts_total_time_norms")
        result.merge_part_time_vectors = self.sum("merge_parts_total_time_vectors")
        result.merge_part_time_points = self.sum("merge_parts_total_time_points")

        self.logger.debug("Gathering ML max processing times.")
        result.ml_processing_time = self.ml_processing_time_stats()

        self.logger.debug("Gathering CPU usage metrics.")
        result.median_cpu_usage = self.median("cpu_utilization_1s", sample_type=metrics.SampleType.Normal)

        self.logger.debug("Gathering garbage collection metrics.")
        result.young_gc_time = self.sum("node_total_young_gen_gc_time")
        result.old_gc_time = self.sum("node_total_old_gen_gc_time")

        self.logger.debug("Gathering segment memory metrics.")
        result.memory_segments = self.median("segments_memory_in_bytes")
        result.memory_doc_values = self.median("segments_doc_values_memory_in_bytes")
        result.memory_terms = self.median("segments_terms_memory_in_bytes")
        result.memory_norms = self.median("segments_norms_memory_in_bytes")
        result.memory_points = self.median("segments_points_memory_in_bytes")
        result.memory_stored_fields = self.median("segments_stored_fields_memory_in_bytes")

        self.logger.debug("Gathering disk metrics.")
        # This metric will only be written for the last iteration (as it can only be determined after the cluster has been shut down)
        result.index_size = self.sum("final_index_size_bytes")
        # we need to use the median here because these two are captured with the indices stats API and thus once per lap. If we'd
        # sum up the values we'd get wrong results for benchmarks that ran for multiple laps.
        result.store_size = self.median("store_size_in_bytes")
        result.translog_size = self.median("translog_size_in_bytes")
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

    def summary_stats(self, metric_name, task_name):
        median = self.store.get_median(metric_name, task=task_name, sample_type=metrics.SampleType.Normal, lap=self.lap)
        unit = self.store.get_unit(metric_name, task=task_name)
        stats = self.store.get_stats(metric_name, task=task_name, sample_type=metrics.SampleType.Normal, lap=self.lap)
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

    def shard_stats(self, metric_name):
        values = self.store.get_raw(metric_name, lap=self.lap, mapper=lambda doc: doc["per-shard"])
        unit = self.store.get_unit(metric_name)
        if values:
            flat_values = [w for v in values for w in v]
            return {
                "min": min(flat_values),
                "median": statistics.median(flat_values),
                "max": max(flat_values),
                "unit": unit
            }
        else:
            return {}

    def ml_processing_time_stats(self):
        values = self.store.get_raw("ml_processing_time")
        result = []
        if values:
            for v in values:
                result.append({
                    "job": v["job"],
                    "min": v["min"],
                    "mean": v["mean"],
                    "median": v["median"],
                    "max": v["max"],
                    "unit": v["unit"]
                })
        return result

    def error_rate(self, task_name):
        return self.store.get_error_rate(task=task_name, sample_type=metrics.SampleType.Normal, lap=self.lap)

    def median(self, metric_name, task_name=None, operation_type=None, sample_type=None):
        return self.store.get_median(metric_name, task=task_name, operation_type=operation_type, sample_type=sample_type,
                                     lap=self.lap)

    def single_latency(self, task, metric_name="latency"):
        sample_type = metrics.SampleType.Normal
        sample_size = self.store.get_count(metric_name, task=task, sample_type=sample_type, lap=self.lap)
        if sample_size > 0:
            percentiles = self.store.get_percentiles(metric_name,
                                                     task=task,
                                                     sample_type=sample_type,
                                                     percentiles=percentiles_for_sample_size(sample_size),
                                                     lap=self.lap)
            # safely encode so we don't have any dots in field names
            safe_percentiles = collections.OrderedDict()
            for k, v in percentiles.items():
                safe_percentiles[encode_float_key(k)] = v
            return safe_percentiles
        else:
            return {}


class Stats:
    def __init__(self, d=None):
        self.op_metrics = self.v(d, "op_metrics", default=[])
        self.node_metrics = self.v(d, "node_metrics", default=[])
        self.total_time = self.v(d, "total_time")
        self.total_time_per_shard = self.v(d, "total_time_per_shard", default={})
        self.indexing_throttle_time = self.v(d, "indexing_throttle_time")
        self.indexing_throttle_time_per_shard = self.v(d, "indexing_throttle_time_per_shard", default={})
        self.merge_time = self.v(d, "merge_time")
        self.merge_time_per_shard = self.v(d, "merge_time_per_shard", default={})
        self.merge_count = self.v(d, "merge_count")
        self.refresh_time = self.v(d, "refresh_time")
        self.refresh_time_per_shard = self.v(d, "refresh_time_per_shard", default={})
        self.refresh_count = self.v(d, "refresh_count")
        self.flush_time = self.v(d, "flush_time")
        self.flush_time_per_shard = self.v(d, "flush_time_per_shard", default={})
        self.flush_count = self.v(d, "flush_count")
        self.merge_throttle_time = self.v(d, "merge_throttle_time")
        self.merge_throttle_time_per_shard = self.v(d, "merge_throttle_time_per_shard", default={})
        self.ml_processing_time = self.v(d, "ml_processing_time", default=[])

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
        self.store_size = self.v(d, "store_size")
        self.translog_size = self.v(d, "translog_size")
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
                        all_results.append(
                            {"task": item["task"], "operation": item["operation"], "name": "throughput", "value": item["throughput"]})
                    if "latency" in item:
                        all_results.append(
                            {"task": item["task"], "operation": item["operation"], "name": "latency", "value": item["latency"]})
                    if "service_time" in item:
                        all_results.append(
                            {"task": item["task"], "operation": item["operation"], "name": "service_time", "value": item["service_time"]})
                    if "error_rate" in item:
                        all_results.append(
                            {"task": item["task"], "operation": item["operation"], "name": "error_rate",
                             "value": {"single": item["error_rate"]}})
            elif metric == "ml_processing_time":
                for item in value:
                    all_results.append({
                        "job": item["job"],
                        "name": "ml_processing_time",
                        "value": {
                            "min": item["min"],
                            "mean": item["mean"],
                            "median": item["median"],
                            "max": item["max"]
                        }
                    })
            elif metric == "node_metrics":
                for item in value:
                    if "startup_time" in item:
                        all_results.append({"node": item["node"], "name": "startup_time", "value": {"single": item["startup_time"]}})
            elif metric.endswith("_time_per_shard"):
                if value:
                    all_results.append({"name": metric, "value": value})
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

    def add_op_metrics(self, task, operation, throughput, latency, service_time, error_rate):
        self.op_metrics.append({
            "task": task,
            "operation": operation,
            "throughput": throughput,
            "latency": latency,
            "service_time": service_time,
            "error_rate": error_rate
        })

    def add_node_metrics(self, node, startup_time):
        self.node_metrics.append({
            "node": node,
            "startup_time": startup_time
        })

    def tasks(self):
        # ensure we can read race.json files before Rally 0.8.0
        return [v.get("task", v["operation"]) for v in self.op_metrics]

    def metrics(self, task):
        # ensure we can read race.json files before Rally 0.8.0
        for r in self.op_metrics:
            if r.get("task", r["operation"]) == task:
                return r
        return None


class SummaryReporter:
    def __init__(self, results, config, revision, current_lap, total_laps):
        self.results = results
        self.report_file = config.opts("reporting", "output.path")
        self.report_format = config.opts("reporting", "format")
        reporting_values = config.opts("reporting", "values")
        self.report_all_values = reporting_values == "all"
        self.report_all_percentile_values = reporting_values == "all-percentiles"
        self.cwd = config.opts("node", "rally.cwd")

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
            print_header(FINAL_SCORE)
        else:
            print_header(LAP_SCORE)

        stats = self.results

        warnings = []
        metrics_table = []
        metrics_table.extend(self.report_totals(stats))
        metrics_table.extend(self.report_merge_part_times(stats))
        metrics_table.extend(self.report_ml_processing_times(stats))

        metrics_table.extend(self.report_cpu_usage(stats))
        metrics_table.extend(self.report_gc_times(stats))

        metrics_table.extend(self.report_disk_usage(stats))
        metrics_table.extend(self.report_segment_memory(stats))
        metrics_table.extend(self.report_segment_counts(stats))

        for record in stats.op_metrics:
            task = record["task"]
            metrics_table.extend(self.report_throughput(record, task))
            metrics_table.extend(self.report_latency(record, task))
            metrics_table.extend(self.report_service_time(record, task))
            metrics_table.extend(self.report_error_rate(record, task))
            self.add_warnings(warnings, record, task)

        self.write_report(metrics_table)

        if warnings:
            for warning in warnings:
                console.warn(warning)

    def add_warnings(self, warnings, values, op):
        if values["throughput"]["median"] is None:
            error_rate = values["error_rate"]
            if error_rate:
                warnings.append("No throughput metrics available for [%s]. Likely cause: Error rate is %.1f%%. Please check the logs."
                                % (op, error_rate * 100))
            else:
                warnings.append("No throughput metrics available for [%s]. Likely cause: The benchmark ended already during warmup." % op)

    def write_report(self, metrics_table):
        write_single_report(self.report_file, self.report_format, self.cwd,
                            headers=["Lap", "Metric", "Task", "Value", "Unit"],
                            data_plain=metrics_table,
                            data_rich=metrics_table, write_header=self.needs_header())

    def report_throughput(self, values, task):
        throughput = values["throughput"]
        unit = throughput["unit"]

        return self.join(
            self.line("Min Throughput", task, throughput["min"], unit, lambda v: "%.2f" % v),
            self.line("Median Throughput", task, throughput["median"], unit, lambda v: "%.2f" % v),
            self.line("Max Throughput", task, throughput["max"], unit, lambda v: "%.2f" % v)
        )

    def report_latency(self, values, task):
        return self.report_percentiles("latency", task, values["latency"])

    def report_service_time(self, values, task):
        return self.report_percentiles("service time", task, values["service_time"])

    def report_percentiles(self, name, task, value):
        lines = []
        if value:
            for percentile in percentiles_for_sample_size(sys.maxsize):
                a_line = self.line("%sth percentile %s" % (percentile, name), task, value.get(encode_float_key(percentile)), "ms",
                                   force=self.report_all_percentile_values)
                self.append_non_empty(lines, a_line)
        return lines

    def report_error_rate(self, values, task):
        return self.join(
            self.line("error rate", task, values["error_rate"], "%", lambda v: "%.2f" % (v * 100.0))
        )

    def report_totals(self, stats):
        lines = []
        lines.extend(self.report_total_time("indexing time", stats.total_time))
        lines.extend(self.report_total_time_per_shard("indexing time", stats.total_time_per_shard))
        lines.extend(self.report_total_time("indexing throttle time", stats.indexing_throttle_time))
        lines.extend(self.report_total_time_per_shard("indexing throttle time", stats.indexing_throttle_time_per_shard))
        lines.extend(self.report_total_time("merge time", stats.merge_time))
        lines.extend(self.report_total_count("merge count", stats.merge_count))
        lines.extend(self.report_total_time_per_shard("merge time", stats.merge_time_per_shard))
        lines.extend(self.report_total_time("merge throttle time", stats.merge_throttle_time))
        lines.extend(self.report_total_time_per_shard("merge throttle time", stats.merge_throttle_time_per_shard))
        lines.extend(self.report_total_time("refresh time", stats.refresh_time))
        lines.extend(self.report_total_count("refresh count", stats.refresh_count))
        lines.extend(self.report_total_time_per_shard("refresh time", stats.refresh_time_per_shard))
        lines.extend(self.report_total_time("flush time", stats.flush_time))
        lines.extend(self.report_total_count("flush count", stats.flush_count))
        lines.extend(self.report_total_time_per_shard("flush time", stats.flush_time_per_shard))
        return lines

    def report_total_time(self, name, total_time):
        unit = "min"
        return self.join(
            self.line("Cumulative {} of primary shards".format(name), "", total_time, unit, convert.ms_to_minutes),
        )

    def report_total_time_per_shard(self, name, total_time_per_shard):
        unit = "min"
        return self.join(
            self.line("Min cumulative {} across primary shards".format(name), "", total_time_per_shard.get("min"), unit, convert.ms_to_minutes),
            self.line("Median cumulative {} across primary shards".format(name), "", total_time_per_shard.get("median"), unit, convert.ms_to_minutes),
            self.line("Max cumulative {} across primary shards".format(name), "", total_time_per_shard.get("max"), unit, convert.ms_to_minutes),
        )

    def report_total_count(self, name, total_count):
        return self.join(
            self.line("Cumulative {} of primary shards".format(name), "", total_count, ""),
        )

    def report_merge_part_times(self, stats):
        # note that these times are not(!) wall clock time results but total times summed up over multiple threads
        unit = "min"
        return self.join(
            self.line("Merge time (postings)", "", stats.merge_part_time_postings, unit, convert.ms_to_minutes),
            self.line("Merge time (stored fields)", "", stats.merge_part_time_stored_fields, unit, convert.ms_to_minutes),
            self.line("Merge time (doc values)", "", stats.merge_part_time_doc_values, unit, convert.ms_to_minutes),
            self.line("Merge time (norms)", "", stats.merge_part_time_norms, unit, convert.ms_to_minutes),
            self.line("Merge time (vectors)", "", stats.merge_part_time_vectors, unit, convert.ms_to_minutes),
            self.line("Merge time (points)", "", stats.merge_part_time_points, unit, convert.ms_to_minutes)
        )

    def report_ml_processing_times(self, stats):
        lines = []
        for processing_time in stats.ml_processing_time:
            job_name = processing_time["job"]
            unit = processing_time["unit"]
            lines.append(self.line("Min ML processing time", job_name, processing_time["min"], unit)),
            lines.append(self.line("Mean ML processing time", job_name, processing_time["mean"], unit)),
            lines.append(self.line("Median ML processing time", job_name, processing_time["median"], unit)),
            lines.append(self.line("Max ML processing time", job_name, processing_time["max"], unit))
        return lines

    def report_cpu_usage(self, stats):
        return self.join(
            self.line("Median CPU usage", "", stats.median_cpu_usage, "%")
        )

    def report_gc_times(self, stats):
        return self.join(
            self.line("Total Young Gen GC", "", stats.young_gc_time, "s", convert.ms_to_seconds),
            self.line("Total Old Gen GC", "", stats.old_gc_time, "s", convert.ms_to_seconds)
        )

    def report_disk_usage(self, stats):
        return self.join(
            self.line("Store size", "", stats.store_size, "GB", convert.bytes_to_gb),
            self.line("Translog size", "", stats.translog_size, "GB", convert.bytes_to_gb),
            self.line("Index size", "", stats.index_size, "GB", convert.bytes_to_gb),
            self.line("Total written", "", stats.bytes_written, "GB", convert.bytes_to_gb)
        )

    def report_segment_memory(self, stats):
        unit = "MB"
        return self.join(
            self.line("Heap used for segments", "", stats.memory_segments, unit, convert.bytes_to_mb),
            self.line("Heap used for doc values", "", stats.memory_doc_values, unit, convert.bytes_to_mb),
            self.line("Heap used for terms", "", stats.memory_terms, unit, convert.bytes_to_mb),
            self.line("Heap used for norms", "", stats.memory_norms, unit, convert.bytes_to_mb),
            self.line("Heap used for points", "", stats.memory_points, unit, convert.bytes_to_mb),
            self.line("Heap used for stored fields", "", stats.memory_stored_fields, unit, convert.bytes_to_mb)
        )

    def report_segment_counts(self, stats):
        return self.join(
            self.line("Segment count", "", stats.segment_count, "")
        )

    def join(self, *args):
        lines = []
        for arg in args:
            self.append_non_empty(lines, arg)
        return lines

    def append_non_empty(self, lines, line):
        if line and len(line) > 0:
            lines.append(line)

    def line(self, k, task, v, unit, converter=lambda x: x, force=False):
        if v is not None or force or self.report_all_values:
            u = unit if v is not None else None
            return [self.lap, k, task, converter(v), u]
        else:
            return []


class ComparisonReporter:
    def __init__(self, config):
        self.report_file = config.opts("reporting", "output.path")
        self.report_format = config.opts("reporting", "format")
        self.cwd = config.opts("node", "rally.cwd")
        self.plain = False

    def report(self, r1, r2):
        # we don't verify anything about the races as it is possible that the user benchmarks two different tracks intentionally
        baseline_stats = Stats(r1.results)
        contender_stats = Stats(r2.results)

        print_internal("")
        print_internal("Comparing baseline")
        print_internal("  Race timestamp: %s" % r1.trial_timestamp)
        if r1.challenge_name:
            print_internal("  Challenge: %s" % r1.challenge_name)
        print_internal("  Car: %s" % r1.car_name)
        print_internal("")
        print_internal("with contender")
        print_internal("  Race timestamp: %s" % r2.trial_timestamp)
        if r2.challenge_name:
            print_internal("  Challenge: %s" % r2.challenge_name)
        print_internal("  Car: %s" % r2.car_name)
        print_header(FINAL_SCORE)

        metric_table_plain = self.metrics_table(baseline_stats, contender_stats, plain=True)
        metric_table_rich = self.metrics_table(baseline_stats, contender_stats, plain=False)
        # Writes metric_table_rich to console, writes metric_table_plain to file
        self.write_report(metric_table_plain, metric_table_rich)

    def metrics_table(self, baseline_stats, contender_stats, plain):
        self.plain = plain
        metrics_table = []
        metrics_table.extend(self.report_total_times(baseline_stats, contender_stats))
        metrics_table.extend(self.report_merge_part_times(baseline_stats, contender_stats))
        metrics_table.extend(self.report_merge_part_times(baseline_stats, contender_stats))
        metrics_table.extend(self.report_ml_processing_times(baseline_stats, contender_stats))
        metrics_table.extend(self.report_gc_times(baseline_stats, contender_stats))
        metrics_table.extend(self.report_disk_usage(baseline_stats, contender_stats))
        metrics_table.extend(self.report_segment_memory(baseline_stats, contender_stats))
        metrics_table.extend(self.report_segment_counts(baseline_stats, contender_stats))

        for t in baseline_stats.tasks():
            if t in contender_stats.tasks():
                metrics_table.extend(self.report_throughput(baseline_stats, contender_stats, t))
                metrics_table.extend(self.report_latency(baseline_stats, contender_stats, t))
                metrics_table.extend(self.report_service_time(baseline_stats, contender_stats, t))
                metrics_table.extend(self.report_error_rate(baseline_stats, contender_stats, t))
        return metrics_table

    def write_report(self, metrics_table, metrics_table_console):
        write_single_report(self.report_file, self.report_format, self.cwd,
                            headers=["Metric", "Task", "Baseline", "Contender", "Diff", "Unit"],
                            data_plain=metrics_table, data_rich=metrics_table_console, write_header=True)

    def report_throughput(self, baseline_stats, contender_stats, task):
        b_min = baseline_stats.metrics(task)["throughput"]["min"]
        b_median = baseline_stats.metrics(task)["throughput"]["median"]
        b_max = baseline_stats.metrics(task)["throughput"]["max"]
        b_unit = baseline_stats.metrics(task)["throughput"]["unit"]

        c_min = contender_stats.metrics(task)["throughput"]["min"]
        c_median = contender_stats.metrics(task)["throughput"]["median"]
        c_max = contender_stats.metrics(task)["throughput"]["max"]

        return self.join(
            self.line("Min Throughput", b_min, c_min, task, b_unit, treat_increase_as_improvement=True),
            self.line("Median Throughput", b_median, c_median, task, b_unit, treat_increase_as_improvement=True),
            self.line("Max Throughput", b_max, c_max, task, b_unit, treat_increase_as_improvement=True)
        )

    def report_latency(self, baseline_stats, contender_stats, task):
        baseline_latency = baseline_stats.metrics(task)["latency"]
        contender_latency = contender_stats.metrics(task)["latency"]
        return self.report_percentiles("latency", task, baseline_latency, contender_latency)

    def report_service_time(self, baseline_stats, contender_stats, task):
        baseline_service_time = baseline_stats.metrics(task)["service_time"]
        contender_service_time = contender_stats.metrics(task)["service_time"]
        return self.report_percentiles("service time", task, baseline_service_time, contender_service_time)

    def report_percentiles(self, name, task, baseline_values, contender_values):
        lines = []
        for percentile in percentiles_for_sample_size(sys.maxsize):
            baseline_value = baseline_values.get(encode_float_key(percentile))
            contender_value = contender_values.get(encode_float_key(percentile))
            self.append_non_empty(lines, self.line("%sth percentile %s" % (percentile, name),
                                                   baseline_value, contender_value, task, "ms", treat_increase_as_improvement=False))
        return lines

    def report_error_rate(self, baseline_stats, contender_stats, task):
        baseline_error_rate = baseline_stats.metrics(task)["error_rate"]
        contender_error_rate = contender_stats.metrics(task)["error_rate"]
        return self.join(
            self.line("error rate", baseline_error_rate, contender_error_rate, task, "%",
                      treat_increase_as_improvement=False, formatter=convert.factor(100.0))
        )

    def report_merge_part_times(self, baseline_stats, contender_stats):
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

    def report_ml_processing_times(self, baseline_stats, contender_stats):
        lines = []
        for baseline in baseline_stats.ml_processing_time:
            job_name = baseline["job"]
            unit = baseline["unit"]
            # O(n^2) but we assume here only a *very* limited number of jobs (usually just one)
            for contender in contender_stats.ml_processing_time:
                if contender["job"] == job_name:
                    lines.append(self.line("Min ML processing time", baseline["min"], contender["min"],
                                           job_name, unit, treat_increase_as_improvement=False))
                    lines.append(self.line("Mean ML processing time", baseline["mean"], contender["mean"],
                                           job_name, unit, treat_increase_as_improvement=False))
                    lines.append(self.line("Median ML processing time", baseline["median"], contender["median"],
                                           job_name, unit, treat_increase_as_improvement=False))
                    lines.append(self.line("Max ML processing time", baseline["max"], contender["max"],
                                           job_name, unit, treat_increase_as_improvement=False))
        return lines

    def report_total_times(self, baseline_stats, contender_stats):
        lines = []
        lines.extend(self.report_total_time(
            "indexing time",
            baseline_stats.total_time, contender_stats.total_time
        ))
        lines.extend(self.report_total_time_per_shard(
            "indexing time",
            baseline_stats.total_time_per_shard, contender_stats.total_time_per_shard
        ))
        lines.extend(self.report_total_time(
            "indexing throttle time",
            baseline_stats.indexing_throttle_time, contender_stats.indexing_throttle_time
        ))
        lines.extend(self.report_total_time_per_shard(
            "indexing throttle time",
            baseline_stats.indexing_throttle_time_per_shard,
            contender_stats.indexing_throttle_time_per_shard
        ))
        lines.extend(self.report_total_time(
            "merge time",
            baseline_stats.merge_time, contender_stats.merge_time,
        ))
        lines.extend(self.report_total_count(
            "merge count",
            baseline_stats.merge_count, contender_stats.merge_count
        ))
        lines.extend(self.report_total_time_per_shard(
            "merge time",
            baseline_stats.merge_time_per_shard,
            contender_stats.merge_time_per_shard
        ))
        lines.extend(self.report_total_time(
            "merge throttle time",
            baseline_stats.merge_throttle_time,
            contender_stats.merge_throttle_time
        ))
        lines.extend(self.report_total_time_per_shard(
            "merge throttle time",
            baseline_stats.merge_throttle_time_per_shard,
            contender_stats.merge_throttle_time_per_shard
        ))
        lines.extend(self.report_total_time(
            "refresh time",
            baseline_stats.refresh_time, contender_stats.refresh_time
        ))
        lines.extend(self.report_total_count(
            "refresh count",
            baseline_stats.refresh_count, contender_stats.refresh_count
        ))
        lines.extend(self.report_total_time_per_shard(
            "refresh time",
            baseline_stats.refresh_time_per_shard,
            contender_stats.refresh_time_per_shard
        ))
        lines.extend(self.report_total_time(
            "flush time",
            baseline_stats.flush_time, contender_stats.flush_time
        ))
        lines.extend(self.report_total_count(
            "flush count",
            baseline_stats.flush_count, contender_stats.flush_count
        ))
        lines.extend(self.report_total_time_per_shard(
            "flush time",
            baseline_stats.flush_time_per_shard, contender_stats.flush_time_per_shard
        ))
        return lines

    def report_total_time(self, name, baseline_total, contender_total):
        unit = "min"
        return self.join(
            self.line("Cumulative {} of primary shards".format(name), baseline_total, contender_total, "", unit,
                      treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
        )

    def report_total_time_per_shard(self, name, baseline_per_shard, contender_per_shard):
        unit = "min"
        return self.join(
            self.line("Min cumulative {} across primary shard".format(name), baseline_per_shard.get("min"), contender_per_shard.get("min"), "", unit,
                      treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
            self.line("Median cumulative {} across primary shard".format(name), baseline_per_shard.get("median"), contender_per_shard.get("median"), "", unit,
                      treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
            self.line("Max cumulative {} across primary shard".format(name), baseline_per_shard.get("max"), contender_per_shard.get("max"), "", unit,
                      treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
        )

    def report_total_count(self, name, baseline_total, contender_total):
        return self.join(
            self.line("Cumulative {} of primary shards".format(name), baseline_total, contender_total, "", "",
                      treat_increase_as_improvement=False)
        )

    def report_gc_times(self, baseline_stats, contender_stats):
        return self.join(
            self.line("Total Young Gen GC", baseline_stats.young_gc_time, contender_stats.young_gc_time, "", "s",
                      treat_increase_as_improvement=False, formatter=convert.ms_to_seconds),
            self.line("Total Old Gen GC", baseline_stats.old_gc_time, contender_stats.old_gc_time, "", "s",
                      treat_increase_as_improvement=False, formatter=convert.ms_to_seconds)
        )

    def report_disk_usage(self, baseline_stats, contender_stats):
        return self.join(
            self.line("Store size", baseline_stats.store_size, contender_stats.store_size, "", "GB",
                      treat_increase_as_improvement=False, formatter=convert.bytes_to_gb),
            self.line("Translog size", baseline_stats.translog_size, contender_stats.translog_size, "", "GB",
                      treat_increase_as_improvement=False, formatter=convert.bytes_to_gb),
            self.line("Index size", baseline_stats.index_size, contender_stats.index_size, "", "GB",
                      treat_increase_as_improvement=False, formatter=convert.bytes_to_gb),
            self.line("Total written", baseline_stats.bytes_written, contender_stats.bytes_written, "", "GB",
                      treat_increase_as_improvement=False, formatter=convert.bytes_to_gb)
        )

    def report_segment_memory(self, baseline_stats, contender_stats):
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

    def report_segment_counts(self, baseline_stats, contender_stats):
        return self.join(
            self.line("Segment count", baseline_stats.segment_count, contender_stats.segment_count,
                      "", "", treat_increase_as_improvement=False)
        )

    def join(self, *args):
        lines = []
        for arg in args:
            self.append_non_empty(lines, arg)
        return lines

    def append_non_empty(self, lines, line):
        if line and len(line) > 0:
            lines.append(line)

    def line(self, metric, baseline, contender, task, unit, treat_increase_as_improvement, formatter=lambda x: x):
        if baseline is not None and contender is not None:
            return [metric, str(task), formatter(baseline), formatter(contender),
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
