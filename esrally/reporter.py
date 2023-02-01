# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import csv
import io
import logging
import sys
from functools import partial

import tabulate

from esrally import exceptions, metrics
from esrally.utils import console, convert
from esrally.utils import io as rio

FINAL_SCORE = r"""
------------------------------------------------------
    _______             __   _____
   / ____(_)___  ____ _/ /  / ___/_________  ________
  / /_  / / __ \/ __ `/ /   \__ \/ ___/ __ \/ ___/ _ \
 / __/ / / / / / /_/ / /   ___/ / /__/ /_/ / /  /  __/
/_/   /_/_/ /_/\__,_/_/   /____/\___/\____/_/   \___/
------------------------------------------------------
            """


def summarize(results, cfg):
    SummaryReporter(results, cfg).report()


def compare(cfg, baseline_id, contender_id):
    if not baseline_id or not contender_id:
        raise exceptions.SystemSetupError("compare needs baseline and a contender")
    race_store = metrics.race_store(cfg)
    ComparisonReporter(cfg).report(race_store.find_by_race_id(baseline_id), race_store.find_by_race_id(contender_id))


def print_internal(message):
    console.println(message, logger=logging.getLogger(__name__).info)


def print_header(message):
    print_internal(console.format.bold(message))


def write_single_report(report_file, report_format, cwd, numbers_align, headers, data_plain, data_rich):
    if report_format == "markdown":
        formatter = partial(format_as_markdown, numbers_align=numbers_align)
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
            f.writelines(formatter(headers, data_plain))


def format_as_markdown(headers, data, numbers_align):
    rendered = tabulate.tabulate(data, headers=headers, tablefmt="pipe", numalign=numbers_align, stralign="right")
    return rendered + "\n"


def format_as_csv(headers, data):
    with io.StringIO() as out:
        writer = csv.writer(out)
        writer.writerow(headers)
        for metric_record in data:
            writer.writerow(metric_record)
        return out.getvalue()


def disk_usage_fields(stats):
    return {
        "inverted index": stats.disk_usage_inverted_index,
        "stored fields": stats.disk_usage_stored_fields,
        "doc values": stats.disk_usage_doc_values,
        "points": stats.disk_usage_points,
        "norms": stats.disk_usage_norms,
        "term vectors": stats.disk_usage_term_vectors,
        "total": stats.disk_usage_total,
    }


def collate_disk_usage_stats(stats):
    collated = {}
    for stat, field_stats in disk_usage_fields(stats).items():
        for field_stat in field_stats:
            collated.setdefault(field_stat["index"], {}).setdefault(field_stat["field"], {})[stat] = field_stat["value"]
    return collated


def total_disk_usage_per_field(stats):
    totals = []
    for field_stat in stats.disk_usage_total:
        totals.append([field_stat["index"], field_stat["value"], field_stat["field"]])
    return totals


class SummaryReporter:
    def __init__(self, results, config):
        self.results = results
        self.report_file = config.opts("reporting", "output.path")
        self.report_format = config.opts("reporting", "format")
        self.numbers_align = config.opts("reporting", "numbers.align", mandatory=False, default_value="decimal")
        reporting_values = config.opts("reporting", "values")
        self.report_all_values = reporting_values == "all"
        self.report_all_percentile_values = reporting_values == "all-percentiles"
        self.show_processing_time = convert.to_bool(config.opts("reporting", "output.processingtime", mandatory=False, default_value=False))
        self.cwd = config.opts("node", "rally.cwd")

    def report(self):
        print_header(FINAL_SCORE)

        stats = self.results

        warnings = []
        metrics_table = []
        metrics_table.extend(self._report_totals(stats))
        metrics_table.extend(self._report_ml_processing_times(stats))

        metrics_table.extend(self._report_gc_metrics(stats))

        metrics_table.extend(self._report_disk_usage(stats))
        metrics_table.extend(self._report_segment_memory(stats))
        metrics_table.extend(self._report_segment_counts(stats))

        metrics_table.extend(self._report_transform_stats(stats))

        metrics_table.extend(self._report_ingest_pipeline_stats(stats))

        metrics_table.extend(self._report_disk_usage_per_field(stats))

        for record in stats.op_metrics:
            task = record["task"]
            metrics_table.extend(self._report_throughput(record, task))
            metrics_table.extend(self._report_latency(record, task))
            metrics_table.extend(self._report_service_time(record, task))
            # this is mostly needed for debugging purposes but not so relevant to end users
            if self.show_processing_time:
                metrics_table.extend(self._report_processing_time(record, task))
            metrics_table.extend(self._report_error_rate(record, task))
            self.add_warnings(warnings, record, task)

        self.write_report(metrics_table)

        if warnings:
            for warning in warnings:
                console.warn(warning)

    def add_warnings(self, warnings, values, op):
        if values["error_rate"] > 0:
            warnings.append(f"Error rate is {round(values['error_rate'] * 100, 2)} for operation '{op}'. Please check the logs.")
        if values["throughput"]["median"] is None:
            error_rate = values["error_rate"]
            if error_rate:
                warnings.append(
                    "No throughput metrics available for [%s]. Likely cause: Error rate is %.1f%%. Please check the logs."
                    % (op, error_rate * 100)
                )
            else:
                warnings.append("No throughput metrics available for [%s]. Likely cause: The benchmark ended already during warmup." % op)

    def write_report(self, metrics_table):
        write_single_report(
            self.report_file,
            self.report_format,
            self.cwd,
            self.numbers_align,
            headers=["Metric", "Task", "Value", "Unit"],
            data_plain=metrics_table,
            data_rich=metrics_table,
        )

    def _report_throughput(self, values, task):
        throughput = values["throughput"]
        unit = throughput["unit"]

        return self._join(
            self._line("Min Throughput", task, throughput["min"], unit, lambda v: "%.2f" % v),
            self._line("Mean Throughput", task, throughput["mean"], unit, lambda v: "%.2f" % v),
            self._line("Median Throughput", task, throughput["median"], unit, lambda v: "%.2f" % v),
            self._line("Max Throughput", task, throughput["max"], unit, lambda v: "%.2f" % v),
        )

    def _report_latency(self, values, task):
        return self._report_percentiles("latency", task, values["latency"])

    def _report_service_time(self, values, task):
        return self._report_percentiles("service time", task, values["service_time"])

    def _report_processing_time(self, values, task):
        return self._report_percentiles("processing time", task, values["processing_time"])

    def _report_percentiles(self, name, task, value):
        lines = []
        if value:
            for percentile in metrics.percentiles_for_sample_size(sys.maxsize):
                percentile_value = value.get(metrics.encode_float_key(percentile))
                a_line = self._line(
                    "%sth percentile %s" % (percentile, name), task, percentile_value, "ms", force=self.report_all_percentile_values
                )
                self._append_non_empty(lines, a_line)
        return lines

    def _report_error_rate(self, values, task):
        return self._join(self._line("error rate", task, values["error_rate"], "%", lambda v: "%.2f" % (v * 100.0)))

    def _report_totals(self, stats):
        lines = []
        lines.extend(self._report_total_time("indexing time", stats.total_time))
        lines.extend(self._report_total_time_per_shard("indexing time", stats.total_time_per_shard))
        lines.extend(self._report_total_time("indexing throttle time", stats.indexing_throttle_time))
        lines.extend(self._report_total_time_per_shard("indexing throttle time", stats.indexing_throttle_time_per_shard))
        lines.extend(self._report_total_time("merge time", stats.merge_time))
        lines.extend(self._report_total_count("merge count", stats.merge_count))
        lines.extend(self._report_total_time_per_shard("merge time", stats.merge_time_per_shard))
        lines.extend(self._report_total_time("merge throttle time", stats.merge_throttle_time))
        lines.extend(self._report_total_time_per_shard("merge throttle time", stats.merge_throttle_time_per_shard))
        lines.extend(self._report_total_time("refresh time", stats.refresh_time))
        lines.extend(self._report_total_count("refresh count", stats.refresh_count))
        lines.extend(self._report_total_time_per_shard("refresh time", stats.refresh_time_per_shard))
        lines.extend(self._report_total_time("flush time", stats.flush_time))
        lines.extend(self._report_total_count("flush count", stats.flush_count))
        lines.extend(self._report_total_time_per_shard("flush time", stats.flush_time_per_shard))
        return lines

    def _report_total_time(self, name, total_time):
        unit = "min"
        return self._join(
            self._line(f"Cumulative {name} of primary shards", "", total_time, unit, convert.ms_to_minutes),
        )

    def _report_total_time_per_shard(self, name, total_time_per_shard):
        unit = "min"
        return self._join(
            self._line(f"Min cumulative {name} across primary shards", "", total_time_per_shard.get("min"), unit, convert.ms_to_minutes),
            self._line(
                f"Median cumulative {name} across primary shards",
                "",
                total_time_per_shard.get("median"),
                unit,
                convert.ms_to_minutes,
            ),
            self._line(f"Max cumulative {name} across primary shards", "", total_time_per_shard.get("max"), unit, convert.ms_to_minutes),
        )

    def _report_total_count(self, name, total_count):
        return self._join(
            self._line(f"Cumulative {name} of primary shards", "", total_count, ""),
        )

    def _report_ml_processing_times(self, stats):
        lines = []
        for processing_time in stats.ml_processing_time:
            job_name = processing_time["job"]
            unit = processing_time["unit"]
            lines.append(self._line("Min ML processing time", job_name, processing_time["min"], unit))
            lines.append(self._line("Mean ML processing time", job_name, processing_time["mean"], unit))
            lines.append(self._line("Median ML processing time", job_name, processing_time["median"], unit))
            lines.append(self._line("Max ML processing time", job_name, processing_time["max"], unit))
        return lines

    def _report_gc_metrics(self, stats):
        return self._join(
            self._line("Total Young Gen GC time", "", stats.young_gc_time, "s", convert.ms_to_seconds),
            self._line("Total Young Gen GC count", "", stats.young_gc_count, ""),
            self._line("Total Old Gen GC time", "", stats.old_gc_time, "s", convert.ms_to_seconds),
            self._line("Total Old Gen GC count", "", stats.old_gc_count, ""),
            self._line("Total ZGC Cycles GC time", "", stats.zgc_cycles_gc_time, "s", convert.ms_to_seconds),
            self._line("Total ZGC Cycles GC count", "", stats.zgc_cycles_gc_count, ""),
            self._line("Total ZGC Pauses GC time", "", stats.zgc_pauses_gc_time, "s", convert.ms_to_seconds),
            self._line("Total ZGC Pauses GC count", "", stats.zgc_pauses_gc_count, ""),
        )

    def _report_disk_usage(self, stats):
        return self._join(
            self._line("Store size", "", stats.store_size, "GB", convert.bytes_to_gb),
            self._line("Translog size", "", stats.translog_size, "GB", convert.bytes_to_gb),
        )

    def _report_segment_memory(self, stats):
        unit = "MB"
        return self._join(
            self._line("Heap used for segments", "", stats.memory_segments, unit, convert.bytes_to_mb),
            self._line("Heap used for doc values", "", stats.memory_doc_values, unit, convert.bytes_to_mb),
            self._line("Heap used for terms", "", stats.memory_terms, unit, convert.bytes_to_mb),
            self._line("Heap used for norms", "", stats.memory_norms, unit, convert.bytes_to_mb),
            self._line("Heap used for points", "", stats.memory_points, unit, convert.bytes_to_mb),
            self._line("Heap used for stored fields", "", stats.memory_stored_fields, unit, convert.bytes_to_mb),
        )

    def _report_segment_counts(self, stats):
        return self._join(self._line("Segment count", "", stats.segment_count, ""))

    def _report_transform_stats(self, stats):
        lines = []
        for processing_time in stats.total_transform_processing_times:
            lines.append(self._line("Transform processing time", processing_time["id"], processing_time["mean"], processing_time["unit"]))
        for index_time in stats.total_transform_index_times:
            lines.append(self._line("Transform indexing time", index_time["id"], index_time["mean"], index_time["unit"]))
        for search_time in stats.total_transform_search_times:
            lines.append(self._line("Transform search time", search_time["id"], search_time["mean"], search_time["unit"]))
        for throughput in stats.total_transform_throughput:
            lines.append(self._line("Transform throughput", throughput["id"], throughput["mean"], throughput["unit"]))

        return lines

    def _report_ingest_pipeline_stats(self, stats):
        return self._join(
            self._line("Total Ingest Pipeline count", "", stats.ingest_pipeline_cluster_count, ""),
            self._line("Total Ingest Pipeline time", "", stats.ingest_pipeline_cluster_time, "s", convert.ms_to_seconds),
            self._line("Total Ingest Pipeline failed", "", stats.ingest_pipeline_cluster_failed, ""),
        )

    def _report_disk_usage_per_field(self, stats):
        collated = collate_disk_usage_stats(stats)
        lines = []
        for index, _total, field in sorted(total_disk_usage_per_field(stats)):
            for stat, value in collated[index][field].items():
                lines.append(
                    self._line(f"{index} {field} {stat}", "", value, convert.bytes_to_human_unit(value), convert.bytes_to_human_value)
                )
        return lines

    def _join(self, *args):
        lines = []
        for arg in args:
            self._append_non_empty(lines, arg)
        return lines

    def _append_non_empty(self, lines, line):
        if line and len(line) > 0:
            lines.append(line)

    def _line(self, k, task, v, unit, converter=lambda x: x, force=False):
        if v is not None or force or self.report_all_values:
            u = unit if v is not None else None
            return [k, task, converter(v), u]
        else:
            return []


class ComparisonReporter:
    def __init__(self, config):
        self.report_file = config.opts("reporting", "output.path")
        self.report_format = config.opts("reporting", "format")
        self.numbers_align = config.opts("reporting", "numbers.align", mandatory=False, default_value="decimal")
        self.cwd = config.opts("node", "rally.cwd")
        self.show_processing_time = convert.to_bool(config.opts("reporting", "output.processingtime", mandatory=False, default_value=False))
        self.plain = False

    def report(self, r1, r2):
        # we don't verify anything about the races as it is possible that the user benchmarks two different tracks intentionally
        baseline_stats = metrics.GlobalStats(r1.results)
        contender_stats = metrics.GlobalStats(r2.results)

        print_internal("")
        print_internal("Comparing baseline")
        print_internal("  Race ID: %s" % r1.race_id)
        print_internal("  Race timestamp: %s" % r1.race_timestamp)
        if r1.challenge_name:
            print_internal("  Challenge: %s" % r1.challenge_name)
        print_internal("  Car: %s" % r1.car_name)
        if r1.user_tags:
            r1_user_tags = ", ".join(["%s=%s" % (k, v) for k, v in sorted(r1.user_tags.items())])
            print_internal("  User tags: %s" % r1_user_tags)
        print_internal("")
        print_internal("with contender")
        print_internal("  Race ID: %s" % r2.race_id)
        print_internal("  Race timestamp: %s" % r2.race_timestamp)
        if r2.challenge_name:
            print_internal("  Challenge: %s" % r2.challenge_name)
        print_internal("  Car: %s" % r2.car_name)
        if r2.user_tags:
            r2_user_tags = ", ".join(["%s=%s" % (k, v) for k, v in sorted(r2.user_tags.items())])
            print_internal("  User tags: %s" % r2_user_tags)
        print_header(FINAL_SCORE)

        metric_table_plain = self._metrics_table(baseline_stats, contender_stats, plain=True)
        metric_table_rich = self._metrics_table(baseline_stats, contender_stats, plain=False)
        # Writes metric_table_rich to console, writes metric_table_plain to file
        self._write_report(metric_table_plain, metric_table_rich)

    def _metrics_table(self, baseline_stats, contender_stats, plain):
        self.plain = plain
        metrics_table = []
        metrics_table.extend(self._report_total_times(baseline_stats, contender_stats))
        metrics_table.extend(self._report_ml_processing_times(baseline_stats, contender_stats))
        metrics_table.extend(self._report_gc_metrics(baseline_stats, contender_stats))
        metrics_table.extend(self._report_disk_usage(baseline_stats, contender_stats))
        metrics_table.extend(self._report_segment_memory(baseline_stats, contender_stats))
        metrics_table.extend(self._report_segment_counts(baseline_stats, contender_stats))
        metrics_table.extend(self._report_transform_processing_times(baseline_stats, contender_stats))
        metrics_table.extend(self._report_ingest_pipeline_counts(baseline_stats, contender_stats))
        metrics_table.extend(self._report_ingest_pipeline_times(baseline_stats, contender_stats))
        metrics_table.extend(self._report_ingest_pipeline_failed(baseline_stats, contender_stats))
        # Skip disk usage stats comparison if the disk_usage_total field does not exist
        if baseline_stats.disk_usage_total and contender_stats.disk_usage_total:
            metrics_table.extend(self._report_disk_usage_stats_per_field(baseline_stats, contender_stats))

        for t in baseline_stats.tasks():
            if t in contender_stats.tasks():
                metrics_table.extend(self._report_throughput(baseline_stats, contender_stats, t))
                metrics_table.extend(self._report_latency(baseline_stats, contender_stats, t))
                metrics_table.extend(self._report_service_time(baseline_stats, contender_stats, t))
                if self.show_processing_time:
                    metrics_table.extend(self._report_processing_time(baseline_stats, contender_stats, t))
                metrics_table.extend(self._report_error_rate(baseline_stats, contender_stats, t))
        return metrics_table

    def _write_report(self, metrics_table, metrics_table_console):
        write_single_report(
            self.report_file,
            self.report_format,
            self.cwd,
            self.numbers_align,
            headers=["Metric", "Task", "Baseline", "Contender", "Diff", "Unit", "Diff %"],
            data_plain=metrics_table,
            data_rich=metrics_table_console,
        )

    def _report_throughput(self, baseline_stats, contender_stats, task):
        b_min = baseline_stats.metrics(task)["throughput"]["min"]
        b_mean = baseline_stats.metrics(task)["throughput"]["mean"]
        b_median = baseline_stats.metrics(task)["throughput"]["median"]
        b_max = baseline_stats.metrics(task)["throughput"]["max"]
        b_unit = baseline_stats.metrics(task)["throughput"]["unit"]

        c_min = contender_stats.metrics(task)["throughput"]["min"]
        c_mean = contender_stats.metrics(task)["throughput"]["mean"]
        c_median = contender_stats.metrics(task)["throughput"]["median"]
        c_max = contender_stats.metrics(task)["throughput"]["max"]

        return self._join(
            self._line("Min Throughput", b_min, c_min, task, b_unit, treat_increase_as_improvement=True),
            self._line("Mean Throughput", b_mean, c_mean, task, b_unit, treat_increase_as_improvement=True),
            self._line("Median Throughput", b_median, c_median, task, b_unit, treat_increase_as_improvement=True),
            self._line("Max Throughput", b_max, c_max, task, b_unit, treat_increase_as_improvement=True),
        )

    def _report_latency(self, baseline_stats, contender_stats, task):
        baseline_latency = baseline_stats.metrics(task)["latency"]
        contender_latency = contender_stats.metrics(task)["latency"]
        return self._report_percentiles("latency", task, baseline_latency, contender_latency)

    def _report_service_time(self, baseline_stats, contender_stats, task):
        baseline_service_time = baseline_stats.metrics(task)["service_time"]
        contender_service_time = contender_stats.metrics(task)["service_time"]
        return self._report_percentiles("service time", task, baseline_service_time, contender_service_time)

    def _report_processing_time(self, baseline_stats, contender_stats, task):
        baseline_processing_time = baseline_stats.metrics(task)["processing_time"]
        contender_processing_time = contender_stats.metrics(task)["processing_time"]
        return self._report_percentiles("processing time", task, baseline_processing_time, contender_processing_time)

    def _report_percentiles(self, name, task, baseline_values, contender_values):
        lines = []
        for percentile in metrics.percentiles_for_sample_size(sys.maxsize):
            baseline_value = baseline_values.get(metrics.encode_float_key(percentile))
            contender_value = contender_values.get(metrics.encode_float_key(percentile))
            self._append_non_empty(
                lines,
                self._line(
                    "%sth percentile %s" % (percentile, name),
                    baseline_value,
                    contender_value,
                    task,
                    "ms",
                    treat_increase_as_improvement=False,
                ),
            )
        return lines

    def _report_error_rate(self, baseline_stats, contender_stats, task):
        baseline_error_rate = baseline_stats.metrics(task)["error_rate"]
        contender_error_rate = contender_stats.metrics(task)["error_rate"]
        return self._join(
            self._line(
                "error rate",
                baseline_error_rate,
                contender_error_rate,
                task,
                "%",
                treat_increase_as_improvement=False,
                formatter=convert.factor(100.0),
            )
        )

    def _report_ml_processing_times(self, baseline_stats, contender_stats):
        lines = []
        for baseline in baseline_stats.ml_processing_time:
            job_name = baseline["job"]
            unit = baseline["unit"]
            # O(n^2) but we assume here only a *very* limited number of jobs (usually just one)
            for contender in contender_stats.ml_processing_time:
                if contender["job"] == job_name:
                    lines.append(
                        self._line(
                            "Min ML processing time", baseline["min"], contender["min"], job_name, unit, treat_increase_as_improvement=False
                        )
                    )
                    lines.append(
                        self._line(
                            "Mean ML processing time",
                            baseline["mean"],
                            contender["mean"],
                            job_name,
                            unit,
                            treat_increase_as_improvement=False,
                        )
                    )
                    lines.append(
                        self._line(
                            "Median ML processing time",
                            baseline["median"],
                            contender["median"],
                            job_name,
                            unit,
                            treat_increase_as_improvement=False,
                        )
                    )
                    lines.append(
                        self._line(
                            "Max ML processing time", baseline["max"], contender["max"], job_name, unit, treat_increase_as_improvement=False
                        )
                    )
        return lines

    def _report_transform_processing_times(self, baseline_stats, contender_stats):
        lines = []
        if baseline_stats.total_transform_processing_times is None:
            return lines
        for baseline in baseline_stats.total_transform_processing_times:
            transform_id = baseline["id"]
            for contender in contender_stats.total_transform_processing_times:
                if contender["id"] == transform_id:
                    lines.append(
                        self._line(
                            "Transform processing time",
                            baseline["mean"],
                            contender["mean"],
                            transform_id,
                            baseline["unit"],
                            treat_increase_as_improvement=False,
                        )
                    )
        for baseline in baseline_stats.total_transform_index_times:
            transform_id = baseline["id"]
            for contender in contender_stats.total_transform_index_times:
                if contender["id"] == transform_id:
                    lines.append(
                        self._line(
                            "Transform indexing time",
                            baseline["mean"],
                            contender["mean"],
                            transform_id,
                            baseline["unit"],
                            treat_increase_as_improvement=False,
                        )
                    )
        for baseline in baseline_stats.total_transform_search_times:
            transform_id = baseline["id"]
            for contender in contender_stats.total_transform_search_times:
                if contender["id"] == transform_id:
                    lines.append(
                        self._line(
                            "Transform search time",
                            baseline["mean"],
                            contender["mean"],
                            transform_id,
                            baseline["unit"],
                            treat_increase_as_improvement=False,
                        )
                    )
        for baseline in baseline_stats.total_transform_throughput:
            transform_id = baseline["id"]
            for contender in contender_stats.total_transform_throughput:
                if contender["id"] == transform_id:
                    lines.append(
                        self._line(
                            "Transform throughput",
                            baseline["mean"],
                            contender["mean"],
                            transform_id,
                            baseline["unit"],
                            treat_increase_as_improvement=True,
                        )
                    )
        return lines

    def _report_ingest_pipeline_counts(self, baseline_stats, contender_stats):
        if baseline_stats.ingest_pipeline_cluster_count is None:
            return []
        return self._join(
            self._line(
                "Total Ingest Pipeline count",
                baseline_stats.ingest_pipeline_cluster_count,
                contender_stats.ingest_pipeline_cluster_count,
                "",
                "",
                treat_increase_as_improvement=False,
            )
        )

    def _report_ingest_pipeline_times(self, baseline_stats, contender_stats):
        if baseline_stats.ingest_pipeline_cluster_time is None:
            return []
        return self._join(
            self._line(
                "Total Ingest Pipeline time",
                baseline_stats.ingest_pipeline_cluster_time,
                contender_stats.ingest_pipeline_cluster_time,
                "",
                "ms",
                treat_increase_as_improvement=False,
            )
        )

    def _report_ingest_pipeline_failed(self, baseline_stats, contender_stats):
        if baseline_stats.ingest_pipeline_cluster_failed is None:
            return []
        return self._join(
            self._line(
                "Total Ingest Pipeline failed",
                baseline_stats.ingest_pipeline_cluster_failed,
                contender_stats.ingest_pipeline_cluster_failed,
                "",
                "",
                treat_increase_as_improvement=False,
            )
        )

    def _report_disk_usage_stats_per_field(self, baseline_stats, contender_stats):
        best = {}
        for index, total, field in total_disk_usage_per_field(baseline_stats):
            best.setdefault(index, {})[field] = total
        collated_baseline = collate_disk_usage_stats(baseline_stats)
        for index, total, field in total_disk_usage_per_field(contender_stats):
            for_idx = best.setdefault(index, {})
            prev = for_idx.get(field, 0)
            if prev < total:
                for_idx[field] = total
        collated_contender = collate_disk_usage_stats(contender_stats)
        totals = []
        for index, for_idx in best.items():
            for field, total in for_idx.items():
                totals.append([index, total, field])
        totals.sort()

        lines = []
        for index, _total, field in totals:
            for stat in disk_usage_fields(baseline_stats):
                baseline_value = collated_baseline[index].get(field, {}).get(stat, 0)
                contender_value = collated_contender[index].get(field, {}).get(stat, 0)
                if baseline_value == 0 and contender_value == 0:
                    continue
                unit = convert.bytes_to_human_unit(min(baseline_value, contender_value))
                lines.append(
                    self._line(
                        f"{index} {field} {stat}",
                        baseline_value,
                        contender_value,
                        "",
                        unit,
                        treat_increase_as_improvement=False,
                        formatter=partial(convert.bytes_to_unit, unit),
                    )
                )
        return lines

    def _report_total_times(self, baseline_stats, contender_stats):
        lines = []
        lines.extend(
            self._report_total_time(
                "indexing time",
                baseline_stats.total_time,
                contender_stats.total_time,
            )
        )
        lines.extend(
            self._report_total_time_per_shard(
                "indexing time",
                baseline_stats.total_time_per_shard,
                contender_stats.total_time_per_shard,
            )
        )
        lines.extend(
            self._report_total_time(
                "indexing throttle time",
                baseline_stats.indexing_throttle_time,
                contender_stats.indexing_throttle_time,
            )
        )
        lines.extend(
            self._report_total_time_per_shard(
                "indexing throttle time",
                baseline_stats.indexing_throttle_time_per_shard,
                contender_stats.indexing_throttle_time_per_shard,
            )
        )
        lines.extend(
            self._report_total_time(
                "merge time",
                baseline_stats.merge_time,
                contender_stats.merge_time,
            )
        )
        lines.extend(
            self._report_total_count(
                "merge count",
                baseline_stats.merge_count,
                contender_stats.merge_count,
            )
        )
        lines.extend(
            self._report_total_time_per_shard(
                "merge time",
                baseline_stats.merge_time_per_shard,
                contender_stats.merge_time_per_shard,
            )
        )
        lines.extend(
            self._report_total_time(
                "merge throttle time",
                baseline_stats.merge_throttle_time,
                contender_stats.merge_throttle_time,
            )
        )
        lines.extend(
            self._report_total_time_per_shard(
                "merge throttle time",
                baseline_stats.merge_throttle_time_per_shard,
                contender_stats.merge_throttle_time_per_shard,
            )
        )
        lines.extend(
            self._report_total_time(
                "refresh time",
                baseline_stats.refresh_time,
                contender_stats.refresh_time,
            )
        )
        lines.extend(
            self._report_total_count(
                "refresh count",
                baseline_stats.refresh_count,
                contender_stats.refresh_count,
            )
        )
        lines.extend(
            self._report_total_time_per_shard(
                "refresh time",
                baseline_stats.refresh_time_per_shard,
                contender_stats.refresh_time_per_shard,
            )
        )
        lines.extend(
            self._report_total_time(
                "flush time",
                baseline_stats.flush_time,
                contender_stats.flush_time,
            )
        )
        lines.extend(
            self._report_total_count(
                "flush count",
                baseline_stats.flush_count,
                contender_stats.flush_count,
            )
        )
        lines.extend(
            self._report_total_time_per_shard(
                "flush time",
                baseline_stats.flush_time_per_shard,
                contender_stats.flush_time_per_shard,
            )
        )
        return lines

    def _report_total_time(self, name, baseline_total, contender_total):
        unit = "min"
        return self._join(
            self._line(
                f"Cumulative {name} of primary shards",
                baseline_total,
                contender_total,
                "",
                unit,
                treat_increase_as_improvement=False,
                formatter=convert.ms_to_minutes,
            ),
        )

    def _report_total_time_per_shard(self, name, baseline_per_shard, contender_per_shard):
        unit = "min"
        return self._join(
            self._line(
                f"Min cumulative {name} across primary shard",
                baseline_per_shard.get("min"),
                contender_per_shard.get("min"),
                "",
                unit,
                treat_increase_as_improvement=False,
                formatter=convert.ms_to_minutes,
            ),
            self._line(
                f"Median cumulative {name} across primary shard",
                baseline_per_shard.get("median"),
                contender_per_shard.get("median"),
                "",
                unit,
                treat_increase_as_improvement=False,
                formatter=convert.ms_to_minutes,
            ),
            self._line(
                f"Max cumulative {name} across primary shard",
                baseline_per_shard.get("max"),
                contender_per_shard.get("max"),
                "",
                unit,
                treat_increase_as_improvement=False,
                formatter=convert.ms_to_minutes,
            ),
        )

    def _report_total_count(self, name, baseline_total, contender_total):
        return self._join(
            self._line(f"Cumulative {name} of primary shards", baseline_total, contender_total, "", "", treat_increase_as_improvement=False)
        )

    def _report_gc_metrics(self, baseline_stats, contender_stats):
        line_method = self._line

        def _time_metric(metric_prefix, description):
            return line_method(
                f"Total {description} GC time",
                getattr(baseline_stats, f"{metric_prefix}_gc_time"),
                getattr(contender_stats, f"{metric_prefix}_gc_time"),
                "",
                "s",
                treat_increase_as_improvement=False,
                formatter=convert.ms_to_seconds,
            )

        def _count_metric(metric_prefix, description):
            return line_method(
                f"Total {description} GC count",
                getattr(baseline_stats, f"{metric_prefix}_gc_count"),
                getattr(contender_stats, f"{metric_prefix}_gc_count"),
                "",
                "",
                treat_increase_as_improvement=False,
            )

        return self._join(
            _time_metric("young", "Young Gen"),
            _count_metric("young", "Young Gen"),
            _time_metric("old", "Old Gen"),
            _count_metric("old", "Old Gen"),
            _time_metric("zgc_cycles", "ZGC Cycles"),
            _count_metric("zgc_cycles", "ZGC Cycles"),
            _time_metric("zgc_pauses", "ZGC Pauses"),
            _count_metric("zgc_pauses", "ZGC Pauses"),
        )

    def _report_disk_usage(self, baseline_stats, contender_stats):
        return self._join(
            self._line(
                "Store size",
                baseline_stats.store_size,
                contender_stats.store_size,
                "",
                "GB",
                treat_increase_as_improvement=False,
                formatter=convert.bytes_to_gb,
            ),
            self._line(
                "Translog size",
                baseline_stats.translog_size,
                contender_stats.translog_size,
                "",
                "GB",
                treat_increase_as_improvement=False,
                formatter=convert.bytes_to_gb,
            ),
        )

    def _report_segment_memory(self, baseline_stats, contender_stats):
        return self._join(
            self._line(
                "Heap used for segments",
                baseline_stats.memory_segments,
                contender_stats.memory_segments,
                "",
                "MB",
                treat_increase_as_improvement=False,
                formatter=convert.bytes_to_mb,
            ),
            self._line(
                "Heap used for doc values",
                baseline_stats.memory_doc_values,
                contender_stats.memory_doc_values,
                "",
                "MB",
                treat_increase_as_improvement=False,
                formatter=convert.bytes_to_mb,
            ),
            self._line(
                "Heap used for terms",
                baseline_stats.memory_terms,
                contender_stats.memory_terms,
                "",
                "MB",
                treat_increase_as_improvement=False,
                formatter=convert.bytes_to_mb,
            ),
            self._line(
                "Heap used for norms",
                baseline_stats.memory_norms,
                contender_stats.memory_norms,
                "",
                "MB",
                treat_increase_as_improvement=False,
                formatter=convert.bytes_to_mb,
            ),
            self._line(
                "Heap used for points",
                baseline_stats.memory_points,
                contender_stats.memory_points,
                "",
                "MB",
                treat_increase_as_improvement=False,
                formatter=convert.bytes_to_mb,
            ),
            self._line(
                "Heap used for stored fields",
                baseline_stats.memory_stored_fields,
                contender_stats.memory_stored_fields,
                "",
                "MB",
                treat_increase_as_improvement=False,
                formatter=convert.bytes_to_mb,
            ),
        )

    def _report_segment_counts(self, baseline_stats, contender_stats):
        return self._join(
            self._line(
                "Segment count",
                baseline_stats.segment_count,
                contender_stats.segment_count,
                "",
                "",
                treat_increase_as_improvement=False,
            )
        )

    def _join(self, *args):
        lines = []
        for arg in args:
            self._append_non_empty(lines, arg)
        return lines

    def _append_non_empty(self, lines, line):
        if line and len(line) > 0:
            lines.append(line)

    def _line(self, metric, baseline, contender, task, unit, treat_increase_as_improvement, formatter=lambda x: x):
        if baseline is not None and contender is not None:
            return [
                metric,
                str(task),
                formatter(baseline),
                formatter(contender),
                self._diff(baseline, contender, treat_increase_as_improvement, formatter),
                unit,
                self._diff(baseline, contender, treat_increase_as_improvement, formatter, as_percentage=True),
            ]
        else:
            return []

    def _diff(self, baseline, contender, treat_increase_as_improvement, formatter=lambda x: x, as_percentage=False):
        def identity(x):
            return x

        def _safe_divide(n, d):
            return n / d if d else 0

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

        if as_percentage:
            diff = _safe_divide(contender - baseline, baseline) * 100.0
            precision = 2
            suffix = "%"
        else:
            diff = formatter(contender - baseline)
            precision = 5
            suffix = ""

        # ensures that numbers that appear as "zero" are also colored neutrally
        threshold = 10**-precision
        formatted = f"{diff:.{precision}f}{suffix}"

        if diff >= threshold:
            return color_greater(f"+{formatted}")
        elif diff <= -threshold:
            return color_smaller(formatted)
        else:
            return color_neutral(formatted)
