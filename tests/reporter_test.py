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
# pylint: disable=protected-access

from unittest import mock

from esrally import config, reporter
from esrally.metrics import GlobalStats
from esrally.utils import convert


class TestFormatter:
    def setup_method(self, method):
        self.empty_header = ["Header"]
        self.empty_data = []

        self.metrics_header = ["Metric", "Task", "Baseline", "Contender", "Diff", "Unit", "Diff %"]
        self.metrics_data = [
            ["Min Throughput", "index", 17300, 18000, 700, "ops/s", "4.04%"],
            ["Median Throughput", "index", 17500, 18500, 1000, "ops/s", "5.71%"],
            ["Max Throughput", "index", 17700, 19000, 1300, "ops/s", "7.34%"],
            ["Bubblegum", "", 1.7, 1.9, 0.2, "bubbles/flavonoid", "11.71%"],
        ]

    def test_formats_as_markdown_empty(self):
        formatted = reporter.format_as_markdown(self.empty_header, self.empty_data, "ignored")
        # 1 header line, 1 separation line + 0 data lines
        assert len(formatted.splitlines()) == 1 + 1 + 0

    def test_formats_as_markdown_numbers_align_decimal(self):
        formatted = reporter.format_as_markdown(self.metrics_header, self.metrics_data, "decimal")
        # 1 header line, 1 separation line + 3 data lines
        assert len(formatted.splitlines()) == 1 + 1 + 4
        assert "|    Min Throughput |  index |    17300   |     18000   |  700   |             ops/s |    4.04% |" in formatted.splitlines()
        assert "|         Bubblegum |        |        1.7 |         1.9 |    0.2 | bubbles/flavonoid |   11.71% |" in formatted.splitlines()

    def test_formats_as_markdown_numbers_align_right(self):
        formatted = reporter.format_as_markdown(self.metrics_header, self.metrics_data, "right")
        # 1 header line, 1 separation line + 3 data lines
        assert len(formatted.splitlines()) == 1 + 1 + 4
        assert "|    Min Throughput |  index |      17300 |       18000 |    700 |             ops/s |    4.04% |" in formatted.splitlines()
        assert "|         Bubblegum |        |        1.7 |         1.9 |    0.2 | bubbles/flavonoid |   11.71% |" in formatted.splitlines()

    def test_formats_as_csv(self):
        formatted = reporter.format_as_csv(self.empty_header, self.empty_data)
        # 1 header line, no separation line + 0 data lines
        assert len(formatted.splitlines()) == 1 + 0

        formatted = reporter.format_as_csv(self.metrics_header, self.metrics_data)
        # 1 header line, no separation line + 3 data lines
        assert len(formatted.splitlines()) == 1 + 4


class TestSummaryReporter:
    def setup_method(self, method):
        config_mock = mock.Mock(config.Config)
        config_mock.opts.return_value = False
        self.reporter = reporter.SummaryReporter(results=None, config=config_mock)

    def test_report_total_time_picks_unit_from_value(self):
        assert self.reporter._report_total_time("indexing time", 250) == [
            ["Cumulative indexing time of primary shards", "", 250, "ms"],
        ]
        assert self.reporter._report_total_time("indexing time", 1500) == [
            ["Cumulative indexing time of primary shards", "", 1.5, "s"],
        ]
        assert self.reporter._report_total_time("indexing time", 90_000) == [
            ["Cumulative indexing time of primary shards", "", 1.5, "min"],
        ]
        assert self.reporter._report_total_time("indexing time", 5_400_000) == [
            ["Cumulative indexing time of primary shards", "", 1.5, "h"],
        ]

    def test_report_total_time_per_shard_picks_unit_per_value(self):
        metrics = self.reporter._report_total_time_per_shard("merge time", {"min": 500, "median": 30_000, "max": 120_000})
        assert metrics == [
            ["Min cumulative merge time across primary shards", "", 500, "ms"],
            ["Median cumulative merge time across primary shards", "", 30, "s"],
            ["Max cumulative merge time across primary shards", "", 2, "min"],
        ]

    def test_report_total_time_skips_missing_value(self):
        assert self.reporter._report_total_time("indexing time", None) == []

    def test_report_gc_metrics(self):
        stats = GlobalStats(
            {
                "young_gc_time": 100,
                "young_gc_count": 1,
                "old_gc_time": 2_000,
                "old_gc_count": 1,
                "zgc_cycles_gc_time": 180_000,
                "zgc_cycles_gc_count": 1,
                "zgc_pauses_gc_time": 0,
                "zgc_pauses_gc_count": 0,
            }
        )
        assert self.reporter._report_gc_metrics(stats) == [
            ["Total Young Gen GC time", "", 100, "ms"],
            ["Total Young Gen GC count", "", 1, ""],
            ["Total Old Gen GC time", "", 2, "s"],
            ["Total Old Gen GC count", "", 1, ""],
            ["Total ZGC Cycles GC time", "", 3, "min"],
            ["Total ZGC Cycles GC count", "", 1, ""],
            ["Total ZGC Pauses GC time", "", 0, "s"],
            ["Total ZGC Pauses GC count", "", 0, ""],
        ]


class TestComparisonReporter:
    def setup_method(self, method):
        config_mock = mock.Mock(config.Config)
        config_mock.opts.return_value = True
        self.reporter = reporter.ComparisonReporter(config_mock)

    def test_diff_percent_divide_by_zero(self):
        formatted = self.reporter._diff(0, 0, False, as_percentage=True)
        assert formatted == "0.00%"

    def test_diff_percent_ignore_formatter(self):
        formatted = self.reporter._diff(1, 0, False, formatter=convert.factor(100.0), as_percentage=True)
        assert formatted == "-100.00%"

    def test_report_gc_metrics(self):
        r1 = GlobalStats(
            {
                "young_gc_time": 100,
                "young_gc_count": 1,
                "old_gc_time": 200,
                "old_gc_count": 1,
                "zgc_cycles_gc_time": 300,
                "zgc_cycles_gc_count": 1,
                "zgc_pauses_gc_time": 400,
                "zgc_pauses_gc_count": 1,
            }
        )

        r2 = GlobalStats(
            {
                "young_gc_time": 200,
                "young_gc_count": 2,
                "old_gc_time": 300,
                "old_gc_count": 2,
                "zgc_cycles_gc_time": 400,
                "zgc_cycles_gc_count": 2,
                "zgc_pauses_gc_time": 500,
                "zgc_pauses_gc_count": 2,
            }
        )
        metrics = self.reporter._report_gc_metrics(r1, r2)
        assert metrics == [
            ["Total Young Gen GC time", "", 0.1, 0.2, "+0.10000", "s", "+100.00%"],
            ["Total Young Gen GC count", "", 1, 2, "+1.00000", "", "+100.00%"],
            ["Total Old Gen GC time", "", 0.2, 0.3, "+0.10000", "s", "+50.00%"],
            ["Total Old Gen GC count", "", 1, 2, "+1.00000", "", "+100.00%"],
            ["Total ZGC Cycles GC time", "", 0.3, 0.4, "+0.10000", "s", "+33.33%"],
            ["Total ZGC Cycles GC count", "", 1, 2, "+1.00000", "", "+100.00%"],
            ["Total ZGC Pauses GC time", "", 0.4, 0.5, "+0.10000", "s", "+25.00%"],
            ["Total ZGC Pauses GC count", "", 1, 2, "+1.00000", "", "+100.00%"],
        ]
