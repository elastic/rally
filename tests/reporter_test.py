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
