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

import io
import json
import re
from unittest import TestCase

import pytest
import ujson

from esrally.driver import runner

@pytest.mark.benchmark(
    group="parse",
    warmup="on",
    warmup_iterations=10000,
    disable_gc=True
)
def test_sort_reverse_and_regexp_small(benchmark):
    benchmark(sort_parsing_candidate_reverse_and_regexp, ParsingBenchmarks.small_page)

@pytest.mark.benchmark(
    group="parse_large",
    warmup="on",
    warmup_iterations=10000,
    disable_gc=True
)
def test_sort_reverse_and_regexp_large(benchmark):
    benchmark(sort_parsing_candidate_reverse_and_regexp, ParsingBenchmarks.large_page)

def sort_parsing_candidate_reverse_and_regexp(response):
    reversed_response = response[::-1]
    sort_pattern = r"(\][^\]]*?\[):\"tros\""
    x = re.search(sort_pattern, reversed_response)
    # return json.loads(x.group(1)[::-1]) # mean 3.6 ms
    return ujson.loads(x.group(1)[::-1]) # mean 1.7 ms

@pytest.mark.benchmark(
    group="parse",
    warmup="on",
    warmup_iterations=10000,
    disable_gc=True
)
def test_sort_rfind_and_regexp_small(benchmark):
    benchmark(sort_parsing_candidate_rfind_and_regexp, ParsingBenchmarks.small_page)

@pytest.mark.benchmark(
    group="parse_large",
    warmup="on",
    warmup_iterations=10000,
    disable_gc=True
)
def test_sort_rfind_and_regexp_large(benchmark):
    benchmark(sort_parsing_candidate_rfind_and_regexp, ParsingBenchmarks.large_page)

def sort_parsing_candidate_rfind_and_regexp(response):
    index_of_last_sort = response.rfind('"sort"')
    sort_pattern = r"sort\":([^\]]*])"
    x = re.search(sort_pattern, response[index_of_last_sort::])
    # return json.loads(x.group(1)[::-1])
    return ujson.loads(x.group(1))

@pytest.mark.benchmark(
    group="parse",
    warmup="on",
    warmup_iterations=10000,
    disable_gc=True
)
def test_sort_end_anchor_regexp(benchmark):
    benchmark(sort_parsing_candidate_end_anchor_regexp, ParsingBenchmarks.small_page)

@pytest.mark.benchmark(
    group="parse_large",
    warmup="on",
    warmup_iterations=10000,
    disable_gc=True
)
def test_sort_end_anchor_regexp_large(benchmark):
    benchmark(sort_parsing_candidate_end_anchor_regexp, ParsingBenchmarks.large_page)

def sort_parsing_candidate_end_anchor_regexp(response):
    # predictably, no difference in using a literal lookahead vs just a surrounding pattern.  room for improvement?
    sort_pattern = r"\"sort\":([^\]]*])\}\]\}\}$"
    x = re.search(sort_pattern, response)
    # return ast.literal_eval(x.group(1)) # mean 8.6 ms
    # return json.loads(x.group(1)) # mean 3.2 ms
    return ujson.loads(x.group(1)) # mean 1.5 ms

@pytest.mark.benchmark(
    group="parse",
    warmup="on",
    warmup_iterations=10000,
    disable_gc=True
)
def test_sort_find_all_regexp_small(benchmark):
    benchmark(sort_parsing_candidate_find_all, ParsingBenchmarks.small_page)

@pytest.mark.benchmark(
    group="parse_large",
    warmup="on",
    warmup_iterations=10000,
    disable_gc=True
)
def test_sort_find_all_regexp_large(benchmark):
    benchmark(sort_parsing_candidate_find_all, ParsingBenchmarks.large_page)

def sort_parsing_candidate_find_all(response):
    sort_pattern = r"\"sort\":([^\]]+])"
    x = re.findall(sort_pattern, response)
    return ujson.loads(x[-1])

@pytest.mark.benchmark(
    group="parse",
    warmup="on",
    warmup_iterations=10000,
    disable_gc=True
)
def test_pit_id_regexp_small(benchmark):
    benchmark(pit_id_parsing_candidate_regexp, ParsingBenchmarks.small_page)

@pytest.mark.benchmark(
    group="parse_large",
    warmup="on",
    warmup_iterations=10000,
    disable_gc=True
)
def test_pit_id_regexp_large(benchmark):
    benchmark(pit_id_parsing_candidate_regexp, ParsingBenchmarks.large_page)

def pit_id_parsing_candidate_regexp(response):
    pit_id_pattern = r'"pit_id":"([^"]*)"' # 0.9 ms
    x = re.search(pit_id_pattern, response)
    return x.group(1)

@pytest.mark.benchmark(
    group="parse",
    warmup="on",
    warmup_iterations=10000,
    disable_gc=True
)
def test_combined_json_small(benchmark):
    benchmark(combined_parsing_candidate_json_loads, ParsingBenchmarks.small_page)

@pytest.mark.benchmark(
    group="parse_large",
    warmup="on",
    warmup_iterations=10000,
    disable_gc=True
)
def test_combined_json_large(benchmark):
    benchmark(combined_parsing_candidate_json_loads, ParsingBenchmarks.large_page)

def combined_parsing_candidate_json_loads(response):
    parsed_response = json.loads(response)
    pit_id = parsed_response.get("pit_id")
    sort = parsed_response.get("hits").get("hits")[-1].get("sort")
    return pit_id, sort

@pytest.mark.benchmark(
    group="parse_large",
    warmup="on",
    warmup_iterations=10000,
    disable_gc=True
)
def test_combined_ijson_large(benchmark):
    benchmark(combined_parsing_candidate_json_loads, ParsingBenchmarks.large_page)

@pytest.mark.benchmark(
    group="parse",
    warmup="on",
    warmup_iterations=10000,
    disable_gc=True
)
def test_combined_ijson_small(benchmark):
    benchmark(combined_parsing_candidate_json_loads, ParsingBenchmarks.small_page)

def combined_parsing_candidate_ijson_loads(response):
    parsed_response = ujson.loads(response)
    pit_id = parsed_response.get("pit_id")
    sort = parsed_response.get("hits").get("hits")[-1].get("sort")
    return pit_id, sort

@pytest.mark.benchmark(
    group="parse",
    warmup="on",
    warmup_iterations=10000,
    disable_gc=True
)
def test_pit_id_parse_small(benchmark):
    page = ParsingBenchmarks.small_page.encode()
    benchmark(pit_id_parsing_candidate_runner_parse, page)

@pytest.mark.benchmark(
    group="parse_large",
    warmup="on",
    warmup_iterations=10000,
    disable_gc=True
)
def test_pit_id_parse_large(benchmark):
    page = ParsingBenchmarks.large_page.encode()
    benchmark(pit_id_parsing_candidate_runner_parse, page)

def pit_id_parsing_candidate_runner_parse(response):
    response_bytes = io.BytesIO(response)
    parsed = runner.parse(response_bytes, ["pit_id"])
    pit_id = parsed["pit_id"]
    return pit_id


class ParsingBenchmarks(TestCase):

    def test_all_candidates(self):
        """
        Quick utility test to ensure all benchmark cases are correct
        """

        pit_id = pit_id_parsing_candidate_runner_parse(self.small_page.encode())
        self.assertEqual("fedcba9876543210", pit_id)

        sort = sort_parsing_candidate_reverse_and_regexp(self.small_page)
        self.assertEqual([1609780186,"2"], sort)

        sort = sort_parsing_candidate_rfind_and_regexp(self.large_page)
        self.assertEqual([1609780186, "2"], sort)

        sort = sort_parsing_candidate_end_anchor_regexp(self.small_page)
        self.assertEqual([1609780186,"2"], sort)

        sort = sort_parsing_candidate_find_all(self.large_page)
        self.assertEqual([1609780186,"2"], sort)

        pit_id = pit_id_parsing_candidate_regexp(self.large_page)
        self.assertEqual("fedcba9876543210", pit_id)

        pit_id, sort = combined_parsing_candidate_json_loads(self.small_page)
        self.assertEqual([1609780186, "2"], sort)
        self.assertEqual("fedcba9876543210", pit_id)

    small_page = """
    {
            "pit_id": "fedcba9876543210",
            "took": 10,
            "timed_out": false,
            "hits": {
                "total": 2,
                "hits": [
                    {
                        "_id": "1",
                         "timestamp": 1609780186,
                         "sort": [1609780186, "1"]
                    },
                    {
                        "_id": "2",
                         "timestamp": 1609780186,
                         "sort": [1609780186, "2"]
                    }
                ]
            }
        }
    """.replace("\n", "").replace(" ", "") # assume client never calls ?pretty :)

    large_page = ("""
        {
            "pit_id": "fedcba9876543210",
            "took": 10,
            "timed_out": false,
            "hits": {
                "total": 2,
                "hits": [""" + """
                    {
                        "_id": "1",
                         "timestamp": 1609780186,
                         "sort": [1609780186, "1"]
                    },""" * 100 + """
                    {
                        "_id": "2",
                         "timestamp": 1609780186,
                         "sort": [1609780186, "2"]
                    }
                ]
            }
        }
    """).replace("\n", "").replace(" ", "")
