import io
from unittest import TestCase

import pytest
import json
import re
import ijson
import ujson

from esrally.driver.runner import parse

'''
    -------------------------------------------------------------------------------------------------- benchmark 'parse': 7 tests --------------------------------------------------------------------------------------------------
    Name (time in ns)                             Min                     Max                  Mean                StdDev                Median                 IQR             Outliers  OPS (Kops/s)            Rounds  Iterations
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    test_pit_id_regexp_small                 842.0000 (1.0)       15,357.8000 (1.0)        967.6130 (1.0)        495.8377 (1.0)        877.4000 (1.0)       36.9000 (1.0)     2898;18495    1,033.4710 (1.0)      114548          10
    test_sort_end_anchor_regexp            1,376.0000 (1.63)      17,252.1000 (1.12)     1,578.4411 (1.63)       762.1846 (1.54)     1,439.9000 (1.64)      39.3000 (1.07)     2372;9288      633.5365 (0.61)      68527          10
    test_sort_find_all_regexp_small        1,415.5000 (1.68)      21,615.6000 (1.41)     1,633.4060 (1.69)       768.2293 (1.55)     1,482.3000 (1.69)      39.6000 (1.07)     3101;9104      612.2177 (0.59)      68218          10
    test_sort_rfind_and_regexp_small       1,473.6000 (1.75)      19,455.6000 (1.27)     1,738.2447 (1.80)       805.7860 (1.63)     1,581.0000 (1.80)      49.2000 (1.33)     2292;7573      575.2930 (0.56)      63316          10
    test_sort_reverse_and_regexp_small     1,685.6000 (2.00)      20,313.9000 (1.32)     1,976.1873 (2.04)       856.6169 (1.73)     1,789.3000 (2.04)      69.7000 (1.89)    2196;11553      506.0249 (0.49)      58306          10
    test_combined_json_small               4,287.0000 (5.09)      98,408.5000 (6.41)     5,576.6400 (5.76)     3,212.7776 (6.48)     4,734.0000 (5.40)     885.0000 (23.98)    6841;7424      179.3194 (0.17)     115943           2
    test_combined_ujson_small              4,374.0000 (5.19)     165,253.0000 (10.76)    5,534.0326 (5.72)     4,102.4746 (8.27)     4,716.0000 (5.37)     383.0000 (10.38)    2122;9639      180.7001 (0.17)      61581           1
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    
    --------------------------------------------------------------------------------------------------- benchmark 'parse_large': 7 tests --------------------------------------------------------------------------------------------------
    Name (time in ns)                              Min                     Max                   Mean                 StdDev                 Median                   IQR              Outliers  OPS (Kops/s)            Rounds  Iterations
    ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    test_pit_id_regexp_large                  842.0000 (1.0)       19,287.7000 (1.0)       1,002.1104 (1.0)         502.7188 (1.0)         884.1000 (1.0)        193.8000 (2.64)      3296;3368      997.8941 (1.0)      117689          10
    test_sort_rfind_and_regexp_large        1,488.2000 (1.77)      19,712.0000 (1.02)      1,763.6199 (1.76)        793.9146 (1.58)      1,594.4500 (1.80)        73.5000 (1.0)      2523;10923      567.0156 (0.57)      64198          10
    test_sort_reverse_and_regexp_large      4,028.5000 (4.78)      80,895.5000 (4.19)      4,657.8202 (4.65)      2,640.3021 (5.25)      4,277.0000 (4.84)       121.5000 (1.65)     2796;12989      214.6927 (0.22)     119905           2
    test_sort_end_anchor_regexp_large       8,658.0000 (10.28)    153,863.0000 (7.98)     10,059.0542 (10.04)     5,262.3357 (10.47)     9,126.0000 (10.32)      230.0000 (3.13)     3690;17134       99.4129 (0.10)     113779           1
    test_sort_find_all_regexp_large        18,455.0000 (21.92)    220,927.0000 (11.45)    20,997.7907 (20.95)     9,013.0615 (17.93)    19,117.0000 (21.62)      505.0000 (6.87)      2636;5355       47.6241 (0.05)      53889           1
    test_combined_json_large               63,015.0000 (74.84)    298,450.0000 (15.47)    76,093.1372 (75.93)    20,354.8730 (40.49)    69,292.0000 (78.38)    6,870.7500 (93.48)     1474;2306       13.1418 (0.01)      15799           1
    test_combined_ujson_large              64,402.0000 (76.49)    298,010.0000 (15.45)    73,103.8249 (72.95)    20,828.7271 (41.43)    66,854.5000 (75.62)    1,825.0000 (24.83)     1183;2480       13.6792 (0.01)      15214           1
    ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
'''
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
    parsed = parse(response_bytes, ["pit_id"])
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