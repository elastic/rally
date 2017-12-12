import os
import pytest

from esrally.track import params, track


cwd = os.path.dirname(__file__)
with open(os.path.join(cwd, "terms.txt"), "r") as ins:
    terms = [line.strip() for line in ins.readlines()]


@pytest.mark.benchmark(
    group="search-params",
    warmup="on",
    warmup_iterations=10000,
    disable_gc=True
)
def test_search_params_with_one_body_param_small(benchmark):
    search = params.SearchParamSource(track=track.Track(name="benchmark-track"), params={
        "index": "_all",
        "body": {
            "suggest": {
                "song-suggest": {
                    "prefix": "nor",
                    "completion": {
                        "field": "suggest",
                        "fuzzy": {
                            "fuzziness": "AUTO"
                        }
                    }
                }
            }
        },
        "body-params": {
            "suggest.song-suggest.prefix": ["a", "b"]
        }

    })
    benchmark(search.params)

@pytest.mark.benchmark(
    group="search-params",
    warmup="on",
    warmup_iterations=10000,
    disable_gc=True
)
def test_search_params_with_one_body_param_long(benchmark):
    search = params.SearchParamSource(track=track.Track(name="benchmark-track"), params={
        "index": "_all",
        "body": {
            "suggest": {
                "song-suggest": {
                    "prefix": "nor",
                    "completion": {
                        "field": "suggest",
                        "fuzzy": {
                            "fuzziness": "AUTO"
                        }
                    }
                }
            }
        },
        "body-params": {
            "suggest.song-suggest.prefix": terms
        }

    })
    benchmark(search.params)


@pytest.mark.benchmark(
    group="search-params",
    warmup="on",
    warmup_iterations=10000,
    disable_gc=True
)
def test_search_params_with_multiple_body_params_long(benchmark):
    search = params.SearchParamSource(track=track.Track(name="benchmark-track"), params={
        "index": "_all",
        "body": {
            "suggest": {
                "song-suggest": {
                    "prefix": "nor",
                    "infix": "nor",
                    "suffix": "nor",
                    "completion": {
                        "field": "suggest",
                        "fuzzy": {
                            "fuzziness": "AUTO"
                        }
                    }
                }
            }
        },
        "body-params": {
            "suggest.song-suggest.prefix": terms,
            "suggest.song-suggest.infix": terms,
            "suggest.song-suggest.suffix": terms
        }
    })
    benchmark(search.params)


@pytest.mark.benchmark(
    group="search-params",
    warmup="on",
    warmup_iterations=10000,
    disable_gc=True
)
def test_search_params_no_body_params(benchmark):
    search = params.SearchParamSource(track=track.Track(name="benchmark-track"), params={
        "index": "_all",
        "body": {
            "suggest": {
                "song-suggest": {
                    "prefix": "nor",
                    "completion": {
                        "field": "suggest",
                        "fuzzy": {
                            "fuzziness": "AUTO"
                        }
                    }
                }
            }
        }
    })
    benchmark(search.params)
