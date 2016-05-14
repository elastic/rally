from esrally.utils import sysstats
from esrally.track import track

PMC_INDEX_NAME = "pmc"
PMC_TYPE_NAME = "articles"


class DefaultQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "default")

    def run(self, es):
        return es.search(index=PMC_INDEX_NAME)


class TermQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "term")

    def run(self, es):
        return es.search(index=PMC_INDEX_NAME, doc_type=PMC_TYPE_NAME, q="body:physician")


class PhraseQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "phrase")

    def run(self, es):
        return es.search(index=PMC_INDEX_NAME, doc_type=PMC_TYPE_NAME, body='''
{
    "query": {
        "match_phrase": {
            "body": "newspaper coverage"
        }
    }
}''')


class MonthlyArticlesAggQuery(track.Query):
    def __init__(self, suffix="", use_request_cache=True):
        track.Query.__init__(self, "articles_monthly_agg" + suffix)
        self.use_request_cache = use_request_cache

    def run(self, es):
        return es.search(index=PMC_INDEX_NAME, doc_type=PMC_TYPE_NAME,
                         request_cache=self.use_request_cache, body='''
    {
      "size": 0,
      "aggs": {
        "articles_over_time" : {
            "date_histogram" : {
                "field" : "timestamp",
                "interval" : "month"
            }
        }
      }
    }''')


class ScrollQuery(track.Query):
    PAGES = 25
    ITEMS_PER_PAGE = 1000

    def __init__(self):
        track.Query.__init__(self, "scroll", normalization_factor=self.PAGES)
        self.scroll_id = None

    def run(self, es):
        r = es.search(
            index=PMC_INDEX_NAME,
            doc_type=PMC_TYPE_NAME,
            sort="_doc",
            scroll="10s",
            size=self.ITEMS_PER_PAGE)
        self.scroll_id = r["_scroll_id"]
        # Note that starting with ES 2.0, the initial call to search() returns already the first result page
        # so we have to retrieve one page less
        for i in range(self.PAGES - 1):
            hit_count = len(r["hits"]["hits"])
            if hit_count == 0:
                # done
                break
            r = es.scroll(scroll_id=self.scroll_id, scroll="10s")

    def close(self, es):
        if self.scroll_id:
            es.clear_scroll(scroll_id=self.scroll_id)
            self.scroll_id = None


pmc_track_setups = [
    track.TrackSetup(
        name="defaults",
        description="append-only, using all default settings.",
        candidate=track.CandidateSettings(),
        benchmark={
            track.BenchmarkPhase.index: track.IndexBenchmarkSettings(index_settings=track.greenNodeSettings, bulk_size=500),
            track.BenchmarkPhase.stats: track.LatencyBenchmarkSettings(iteration_count=100),
            track.BenchmarkPhase.search: track.LatencyBenchmarkSettings(iteration_count=1000)
        }
    ),
    track.TrackSetup(
        name="4gheap",
        description="same as Defaults except using a 4 GB heap (ES_HEAP_SIZE), because the ES default (-Xmx1g) sometimes hits OOMEs.",
        candidate=track.CandidateSettings(heap="4g"),
        benchmark={
            track.BenchmarkPhase.index: track.IndexBenchmarkSettings(index_settings=track.greenNodeSettings, bulk_size=500)
        }
    ),

    track.TrackSetup(
        name="fastsettings",
        description="append-only, using 4 GB heap, and these settings: <pre>%s</pre>" % track.benchmarkFastSettings,
        candidate=track.CandidateSettings(heap="4g"),
        benchmark={
            track.BenchmarkPhase.index: track.IndexBenchmarkSettings(index_settings=track.benchmarkFastSettings, bulk_size=500)
        }
    ),

    track.TrackSetup(
        name="fastupdates",
        description="the same as fast, except we pass in an ID (worst case random UUID) for each document and 25% of the time the ID "
                    "already exists in the index.",
        candidate=track.CandidateSettings(heap="4g"),
        benchmark={
            track.BenchmarkPhase.index: track.IndexBenchmarkSettings(index_settings=track.benchmarkFastSettings,
                                                                     bulk_size=500,
                                                                     id_conflicts=track.IndexIdConflict.SequentialConflicts)
        }
    ),

    track.TrackSetup(
        name="two_nodes_defaults",
        description="append-only, using all default settings, but runs 2 nodes on 1 box (5 shards, 1 replica).",
        # integer divide!
        candidate=track.CandidateSettings(nodes=2,
                                          processors=sysstats.logical_cpu_cores() // 2),
        benchmark={
            track.BenchmarkPhase.index: track.IndexBenchmarkSettings(index_settings=track.greenNodeSettings, bulk_size=500)
        }
    ),

    track.TrackSetup(
        name="defaults_verbose_iw",
        description="Based on defaults but specifically set up to gather merge part times.",
        candidate=track.CandidateSettings(logging_config=track.mergePartsLogConfig),
        benchmark={
            track.BenchmarkPhase.index: track.IndexBenchmarkSettings(index_settings=track.greenNodeSettings, bulk_size=500)
        }
    ),
]

pmcTrackSpec = track.Track(
    name="pmc",
    short_description="Full text benchmark containing 574.199 papers from PMC",
    description="This test indexes 574.199 papers from PMC (total 23.2 GB json) using 8 client threads and 500 docs per bulk "
                "request against Elasticsearch",
    source_root_url="http://benchmarks.elastic.co/corpora/pmc",
    index_name=PMC_INDEX_NAME,
    type_name=PMC_TYPE_NAME,
    number_of_documents=574199,
    compressed_size_in_bytes=5928712141,
    uncompressed_size_in_bytes=23256051757,
    document_file_name="documents.json.bz2",
    mapping_file_name="mappings.json",
    # Queries to use in the search benchmark
    queries=[
        DefaultQuery(),
        TermQuery(),
        PhraseQuery(),
        MonthlyArticlesAggQuery(use_request_cache=False),
        MonthlyArticlesAggQuery(suffix="_cached", use_request_cache=True),
        ScrollQuery()
    ],
    track_setups=pmc_track_setups
)
