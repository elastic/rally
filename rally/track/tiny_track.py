from rally.track import track


class DefaultQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "default")

    def run(self, es):
        return es.search(index=tinyTrackSpec.index_name)


class TermQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "term")

    def run(self, es):
        return es.search(index=tinyTrackSpec.index_name, doc_type=tinyTrackSpec.type_name, q="country_code:AT")


class PhraseQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "phrase")

    def run(self, es):
        return es.search(index=tinyTrackSpec.index_name, doc_type=tinyTrackSpec.type_name, body='''
{
    "query": {
        "match_phrase": {
            "name": "Sankt Georgen"
        }
    }
}''')


class CountryAggQuery(track.Query):
    def __init__(self, suffix="", use_request_cache=True):
        track.Query.__init__(self, "country_agg" + suffix)
        self.use_request_cache = use_request_cache

    def run(self, es):
        return es.search(index=tinyTrackSpec.index_name, doc_type=tinyTrackSpec.type_name,
                         request_cache=self.use_request_cache, body='''
    {
      "size": 0,
      "aggs": {
        "country_population": {
          "terms": {
            "field": "country_code"
          },
          "aggs": {
            "sum_population": {
                "sum": {
                  "field": "population"
                }
              }
            }
        }
      }
    }''')


class ScrollQuery(track.Query):
    PAGES = 25
    ITEMS_PER_PAGE = 1000

    def __init__(self):
        track.Query.__init__(self, "scroll", normalization_factor=self.PAGES)

    def run(self, es):
        r = es.search(
            index=tinyTrackSpec.index_name,
            doc_type=tinyTrackSpec.type_name,
            sort='_doc',
            scroll='10m',
            size=self.ITEMS_PER_PAGE)
        # Note that starting with ES 2.0, the initial call to search() returns already the first result page
        # so we have to retrieve one page less
        for i in range(self.PAGES - 1):
            hit_count = len(r["hits"]["hits"])
            if hit_count == 0:
                # done
                break
            r = es.scroll(scroll_id=r["_scroll_id"], scroll="10m")


tinyTrackSpec = track.Track(
    name="tiny",
    description="This track is based on the geonames track and is just intended for quick integration test during development. This is NOT "
                "a serious benchmark.",
    source_root_url="http://benchmarks.elastic.co/corpora/tiny",
    index_name="tiny",
    type_name="type",
    number_of_documents=2000,
    compressed_size_in_bytes=28333,
    uncompressed_size_in_bytes=564930,
    document_file_name="documents.json.bz2",
    mapping_file_name="mappings.json",
    estimated_benchmark_time_in_minutes=1,
    # Queries to use in the search benchmark
    queries=[
        DefaultQuery(),
        TermQuery(),
        PhraseQuery(),
        CountryAggQuery(use_request_cache=False),
        CountryAggQuery(suffix="_cached", use_request_cache=True),
        ScrollQuery()
    ],
    track_setups=track.track_setups
)
