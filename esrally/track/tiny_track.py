from esrally.track import track

TINY_INDEX_NAME = "tiny"
TINY_TYPE_NAME = "type"


class DefaultQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "default")

    def run(self, es):
        return es.search(index=TINY_INDEX_NAME)


class TermQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "term")

    def run(self, es):
        return es.search(index=TINY_INDEX_NAME, doc_type=TINY_TYPE_NAME, q="country_code:AT")


class PhraseQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "phrase")

    def run(self, es):
        return es.search(index=TINY_INDEX_NAME, doc_type=TINY_TYPE_NAME, body='''
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
        return es.search(index=TINY_INDEX_NAME, doc_type=TINY_TYPE_NAME,
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
        self.scroll_id = None

    def run(self, es):
        r = es.search(
            index=TINY_INDEX_NAME,
            doc_type=TINY_TYPE_NAME,
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


tinyTrackSpec = track.Track(
    name="tiny",
    short_description="First 2k documents of the geonames track for local tests",
    description="This test indexes 8.6M documents (POIs from Geonames, total 2.8 GB json) using 8 client threads and 5000 docs per bulk "
                "request against Elasticsearch",
    source_root_url="http://benchmarks.elastic.co/corpora/tiny",
    indices=[
        track.Index(name=TINY_INDEX_NAME, types=[
            track.Type(
                name=TINY_TYPE_NAME,
                mapping_file_name="mappings.json",
                document_file_name="documents.json.bz2",
                number_of_documents=2000,
                compressed_size_in_bytes=28333,
                uncompressed_size_in_bytes=564930)
        ])
    ],
    # Queries to use in the search benchmark
    queries=[
        DefaultQuery(),
        TermQuery(),
        PhraseQuery(),
        CountryAggQuery(use_request_cache=False),
        CountryAggQuery(suffix="_cached", use_request_cache=True),
        ScrollQuery()
    ],
    track_setups=track.track_setups)

