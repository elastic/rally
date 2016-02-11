from rally.track import track


class DefaultQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "default")

    def run(self, es):
        return es.search(index=geonamesTrackSpec.index_name)


class TermQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "term")

    def run(self, es):
        return es.search(index=geonamesTrackSpec.index_name, doc_type=geonamesTrackSpec.type_name, q="country_code:AT")


class CountryAggQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "country_agg")

    def run(self, es):
        return es.search(index=geonamesTrackSpec.index_name, doc_type=geonamesTrackSpec.type_name, body='''
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
            index=geonamesTrackSpec.index_name,
            doc_type=geonamesTrackSpec.type_name,
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


track.Track(
    name="geonames",
    description="This test indexes 8.6M documents (POIs from Geonames, total 2.8 GB json) using 8 client threads and 5000 docs per bulk "
                "request against Elasticsearch",
    source_url="http://benchmarks.elastic.co/corpora/geonames/documents.json.bz2",
    mapping_url="http://benchmarks.elastic.co/corpora/geonames/mappings.json",
    index_name="geonames",
    type_name="type",
    number_of_documents=8647880,
    compressed_size_in_bytes=197857614,
    uncompressed_size_in_bytes=2790927196,
    local_file_name="documents.json.bz2",
    local_mapping_name="mappings.json",
    # for defaults alone, it's just around 20 minutes, for all it's about 60
    estimated_benchmark_time_in_minutes=20,
    # Queries to use in the search benchmark
    queries=[DefaultQuery(), TermQuery(), CountryAggQuery(), ScrollQuery()],
    track_setups=track.track_setups
)
