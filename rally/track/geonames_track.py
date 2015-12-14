import rally.track.track
import rally.cluster
import rally.utils.sysstats

geonamesBenchmarkFastSettings = '''
index.refresh_interval: 30s

index.number_of_shards: 6
index.number_of_replicas: 0

index.translog.flush_threshold_size: 4g
index.translog.flush_threshold_ops: 500000
'''


class DefaultQuery(rally.track.track.Query):
  def __init__(self):
    rally.track.track.Query.__init__(self, "default")

  def run(self, es):
    return es.search(index=geonamesTrackSpec.index_name)


class TermQuery(rally.track.track.Query):
  def __init__(self):
    rally.track.track.Query.__init__(self, "term")

  def run(self, es):
    return es.search(index=geonamesTrackSpec.index_name, doc_type=geonamesTrackSpec.type_name, q='country_code:AT')


class CountryAggQuery(rally.track.track.Query):
  def __init__(self):
    rally.track.track.Query.__init__(self, "country_agg")

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


class ScrollQuery(rally.track.track.Query):
  PAGES = 25
  ITEMS_PER_PAGE = 1000

  def __init__(self):
    rally.track.track.Query.__init__(self, "scroll", normalization_factor=self.PAGES)

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
      hit_count = len(r['hits']['hits'])
      if hit_count == 0:
        # done
        break
      r = es.scroll(scroll_id=r['_scroll_id'], scroll='10m')


geonamesTrackSpec = rally.track.track.Track(
  name="Geonames",
  description="This test indexes 8.6M documents (POIs from Geonames, total 2.8 GB json) using 8 client threads and 5000 docs per bulk request against Elasticsearch",
  # TODO dm: Change URL schema to: $ROOT/$benchmark-name/$index-name/$type-name/ (see https://github.com/elastic/rally/issues/26)
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
  # TODO dm: Be very wary of the order here!!! reporter.py assumes this order - see similar comment there
  track_setups=[
    rally.track.track.TrackSetup(
      name="defaults",
      description="append-only, using all default settings.",
      candidate_settings=rally.track.track.CandidateSettings(),
      benchmark_settings=rally.track.track.BenchmarkSettings(benchmark_search=True)
    ),
    # rally.track.track.TrackSetup(
    #   name="4gheap",
    #   description="same as Defaults except using a 4 GB heap (ES_HEAP_SIZE), because the ES default (-Xmx1g) sometimes hits OOMEs.",
    #   candidate_settings=rally.track.track.CandidateSettings(heap='4g'),
    #   benchmark_settings=rally.track.track.TestSettings()
    # ),
    #
    # rally.track.track.TrackSetup(
    #   name="fastsettings",
    #   description="append-only, using 4 GB heap, and these settings: <pre>%s</pre>" % geonamesBenchmarkFastSettings,
    #   candidate_settings=rally.track.track.CandidateSettings(custom_config_snippet=geonamesBenchmarkFastSettings, heap='4g'),
    #   benchmark_settings=rally.track.track.TestSettings(),
    #   required_cluster_status=rally.cluster.ClusterStatus.green
    # ),
    #
    # rally.track.track.TrackSetup(
    #   name="fastupdates",
    #   description="the same as fast, except we pass in an ID (worst case random UUID) for each document and 25% of the time the ID already exists in the index.",
    #   candidate_settings=rally.track.track.CandidateSettings(custom_config_snippet=geonamesBenchmarkFastSettings, heap='4g'),
    #   benchmark_settings=rally.track.track.TestSettings(id_conflicts=track.IndexIdConflict.SequentialConflicts),
    #   required_cluster_status=rally.cluster.ClusterStatus.green
    # ),
    #
    # rally.track.track.TrackSetup(
    #   name="two_nodes_defaults",
    #   description="append-only, using all default settings, but runs 2 nodes on 1 box (5 shards, 1 replica).",
    #   # integer divide!
    #   candidate_settings=track.CandidateSettings(nodes=2, processors=sysstats.number_of_cpu_cores() // 2),
    #   benchmark_settings=track.TestSettings(),
    #   required_cluster_status=rally.cluster.ClusterStatus.green
    # ),
  ]
)
