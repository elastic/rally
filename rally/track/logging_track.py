import rally.track.track
import rally.cluster
import rally.utils.sysstats

loggingBenchmarkFastSettings = '''
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
    return es.search(index='logs')


class TermQuery(rally.track.track.Query):
  def __init__(self):
    rally.track.track.Query.__init__(self, "term")

  def run(self, es):
    return es.search(index='logs', doc_type='type', q='message:en')


class PhraseQuery(rally.track.track.Query):
  def __init__(self):
    rally.track.track.Query.__init__(self, "phrase")

  def run(self, es):
    return es.search(index='logs', doc_type='type', q='"ab_international.languages.ja+off"')


class HourlyAggQuery(rally.track.track.Query):
  def __init__(self):
    rally.track.track.Query.__init__(self, "hourly_agg")

  def run(self, es):
    return es.search(index='logs', doc_type='type', body='''
  {
      "size": 0,
      "aggregations": {
    "by_hour": {
        "date_histogram": {
      "field": "@timestamp",
      "interval": "hour"
        }
    }
      }
  }''')


class ScrollAllQuery(rally.track.track.Query):
  def __init__(self):
    rally.track.track.Query.__init__(self, "scroll", normalization_factor=25)

  def run(self, es):
    # Scroll, 1K docs at a time, 25 times:
    r = es.search(index='logs', doc_type='type', sort='_doc', scroll='10m', size=1000)
    count = 1
    for i in range(24):
      numHits = len(r['hits']['hits'])
      if numHits == 0:
        # done
        break
      r = es.scroll(scroll_id=r['_scroll_id'], scroll='10m')
      count += 1


# TODO dm: reintroduce 'ec2.i2.2xlarge' although it's more of an environment than a new benchmark... -> EC2 support!
loggingTrackSpec = rally.track.track.Track(
  name="Logging",
  description="This test indexes 6.9M short documents (log lines, total 14 GB json) using 8 client threads and 5000 docs per _bulk "
              "request against a single node running on a dual Xeon X5680 (12 real cores, 24 with hyperthreading) and 48 GB RAM.",
  source_url="s3://users.elasticsearch.org/etsy/jls/web-access_log-20140408.json.gz",
  # TODO dm: Have Mike upload the mappings file
  mapping_url="s3://users.elasticsearch.org/etsy/jls/mappings.json",
  index_name="logs",
  type_name="type",
  number_of_documents=6881288,
  compressed_size_in_bytes=1843865288,
  uncompressed_size_in_bytes=14641454513,
  local_file_name="web-access_log-20140408.json.gz",
  # for defaults alone, it's just around 20 minutes, for all it's about 60
  estimated_benchmark_time_in_minutes=20,
  # Queries to use in the search benchmark
  queries=[DefaultQuery(), TermQuery(), PhraseQuery(), HourlyAggQuery(), ScrollAllQuery()],
  # TODO dm: Be very wary of the order here!!! reporter.py assumes this order - see similar comment there
  track_setups=[
    rally.track.track.TrackSetup(
      name="defaults",
      description="append-only, using all default settings.",
      candidate_settings=rally.track.track.CandidateSettings(),
      benchmark_settings=rally.track.track.BenchmarkSettings(benchmark_search=True)
    ),

    rally.track.track.TrackSetup(
      name="4gheap",
      description="same as Defaults except using a 4 GB heap (ES_HEAP_SIZE), because the ES default (-Xmx1g) sometimes hits OOMEs.",
      candidate_settings=rally.track.track.CandidateSettings(heap='4g'),
      benchmark_settings=rally.track.track.BenchmarkSettings()
    ),

    rally.track.track.TrackSetup(
      name="fastsettings",
      description="append-only, using 4 GB heap, and these settings: <pre>%s</pre>" % loggingBenchmarkFastSettings,
      candidate_settings=rally.track.track.CandidateSettings(custom_config_snippet=loggingBenchmarkFastSettings, heap='4g'),
      benchmark_settings=rally.track.track.BenchmarkSettings(),
      required_cluster_status=rally.cluster.ClusterStatus.green
    ),

    rally.track.track.TrackSetup(
      name="fastupdates",
      description="the same as fast, except we pass in an ID (worst case random UUID) for each document and 25% of the time the ID already exists in the index.",
      candidate_settings=rally.track.track.CandidateSettings(custom_config_snippet=loggingBenchmarkFastSettings, heap='4g'),
      benchmark_settings=rally.track.track.BenchmarkSettings(id_conflicts=rally.track.track.IndexIdConflict.SequentialConflicts),
      required_cluster_status=rally.cluster.ClusterStatus.green
    ),

    rally.track.track.TrackSetup(
      name="two_nodes_defaults",
      description="append-only, using all default settings, but runs 2 nodes on 1 box (5 shards, 1 replica).",
      # integer divide!
      candidate_settings=rally.track.track.CandidateSettings(nodes=2, processors=rally.utils.sysstats.number_of_cpu_cores() // 2),
      benchmark_settings=rally.track.track.BenchmarkSettings(),
      required_cluster_status=rally.cluster.ClusterStatus.green
    ),

    # TODO dm: Reintroduce beast2
    # rally.track.track.TrackSetup(
    #  name="two_nodes_defaults",
    #  description="",
    #  # integer divide!
    #  candidate_settings=rally.track.track.CandidateSettings(custom_config_snippet=loggingBenchmarkFastSettings, heap='8g', nodes=4, processors=9,
    #  benchmark_settings=rally.track.track.TestSettings(),
    #  required_cluster_status=rally.cluster.ClusterStatus.green
    # ),
  ]
)
