import rally.track.track
import rally.cluster
import rally.utils.sysstats

geopointBenchmarkFastSettings = '''
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
    return es.search(index=geopointTrackSpec.index_name)


class BBoxQuery(rally.track.track.Query):
  def __init__(self):
    rally.track.track.Query.__init__(self, "bbox")

  def run(self, es):
    return es.search(index=geopointTrackSpec.index_name, doc_type=geopointTrackSpec.type_name, body='''
    {
      "query" : {
        "geo_bounding_box" : {
          "location" : {
            "top_left" : [-0.1, 61.0],
            "bottom_right" : [15.0, 48.0]
          }
        }
      }
    }''')


class DistanceQuery(rally.track.track.Query):
  def __init__(self):
    rally.track.track.Query.__init__(self, "distance")

  def run(self, es):
    return es.search(index=geopointTrackSpec.index_name, doc_type=geopointTrackSpec.type_name, body='''
    {
      "query" : {
        "geo_distance" : {
          "distance" : "200km",
          "location" : [7.0, 55.0]
        }
      }
    }''')


class DistanceRangeQuery(rally.track.track.Query):
  def __init__(self):
    rally.track.track.Query.__init__(self, "distanceRange")

  def run(self, es):
    return es.search(index=geopointTrackSpec.index_name, doc_type=geopointTrackSpec.type_name, body='''
    {
      "query" : {
        "geo_distance_range" : {
          "from" : "200km",
          "to" : "400km",
          "location" : [7.0, 55.0]
        }
      }
    }''')


class PolygonQuery(rally.track.track.Query):
  def __init__(self):
    rally.track.track.Query.__init__(self, "polygon")

  def run(self, es):
    return es.search(index=geopointTrackSpec.index_name, doc_type=geopointTrackSpec.type_name, body='''
    {
      "query" : {
        "geo_polygon" : {
          "location" : {
            "points" : [[-0.1, 49.0],
                        [5.0, 48.0],
                        [15.0, 49.0],
                        [14.0, 60.0],
                        [-0.1, 61.0],
                        [-0.1, 49.0]]
          }
        }
      }
    }''')

geopointTrackSpec = rally.track.track.Track(
  name="Geopoint",
  description="This test indexes 60.8M documents (POIs from PlanetOSM, total 3.5 GB json) using 8 client threads and 5000 docs per bulk request against Elasticsearch",
  # TODO dm: Change URL schema to: $ROOT/$benchmark-name/$index-name/$type-name/ (see https://github.com/elastic/rally/issues/26)
  source_url="file:///data/benchmarks/geopoint/documents.json.bz2",
  mapping_url="file:///data/benchmarks/geopoint/mappings.json",
  index_name="osmgeopoints",
  type_name="type",
  number_of_documents=60844404,
  compressed_size_in_bytes=711754140,
  uncompressed_size_in_bytes=3769692039,
  local_file_name="documents.json",
  local_mapping_name="mappings.json",
  # for defaults alone, it's just around 20 minutes, for all it's about 60
  estimated_benchmark_time_in_minutes=20,
  # Queries to use in the search benchmark
  queries=[PolygonQuery(), BBoxQuery(), DistanceQuery(), DistanceRangeQuery()],
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
      description="append-only, using 4 GB heap, and these settings: <pre>%s</pre>" % geopointBenchmarkFastSettings,
      candidate_settings=rally.track.track.CandidateSettings(custom_config_snippet=geopointBenchmarkFastSettings, heap='4g'),
      benchmark_settings=rally.track.track.BenchmarkSettings(),
      required_cluster_status=rally.cluster.ClusterStatus.green
    ),

    rally.track.track.TrackSetup(
      name="fastupdates",
      description="the same as fast, except we pass in an ID (worst case random UUID) for each document and 25% of the time the ID already exists in the index.",
      candidate_settings=rally.track.track.CandidateSettings(custom_config_snippet=geopointBenchmarkFastSettings, heap='4g'),
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
  ]
)
