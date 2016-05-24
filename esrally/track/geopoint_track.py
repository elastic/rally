from esrally.track import track

GEO_POINT_INDEX_NAME = "osmgeopoints"
GEO_POINT_TYPE_NAME = "type"


class DefaultQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "default")

    def run(self, es):
        return es.search(index=GEO_POINT_INDEX_NAME)


class BBoxQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "bbox")

    def run(self, es):
        return es.search(index=GEO_POINT_INDEX_NAME, doc_type=GEO_POINT_TYPE_NAME, body='''
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


class DistanceQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "distance")

    def run(self, es):
        return es.search(index=GEO_POINT_INDEX_NAME, doc_type=GEO_POINT_TYPE_NAME, body='''
    {
      "query" : {
        "geo_distance" : {
          "distance" : "200km",
          "location" : [7.0, 55.0]
        }
      }
    }''')


class DistanceRangeQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "distanceRange")

    def run(self, es):
        return es.search(index=GEO_POINT_INDEX_NAME, doc_type=GEO_POINT_TYPE_NAME, body='''
    {
      "query" : {
        "geo_distance_range" : {
          "from" : "200km",
          "to" : "400km",
          "location" : [7.0, 55.0]
        }
      }
    }''')


class PolygonQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "polygon")

    def run(self, es):
        return es.search(index=GEO_POINT_INDEX_NAME, doc_type=GEO_POINT_TYPE_NAME, body='''
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


geopointTrackSpec = track.Track(
    name="geopoint",
    short_description="60.8M POIs from PlanetOSM",
    description="This test indexes 60.8M documents (POIs from PlanetOSM, total 2.3 GB json) using 8 client threads and 5000 docs per bulk "
                "request against Elasticsearch",
    source_root_url="http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/geopoint",
    index_name=GEO_POINT_INDEX_NAME,
    type_name=GEO_POINT_TYPE_NAME,
    number_of_documents=60844404,
    compressed_size_in_bytes=505295401,
    uncompressed_size_in_bytes=2448564579,
    document_file_name="documents.json.bz2",
    mapping_file_name="mappings.json",
    challenges=[
        track.Challenge(
            name="append-no-conflicts",
            description="Append documents without any ID conflicts",
            benchmark={
                track.BenchmarkPhase.index: track.IndexBenchmarkSettings(index_settings=track.greenNodeSettings),
                track.BenchmarkPhase.stats: track.LatencyBenchmarkSettings(warmup_iteration_count=100, iteration_count=100),
                track.BenchmarkPhase.search: track.LatencyBenchmarkSettings(warmup_iteration_count=1000, iteration_count=1000,
                                                                            queries=[PolygonQuery(),
                                                                                     BBoxQuery(),
                                                                                     DistanceQuery(),
                                                                                     DistanceRangeQuery()])
            }
        ),
        track.Challenge(
            name="append-fast-no-conflicts",
            description="append-only, using 4 GB heap, and these settings: <pre>%s</pre>" % track.benchmarkFastSettings,
            benchmark={
                track.BenchmarkPhase.index: track.IndexBenchmarkSettings(index_settings=track.benchmarkFastSettings)
            }
        ),

        track.Challenge(
            name="append-fast-with-conflicts",
            description="the same as fast, except we pass in an ID (worst case random UUID) for each document and 25% of the time the ID "
                        "already exists in the index.",
            benchmark={
                track.BenchmarkPhase.index: track.IndexBenchmarkSettings(index_settings=track.benchmarkFastSettings,
                                                                         id_conflicts=track.IndexIdConflict.SequentialConflicts)
            }
        )
    ]
)
