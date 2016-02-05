from rally.track import track


class DefaultQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "default")

    def run(self, es):
        return es.search(index=geopointTrackSpec.index_name)


class BBoxQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "bbox")

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


class DistanceQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "distance")

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


class DistanceRangeQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "distanceRange")

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


class PolygonQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "polygon")

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


geopointTrackSpec = track.Track(
    name="Geopoint",
    description="This test indexes 60.8M documents (POIs from PlanetOSM, total 3.5 GB json) using 8 client threads and 5000 docs per bulk "
                "request against Elasticsearch",
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
    track_setups=track.track_setups
)
