from rally.track import track

class PercolatorQuery1(track.Query):
    def __init__(self):
        track.Query.__init__(self, "percolator query 1")

    def run(self, es):
        return es.search(index=percolatorTrackSpec.index_name, doc_type=percolatorTrackSpec.type_name, body='''
    {
      "query" : {
        "percolator" : {
          "document_type" : "content",
          "document" : {
            "body" : "president bush"
          }
        }
      }
    }''')

class PercolatorQuery2(track.Query):
    def __init__(self):
        track.Query.__init__(self, "percolator query 2")

    def run(self, es):
        return es.search(index=percolatorTrackSpec.index_name, doc_type=percolatorTrackSpec.type_name, body='''
    {
      "query" : {
        "percolator" : {
          "document_type" : "content",
          "document" : {
            "body" : "saddam hussein"
          }
        }
      }
    }''')

class PercolatorQuery3(track.Query):
    def __init__(self):
        track.Query.__init__(self, "percolator query 3")

    def run(self, es):
        return es.search(index=percolatorTrackSpec.index_name, doc_type=percolatorTrackSpec.type_name, body='''
    {
      "query" : {
        "percolator" : {
          "document_type" : "content",
          "document" : {
            "body" : "hurricane katrina"
          }
        }
      }
    }''')

class PercolatorQuery4(track.Query):
    def __init__(self):
        track.Query.__init__(self, "percolator query 4")

    def run(self, es):
        return es.search(index=percolatorTrackSpec.index_name, doc_type=percolatorTrackSpec.type_name, body='''
    {
      "query" : {
        "percolator" : {
          "document_type" : "content",
          "document" : {
            "body" : "google"
          }
        }
      }
    }''')

class PercolatorQuery4AsConstantScoreQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "percolator query 4 wrapped in constant_score query")

    def run(self, es):
        return es.search(index=percolatorTrackSpec.index_name, doc_type=percolatorTrackSpec.type_name, body='''
    {
      "query" : {
        "constant_score": {
            "query": {
                "percolator" : {
                    "document_type" : "content",
                    "document" : {
                        "body" : "google"
                    }
                }
            }
        }   
      }
    }''')

class PercolatorQueryWithHighlighting(track.Query):
    def __init__(self):
        track.Query.__init__(self, "percolator query with highlighting")

    def run(self, es):
        return es.search(index=percolatorTrackSpec.index_name, doc_type=percolatorTrackSpec.type_name, body='''
    {
      "query": {
        "percolator" : {
          "document_type" : "content",
          "document" : {
            "body" : "Israeli prime minister Ariel Sharon suffers a massive stroke; he is replaced by acting prime minister Ehud Olmert"
          }
        }
      },
      "highlight": {
        "fields": {
          "body": {}
        }
      }
    }''') 

percolatorTrackSpec = track.Track(
    name="percolator",
    description="This test indexes 2M AOL queries and use the percolator query to match",
    source_root_url="http://benchmarks.elastic.co/corpora/percolator",
    indices=[
        track.Index(name="queries", types=[
            # The type for the percolator queries:
            track.Type(
                name=".percolator",
                mapping_file_name="queries-mappings.json",
                document_file_name="queries.json.bz2",
                number_of_documents=2000000,
                compressed_size_in_bytes=123502,
                uncompressed_size_in_bytes=148039748
            ),
            # The used for documents being percolated:
            track.Type(
                name="content",
                mapping_file_name="percolator-mappings.json"
            )
        ])
    ],
    estimated_benchmark_time_in_minutes=5,
    # Queries to use in the search benchmark
    queries=[PercolatorQuery1(), PercolatorQuery2(), PercolatorQuery3(), PercolatorQuery4(), PercolatorQuery4AsConstantScoreQuery(), PercolatorQueryWithHighlighting()],
    track_setups=[track.TrackSetup(
        name="4gheap",
        description="same as Defaults except using a 4 GB heap (ES_HEAP_SIZE), because the ES default (-Xmx1g) sometimes hits OOMEs.",
        candidate_settings=track.CandidateSettings(index_settings=track.greenNodeSettings, heap="4g"),
        benchmark_settings=track.BenchmarkSettings(benchmark_search=True)
    )]
)