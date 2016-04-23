from esrally.track import track

PERCOLATOR_INDEX_NAME = "queries"
PERCOLATOR_TYPE_NAME = ".percolator"

# Workaround to support multiple versions (this is not how this will be handled in the future..)
percolatorIndexSettings = {
    "master": {
        "index.number_of_replicas": 0,
        "index.queries.cache.enabled": False
    },
    "5.0.0-alpha1": {
        "index.number_of_replicas": 0,
        "index.queries.cache.type": "none"
    }
}


class PercolatorQuery(track.Query):
    def __init__(self, content):
        track.Query.__init__(self, "percolator query with content: %s" % content)
        self.content = content

    def run(self, es):
        return es.search(index=PERCOLATOR_INDEX_NAME, doc_type=PERCOLATOR_TYPE_NAME, body='''
    {
      "query" : {
        "percolator" : {
          "document_type" : "content",
          "document" : {
            "body" : "%s"
          }
        }
      }
    }''' % self.content)


class PercolatorQueryNoScoring(track.Query):
    def __init__(self, content):
        track.Query.__init__(self, "non scoring percolator query with content: %s" % content)
        self.content = content

    def run(self, es):
        return es.search(index=PERCOLATOR_INDEX_NAME, doc_type=PERCOLATOR_TYPE_NAME, body='''
    {
      "query" : {
        "constant_score": {
            "query": {
                "percolator" : {
                    "document_type" : "content",
                    "document" : {
                        "body" : "%s"
                    }
                }
            }
        }   
      }
    }''' % self.content)


class PercolatorQueryWithHighlighting(track.Query):
    def __init__(self):
        track.Query.__init__(self, "percolator query with highlighting")

    def run(self, es):
        return es.search(index=PERCOLATOR_INDEX_NAME, doc_type=PERCOLATOR_TYPE_NAME, body='''
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
                mapping_file_name="queries-mapping.json",
                document_file_name="queries.json.bz2",
                number_of_documents=2000000,
                compressed_size_in_bytes=123502,
                uncompressed_size_in_bytes=148039748
            ),
            # The used for documents being percolated:
            track.Type(
                name="content",
                mapping_file_name="document-mapping.json"
            )
        ])
    ],
    estimated_benchmark_time_in_minutes=5,
    # Queries to use in the search benchmark
    queries=[
        PercolatorQuery(content="president bush"),
        PercolatorQuery(content="saddam hussein"),
        PercolatorQuery(content="hurricane katrina"),
        PercolatorQuery(content="google"),
        PercolatorQueryNoScoring(content="google"),
        PercolatorQueryWithHighlighting(),
        PercolatorQuery(content="ignore me"),
        PercolatorQueryNoScoring(content="ignore me")
    ],
    track_setups=[track.TrackSetup(
        name="4gheap",
        description="same as Defaults except using a 4 GB heap (ES_HEAP_SIZE), because the ES default (-Xmx1g) sometimes hits OOMEs.",
        candidate=track.CandidateSettings(index_settings=percolatorIndexSettings, heap="4g"),
        benchmark={
            track.BenchmarkPhase.index: track.IndexBenchmarkSettings(),
            track.BenchmarkPhase.stats: track.LatencyBenchmarkSettings(iteration_count=100),
            track.BenchmarkPhase.search: track.LatencyBenchmarkSettings(iteration_count=100)
        }
    )]
)
