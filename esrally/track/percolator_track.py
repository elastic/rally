from esrally.track import track

PERCOLATOR_INDEX_NAME = "queries"
PERCOLATOR_TYPE_NAME = "percolator"

# Workaround to support multiple versions (this is not how this will be handled in the future..)
percolatorIndexSettings = {
    "master": {
        "index.number_of_replicas": 0,
        "index.queries.cache.enabled": False
    },
    "5.0.0-alpha2": {
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
        "percolate" : {
          "field" : "query",
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
                "percolate" : {
                    "field" : "query",
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
        "percolate" : {
          "field" : "query",
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
    short_description="Percolator benchmark based on 2M AOL queries",
    description="This benchmark indexes 2M AOL queries and use the percolate query to match",
    source_root_url="http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/percolator",
    indices=[
        track.Index(name="queries", types=[
            # The type for the percolator queries:
            track.Type(
                name="percolator",
                mapping_file_name="queries-mapping.json",
                document_file_name="queries.json.bz2",
                number_of_documents=2000000,
                compressed_size_in_bytes=123502,
                uncompressed_size_in_bytes=148039748
            ),
            # The type used for documents being percolated:
            track.Type(
                name="content",
                mapping_file_name="document-mapping.json"
            )
        ])
    ],
    challenges=[track.Challenge(
        name="append-no-conflicts",
        description="Append documents without any ID conflicts",
        benchmark={
            track.BenchmarkPhase.index: track.IndexBenchmarkSettings(index_settings=percolatorIndexSettings),
            track.BenchmarkPhase.stats: track.LatencyBenchmarkSettings(warmup_iteration_count=100, iteration_count=100),
            track.BenchmarkPhase.search: track.LatencyBenchmarkSettings(warmup_iteration_count=100, iteration_count=100,
                                                                        queries=[
                                                                            PercolatorQuery(content="president bush"),
                                                                            PercolatorQuery(content="saddam hussein"),
                                                                            PercolatorQuery(content="hurricane katrina"),
                                                                            PercolatorQuery(content="google"),
                                                                            PercolatorQueryNoScoring(content="google"),
                                                                            PercolatorQueryWithHighlighting(),
                                                                            PercolatorQuery(content="ignore me"),
                                                                            PercolatorQueryNoScoring(content="ignore me")
                                                                        ])
        }
    )]
)
