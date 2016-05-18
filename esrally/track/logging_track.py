from datetime import datetime
from esrally.track import track

# actually this should be one index per day (for testing Instant Kibana)
LOGGING_INDEX_PREFIX = "logs-"
LOGGING_INDEX_PATTERN = "%s*" % LOGGING_INDEX_PREFIX
LOGGING_TYPE_NAME = "type"


class DefaultQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "default")

    def run(self, es):
        return es.search(index=LOGGING_INDEX_PATTERN)


class TermQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "term")

    def run(self, es):
        # TODO dm: Fix me: Add a proper term query here
        return es.search(index=LOGGING_INDEX_PATTERN, doc_type=LOGGING_TYPE_NAME, q="")


class RangeQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "range")
        first_day = datetime.strptime("26.04.1998", "%d.%m.%Y")
        now = datetime.now()
        # always query all but the first 10 days in the data set (there are 88 days)
        self.diff_days = (now - first_day).days - 10

    def run(self, es):
        return es.search(index=LOGGING_INDEX_PATTERN, doc_type=LOGGING_TYPE_NAME, body='''
{
  "query": {
    "range": {
      "@timestamp": {
        "gte": "now-%dd/d",
        "lt": "now/d"
      }
    }
  }
}
''' % self.diff_days)


#TODO dm: Turn off request cache?
class HourlyAggQuery(track.Query):
    def __init__(self):
        track.Query.__init__(self, "hourly_agg")

    def run(self, es):
        return es.search(index=LOGGING_INDEX_PATTERN, doc_type=LOGGING_TYPE_NAME, body='''
  {
  "size": 0,
  "aggs": {
    "by_hour": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "hour"
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
            index=LOGGING_INDEX_PATTERN,
            doc_type=LOGGING_TYPE_NAME,
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



logging_indices = [
    # date pattern, uncompressed, compressed, num docs
    # ("30041998", 42761, 2574, 325),
    # ("01051998", 159533957, 5995048, 1193254),
    # ("02051998", 101584272, 3892369, 755705),
    # ("03051998", 102351889, 3930074, 759462),
    # ("04051998", 170093830, 6403163, 1262355),
    # ("05051998", 203888981, 7735877, 1521490),
    # ("06051998", 226694959, 8572377, 1687378),
    # ("07051998", 229348975, 8645476, 1706185),
    # ("08051998", 220548339, 8368724, 1649294),
    # ("09051998", 136087579, 5259605, 1015056),
    # ("10051998", 115069802, 4459406, 856126),
    # ("11051998", 229599377, 8722505, 1710378),
    # ("12051998", 313548483, 10250401, 2390900),
    # ("13051998", 232771638, 9301395, 1728940),
    # ("14051998", 256276051, 10053754, 1907968),
    # ("15051998", 279662653, 10737399, 2083564),
    # ("16051998", 233338908, 8867329, 1748699),
    # ("17051998", 198815169, 7705242, 1483014),
    # ("18051998", 206312374, 7819108, 1544561),
    # ("19051998", 380093428, 13792061, 2848216),
    # ("20051998", 407446295, 15179923, 3038231),
    # ("21051998", 317587117, 11822364, 2372684),
    # ("22051998", 439888196, 16435208, 3278410),
    # ("23051998", 313929092, 11903914, 2339981),
    # ("24051998", 298974313, 11322717, 2225196),
    ("25051998", 467676639, 17520064, 3480705),
    ("26051998", 278213029, 10460503, 2072251),
    ("27051998", 522837325, 19715617, 3899565),
    ("28051998", 28420256, 1088523, 212087),
    ("29051998", 71993994, 2718454, 537171),
    ("30051998", 12985999, 496377, 96914),
    ("31051998", 56192882, 2163396, 418067),
    ("01061998", 85402320, 3251168, 635635),
    ("02061998", 80028474, 3057268, 596576),
    ("03061998", 219224461, 8360743, 1636775),
]


def create_indices():
    indices = []
    for index in logging_indices:
        if index:
            date, uncompressed, compressed, num_docs = index
            indices.append(track.Index(name=LOGGING_INDEX_PREFIX + date, types=[
                track.Type(
                    name=LOGGING_TYPE_NAME,
                    mapping_file_name="mappings.json",
                    document_file_name="documents-%s.json.bz2" % date,
                    number_of_documents=num_docs,
                    compressed_size_in_bytes=compressed,
                    uncompressed_size_in_bytes=uncompressed)]))
    return indices


loggingTrackSpec = track.Track(
    name="logging",
    short_description="Logging benchmark",
    description="This benchmark indexes HTTP server log data from the 1998 world cup.",
    source_root_url="http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/logging",
    indices=create_indices(),
    queries=[
        DefaultQuery(),
        TermQuery(),
        RangeQuery(),
        HourlyAggQuery(),
        ScrollQuery()
    ],
    challenges=track.challenges)


