import logging
import elasticsearch
import time
import certifi

logger = logging.getLogger("rally.metrics")


class EsMetricsStore:
  def __init__(self, config):
    self._config = config
    host = self._config.opts("reporting", "datastore.host")
    port = self._config.opts("reporting", "datastore.port")
    # poor man's boolean conversion
    secure = self._config.opts("reporting", "datastore.secure") == "True"
    user = self._config.opts("reporting", "datastore.user")
    password = self._config.opts("reporting", "datastore.password")
    self._environment_name = config.opts("system", "env.name")

    if user and password:
      auth = (user, password)
    else:
      auth = None

    self._client = elasticsearch.Elasticsearch(hosts=[{"host": host, "port": port}],
                                               use_ssl=secure, http_auth=auth, verify_certs=True, ca_certs=certifi.where())
    self._invocation = None
    self._track = None
    self._track_setup = None
    self._index = None

  def open(self, invocation, track, track_setup_name, create=False):
    self._invocation = invocation
    self._track = track.name
    self._track_setup = track_setup_name
    invocation_ts = '%04d' % invocation.year
    self._index = "rally-%s" % invocation_ts
    if create:
      script_dir = self._config.opts("system", "rally.root")
      mapping_template = "%s/resources/rally-mapping.json" % script_dir
      self._client.indices.put_template("rally", open(mapping_template).read())
      # ignore 400 cause by IndexAlreadyExistsException when creating an index
      self._client.indices.create(index=self._index, ignore=400)
    # ensure we can search immediately after opening
    self._client.indices.refresh(index=self._index)

  def close(self):
    # no-op here for the time being for compatibility
    pass

  # should be an int
  def put_count(self, name, count, unit=None):
    self._put(name, count, unit)

  # should be a float
  def put_value(self, name, value, unit):
    self._put(name, value, unit)

  def _put(self, name, value, unit):
    # TODO dm: Check the overhead and just cache and push afterwards to ES if it is too high
    doc = {
      "@timestamp": self._millis(time.time()),
      "trial-timestamp": self._seconds(self._invocation),
      "environment": self._environment_name,
      "track": self._track,
      "track-setup": self._track_setup,
      "name": name,
      "value": value,
      "unit": unit
    }
    self._client.create(index=self._index, doc_type="metrics", body=doc)

  def _millis(self, t):
    return int(round(t))

  def _seconds(self, t):
    return '%04d%02d%02dT%02d%02d%02dZ' % (t.year, t.month, t.day, t.hour, t.minute, t.second)

  def get_one(self, name):
    v = self.get(name)
    if v:
      return v[0]
    else:
      return v

  def get(self, name):
    query = {
      "query": {
        "bool": {
          "filter": [
            {
              "term": {
                "trial-timestamp": self._seconds(self._invocation)
              }
            },
            {
              "term": {
                "environment": self._environment_name
              }
            },
            {
              "term": {
                "track": self._track
              }
            },
            {
              "term": {
                "track-setup": self._track_setup
              }
            },
            {
              "term": {
                "name": name
              }
            }
          ]
        }
      }
    }
    result = self._client.search(index=self._index, doc_type="metrics", body=query)
    return [v["_source"]["value"] for v in result["hits"]["hits"]]