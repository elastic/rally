import logging
import collections
import elasticsearch
import elasticsearch.helpers
import certifi

from rally import time, exceptions

logger = logging.getLogger("rally.metrics")


class EsClient:
    """
    Provides a stripped-down client interface that is easier to exchange for testing
    """

    def __init__(self, client):
        self._client = client

    def put_template(self, name, template):
        return self.guarded(self._client.indices.put_template, name, template)

    def create_index(self, index):
        # ignore 400 cause by IndexAlreadyExistsException when creating an index
        return self.guarded(self._client.indices.create, index=index, ignore=400)

    def exists(self, index):
        return self.guarded(self._client.indices.exists, index=index)

    def refresh(self, index):
        return self.guarded(self._client.indices.refresh, index=index)

    def create_document(self, index, doc_type, body):
        return self.guarded(self._client.create, index=index, doc_type=doc_type, body=body)

    def bulk_index(self, index, doc_type, items):
        self.guarded(elasticsearch.helpers.bulk, self._client, items, index=index, doc_type=doc_type)

    def search(self, index, doc_type, body):
        return self.guarded(self._client.search, index=index, doc_type=doc_type, body=body)

    def guarded(self, target, *args, **kwargs):
        try:
            return target(*args, **kwargs)
        except elasticsearch.exceptions.ConnectionError:
            msg = "Could not connect to metrics store %s. Please check that it is running and retry." % self._client.transport.hosts
            raise exceptions.SystemSetupError(msg)


class EsClientFactory:
    def __init__(self, config):
        self._config = config
        host = self._config.opts("reporting", "datastore.host")
        port = self._config.opts("reporting", "datastore.port")
        # poor man's boolean conversion
        secure = self._config.opts("reporting", "datastore.secure") == "True"
        user = self._config.opts("reporting", "datastore.user")
        password = self._config.opts("reporting", "datastore.password")

        if user and password:
            auth = (user, password)
        else:
            auth = None
        self._client = elasticsearch.Elasticsearch(hosts=[{"host": host, "port": port}],
                                                   use_ssl=secure, http_auth=auth, verify_certs=True, ca_certs=certifi.where())

    def create(self):
        return EsClient(self._client)


class IndexTemplateProvider:
    """
    Abstracts how the Rally index template is retrieved.
    """

    def __init__(self, config):
        self._config = config

    def template(self):
        script_dir = self._config.opts("system", "rally.root")
        mapping_template = "%s/resources/rally-mapping.json" % script_dir
        return open(mapping_template).read()


class EsMetricsStore:
    METRICS_DOC_TYPE = "metrics"
    """
    A metrics store backed by Elasticsearch.
    """

    def __init__(self,
                 config,
                 client_factory_class=EsClientFactory,
                 index_template_provider_class=IndexTemplateProvider,
                 clock=time.Clock):
        self._config = config
        self._invocation = None
        self._track = None
        self._track_setup = None
        self._index = None
        self._docs = None
        self._environment_name = config.opts("system", "env.name")
        self._client = client_factory_class(config).create()
        self._index_template_provider = index_template_provider_class(config)
        self._clock = clock

    def open(self, invocation, track_name, track_setup_name, create=False):
        self._invocation = time.to_iso8601(invocation)
        self._track = track_name
        self._track_setup = track_setup_name
        self._index = "rally-%04d" % invocation.year
        self._docs = []
        # reduce a bit of noise in the metrics cluster log
        if create and not self._client.exists(index=self._index):
            self._client.put_template("rally", self._get_template())
            self._client.create_index(index=self._index)
        # ensure we can search immediately after opening
        self._client.refresh(index=self._index)

    def _get_template(self):
        return self._index_template_provider.template()

    def close(self):
        self._client.bulk_index(index=self._index, doc_type=EsMetricsStore.METRICS_DOC_TYPE, items=self._docs)
        self._docs = []

    # should be an int
    def put_count(self, name, count, unit=None):
        self._put(name, count, unit)

    # should be a float
    def put_value(self, name, value, unit):
        self._put(name, value, unit)

    def _put(self, name, value, unit):
        doc = {
            "@timestamp": time.to_unix_timestamp(self._clock.now()),
            "trial-timestamp": self._invocation,
            "environment": self._environment_name,
            "track": self._track,
            "track-setup": self._track_setup,
            "name": name,
            "value": value,
            "unit": unit
        }
        self._docs.append(doc)

    def get_one(self, name):
        v = self.get(name)
        if v:
            return v[0]
        else:
            return None

    def get(self, name):
        query = {
            "query": self._query_by_name(name)
        }
        result = self._client.search(index=self._index, doc_type=EsMetricsStore.METRICS_DOC_TYPE, body=query)
        return [v["_source"]["value"] for v in result["hits"]["hits"]]

    def get_stats(self, name):
        query = {
            "query": self._query_by_name(name),
            "aggs": {
                "metric_stats": {
                    "stats": {
                        "field": "value"
                    }
                }
            }
        }
        result = self._client.search(index=self._index, doc_type=EsMetricsStore.METRICS_DOC_TYPE, body=query)
        return result["aggregations"]["metric_stats"]

    def get_percentiles(self, name, percentiles=None):
        if percentiles is None:
            percentiles = [99, 99.9, 100]
        query = {
            "query": self._query_by_name(name),
            "aggs": {
                "percentile_stats": {
                    "percentiles": {
                        "field": "value",
                        "percents": percentiles
                    }
                }
            }
        }
        result = self._client.search(index=self._index, doc_type=EsMetricsStore.METRICS_DOC_TYPE, body=query)
        if result["hits"]["total"] > 0:
            raw = result["aggregations"]["percentile_stats"]["values"]
            return collections.OrderedDict(sorted(raw.items(), key=lambda t: float(t[0])))
        else:
            return None

    def _query_by_name(self, name):
        return {
            "bool": {
                "filter": [
                    {
                        "term": {
                            "trial-timestamp": self._invocation
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
