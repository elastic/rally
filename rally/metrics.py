import logging
import collections
from enum import Enum

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
    """
    Abstracts how the Elasticsearch client is created. Intended for testing.
    """
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
    Abstracts how the Rally index template is retrieved. Intended for testing.
    """

    def __init__(self, config):
        self._config = config

    def template(self):
        script_dir = self._config.opts("system", "rally.root")
        mapping_template = "%s/resources/rally-mapping.json" % script_dir
        return open(mapping_template).read()


class MetaInfoScope(Enum):
    """
    Defines the scope of a meta-information. Meta-information provides more context for a metric, for example the concrete version
    of Elasticsearch that has been benchmarked or environment information like CPU model or OS.
    """
    cluster = 1
    """
    Cluster level meta-information is valid for all nodes in the cluster (e.g. the benchmarked Elasticsearch version)
    """
    # host = 2
    """
    Host level meta-information is valid for all nodes on the same host (e.g. the OS name and version)
    """
    node = 3
    """
    Node level meta-information is valid for a single node (e.g. GC times)
    """


class EsMetricsStore:
    """
    A metrics store backed by Elasticsearch.
    """
    METRICS_DOC_TYPE = "metrics"

    def __init__(self,
                 config,
                 client_factory_class=EsClientFactory,
                 index_template_provider_class=IndexTemplateProvider,
                 clock=time.Clock):
        """
        Creates a new metrics store.

        :param config: The config object. Mandatory.
        :param client_factory_class: This parameter is optional and needed for testing.
        :param index_template_provider_class: This parameter is optional and needed for testing.
        :param clock: This parameter is optional and needed for testing.
        """
        self._config = config
        self._invocation = None
        self._track = None
        self._track_setup = None
        self._index = None
        self._docs = None
        self._environment_name = config.opts("system", "env.name")
        self._meta_info = {
            MetaInfoScope.cluster: {},
            MetaInfoScope.node: {}
        }
        self._client = client_factory_class(config).create()
        self._index_template_provider = index_template_provider_class(config)
        self._clock = clock

    def open(self, invocation, track_name, track_setup_name, create=False):
        """
        Opens a metrics store for a specific invocation, track and track setup.

        :param invocation: The invocation (timestamp).
        :param track_name: Track name.
        :param track_setup_name: Track setup name.
        :param create: True if an index should be created (if necessary). This is typically True, when attempting to write metrics and
        False when it is just opened for reading (as we can assume all necessary indices exist at this point).
        """
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
        """
        Closes the metric store. Note that it is mandatory to close the metrics store when it is no longer needed as it only persists
        metrics on close (in order to avoid additional latency during the benchmark).
        """
        self._client.bulk_index(index=self._index, doc_type=EsMetricsStore.METRICS_DOC_TYPE, items=self._docs)
        self._docs = []

    def add_meta_info(self, scope, scope_key, key, value):
        """
        Adds new meta information to the metrics store. All metrics entries that are created after calling this method are guaranteed to
        contain the added meta info (provided is on the same level or a level below, e.g. a cluster level metric will not contain node
        level meta information but all cluster level meta information will be contained in a node level metrics record).

        :param scope: The scope of the meta information. See MetaInfoScope.
        :param scope_key: The key within the scope. For cluster level metrics None is expected, for node level metrics the node name.
        :param key: The key of the meta information.
        :param value: The value of the meta information.
        """
        if scope == MetaInfoScope.cluster:
            self._meta_info[MetaInfoScope.cluster][key] = value
        elif scope == MetaInfoScope.node:
            if scope_key not in self._meta_info[MetaInfoScope.node]:
                self._meta_info[MetaInfoScope.node][scope_key] = {}
            self._meta_info[MetaInfoScope.node][scope_key][key] = value
        else:
            raise exceptions.ImproperlyConfigured("Unknown meta info scope [%s]" % scope)

    def put_count_cluster_level(self, name, count, unit=None):
        """
        Adds a new cluster level counter metric.

        :param name: The name of the metric.
        :param count: The metric value. It is expected to be of type int (otherwise use put_value_*).
        :param unit: A count may or may not have unit.
        """
        self._put(MetaInfoScope.cluster, None, name, count, unit)

    def put_count_node_level(self, node_name, name, count, unit=None):
        """
        Adds a new node level counter metric.

        :param name: The name of the metric.
        :param node_name: The name of the cluster node for which this metric has been determined.
        :param count: The metric value. It is expected to be of type int (otherwise use put_value_*).
        :param unit: A count may or may not have unit.
        """
        self._put(MetaInfoScope.node, node_name, name, count, unit)

    # should be a float
    def put_value_cluster_level(self, name, value, unit):
        """
        Adds a new cluster level value metric.

        :param name: The name of the metric.
        :param value: The metric value. It is expected to be of type float (otherwise use put_count_*).
        :param unit: The unit of this metric value (e.g. ms, docs/s).
        """
        self._put(MetaInfoScope.cluster, None, name, value, unit)

    def put_value_node_level(self, node_name, name, value, unit):
        """
        Adds a new node level value metric.

        :param name: The name of the metric.
        :param node_name: The name of the cluster node for which this metric has been determined.
        :param value: The metric value. It is expected to be of type float (otherwise use put_count_*).
        :param unit: The unit of this metric value (e.g. ms, docs/s)
        """
        self._put(MetaInfoScope.node, node_name, name, value, unit)

    def _put(self, level, level_key, name, value, unit):
        if level == MetaInfoScope.cluster:
            meta = self._meta_info[MetaInfoScope.cluster]
        elif level == MetaInfoScope.node:
            meta = self._meta_info[MetaInfoScope.cluster].copy()
            meta.update(self._meta_info[MetaInfoScope.node][level_key])
        else:
            raise exceptions.ImproperlyConfigured("Unknown meta info level [%s] for metric [%s]" % (level, name))

        doc = {
            "@timestamp": time.to_epoch_millis(self._clock.now()),
            "trial-timestamp": self._invocation,
            "environment": self._environment_name,
            "track": self._track,
            "track-setup": self._track_setup,
            "name": name,
            "value": value,
            "unit": unit,
            "meta": meta
        }
        self._docs.append(doc)

    def get_one(self, name):
        """
        Gets one value for the given metric name (even if there should be more than one).

        :param name: The metric name to query.
        :return: The corresponding value for the given metric name or None if there is no value.
        """
        v = self.get(name)
        if v:
            return v[0]
        else:
            return None

    def get(self, name):
        """
        Gets all raw values for the given metric name.

        :param name: The metric name to query.
        :return: A list of all values for the given metric.
        """
        query = {
            "query": self._query_by_name(name)
        }
        result = self._client.search(index=self._index, doc_type=EsMetricsStore.METRICS_DOC_TYPE, body=query)
        return [v["_source"]["value"] for v in result["hits"]["hits"]]

    def get_stats(self, name):
        """
        Gets standard statistics for the given metric name.

        :param name: The metric name to query.
        :return: A metric_stats structure. For details please refer to
        https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-stats-aggregation.html
        """
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
        """
        Retrieves percentile metrics for the given metric name.

        :param name: The metric name to query.
        :param percentiles: An optional list of percentiles to show. If None is provided, by default the 99th, 99.9th and 100th percentile
        are determined. Ensure that there are enough data points in the metrics store (e.g. it makes no sense to retrieve a 99.9999
        percentile when there are only 10 values).
        :return: An ordered dictionary of the determined percentile values in ascending order. Key is the percentile, value is the
        determined value at this percentile. If no percentiles could be determined None is returned.
        """
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
