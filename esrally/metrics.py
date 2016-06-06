import collections
import datetime
import logging
import math
import statistics
from enum import Enum

import certifi
import elasticsearch
import elasticsearch.helpers

from esrally import time, exceptions, track

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

    def index(self, index, doc_type, item):
        self.guarded(self._client.create, index=index, doc_type=doc_type, body=item)

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
        logger.info("Creating connection to metrics store at %s:%s" % (host, port))
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


def metrics_store(config):
    """
    Creates a proper metrics store based on the current configuration.
    :param config: Config object. Mandatory.
    :return: A metrics store implementation.
    """
    if config.opts("reporting", "datastore.type") == "elasticsearch":
        logger.info("Creating ES metrics store")
        return EsMetricsStore(config)
    else:
        logger.info("Creating in-memory metrics store")
        return InMemoryMetricsStore(config)


class MetricsStore:
    """
    Abstract metrics store
    """

    def __init__(self, config, clock=time.Clock):
        """
        Creates a new metrics store.

        :param config: The config object. Mandatory.
        :param clock: This parameter is optional and needed for testing.
        """
        self._config = config
        self._invocation = None
        self._track = None
        self._challenge = None
        self._car = None
        self._environment_name = config.opts("system", "env.name")
        self._meta_info = {
            MetaInfoScope.cluster: {},
            MetaInfoScope.node: {}
        }
        self._clock = clock
        self._stop_watch = self._clock.stop_watch()

    def open(self, invocation, track_name, challenge_name, car_name, create=False):
        """
        Opens a metrics store for a specific invocation, track, challenge and car.

        :param invocation: The invocation (timestamp).
        :param track_name: Track name.
        :param challenge_name: Challenge name.
        :param car_name: Car name.
        :param create: True if an index should be created (if necessary). This is typically True, when attempting to write metrics and
        False when it is just opened for reading (as we can assume all necessary indices exist at this point).
        """
        self._invocation = time.to_iso8601(invocation)
        self._track = track_name
        self._challenge = challenge_name
        self._car = car_name
        logger.info("Opening metrics store for invocation=[%s], track=[%s], challenge=[%s], car=[%s]" %
                    (self._invocation, track_name, challenge_name, car_name))
        user_tag = self._config.opts("system", "user.tag", mandatory=False)
        if user_tag and user_tag.strip() != "":
            try:
                user_tag_key, user_tag_value = user_tag.split(":")
                # prefix user tag with "tag_" in order to avoid clashes with our internal meta data
                self.add_meta_info(MetaInfoScope.cluster, None, "tag_%s" % user_tag_key, user_tag_value)
            except ValueError:
                msg = "User tag key and value have to separated by a ':'. Invalid value [%s]" % user_tag
                logger.exception(msg)
                raise exceptions.SystemSetupError(msg)
        self._stop_watch.start()

    def close(self):
        """
        Closes the metric store. Note that it is mandatory to close the metrics store when it is no longer needed as it only persists
        metrics on close (in order to avoid additional latency during the benchmark).
        """
        raise NotImplementedError("abstract method")

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

    def put_count_cluster_level(self, name, count, unit=None, sample_type="normal"):
        """
        Adds a new cluster level counter metric.

        :param name: The name of the metric.
        :param count: The metric value. It is expected to be of type int (otherwise use put_value_*).
        :param unit: A count may or may not have unit.
        :param sample_type Whether this is a warmup or a normal measurement sample. Defaults to "normal".
        """
        self._put(MetaInfoScope.cluster, None, name, count, unit, sample_type)

    def put_count_node_level(self, node_name, name, count, unit=None, sample_type="normal"):
        """
        Adds a new node level counter metric.

        :param name: The name of the metric.
        :param node_name: The name of the cluster node for which this metric has been determined.
        :param count: The metric value. It is expected to be of type int (otherwise use put_value_*).
        :param unit: A count may or may not have unit.
        :param sample_type Whether this is a warmup or a normal measurement sample. Defaults to "normal".
        """
        self._put(MetaInfoScope.node, node_name, name, count, unit, sample_type)

    # should be a float
    def put_value_cluster_level(self, name, value, unit, sample_type="normal"):
        """
        Adds a new cluster level value metric.

        :param name: The name of the metric.
        :param value: The metric value. It is expected to be of type float (otherwise use put_count_*).
        :param unit: The unit of this metric value (e.g. ms, docs/s).
        :param sample_type Whether this is a warmup or a normal measurement sample. Defaults to "normal".
        """
        self._put(MetaInfoScope.cluster, None, name, value, unit, sample_type)

    def put_value_node_level(self, node_name, name, value, unit, sample_type="normal"):
        """
        Adds a new node level value metric.

        :param name: The name of the metric.
        :param node_name: The name of the cluster node for which this metric has been determined.
        :param value: The metric value. It is expected to be of type float (otherwise use put_count_*).
        :param unit: The unit of this metric value (e.g. ms, docs/s)
        :param sample_type Whether this is a warmup or a normal measurement sample. Defaults to "normal".
        """
        self._put(MetaInfoScope.node, node_name, name, value, unit, sample_type)

    def _put(self, level, level_key, name, value, unit, sample_type):
        if level == MetaInfoScope.cluster:
            meta = self._meta_info[MetaInfoScope.cluster]
        elif level == MetaInfoScope.node:
            meta = self._meta_info[MetaInfoScope.cluster].copy()
            meta.update(self._meta_info[MetaInfoScope.node][level_key])
        else:
            raise exceptions.ImproperlyConfigured("Unknown meta info level [%s] for metric [%s]" % (level, name))

        doc = {
            "@timestamp": time.to_epoch_millis(self._clock.now()),
            "relative-time": int(self._stop_watch.split_time() * 1000 * 1000),
            "trial-timestamp": self._invocation,
            "environment": self._environment_name,
            "track": self._track,
            "challenge": self._challenge,
            "car": self._car,
            "name": name,
            "value": value,
            "unit": unit,
            "sample-type": sample_type,
            "meta": meta
        }
        self._add(doc)

    def _add(self, doc):
        """
        Adds a new document to the metrics store

        :param doc: The new document.
        """
        raise NotImplementedError("abstract method")

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
        raise NotImplementedError("abstract method")

    def get_stats(self, name):
        """
        Gets standard statistics for the given metric name.

        :param name: The metric name to query.
        :return: A metric_stats structure.
        """
        raise NotImplementedError("abstract method")

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
        raise NotImplementedError("abstract method")


def index_name(ts):
    return "rally-%04d" % ts.year


class EsMetricsStore(MetricsStore):
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
        MetricsStore.__init__(self, config, clock)
        self._index = None
        self._client = client_factory_class(config).create()
        self._index_template_provider = index_template_provider_class(config)
        self._docs = None

    def open(self, invocation, track_name, challenge_name, car_name, create=False):
        self._docs = []
        MetricsStore.open(self, invocation, track_name, challenge_name, car_name, create)
        self._index = index_name(invocation)
        # reduce a bit of noise in the metrics cluster log
        if create:
            # always update the mapping to the latest version
            self._client.put_template("rally", self._get_template())
            if not self._client.exists(index=self._index):
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
        logger.info("Successfully added %d metrics documents for invocation=[%s], track=[%s], challenge=[%s], car=[%s]." %
                    (len(self._docs), self._invocation, self._track, self._challenge, self._car))
        self._docs = []

    def _add(self, doc):
        self._docs.append(doc)

    def get(self, name):
        """
        Gets all raw values for the given metric name.

        :param name: The metric name to query.
        :return: A list of all values for the given metric.
        """
        query = {
            "query": self._query_by_name(name)
        }
        logger.debug("Issuing get against index=[%s], doc_type=[%s], query=[%s]" % (self._index, EsMetricsStore.METRICS_DOC_TYPE, query))
        result = self._client.search(index=self._index, doc_type=EsMetricsStore.METRICS_DOC_TYPE, body=query)
        logger.debug("Metrics query produced %s results." % result["hits"]["total"])
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
        logger.debug("Issuing get_stats against index=[%s], doc_type=[%s], query=[%s]" %
                     (self._index, EsMetricsStore.METRICS_DOC_TYPE, query))
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
        logger.debug("Issuing get_percentiles against index=[%s], doc_type=[%s], query=[%s]" %
                     (self._index, EsMetricsStore.METRICS_DOC_TYPE, query))
        result = self._client.search(index=self._index, doc_type=EsMetricsStore.METRICS_DOC_TYPE, body=query)
        hits = result["hits"]["total"]
        logger.debug("get_percentiles produced %d hits" % hits)
        if hits > 0:
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
                            "challenge": self._challenge
                        }
                    },
                    {
                        "term": {
                            "car": self._car
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


class InMemoryMetricsStore(MetricsStore):
    # global per process
    DOCS = []

    def __init__(self, config, clock=time.Clock, clear=False):
        """

        Creates a new metrics store.

        :param config: The config object. Mandatory.
        :param clock: This parameter is optional and needed for testing.
        :param clear: iff True, the internal state will be cleared. This should never be used in production and is only intended for tests.
        """
        super().__init__(config, clock)
        if clear:
            InMemoryMetricsStore.DOCS = []

    def _add(self, doc):
        InMemoryMetricsStore.DOCS.append(doc)

    def close(self):
        pass

    def get_percentiles(self, name, percentiles=None):
        if percentiles is None:
            percentiles = [99, 99.9, 100]
        result = collections.OrderedDict()
        values = self.get(name)
        if len(values) > 0:
            sorted_values = sorted(values)
            for percentile in percentiles:
                result[percentile] = self.percentile_value(sorted_values, percentile)
        return result

    def percentile_value(self, sorted_values, percentile):
        """
        Calculates a percentile value for a given list of values and a percentile.

        The implementation is based on http://onlinestatbook.com/2/introduction/percentiles.html

        :param sorted_values: A sorted list of raw values for which a percentile should be calculated.
        :param percentile: A percentile between [0, 100]
        :return: the corresponding percentile value.
        """
        rank = float(percentile) / 100.0 * (len(sorted_values) - 1)
        #rank = (percentile / 100.0 * (len(sorted_values))) - 1
        #rank = percentile / 100.0 * len(sorted_values) - 0.5
        #print("rank[%f] = %f" % (percentile, rank))
        if rank == int(rank):
            return sorted_values[int(rank)]
        else:
            lr = math.floor(rank)
            lr_next = math.ceil(rank)
            fr = rank - lr
            lower_score = sorted_values[lr]
            higher_score = sorted_values[lr_next]
            return lower_score + (higher_score - lower_score) * fr

    def get_stats(self, name):
        values = self.get(name)
        sorted_values = sorted(values)
        return {
            "count": len(sorted_values),
            "min": sorted_values[0],
            "max": sorted_values[-1],
            "avg": statistics.mean(sorted_values),
            "sum": sum(sorted_values)
        }

    def get(self, name):
        return [doc["value"] for doc in InMemoryMetricsStore.DOCS if doc["name"] == name]


def race_store(config):
    """
    Creates a proper race store based on the current configuration.
    :param config: Config object. Mandatory.
    :return: A race store implementation.
    """
    if config.opts("reporting", "datastore.type") == "elasticsearch":
        logger.info("Creating ES race store")
        return EsRaceStore(config)
    else:
        logger.info("Creating in-memory race store")
        return InMemoryRaceStore(config)


class InMemoryRaceStore:
    def __init__(self, config):
        self.config = config

    def store_race(self, t):
        pass

    def list(self):
        return []

    def find_by_timestamp(self, timestamp):
        return None


class EsRaceStore:
    RACE_DOC_TYPE = "races"

    def __init__(self,
                 config,
                 client_factory_class=EsClientFactory,
                 index_template_provider_class=IndexTemplateProvider):
        """
        Creates a new metrics store.

        :param config: The config object. Mandatory.
        :param client_factory_class: This parameter is optional and needed for testing.
        """
        self.config = config
        self.environment_name = config.opts("system", "env.name")
        self.client = client_factory_class(config).create()
        self.index_template_provider = index_template_provider_class(config)

    def store_race(self, t):
        # always update the mapping to the latest version
        self.client.put_template("rally", self.index_template_provider.template())

        trial_timestamp = self.config.opts("meta", "time.start")

        selected_challenge = {}
        for challenge in t.challenges:
            if challenge.name == self.config.opts("benchmarks", "challenge"):
                selected_challenge["name"] = challenge.name
                if track.BenchmarkPhase.index in challenge.benchmark:
                    selected_challenge["benchmark-phase-index"] = True
                if track.BenchmarkPhase.stats in challenge.benchmark:
                    selected_challenge["benchmark-phase-stats"] = {
                        "sample-size": challenge.benchmark[track.BenchmarkPhase.stats].iteration_count
                    }
                if track.BenchmarkPhase.search in challenge.benchmark:
                    c = challenge.benchmark[track.BenchmarkPhase.search]
                    selected_challenge["benchmark-phase-search"] = {
                        "queries": [q.name for q in c.queries],
                        "sample-size": c.iteration_count
                    }

        doc = {
            "environment": self.environment_name,
            "trial-timestamp": time.to_iso8601(trial_timestamp),
            "pipeline": self.config.opts("system", "pipeline"),
            "revision": self.config.opts("source", "revision"),
            "distribution-version": self.config.opts("source", "distribution.version"),
            "track": t.name,
            "selected-challenge": selected_challenge,
            "car": self.config.opts("benchmarks", "car"),
            "rounds": self.config.opts("benchmarks", "rounds"),
            "target-hosts": self.config.opts("launcher", "external.target.hosts"),
            "user-tag": self.config.opts("system", "user.tag")
        }
        self.client.index(index_name(trial_timestamp), EsRaceStore.RACE_DOC_TYPE, doc)

    def list(self):
        filters = [{
            "term": {
                "environment": self.environment_name
            }
        }]

        query = {
            "query": {
                "bool": {
                    "filter": filters
                }
            },
            "size": int(self.config.opts("system", "list.races.max_results")),
            "sort": [
                {
                    "trial-timestamp": {
                        "order": "desc"
                    }
                }
            ]
        }
        result = self.client.search(index="rally-*", doc_type=EsRaceStore.RACE_DOC_TYPE, body=query)
        if result["hits"]["total"] > 0:
            return [Race(v["_source"]) for v in result["hits"]["hits"]]
        else:
            return None

    def find_by_timestamp(self, timestamp):
        filters = [{
            "term": {
                "environment": self.environment_name
            }
        },
            {
                "term": {
                    "trial-timestamp": timestamp
                }
            }]

        query = {
            "query": {
                "bool": {
                    "filter": filters
                }
            }
        }
        result = self.client.search(index="rally-*", doc_type=EsRaceStore.RACE_DOC_TYPE, body=query)
        if result["hits"]["total"] == 1:
            return Race(result["hits"]["hits"][0]["_source"])
        else:
            return None


class Race:
    def __init__(self, source):
        self.environment = source["environment"]
        self.trial_timestamp = datetime.datetime.strptime(source["trial-timestamp"], "%Y%m%dT%H%M%SZ")
        self.pipeline = source["pipeline"]
        self.revision = source["revision"]
        self.distribution_version = source["distribution-version"]
        self.track = source["track"]
        self.rounds = source["rounds"]
        self.challenge = SelectedChallenge(source["selected-challenge"])
        self.car = source["car"]
        self.target_hosts = source["target-hosts"]
        self.user_tag = source["user-tag"]


class SelectedChallenge:
    def __init__(self, source):
        self.name = source["name"]
        self.benchmark_indexing = source["benchmark-phase-index"]
        self.benchmark_stats = "benchmark-phase-stats" in source
        if self.benchmark_stats:
            self.stats_sample_size = int(source["benchmark-phase-stats"]["sample-size"])
        else:
            self.stats_sample_size = 0
        self.benchmark_search = "benchmark-phase-search" in source
        if self.benchmark_search:
            self.queries = source["benchmark-phase-search"]["queries"]
            self.search_sample_size = int(source["benchmark-phase-search"]["sample-size"])
        else:
            self.queries = []
            self.search_sample_size = 0

    def __str__(self):
        return self.name
