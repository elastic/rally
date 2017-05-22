import collections
import logging
import math
import pickle
import statistics
import sys
import zlib
from enum import Enum, IntEnum

import certifi
import tabulate
from esrally import time, exceptions, config, version, paths
from esrally.utils import console, io, versions

logger = logging.getLogger("rally.metrics")


class EsClient:
    """
    Provides a stripped-down client interface that is easier to exchange for testing
    """

    def __init__(self, client):
        self._client = client

    def put_template(self, name, template):
        return self.guarded(self._client.indices.put_template, name, template)

    def template_exists(self, name):
        return self.guarded(self._client.indices.exists_template, name)

    def delete_template(self,  name):
        self.guarded(self._client.indices.delete_template, name)

    def create_index(self, index):
        # ignore 400 cause by IndexAlreadyExistsException when creating an index
        return self.guarded(self._client.indices.create, index=index, ignore=400)

    def exists(self, index):
        return self.guarded(self._client.indices.exists, index=index)

    def refresh(self, index):
        return self.guarded(self._client.indices.refresh, index=index)

    def bulk_index(self, index, doc_type, items):
        import elasticsearch.helpers
        self.guarded(elasticsearch.helpers.bulk, self._client, items, index=index, doc_type=doc_type)

    def index(self, index, doc_type, item):
        self.guarded(self._client.index, index=index, doc_type=doc_type, body=item)

    def search(self, index, doc_type, body):
        return self.guarded(self._client.search, index=index, doc_type=doc_type, body=body)

    def guarded(self, target, *args, **kwargs):
        import elasticsearch
        try:
            return target(*args, **kwargs)
        except elasticsearch.exceptions.AuthenticationException:
            # we know that it is just one host (see EsClientFactory)
            node = self._client.transport.hosts[0]
            msg = "The configured user could not authenticate against your Elasticsearch metrics store running on host [%s] at " \
                  "port [%s] (wrong password?). Please fix the configuration in [%s]." % \
                  (node["host"], node["port"], config.ConfigFile().location)
            logger.exception(msg)
            raise exceptions.SystemSetupError(msg)
        except elasticsearch.exceptions.AuthorizationException:
            node = self._client.transport.hosts[0]
            msg = "The configured user does not have enough privileges to run the operation [%s] against your Elasticsearch metrics " \
                  "store running on host [%s] at port [%s]. Please adjust your x-pack configuration or specify a user with enough " \
                  "privileges in the configuration in [%s]." % (target.__name__, node["host"], node["port"], config.ConfigFile().location)
            logger.exception(msg)
            raise exceptions.SystemSetupError(msg)
        except elasticsearch.exceptions.ConnectionError:
            node = self._client.transport.hosts[0]
            msg = "Could not connect to your Elasticsearch metrics store. Please check that it is running on host [%s] at port [%s] or " \
                  "fix the configuration in [%s]." % (node["host"], node["port"], config.ConfigFile().location)
            logger.exception(msg)
            raise exceptions.SystemSetupError(msg)
        except elasticsearch.exceptions.ElasticsearchException:
            node = self._client.transport.hosts[0]
            msg = "An unknown error occurred while running the operation [%s] against your Elasticsearch metrics store on host [%s] at " \
                  "port [%s]." % (target.__name__, node["host"], node["port"])
            logger.exception(msg)
            # this does not necessarily mean it's a system setup problem...
            raise exceptions.RallyError(msg)


class EsClientFactory:
    """
    Abstracts how the Elasticsearch client is created. Intended for testing.
    """

    def __init__(self, cfg):
        self._config = cfg
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
        import elasticsearch
        self._client = elasticsearch.Elasticsearch(hosts=[{"host": host, "port": port}],
                                                   use_ssl=secure, http_auth=auth, verify_certs=True, ca_certs=certifi.where())

    def create(self):
        return EsClient(self._client)


class IndexTemplateProvider:
    """
    Abstracts how the Rally index template is retrieved. Intended for testing.
    """

    def __init__(self, cfg):
        self.script_dir = cfg.opts("node", "rally.root")

    def metrics_template(self):
        return self._read("metrics-template")

    def races_template(self):
        return self._read("races-template")

    def results_template(self):
        return self._read("results-template")

    def _read(self, template_name):
        return open("%s/resources/%s.json" % (self.script_dir, template_name)).read()


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


def metrics_store(cfg, read_only=True, invocation=None, track=None, challenge=None, car=None):
    """
    Creates a proper metrics store based on the current configuration.

    :param cfg: Config object.
    :param read_only: Whether to open the metrics store only for reading (Default: True).
    :return: A metrics store implementation.
    """
    if cfg.opts("reporting", "datastore.type") == "elasticsearch":
        logger.info("Creating ES metrics store")
        store = EsMetricsStore(cfg)
    else:
        logger.info("Creating in-memory metrics store")
        store = InMemoryMetricsStore(cfg)

    selected_invocation = cfg.opts("system", "time.start") if invocation is None else invocation
    selected_car = cfg.opts("mechanic", "car.name") if car is None else car

    store.open(selected_invocation, track, challenge, selected_car, create=not read_only)
    return store


class SampleType(IntEnum):
    Warmup = 0,
    Normal = 1


class MetricsStore:
    """
    Abstract metrics store
    """

    def __init__(self, cfg, clock=time.Clock, meta_info=None, lap=None):
        """
        Creates a new metrics store.

        :param cfg: The config object. Mandatory.
        :param clock: This parameter is optional and needed for testing.
        :param meta_info: This parameter is optional and intended for creating a metrics store with a previously serialized meta-info.
        :param lap: This parameter is optional and intended for creating a metrics store with a previously serialized lap.
        """
        self._config = cfg
        self._invocation = None
        self._track = None
        self._challenge = None
        self._car = None
        self._lap = lap
        self._environment_name = cfg.opts("system", "env.name")
        if meta_info is None:
            self._meta_info = {
                MetaInfoScope.cluster: {},
                MetaInfoScope.node: {}
            }
        else:
            self._meta_info = meta_info
        self._clock = clock
        self._stop_watch = self._clock.stop_watch()

    def open(self, invocation=None, track_name=None, challenge_name=None, car_name=None, ctx=None, create=False):
        """
        Opens a metrics store for a specific invocation, track, challenge and car.

        :param invocation: The invocation (timestamp).
        :param track_name: Track name.
        :param challenge_name: Challenge name.
        :param car_name: Car name.
        :param ctx: An metrics store open context retrieved from another metrics store with ``#open_context``.
        :param create: True if an index should be created (if necessary). This is typically True, when attempting to write metrics and
        False when it is just opened for reading (as we can assume all necessary indices exist at this point).
        """
        if ctx:
            self._invocation = ctx["invocation"]
            self._track = ctx["track"]
            self._challenge = ctx["challenge"]
            self._car = ctx["car"]
        else:
            self._invocation = time.to_iso8601(invocation)
            self._track = track_name
            self._challenge = challenge_name
            self._car = car_name
        assert self._invocation is not None, "Attempting to open metrics store without an invocation"
        assert self._track is not None, "Attempting to open metrics store without a track"
        assert self._challenge is not None, "Attempting to open metrics store without a challenge"
        assert self._car is not None, "Attempting to open metrics store without a car"

        logger.info("Opening metrics store for invocation=[%s], track=[%s], challenge=[%s], car=[%s]" %
                    (self._invocation, self._track, self._challenge, self._car))
        user_tag = self._config.opts("race", "user.tag", mandatory=False)
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

    @property
    def lap(self):
        return self._lap

    @lap.setter
    def lap(self, lap):
        self._lap = lap

    def flush(self):
        """
        Explicitly flushes buffered metrics to the metric store. It is not required to flush before closing the metrics store.
        """
        raise NotImplementedError("abstract method")

    def close(self):
        logger.info("Closing metrics store.")
        """
        Closes the metric store. Note that it is mandatory to close the metrics store when it is no longer needed as it only persists
        metrics on close (in order to avoid additional latency during the benchmark).
        """
        self.flush()
        self.clear_meta_info()
        self.lap = None

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
            raise exceptions.SystemSetupError("Unknown meta info scope [%s]" % scope)

    def clear_meta_info(self):
        """
        Clears all internally stored meta-info. This is considered Rally internal API and not intended for normal client consumption.
        """
        self._meta_info = {
            MetaInfoScope.cluster: {},
            MetaInfoScope.node: {}
        }

    @property
    def meta_info(self):
        """
        :return: All currently stored meta-info. This is considered Rally internal API and not intended for normal client consumption.
        """
        return self._meta_info

    @meta_info.setter
    def meta_info(self, meta_info):
        self._meta_info = meta_info

    @property
    def open_context(self):
        return {
            "invocation": self._invocation,
            "track": self._track,
            "challenge": self._challenge,
            "car": self._car
        }

    def put_count_cluster_level(self, name, count, unit=None, operation=None, operation_type=None, sample_type=SampleType.Normal,
                                absolute_time=None, relative_time=None, meta_data=None):
        """
        Adds a new cluster level counter metric.

        :param name: The name of the metric.
        :param count: The metric value. It is expected to be of type int (otherwise use put_value_*).
        :param unit: A count may or may not have unit.
        :param operation The operation name to which this value applies. Optional. Defaults to None.
        :param operation_type The operation type to which this value applies. Optional. Defaults to None.
        :param sample_type Whether this is a warmup or a normal measurement sample. Defaults to SampleType.Normal.
        :param absolute_time The absolute timestamp in seconds since epoch when this metric record is stored. Defaults to None. The metrics
               store will derive the timestamp automatically.
        :param relative_time The relative timestamp in seconds since the start of the benchmark when this metric record is stored.
               Defaults to None. The metrics store will derive the timestamp automatically.
        :param meta_data: A dict, containing additional key-value pairs. Defaults to None.
        """
        self._put(MetaInfoScope.cluster, None, name, count, unit, operation, operation_type, sample_type, absolute_time, relative_time,
                  meta_data)

    def put_count_node_level(self, node_name, name, count, unit=None, operation=None, operation_type=None, sample_type=SampleType.Normal,
                             absolute_time=None, relative_time=None, meta_data=None):
        """
        Adds a new node level counter metric.

        :param name: The name of the metric.
        :param node_name: The name of the cluster node for which this metric has been determined.
        :param count: The metric value. It is expected to be of type int (otherwise use put_value_*).
        :param unit: A count may or may not have unit.
        :param operation The operation name to which this value applies. Optional. Defaults to None.
        :param operation_type The operation type to which this value applies. Optional. Defaults to None.
        :param sample_type Whether this is a warmup or a normal measurement sample. Defaults to SampleType.Normal.
        :param absolute_time The absolute timestamp in seconds since epoch when this metric record is stored. Defaults to None. The metrics
               store will derive the timestamp automatically.
        :param relative_time The relative timestamp in seconds since the start of the benchmark when this metric record is stored.
               Defaults to None. The metrics store will derive the timestamp automatically.
        :param meta_data: A dict, containing additional key-value pairs. Defaults to None.
        """
        self._put(MetaInfoScope.node, node_name, name, count, unit, operation, operation_type, sample_type, absolute_time, relative_time,
                  meta_data)

    # should be a float
    def put_value_cluster_level(self, name, value, unit, operation=None, operation_type=None, sample_type=SampleType.Normal,
                                absolute_time=None, relative_time=None, meta_data=None):
        """
        Adds a new cluster level value metric.

        :param name: The name of the metric.
        :param value: The metric value. It is expected to be of type float (otherwise use put_count_*).
        :param unit: The unit of this metric value (e.g. ms, docs/s).
        :param operation The operation name to which this value applies. Optional. Defaults to None.
        :param operation_type The operation type to which this value applies. Optional. Defaults to None.
        :param sample_type Whether this is a warmup or a normal measurement sample. Defaults to SampleType.Normal.
        :param absolute_time The absolute timestamp in seconds since epoch when this metric record is stored. Defaults to None. The metrics
               store will derive the timestamp automatically.
        :param relative_time The relative timestamp in seconds since the start of the benchmark when this metric record is stored.
               Defaults to None. The metrics store will derive the timestamp automatically.
       :param meta_data: A dict, containing additional key-value pairs. Defaults to None.
        """
        self._put(MetaInfoScope.cluster, None, name, value, unit, operation, operation_type, sample_type, absolute_time, relative_time,
                  meta_data)

    def put_value_node_level(self, node_name, name, value, unit, operation=None, operation_type=None, sample_type=SampleType.Normal,
                             absolute_time=None, relative_time=None, meta_data=None):
        """
        Adds a new node level value metric.

        :param name: The name of the metric.
        :param node_name: The name of the cluster node for which this metric has been determined.
        :param value: The metric value. It is expected to be of type float (otherwise use put_count_*).
        :param unit: The unit of this metric value (e.g. ms, docs/s)
        :param operation The operation name to which this value applies. Optional. Defaults to None.
        :param operation_type The operation type to which this value applies. Optional. Defaults to None.
        :param sample_type Whether this is a warmup or a normal measurement sample. Defaults to SampleType.Normal.
        :param absolute_time The absolute timestamp in seconds since epoch when this metric record is stored. Defaults to None. The metrics
               store will derive the timestamp automatically.
        :param relative_time The relative timestamp in seconds since the start of the benchmark when this metric record is stored.
               Defaults to None. The metrics store will derive the timestamp automatically.
        :param meta_data: A dict, containing additional key-value pairs. Defaults to None.
        """
        self._put(MetaInfoScope.node, node_name, name, value, unit, operation, operation_type, sample_type, absolute_time, relative_time,
                  meta_data)

    def _put(self, level, level_key, name, value, unit, operation, operation_type, sample_type, absolute_time=None, relative_time=None,
             meta_data=None):
        if level == MetaInfoScope.cluster:
            meta = self._meta_info[MetaInfoScope.cluster].copy()
        elif level == MetaInfoScope.node:
            meta = self._meta_info[MetaInfoScope.cluster].copy()
            meta.update(self._meta_info[MetaInfoScope.node][level_key])
        else:
            raise exceptions.SystemSetupError("Unknown meta info level [%s] for metric [%s]" % (level, name))
        if meta_data:
            meta.update(meta_data)

        if absolute_time is None:
            absolute_time = self._clock.now()
        if relative_time is None:
            relative_time = self._stop_watch.split_time()

        doc = {
            "@timestamp": time.to_epoch_millis(absolute_time),
            "relative-time": int(relative_time * 1000 * 1000),
            "trial-timestamp": self._invocation,
            "environment": self._environment_name,
            "track": self._track,
            "lap": self._lap,
            "challenge": self._challenge,
            "car": self._car,
            "name": name,
            "value": value,
            "unit": unit,
            "sample-type": sample_type.name.lower(),
            "meta": meta
        }
        if operation:
            doc["operation"] = operation
        if operation_type:
            doc["operation-type"] = operation_type

        assert self.lap is not None, "Attempting to store [%s] without a lap." % doc
        self._add(doc)

    def bulk_add(self, memento):
        """
        Adds raw metrics store documents previously created with #to_externalizable()

        :param memento: The external representation as returned by #to_externalizable().
        """
        import json

        approach, param = memento
        if approach == "mem":
            logger.info("Restoring in-memory representation of metrics store.")
            for doc in pickle.loads(zlib.decompress(param)):
                self._add(doc)
        elif approach == "file":
            logger.info("Restoring file representation of metrics store from [%s]." % param)
            try:
                with open(param, mode="rt", encoding="UTF-8") as f:
                    for doc in json.load(f):
                        self._add(doc)
            except IOError:
                logger.exception("Could not restore metrics from [%s]." % param)
                raise exceptions.DataError("Could not transfer metrics.")
        else:
            raise ValueError("Unrecognized externalization approach [%s]" % approach)

    def _add(self, doc):
        """
        Adds a new document to the metrics store

        :param doc: The new document.
        """
        raise NotImplementedError("abstract method")

    def get_one(self, name, operation=None, operation_type=None, sample_type=None, lap=None):
        """
        Gets one value for the given metric name (even if there should be more than one).

        :param name: The metric name to query.
        :param operation The operation name to query. Optional.
        :param operation_type The operation type to query. Optional.
        :param sample_type The sample type to query. Optional. By default, all samples are considered.
        :param lap The lap to query. Optional. By default, all laps are considered.
        :return: The corresponding value for the given metric name or None if there is no value.
        """
        return self._first_or_none(self.get(name, operation, operation_type, sample_type, lap))

    def _first_or_none(self, values):
        return values[0] if values else None

    def get(self, name, operation=None, operation_type=None, sample_type=None, lap=None):
        """
        Gets all raw values for the given metric name.

        :param name: The metric name to query.
        :param operation The operation name to query. Optional.
        :param operation_type The operation type to query. Optional.
        :param sample_type The sample type to query. Optional. By default, all samples are considered.
        :param lap The lap to query. Optional. By default, all laps are considered.
        :return: A list of all values for the given metric.
        """
        return self._get(name, operation, operation_type, sample_type, lap, lambda doc: doc["value"])

    def get_unit(self, name, operation=None, operation_type=None):
        """
        Gets the unit for the given metric name.

        :param name: The metric name to query.
        :param operation The operation name to query. Optional.
        :param operation_type The operation type to query. Optional.
        :return: The corresponding unit for the given metric name or None if no metric record is available.
        """
        # does not make too much sense to ask for a sample type here
        return self._first_or_none(self._get(name, operation, operation_type, None, None, lambda doc: doc["unit"]))

    def _get(self, name, operation, operation_type, sample_type, lap, mapper):
        raise NotImplementedError("abstract method")

    def get_count(self, name, operation=None, operation_type=None, sample_type=None, lap=None):
        """

        :param name: The metric name to query.
        :param operation The operation name to query. Optional.
        :param operation_type The operation type to query. Optional.
        :param sample_type The sample type to query. Optional. By default, all samples are considered.
        :param lap The lap to query. Optional. By default, all laps are considered.
        :return: The number of samples for this metric.
        """
        stats = self.get_stats(name, operation, operation_type, sample_type, lap)
        if stats:
            return stats["count"]
        else:
            return 0

    def get_error_rate(self, operation, operation_type=None, sample_type=None, lap=None):
        """
        Gets the error rate for a specific operation.

        :param operation The operation name to query.
        :param operation_type The operation type to query. Optional.
        :param sample_type The sample type to query. Optional. By default, all samples are considered.
        :param lap The lap to query. Optional. By default, all laps are considered.
        :return: A float between 0.0 and 1.0 (inclusive) representing the error rate.
        """
        raise NotImplementedError("abstract method")

    def get_stats(self, name, operation=None, operation_type=None, sample_type=None, lap=None):
        """
        Gets standard statistics for the given metric.

        :param name: The metric name to query.
        :param operation The operation name to query. Optional.
        :param operation_type The operation type to query. Optional.
        :param sample_type The sample type to query. Optional. By default, all samples are considered.
        :param lap The lap to query. Optional. By default, all laps are considered.
        :return: A metric_stats structure.
        """
        raise NotImplementedError("abstract method")

    def get_percentiles(self, name, operation=None, operation_type=None, sample_type=None, lap=None, percentiles=None):
        """
        Retrieves percentile metrics for the given metric.

        :param name: The metric name to query.
        :param operation The operation name to query. Optional.
        :param operation_type The operation type to query. Optional.
        :param sample_type The sample type to query. Optional. By default, all samples are considered.
        :param lap The lap to query. Optional. By default, all laps are considered.
        :param percentiles: An optional list of percentiles to show. If None is provided, by default the 99th, 99.9th and 100th percentile
        are determined. Ensure that there are enough data points in the metrics store (e.g. it makes no sense to retrieve a 99.9999
        percentile when there are only 10 values).
        :return: An ordered dictionary of the determined percentile values in ascending order. Key is the percentile, value is the
        determined value at this percentile. If no percentiles could be determined None is returned.
        """
        raise NotImplementedError("abstract method")

    def get_median(self, name, operation=None, operation_type=None, sample_type=None, lap=None):
        """
        Retrieves median value of the given metric.

        :param name: The metric name to query.
        :param operation The operation name to query. Optional.
        :param operation_type The operation type to query. Optional.
        :param sample_type The sample type to query. Optional. By default, all samples are considered.
        :param lap The lap to query. Optional. By default, all laps are considered.
        :return: The median value.
        """
        median = "50.0"
        percentiles = self.get_percentiles(name, operation, operation_type, sample_type, lap, percentiles=[median])
        return percentiles[median] if percentiles else None


class EsMetricsStore(MetricsStore):
    """
    A metrics store backed by Elasticsearch.
    """
    METRICS_DOC_TYPE = "metrics"

    def __init__(self,
                 cfg,
                 client_factory_class=EsClientFactory,
                 index_template_provider_class=IndexTemplateProvider,
                 clock=time.Clock, meta_info=None, lap=None):
        """
        Creates a new metrics store.

        :param config: The config object. Mandatory.
        :param client_factory_class: This parameter is optional and needed for testing.
        :param index_template_provider_class: This parameter is optional and needed for testing.
        :param clock: This parameter is optional and needed for testing.
        :param meta_info: This parameter is optional and intended for creating a metrics store with a previously serialized meta-info.
        :param lap: This parameter is optional and intended for creating a metrics store with a previously serialized lap.
        """
        MetricsStore.__init__(self, cfg=cfg, clock=clock, meta_info=meta_info, lap=lap)
        self._index = None
        self._client = client_factory_class(cfg).create()
        self._index_template_provider = index_template_provider_class(cfg)
        self._docs = None

    def open(self, invocation=None, track_name=None, challenge_name=None, car_name=None, ctx=None, create=False):
        self._docs = []
        MetricsStore.open(self, invocation, track_name, challenge_name, car_name, ctx, create)
        self._index = self.index_name(invocation)
        # reduce a bit of noise in the metrics cluster log
        if create:
            # Remove the old index template, named "rally"
            #TODO dm: Remove this fallback after some grace period
            if self._client.template_exists("rally"):
                self._client.delete_template("rally")
            # always update the mapping to the latest version
            self._client.put_template("rally-metrics", self._get_template())
            if not self._client.exists(index=self._index):
                self._client.create_index(index=self._index)
        # ensure we can search immediately after opening
        self._client.refresh(index=self._index)

    def index_name(self, ts):
        return "rally-metrics-%04d-%02d" % (ts.year, ts.month)

    def _get_template(self):
        return self._index_template_provider.metrics_template()

    def flush(self):
        self._client.bulk_index(index=self._index, doc_type=EsMetricsStore.METRICS_DOC_TYPE, items=self._docs)
        logger.info("Successfully added %d metrics documents for invocation=[%s], track=[%s], challenge=[%s], car=[%s]." %
                    (len(self._docs), self._invocation, self._track, self._challenge, self._car))
        self._docs = []
        # ensure we can search immediately after flushing
        self._client.refresh(index=self._index)

    def _add(self, doc):
        self._docs.append(doc)

    def _get(self, name, operation, operation_type, sample_type, lap, mapper):
        query = {
            "query": self._query_by_name(name, operation, operation_type, sample_type, lap)
        }
        logger.debug("Issuing get against index=[%s], doc_type=[%s], query=[%s]" % (self._index, EsMetricsStore.METRICS_DOC_TYPE, query))
        result = self._client.search(index=self._index, doc_type=EsMetricsStore.METRICS_DOC_TYPE, body=query)
        logger.debug("Metrics query produced [%s] results." % result["hits"]["total"])
        return [mapper(v["_source"]) for v in result["hits"]["hits"]]

    def get_error_rate(self, operation, operation_type=None, sample_type=None, lap=None):
        query = {
            "query": self._query_by_name("service_time", operation, operation_type, sample_type, lap),
            "size": 0,
            "aggs": {
                "error_rate": {
                    "terms": {
                        "field": "meta.success"
                    }
                }
            }
        }
        logger.debug("Issuing get_error_rate against index=[%s], doc_type=[%s], query=[%s]" %
                     (self._index, EsMetricsStore.METRICS_DOC_TYPE, query))
        result = self._client.search(index=self._index, doc_type=EsMetricsStore.METRICS_DOC_TYPE, body=query)
        buckets = result["aggregations"]["error_rate"]["buckets"]
        logger.debug("Query returned [%d] buckets." % len(buckets))
        count_success = 0
        count_errors = 0
        for bucket in buckets:
            k = bucket["key_as_string"]
            doc_count = int(bucket["doc_count"])
            logger.debug("Processing key [%s] with [%d] docs." % (k, doc_count))
            if k == "true":
                count_success = doc_count
            elif k == "false":
                count_errors = doc_count
            else:
                logger.warning("Unrecognized bucket key [%s] with [%d] docs." % (k, doc_count))

        if count_errors == 0:
            return 0.0
        elif count_success == 0:
            return 1.0
        else:
            return count_errors / (count_errors + count_success)

    def get_stats(self, name, operation=None, operation_type=None, sample_type=None, lap=None):
        """
        Gets standard statistics for the given metric name.

        :return: A metric_stats structure. For details please refer to
        https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-stats-aggregation.html
        """
        query = {
            "query": self._query_by_name(name, operation, operation_type, sample_type, lap),
            "size": 0,
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

    def get_percentiles(self, name, operation=None, operation_type=None, sample_type=None, lap=None, percentiles=None):
        if percentiles is None:
            percentiles = [99, 99.9, 100]
        query = {
            "query": self._query_by_name(name, operation, operation_type, sample_type, lap),
            "size": 0,
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

    def _query_by_name(self, name, operation, operation_type, sample_type, lap):
        q = {
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
        if operation:
            q["bool"]["filter"].append({
                "term": {
                    "operation": operation
                }
            })
        if operation_type:
            q["bool"]["filter"].append({
                "term": {
                    "operation-type": operation_type.name
                }
            })
        if sample_type:
            q["bool"]["filter"].append({
                "term": {
                    "sample-type": sample_type.name.lower()
                }
            })
        if lap is not None:
            q["bool"]["filter"].append({
                "term": {
                    "lap": lap
                }
            })
        return q


class InMemoryMetricsStore(MetricsStore):
    def __init__(self, cfg, clock=time.Clock, meta_info=None, lap=None):
        """

        Creates a new metrics store.

        :param cfg: The config object. Mandatory.
        :param clock: This parameter is optional and needed for testing.
        :param meta_info: This parameter is optional and intended for creating a metrics store with a previously serialized meta-info.
        :param lap: This parameter is optional and intended for creating a metrics store with a previously serialized lap.
        """
        super().__init__(cfg=cfg, clock=clock, meta_info=meta_info, lap=lap)
        self.docs = []

    def __del__(self):
        """
        Deletes the metrics store instance.
        """
        del self.docs

    def _add(self, doc):
        self.docs.append(doc)

    def flush(self):
        pass

    def to_externalizable(self, clear=False, spill_to_disk=False):
        docs = self.docs
        if clear:
            self.docs = []
        if spill_to_disk:
            import os
            import json
            import tempfile

            path = os.path.join(tempfile.mkdtemp(prefix="rally"), "metrics.json")
            logger.info("Writing [%d] metrics records temporarily to [%s]." % (len(docs), path))
            with open(path, "wt", encoding="UTF-8") as f:
                json.dump(docs, f)
            return "file", path
        else:
            compressed = zlib.compress(pickle.dumps(docs))
            logger.info("Compression changed size of metric store from [%d] bytes to [%d] bytes" %
                        (sys.getsizeof(docs), sys.getsizeof(compressed)))
            return "mem", compressed

    def bulk_add(self, memento):
        approach, docs = memento
        if approach == "mem" and docs == self.docs:
            return
        else:
            super().bulk_add(memento)

    def get_percentiles(self, name, operation=None, operation_type=None, sample_type=None, lap=None, percentiles=None):
        if percentiles is None:
            percentiles = [99, 99.9, 100]
        result = collections.OrderedDict()
        values = self.get(name, operation, operation_type, sample_type, lap)
        if len(values) > 0:
            sorted_values = sorted(values)
            for percentile in percentiles:
                result[percentile] = self.percentile_value(sorted_values, percentile)
        return result

    @staticmethod
    def percentile_value(sorted_values, percentile):
        """
        Calculates a percentile value for a given list of values and a percentile.

        The implementation is based on http://onlinestatbook.com/2/introduction/percentiles.html

        :param sorted_values: A sorted list of raw values for which a percentile should be calculated.
        :param percentile: A percentile between [0, 100]
        :return: the corresponding percentile value.
        """
        rank = float(percentile) / 100.0 * (len(sorted_values) - 1)
        if rank == int(rank):
            return sorted_values[int(rank)]
        else:
            lr = math.floor(rank)
            lr_next = math.ceil(rank)
            fr = rank - lr
            lower_score = sorted_values[lr]
            higher_score = sorted_values[lr_next]
            return lower_score + (higher_score - lower_score) * fr

    def get_error_rate(self, operation, operation_type=None, sample_type=None, lap=None):
        error = 0
        total_count = 0
        for doc in self.docs:
            # we can use any request metrics record (i.e. service time or latency)
            if doc["name"] == "service_time" and doc["operation"] == operation and \
                    (operation_type is None or doc["operation-type"] == operation_type.name) and \
                    (sample_type is None or doc["sample-type"] == sample_type.name.lower()) and \
                    (lap is None or doc["lap"] == lap):
                total_count += 1
                if doc["meta"]["success"] is False:
                    error += 1
        if total_count > 0:
            return error / total_count
        else:
            return 0.0

    def get_stats(self, name, operation=None, operation_type=None, sample_type=SampleType.Normal, lap=None):
        values = self.get(name, operation, operation_type, sample_type, lap)
        sorted_values = sorted(values)
        if len(sorted_values) > 0:
            return {
                "count": len(sorted_values),
                "min": sorted_values[0],
                "max": sorted_values[-1],
                "avg": statistics.mean(sorted_values),
                "sum": sum(sorted_values)
            }
        else:
            return None

    def _get(self, name, operation, operation_type, sample_type, lap, mapper):
        return [mapper(doc)
                for doc in self.docs
                if doc["name"] == name and
                (operation is None or doc["operation"] == operation) and
                (operation_type is None or doc["operation-type"] == operation_type.name) and
                (sample_type is None or doc["sample-type"] == sample_type.name.lower()) and
                (lap is None or doc["lap"] == lap)
                ]


def race_store(cfg):
    """
    Creates a proper race store based on the current configuration.
    :param cfg: Config object. Mandatory.
    :return: A race store implementation.
    """
    if cfg.opts("reporting", "datastore.type") == "elasticsearch":
        logger.info("Creating ES race store")
        return CompositeRaceStore(EsRaceStore(cfg), EsResultsStore(cfg), FileRaceStore(cfg))
    else:
        logger.info("Creating file race store")
        return FileRaceStore(cfg)


def list_races(cfg):
    races = []
    for race in race_store(cfg).list():
        races.append([time.to_iso8601(race.trial_timestamp), race.track, race.challenge, race.car, race.user_tag])

    if len(races) > 0:
        console.println("\nRecent races:\n")
        console.println(tabulate.tabulate(races, headers=["Race Timestamp", "Track", "Challenge", "Car", "User Tag"]))
    else:
        console.println("")
        console.println("No recent races found.")


def create_race(cfg, track, challenge):
    car = cfg.opts("mechanic", "car.name")
    environment_name = cfg.opts("system", "env.name")
    trial_timestamp = cfg.opts("system", "time.start")
    total_laps = cfg.opts("race", "laps")
    user_tag = cfg.opts("race", "user.tag")
    pipeline = cfg.opts("race", "pipeline")
    rally_version = version.version()

    return Race(rally_version, environment_name, trial_timestamp, pipeline, user_tag, track, challenge, car, total_laps)


class Race:
    def __init__(self, rally_version, environment_name, trial_timestamp, pipeline, user_tag, track, challenge, car, total_laps,
                 cluster=None, lap_results=None, results=None):
        if results is None:
            results = {}
        if lap_results is None:
            lap_results = []
        self.rally_version = rally_version
        self.environment_name = environment_name
        self.trial_timestamp = trial_timestamp
        self.pipeline = pipeline
        self.user_tag = user_tag
        self.track = track
        self.challenge = challenge
        self.car = car
        self.total_laps = total_laps
        # will be set later - contains hosts, revision and distribution_version - maybe add more data later (-> gather facts)
        self.cluster = cluster
        self.lap_results = lap_results
        self.results = results

    @property
    def track_name(self):
        return str(self.track)

    @property
    def challenge_name(self):
        return str(self.challenge)

    @property
    def revision(self):
        # minor simplification for reporter
        return self.cluster.revision

    def add_lap_results(self, results):
        self.lap_results.append(results)

    def add_final_results(self, results):
        self.results = results

    def results_of_lap_number(self, lap):
        # laps are 1 indexed but we store the data zero indexed
        return self.lap_results[lap - 1]

    def as_dict(self):
        """
        :return: A dict representation suitable for persisting this race instance as JSON.
        """
        return {
            "rally-version": self.rally_version,
            "environment": self.environment_name,
            "trial-timestamp": time.to_iso8601(self.trial_timestamp),
            "pipeline": self.pipeline,
            "user-tag": self.user_tag,
            "track": self.track_name,
            "challenge": self.challenge_name,
            "car": self.car,
            "total-laps": self.total_laps,
            "cluster": self.cluster.as_dict(),
            "results": self.results.as_dict()
        }

    def to_result_dicts(self):
        """
        :return: a list of dicts, suitable for persisting the results of this race in a format that is Kibana-friendly.
        """
        result_template = {
            "environment": self.environment_name,
            "trial-timestamp": time.to_iso8601(self.trial_timestamp),
            "distribution-version": self.cluster.distribution_version,
            "distribution-major-version": versions.major_version(self.cluster.distribution_version),
            "user-tag": self.user_tag,
            "track": self.track_name,
            "challenge": self.challenge_name,
            "car": self.car,
            # allow to logically delete records, e.g. for UI purposes when we only want to show the latest result on graphs
            "active": True
        }
        all_results = []

        for item in self.results.as_flat_list():
            result = result_template.copy()
            result.update(item)
            all_results.append(result)

        return all_results

    @classmethod
    def from_dict(cls, d):
        # Don't restore a few properties like cluster because they (a) cannot be reconstructed easily without knowledge of other modules
        # and (b) it is not necessary for this use case.
        return Race(d["rally-version"], d["environment"], time.from_is8601(d["trial-timestamp"]), d["pipeline"], d["user-tag"],
                    d["track"], d["challenge"], d["car"], d["total-laps"], results=d["results"])


class RaceStore:
    def __init__(self, cfg):
        self.cfg = cfg
        self.environment_name = cfg.opts("system", "env.name")
        self.trial_timestamp = cfg.opts("system", "time.start")
        self.current_race = None

    def find_by_timestamp(self, timestamp):
        raise NotImplementedError("abstract method")

    def list(self):
        raise NotImplementedError("abstract method")

    def store_race(self, race):
        self._store(race.as_dict())

    def _store(self, doc):
        raise NotImplementedError("abstract method")

    def _max_results(self):
        return int(self.cfg.opts("system", "list.races.max_results"))


# Does not inherit from RaceStore as it is only a delegator with the same API.
class CompositeRaceStore:
    """
    Internal helper class to store races as file and to Elasticsearch in case users want Elasticsearch as a race store.
    
    It provides the same API as RaceStore. It delegates writes to all stores and all read operations only the Elasticsearch race store.
    """
    def __init__(self, es_store, es_results_store, file_store):
        self.es_store = es_store
        self.es_results_store = es_results_store
        self.file_store = file_store

    def find_by_timestamp(self, timestamp):
        return self.es_store.find_by_timestamp(timestamp)

    def store_race(self, race):
        self.file_store.store_race(race)
        self.es_store.store_race(race)
        self.es_results_store.store_results(race)

    def list(self):
        return self.es_store.list()


class FileRaceStore(RaceStore):
    def __init__(self, cfg):
        super().__init__(cfg)
        self.user_provided_start_timestamp = self.cfg.opts("system", "time.start.user_provided", mandatory=False, default_value=False)
        self.races_path = paths.races_root(self.cfg)
        self.race_path = paths.race_root(self.cfg)

    def _store(self, doc):
        import json
        io.ensure_dir(self.race_path)
        # if the user has overridden the effective start date we guarantee a unique file name but do not let them use them for tournaments.
        with open(self._output_file_name(doc), mode="w", encoding="UTF-8") as f:
            f.write(json.dumps(doc, indent=True))

    def _output_file_name(self, doc):
        if self.user_provided_start_timestamp:
            suffix = "_%s_%s_%s" % (doc["track"], doc["challenge"], doc["car"])
        else:
            suffix = ""
        return "%s/race%s.json" % (self.race_path, suffix)

    def list(self):
        import glob
        results = glob.glob("%s/*/race.json" % self.races_path)
        all_races = self._to_races(results)
        return all_races[:self._max_results()]

    def find_by_timestamp(self, timestamp):
        race_file = "%s/race.json" % paths.race_root(cfg=self.cfg, start=time.from_is8601(timestamp))
        if io.exists(race_file):
            races = self._to_races([race_file])
            if races:
                return races[0]
        return None

    def _to_races(self, results):
        import json
        races = []
        for result in results:
            # noinspection PyBroadException
            try:
                with open(result) as f:
                    races.append(Race.from_dict(json.loads(f.read())))
            except BaseException:
                logger.exception("Could not load race file [%s] (incompatible format?) Skipping..." % result)
        return sorted(races, key=lambda r: r.trial_timestamp, reverse=True)


class EsRaceStore(RaceStore):
    INDEX_PREFIX = "rally-races-"
    RACE_DOC_TYPE = "races"

    def __init__(self, cfg, client_factory_class=EsClientFactory, index_template_provider_class=IndexTemplateProvider):
        """
        Creates a new metrics store.

        :param cfg: The config object. Mandatory.
        :param client_factory_class: This parameter is optional and needed for testing.
        :param index_template_provider_class: This parameter is optional and needed for testing.
        """
        super().__init__(cfg)
        self.client = client_factory_class(cfg).create()
        self.index_template_provider = index_template_provider_class(cfg)

    def _store(self, doc):
        # always update the mapping to the latest version
        self.client.put_template("rally-races", self.index_template_provider.races_template())
        self.client.index(index=self.index_name(), doc_type=EsRaceStore.RACE_DOC_TYPE, item=doc)

    def index_name(self):
        return "%s%04d-%02d" % (EsRaceStore.INDEX_PREFIX, self.trial_timestamp.year, self.trial_timestamp.month)

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
            "size": self._max_results(),
            "sort": [
                {
                    "trial-timestamp": {
                        "order": "desc"
                    }
                }
            ]
        }
        result = self.client.search(index="%s*" % EsRaceStore.INDEX_PREFIX, doc_type=EsRaceStore.RACE_DOC_TYPE, body=query)
        if result["hits"]["total"] > 0:
            return [Race.from_dict(v["_source"]) for v in result["hits"]["hits"]]
        else:
            return []

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
        result = self.client.search(index="%s*" % EsRaceStore.INDEX_PREFIX, doc_type=EsRaceStore.RACE_DOC_TYPE, body=query)
        if result["hits"]["total"] == 1:
            return Race.from_dict(result["hits"]["hits"][0]["_source"])
        else:
            return None


class EsResultsStore:
    """
    Stores the results of a race in a format that is better suited for reporting with Kibana.
    """
    INDEX_PREFIX = "rally-results-"
    RESULTS_DOC_TYPE = "results"

    def __init__(self, cfg, client_factory_class=EsClientFactory, index_template_provider_class=IndexTemplateProvider):
        """
        Creates a new results store.

        :param cfg: The config object. Mandatory.
        :param client_factory_class: This parameter is optional and needed for testing.
        :param index_template_provider_class: This parameter is optional and needed for testing.
        """
        self.cfg = cfg
        self.trial_timestamp = cfg.opts("system", "time.start")
        self.client = client_factory_class(cfg).create()
        self.index_template_provider = index_template_provider_class(cfg)

    def store_results(self, race):
        # always update the mapping to the latest version
        self.client.put_template("rally-results", self.index_template_provider.results_template())
        self.client.bulk_index(index=self.index_name(), doc_type=EsResultsStore.RESULTS_DOC_TYPE, items=race.to_result_dicts())

    def index_name(self):
        return "%s%04d-%02d" % (EsResultsStore.INDEX_PREFIX, self.trial_timestamp.year, self.trial_timestamp.month)
