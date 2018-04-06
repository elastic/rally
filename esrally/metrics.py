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
        max_execution_count = 3
        execution_count = 0

        while execution_count < max_execution_count:
            execution_count += 1
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
                      "privileges in the configuration in [%s]." % \
                      (target.__name__, node["host"], node["port"], config.ConfigFile().location)
                logger.exception(msg)
                raise exceptions.SystemSetupError(msg)
            except elasticsearch.exceptions.ConnectionTimeout:
                if execution_count < max_execution_count:
                    logger.info("Received a connection timeout from the metrics store in attempt [%d/%d]." %
                                (execution_count, max_execution_count))
                    time.sleep(1)
                else:
                    operation = target.__name__
                    logger.exception("Got a connection timeout while running [%s] (retried %d times)." % (operation, max_execution_count))
                    node = self._client.transport.hosts[0]
                    msg = "A connection timeout occurred while running the operation [%s] against your Elasticsearch metrics store on " \
                          "host [%s] at port [%s]." % (operation, node["host"], node["port"])
                    raise exceptions.RallyError(msg)
            except elasticsearch.exceptions.ConnectionError:
                node = self._client.transport.hosts[0]
                msg = "Could not connect to your Elasticsearch metrics store. Please check that it is running on host [%s] at port [%s]" \
                      " or fix the configuration in [%s]." % (node["host"], node["port"], config.ConfigFile().location)
                logger.exception(msg)
                raise exceptions.SystemSetupError(msg)
            except elasticsearch.TransportError as e:
                # gateway timeout - let's wait a bit and retry
                if e.status_code == 504 and execution_count < max_execution_count:
                    logger.info("Received a gateway timeout from the metrics store in attempt [%d/%d]." %
                                (execution_count, max_execution_count))
                    time.sleep(1)
                else:
                    node = self._client.transport.hosts[0]
                    msg = "A transport error occurred while running the operation [%s] against your Elasticsearch metrics store on " \
                          "host [%s] at port [%s]." % (target.__name__, node["host"], node["port"])
                    logger.exception(msg)
                    raise exceptions.RallyError(msg)

            except elasticsearch.exceptions.ElasticsearchException:
                node = self._client.transport.hosts[0]
                msg = "An unknown error occurred while running the operation [%s] against your Elasticsearch metrics store on host [%s] " \
                      "at port [%s]." % (target.__name__, node["host"], node["port"])
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
        verify = self._config.opts("reporting", "datastore.ssl.verification_mode", default_value="full", mandatory=False) != "none"
        ca_path = self._config.opts("reporting", "datastore.ssl.certificate_authorities", default_value=None, mandatory=False)

        from esrally import client

        # Instead of duplicating code, we're just adapting the metrics store specific properties to match the regular client options.
        client_options = {
            "use_ssl": secure,
            "verify_certs": verify,
            "timeout": 120
        }
        if ca_path:
            client_options["ca_certs"] = ca_path
        if user and password:
            client_options["basic_auth_user"] = user
            client_options["basic_auth_password"] = password

        logger.info("Creating connection to metrics store at %s:%s", host, port)
        factory = client.EsClientFactory(hosts=[{"host": host, "port": port}], client_options=client_options)
        self._client = factory.create()

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
        with open("%s/resources/%s.json" % (self.script_dir, template_name), encoding="utf-8") as f:
            return f.read()


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


def metrics_store(cfg, read_only=True, track=None, challenge=None, car=None, meta_info=None, lap=None):
    """
    Creates a proper metrics store based on the current configuration.

    :param cfg: Config object.
    :param read_only: Whether to open the metrics store only for reading (Default: True).
    :return: A metrics store implementation.
    """
    cls = metrics_store_class(cfg)
    store = cls(cfg=cfg, meta_info=meta_info, lap=lap)
    logger.info("Creating %s" % str(store))

    trial_id = cfg.opts("system", "trial.id")
    trial_timestamp = cfg.opts("system", "time.start")
    selected_car = cfg.opts("mechanic", "car.names") if car is None else car

    store.open(trial_id, trial_timestamp, track, challenge, selected_car, create=not read_only)
    return store


def metrics_store_class(cfg):
    if cfg.opts("reporting", "datastore.type") == "elasticsearch":
        return EsMetricsStore
    else:
        return InMemoryMetricsStore


def extract_user_tags_from_config(cfg):
    """
    Extracts user tags into a structured dict

    :param cfg: The current configuration object.
    :return: A dict containing user tags. If no user tags are given, an empty dict is returned.
    """
    user_tags = cfg.opts("race", "user.tag", mandatory=False)
    return extract_user_tags_from_string(user_tags)


def extract_user_tags_from_string(user_tags):
    """
    Extracts user tags into a structured dict

    :param user_tags: A string containing user tags (tags separated by comma, key and value separated by colon).
    :return: A dict containing user tags. If no user tags are given, an empty dict is returned.
    """
    user_tags_dict = {}
    if user_tags and user_tags.strip() != "":
        try:
            for user_tag in user_tags.split(","):
                user_tag_key, user_tag_value = user_tag.split(":")
                user_tags_dict[user_tag_key] = user_tag_value
        except ValueError:
            msg = "User tag keys and values have to separated by a ':'. Invalid value [%s]" % user_tags
            logger.exception(msg)
            raise exceptions.SystemSetupError(msg)
    return user_tags_dict


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
        self._trial_id = None
        self._trial_timestamp = None
        self._track = None
        self._track_params = cfg.opts("track", "params")
        self._challenge = None
        self._car = None
        self._car_name = None
        self._lap = lap
        self._environment_name = cfg.opts("system", "env.name")
        self.opened = False
        if meta_info is None:
            self._meta_info = {
                MetaInfoScope.cluster: {},
                MetaInfoScope.node: {}
            }
        else:
            self._meta_info = meta_info
        self._clock = clock
        self._stop_watch = self._clock.stop_watch()

    def open(self, trial_id=None, trial_timestamp=None, track_name=None, challenge_name=None, car_name=None, ctx=None, create=False):
        """
        Opens a metrics store for a specific trial, track, challenge and car.

        :param trial_id: The trial id. This attribute is sufficient to uniquely identify a challenge.
        :param trial_timestamp: The trial timestamp as a datetime.
        :param track_name: Track name.
        :param challenge_name: Challenge name.
        :param car_name: Car name.
        :param ctx: An metrics store open context retrieved from another metrics store with ``#open_context``.
        :param create: True if an index should be created (if necessary). This is typically True, when attempting to write metrics and
        False when it is just opened for reading (as we can assume all necessary indices exist at this point).
        """
        if ctx:
            self._trial_id = ctx["trial-id"]
            self._trial_timestamp = ctx["trial-timestamp"]
            self._track = ctx["track"]
            self._challenge = ctx["challenge"]
            self._car = ctx["car"]
        else:
            self._trial_id = trial_id
            self._trial_timestamp = time.to_iso8601(trial_timestamp)
            self._track = track_name
            self._challenge = challenge_name
            self._car = car_name
        assert self._trial_id is not None, "Attempting to open metrics store without a trial id"
        assert self._trial_timestamp is not None, "Attempting to open metrics store without a trial timestamp"
        assert self._track is not None, "Attempting to open metrics store without a track"
        assert self._challenge is not None, "Attempting to open metrics store without a challenge"
        assert self._car is not None, "Attempting to open metrics store without a car"

        self._car_name = "+".join(self._car) if isinstance(self._car, list) else self._car

        logger.info("Opening metrics store for trial timestamp=[%s], track=[%s], challenge=[%s], car=[%s]" %
                    (self._trial_timestamp, self._track, self._challenge, self._car))

        user_tags = extract_user_tags_from_config(self._config)
        for k, v in user_tags.items():
            # prefix user tag with "tag_" in order to avoid clashes with our internal meta data
            self.add_meta_info(MetaInfoScope.cluster, None, "tag_%s" % k, v)
        # Don't store it for each metrics record as it's probably sufficient on race level
        # self.add_meta_info(MetaInfoScope.cluster, None, "rally_version", version.version())
        self._stop_watch.start()
        self.opened = True

    def reset_relative_time(self):
        """
        Resets the internal relative-time counter to zero.
        """
        self._stop_watch.start()

    @property
    def lap(self):
        return self._lap

    @lap.setter
    def lap(self, lap):
        self._lap = lap

    def flush(self, refresh=True):
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
        self.opened = False

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

    def merge_meta_info(self, to_merge):
        """
        Merges the current meta info with another one.

        :param to_merge: A meta info representation that should be merged with the current one.
        """
        if MetaInfoScope.cluster in to_merge:
            self._meta_info[MetaInfoScope.cluster].update(to_merge[MetaInfoScope.cluster])
        if MetaInfoScope.node in to_merge:
            self._meta_info[MetaInfoScope.node].update(to_merge[MetaInfoScope.node])

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
            "trial-id": self._trial_id,
            "trial-timestamp": self._trial_timestamp,
            "track": self._track,
            "challenge": self._challenge,
            "car": self._car
        }

    def put_count_cluster_level(self, name, count, unit=None, task=None, operation=None, operation_type=None, sample_type=SampleType.Normal,
                                absolute_time=None, relative_time=None, meta_data=None):
        """
        Adds a new cluster level counter metric.

        :param name: The name of the metric.
        :param count: The metric value. It is expected to be of type int (otherwise use put_value_*).
        :param unit: A count may or may not have unit.
        :param task: The task name to which this value applies. Optional. Defaults to None.
        :param operation: The operation name to which this value applies. Optional. Defaults to None.
        :param operation_type: The operation type to which this value applies. Optional. Defaults to None.
        :param sample_type: Whether this is a warmup or a normal measurement sample. Defaults to SampleType.Normal.
        :param absolute_time: The absolute timestamp in seconds since epoch when this metric record is stored. Defaults to None. The metrics
               store will derive the timestamp automatically.
        :param relative_time: The relative timestamp in seconds since the start of the benchmark when this metric record is stored.
               Defaults to None. The metrics store will derive the timestamp automatically.
        :param meta_data: A dict, containing additional key-value pairs. Defaults to None.
        """
        self._put(MetaInfoScope.cluster, None, name, count, unit, task, operation, operation_type, sample_type, absolute_time,
                  relative_time, meta_data)

    def put_count_node_level(self, node_name, name, count, unit=None, task=None, operation=None, operation_type=None,
                             sample_type=SampleType.Normal, absolute_time=None, relative_time=None, meta_data=None):
        """
        Adds a new node level counter metric.

        :param name: The name of the metric.
        :param node_name: The name of the cluster node for which this metric has been determined.
        :param count: The metric value. It is expected to be of type int (otherwise use put_value_*).
        :param unit: A count may or may not have unit.
        :param task: The task name to which this value applies. Optional. Defaults to None.
        :param operation: The operation name to which this value applies. Optional. Defaults to None.
        :param operation_type: The operation type to which this value applies. Optional. Defaults to None.
        :param sample_type Whether this is a warmup or a normal measurement sample. Defaults to SampleType.Normal.
        :param absolute_time: The absolute timestamp in seconds since epoch when this metric record is stored. Defaults to None. The metrics
               store will derive the timestamp automatically.
        :param relative_time: The relative timestamp in seconds since the start of the benchmark when this metric record is stored.
               Defaults to None. The metrics store will derive the timestamp automatically.
        :param meta_data: A dict, containing additional key-value pairs. Defaults to None.
        """
        self._put(MetaInfoScope.node, node_name, name, count, unit, task, operation, operation_type, sample_type, absolute_time,
                  relative_time, meta_data)

    # should be a float
    def put_value_cluster_level(self, name, value, unit, task=None, operation=None, operation_type=None, sample_type=SampleType.Normal,
                                absolute_time=None, relative_time=None, meta_data=None):
        """
        Adds a new cluster level value metric.

        :param name: The name of the metric.
        :param value: The metric value. It is expected to be of type float (otherwise use put_count_*).
        :param unit: The unit of this metric value (e.g. ms, docs/s).
        :param task: The task name to which this value applies. Optional. Defaults to None.
        :param operation: The operation name to which this value applies. Optional. Defaults to None.
        :param operation_type: The operation type to which this value applies. Optional. Defaults to None.
        :param sample_type: Whether this is a warmup or a normal measurement sample. Defaults to SampleType.Normal.
        :param absolute_time: The absolute timestamp in seconds since epoch when this metric record is stored. Defaults to None. The metrics
               store will derive the timestamp automatically.
        :param relative_time: The relative timestamp in seconds since the start of the benchmark when this metric record is stored.
               Defaults to None. The metrics store will derive the timestamp automatically.
        :param meta_data: A dict, containing additional key-value pairs. Defaults to None.
        """
        self._put(MetaInfoScope.cluster, None, name, value, unit, task, operation, operation_type, sample_type, absolute_time,
                  relative_time, meta_data)

    def put_value_node_level(self, node_name, name, value, unit, task=None, operation=None, operation_type=None,
                             sample_type=SampleType.Normal, absolute_time=None, relative_time=None, meta_data=None):
        """
        Adds a new node level value metric.

        :param name: The name of the metric.
        :param node_name: The name of the cluster node for which this metric has been determined.
        :param value: The metric value. It is expected to be of type float (otherwise use put_count_*).
        :param unit: The unit of this metric value (e.g. ms, docs/s)
        :param task: The task name to which this value applies. Optional. Defaults to None.
        :param operation: The operation name to which this value applies. Optional. Defaults to None.
        :param operation_type: The operation type to which this value applies. Optional. Defaults to None.
        :param sample_type: Whether this is a warmup or a normal measurement sample. Defaults to SampleType.Normal.
        :param absolute_time: The absolute timestamp in seconds since epoch when this metric record is stored. Defaults to None. The metrics
               store will derive the timestamp automatically.
        :param relative_time: The relative timestamp in seconds since the start of the benchmark when this metric record is stored.
               Defaults to None. The metrics store will derive the timestamp automatically.
        :param meta_data: A dict, containing additional key-value pairs. Defaults to None.
        """
        self._put(MetaInfoScope.node, node_name, name, value, unit, task, operation, operation_type, sample_type, absolute_time,
                  relative_time, meta_data)

    def _put(self, level, level_key, name, value, unit, task, operation, operation_type, sample_type, absolute_time=None,
             relative_time=None, meta_data=None):
        if level == MetaInfoScope.cluster:
            meta = self._meta_info[MetaInfoScope.cluster].copy()
        elif level == MetaInfoScope.node:
            meta = self._meta_info[MetaInfoScope.cluster].copy()
            if level_key in self._meta_info[MetaInfoScope.node]:
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
            "trial-id": self._trial_id,
            "trial-timestamp": self._trial_timestamp,
            "environment": self._environment_name,
            "track": self._track,
            "lap": self._lap,
            "challenge": self._challenge,
            "car": self._car_name,
            "name": name,
            "value": value,
            "unit": unit,
            "sample-type": sample_type.name.lower(),
            "meta": meta
        }
        if task:
            doc["task"] = task
        if operation:
            doc["operation"] = operation
        if operation_type:
            doc["operation-type"] = operation_type
        if self._track_params:
            doc["track-params"] = self._track_params

        assert self.lap is not None, "Attempting to store [%s] without a lap." % doc
        self._add(doc)

    def bulk_add(self, memento):
        """
        Adds raw metrics store documents previously created with #to_externalizable()

        :param memento: The external representation as returned by #to_externalizable().
        """
        if memento:
            logger.info("Restoring in-memory representation of metrics store.")
            for doc in pickle.loads(zlib.decompress(memento)):
                self._add(doc)

    def to_externalizable(self, clear=False):
        raise NotImplementedError("abstract method")

    def _add(self, doc):
        """
        Adds a new document to the metrics store

        :param doc: The new document.
        """
        raise NotImplementedError("abstract method")

    def get_one(self, name, sample_type=None, lap=None):
        """
        Gets one value for the given metric name (even if there should be more than one).

        :param name: The metric name to query.
        :param sample_type The sample type to query. Optional. By default, all samples are considered.
        :param lap The lap to query. Optional. By default, all laps are considered.
        :return: The corresponding value for the given metric name or None if there is no value.
        """
        return self._first_or_none(self.get(name=name, sample_type=sample_type, lap=lap))

    def _first_or_none(self, values):
        return values[0] if values else None

    def get(self, name, task=None, operation_type=None, sample_type=None, lap=None):
        """
        Gets all raw values for the given metric name.

        :param name: The metric name to query.
        :param task The task name to query. Optional.
        :param operation_type The operation type to query. Optional.
        :param sample_type The sample type to query. Optional. By default, all samples are considered.
        :param lap The lap to query. Optional. By default, all laps are considered.
        :return: A list of all values for the given metric.
        """
        return self._get(name, task, operation_type, sample_type, lap, lambda doc: doc["value"])

    def get_raw(self, name, task=None, operation_type=None, sample_type=None, lap=None, mapper=lambda doc: doc):
        """
        Gets all raw records for the given metric name.

        :param name: The metric name to query.
        :param task The task name to query. Optional.
        :param operation_type The operation type to query. Optional.
        :param sample_type The sample type to query. Optional. By default, all samples are considered.
        :param lap The lap to query. Optional. By default, all laps are considered.
        :param mapper A record mapper. By default, the complete record is returned.
        :return: A list of all raw records for the given metric.
        """
        return self._get(name, task, operation_type, sample_type, lap, mapper)

    def get_unit(self, name, task=None):
        """
        Gets the unit for the given metric name.

        :param name: The metric name to query.
        :param task The task name to query. Optional.
        :return: The corresponding unit for the given metric name or None if no metric record is available.
        """
        # does not make too much sense to ask for a sample type here
        return self._first_or_none(self._get(name, task, None, None, None, lambda doc: doc["unit"]))

    def _get(self, name, task, operation_type, sample_type, lap, mapper):
        raise NotImplementedError("abstract method")

    def get_count(self, name, task=None, operation_type=None, sample_type=None, lap=None):
        """

        :param name: The metric name to query.
        :param task The task name to query. Optional.
        :param operation_type The operation type to query. Optional.
        :param sample_type The sample type to query. Optional. By default, all samples are considered.
        :param lap The lap to query. Optional. By default, all laps are considered.
        :return: The number of samples for this metric.
        """
        stats = self.get_stats(name, task, operation_type, sample_type, lap)
        if stats:
            return stats["count"]
        else:
            return 0

    def get_error_rate(self, task, operation_type=None, sample_type=None, lap=None):
        """
        Gets the error rate for a specific task.

        :param task The task name to query.
        :param operation_type The operation type to query. Optional.
        :param sample_type The sample type to query. Optional. By default, all samples are considered.
        :param lap The lap to query. Optional. By default, all laps are considered.
        :return: A float between 0.0 and 1.0 (inclusive) representing the error rate.
        """
        raise NotImplementedError("abstract method")

    def get_stats(self, name, task=None, operation_type=None, sample_type=None, lap=None):
        """
        Gets standard statistics for the given metric.

        :param name: The metric name to query.
        :param task The task name to query. Optional.
        :param operation_type The operation type to query. Optional.
        :param sample_type The sample type to query. Optional. By default, all samples are considered.
        :param lap The lap to query. Optional. By default, all laps are considered.
        :return: A metric_stats structure.
        """
        raise NotImplementedError("abstract method")

    def get_percentiles(self, name, task=None, operation_type=None, sample_type=None, lap=None, percentiles=None):
        """
        Retrieves percentile metrics for the given metric.

        :param name: The metric name to query.
        :param task The task name to query. Optional.
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

    def get_median(self, name, task=None, operation_type=None, sample_type=None, lap=None):
        """
        Retrieves median value of the given metric.

        :param name: The metric name to query.
        :param task The task name to query. Optional.
        :param operation_type The operation type to query. Optional.
        :param sample_type The sample type to query. Optional. By default, all samples are considered.
        :param lap The lap to query. Optional. By default, all laps are considered.
        :return: The median value.
        """
        median = "50.0"
        percentiles = self.get_percentiles(name, task, operation_type, sample_type, lap, percentiles=[median])
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

        :param cfg: The config object. Mandatory.
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

    def open(self, trial_id=None, trial_timestamp=None, track_name=None, challenge_name=None, car_name=None, ctx=None, create=False):
        self._docs = []
        MetricsStore.open(self, trial_id, trial_timestamp, track_name, challenge_name, car_name, ctx, create)
        self._index = self.index_name()
        # reduce a bit of noise in the metrics cluster log
        if create:
            # always update the mapping to the latest version
            self._client.put_template("rally-metrics", self._get_template())
            if not self._client.exists(index=self._index):
                self._client.create_index(index=self._index)
        # ensure we can search immediately after opening
        self._client.refresh(index=self._index)

    def index_name(self):
        ts = time.from_is8601(self._trial_timestamp)
        return "rally-metrics-%04d-%02d" % (ts.year, ts.month)

    def _get_template(self):
        return self._index_template_provider.metrics_template()

    def flush(self, refresh=True):
        if self._docs:
            self._client.bulk_index(index=self._index, doc_type=EsMetricsStore.METRICS_DOC_TYPE, items=self._docs)
            logger.info("Successfully added %d metrics documents for trial timestamp=[%s], track=[%s], challenge=[%s], car=[%s]." %
                        (len(self._docs), self._trial_timestamp, self._track, self._challenge, self._car))
        self._docs = []
        # ensure we can search immediately after flushing
        if refresh:
            self._client.refresh(index=self._index)

    def _add(self, doc):
        self._docs.append(doc)

    def _get(self, name, task, operation_type, sample_type, lap, mapper):
        query = {
            "query": self._query_by_name(name, task, operation_type, sample_type, lap)
        }
        logger.debug("Issuing get against index=[%s], doc_type=[%s], query=[%s]" % (self._index, EsMetricsStore.METRICS_DOC_TYPE, query))
        result = self._client.search(index=self._index, doc_type=EsMetricsStore.METRICS_DOC_TYPE, body=query)
        logger.debug("Metrics query produced [%s] results." % result["hits"]["total"])
        return [mapper(v["_source"]) for v in result["hits"]["hits"]]

    def get_error_rate(self, task, operation_type=None, sample_type=None, lap=None):
        query = {
            "query": self._query_by_name("service_time", task, operation_type, sample_type, lap),
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

    def get_stats(self, name, task=None, operation_type=None, sample_type=None, lap=None):
        """
        Gets standard statistics for the given metric name.

        :return: A metric_stats structure. For details please refer to
        https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-stats-aggregation.html
        """
        query = {
            "query": self._query_by_name(name, task, operation_type, sample_type, lap),
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

    def get_percentiles(self, name, task=None, operation_type=None, sample_type=None, lap=None, percentiles=None):
        if percentiles is None:
            percentiles = [99, 99.9, 100]
        query = {
            "query": self._query_by_name(name, task, operation_type, sample_type, lap),
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

    def _query_by_name(self, name, task, operation_type, sample_type, lap):
        q = {
            "bool": {
                "filter": [
                    {
                        "term": {
                            "trial-id": self._trial_id
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
        if task:
            q["bool"]["filter"].append({
                "term": {
                    "task": task
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

    def to_externalizable(self, clear=False):
        # no need for an externalizable representation - stores everything directly
        return None

    def __str__(self):
        return "Elasticsearch metrics store"


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

    def flush(self, refresh=True):
        pass

    def to_externalizable(self, clear=False):
        docs = self.docs
        if clear:
            self.docs = []
        compressed = zlib.compress(pickle.dumps(docs))
        logger.info("Compression changed size of metric store from [%d] bytes to [%d] bytes" %
                    (sys.getsizeof(docs, -1), sys.getsizeof(compressed, -1)))
        return compressed

    def get_percentiles(self, name, task=None, operation_type=None, sample_type=None, lap=None, percentiles=None):
        if percentiles is None:
            percentiles = [99, 99.9, 100]
        result = collections.OrderedDict()
        values = self.get(name, task, operation_type, sample_type, lap)
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

    def get_error_rate(self, task, operation_type=None, sample_type=None, lap=None):
        error = 0
        total_count = 0
        for doc in self.docs:
            # we can use any request metrics record (i.e. service time or latency)
            if doc["name"] == "service_time" and doc["task"] == task and \
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

    def get_stats(self, name, task=None, operation_type=None, sample_type=SampleType.Normal, lap=None):
        values = self.get(name, task, operation_type, sample_type, lap)
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

    def _get(self, name, task, operation_type, sample_type, lap, mapper):
        return [mapper(doc)
                for doc in self.docs
                if doc["name"] == name and
                (task is None or doc["task"] == task) and
                (operation_type is None or doc["operation-type"] == operation_type.name) and
                (sample_type is None or doc["sample-type"] == sample_type.name.lower()) and
                (lap is None or doc["lap"] == lap)
                ]

    def __str__(self):
        return "in-memory metrics store"


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
    def format_dict(d):
        if d:
            return ", ".join(["%s=%s" % (k, v) for k, v in d.items()])
        else:
            return None

    races = []
    for race in race_store(cfg).list():
        races.append([time.to_iso8601(race.trial_timestamp), race.track, format_dict(race.track_params), race.challenge, race.car_name,
                      format_dict(race.user_tags)])

    if len(races) > 0:
        console.println("\nRecent races:\n")
        console.println(tabulate.tabulate(races, headers=["Race Timestamp", "Track", "Track Parameters", "Challenge", "Car", "User Tags"]))
    else:
        console.println("")
        console.println("No recent races found.")


def create_race(cfg, track, challenge):
    car = cfg.opts("mechanic", "car.names")
    environment = cfg.opts("system", "env.name")
    trial_id = cfg.opts("system", "trial.id")
    trial_timestamp = cfg.opts("system", "time.start")
    total_laps = cfg.opts("race", "laps")
    user_tags = extract_user_tags_from_config(cfg)
    pipeline = cfg.opts("race", "pipeline")
    track_params = cfg.opts("track", "params")
    rally_version = version.version()

    return Race(rally_version, environment, trial_id, trial_timestamp, pipeline, user_tags, track, track_params, challenge, car, total_laps)


class Race:
    def __init__(self, rally_version, environment_name, trial_id, trial_timestamp, pipeline, user_tags, track, track_params, challenge, car,
                 total_laps, cluster=None, lap_results=None, results=None):
        if results is None:
            results = {}
        if lap_results is None:
            lap_results = []
        self.rally_version = rally_version
        self.environment_name = environment_name
        self.trial_id = trial_id
        self.trial_timestamp = trial_timestamp
        self.pipeline = pipeline
        self.user_tags = user_tags
        self.track = track
        self.track_params = track_params
        self.challenge = challenge
        self.car = car
        self.total_laps = total_laps
        # will be set later - contains hosts, revision, distribution_version, ...
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
    def car_name(self):
        return "+".join(self.car) if isinstance(self.car, list) else self.car

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
        d = {
            "rally-version": self.rally_version,
            "environment": self.environment_name,
            "trial-id": self.trial_id,
            "trial-timestamp": time.to_iso8601(self.trial_timestamp),
            "pipeline": self.pipeline,
            "user-tags": self.user_tags,
            "track": self.track_name,
            "challenge": self.challenge_name,
            "car": self.car,
            "total-laps": self.total_laps,
            "cluster": self.cluster.as_dict(),
            "results": self.results.as_dict()
        }
        if self.track_params:
            d["track-params"] = self.track_params
        return d

    def to_result_dicts(self):
        """
        :return: a list of dicts, suitable for persisting the results of this race in a format that is Kibana-friendly.
        """
        result_template = {
            "rally-version": self.rally_version,
            "environment": self.environment_name,
            "trial-id": self.trial_id,
            "trial-timestamp": time.to_iso8601(self.trial_timestamp),
            "distribution-version": self.cluster.distribution_version,
            "distribution-major-version": versions.major_version(self.cluster.distribution_version),
            "user-tags": self.user_tags,
            "track": self.track_name,
            "challenge": self.challenge_name,
            "car": self.car_name,
            "node-count": len(self.cluster.nodes),
            # allow to logically delete records, e.g. for UI purposes when we only want to show the latest result on graphs
            "active": True
        }
        plugins = set()
        for node in self.cluster.nodes:
            plugins.update(node.plugins)

        if plugins:
            result_template["plugins"] = list(plugins)

        if self.track_params:
            result_template["track-params"] = self.track_params

        all_results = []

        for item in self.results.as_flat_list():
            result = result_template.copy()
            result.update(item)
            all_results.append(result)

        return all_results

    @classmethod
    def from_dict(cls, d):
        # for backwards compatibility with Rally < 0.9.2
        if "user-tag" in d:
            user_tags = extract_user_tags_from_string(d["user-tag"])
        elif "user-tags" in d:
            user_tags = d["user-tags"]
        else:
            user_tags = {}

        # Don't restore a few properties like cluster because they (a) cannot be reconstructed easily without knowledge of other modules
        # and (b) it is not necessary for this use case.
        return Race(d["rally-version"], d["environment"], d["trial-id"], time.from_is8601(d["trial-timestamp"]), d["pipeline"], user_tags,
                    d["track"], d.get("track-params"), d["challenge"], d["car"], d["total-laps"], results=d["results"])


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
        with open(self._output_file_name(doc), mode="wt", encoding="utf-8") as f:
            f.write(json.dumps(doc, indent=True, ensure_ascii=False))

    def _output_file_name(self, doc):
        if self.user_provided_start_timestamp:
            suffix = "_%s_%s_%s" % (doc["track"], doc["challenge"], "+".join(doc["car"]))
        else:
            suffix = ""
        return "%s/race%s.json" % (self.race_path, suffix)

    def list(self):
        import glob
        results = glob.glob("%s/*/race*.json" % self.races_path)
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
                with open(result, mode="rt", encoding="utf-8") as f:
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
