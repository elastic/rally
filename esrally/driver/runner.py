import sys
import types
import time
import logging
from collections import Counter, OrderedDict

from esrally import exceptions, track

logger = logging.getLogger("rally.driver")

# Mapping from operation type to specific runner
__RUNNERS = {}


def runner_for(operation_type):
    try:
        return __RUNNERS[operation_type]
    except KeyError:
        raise exceptions.RallyError("No runner available for operation type [%s]" % operation_type)


def register_runner(operation_type, runner):
    if getattr(runner, "multi_cluster", False) == True:
        if "__enter__" in dir(runner) and "__exit__" in dir(runner):
            logger.info("Registering runner object [%s] for [%s]." % (str(runner), str(operation_type)))
            __RUNNERS[operation_type] = MultiClusterDelegatingRunner(runner, str(runner), context_manager_enabled=True)
        else:
            logger.info("Registering context-manager capable runner object [%s] for [%s]." % (str(runner), str(operation_type)))
            __RUNNERS[operation_type] = MultiClusterDelegatingRunner(runner, str(runner))
    # we'd rather use callable() but this will erroneously also classify a class as callable...
    elif isinstance(runner, types.FunctionType):
        logger.info("Registering runner function [%s] for [%s]." % (str(runner), str(operation_type)))
        __RUNNERS[operation_type] = SingleClusterDelegatingRunner(runner, runner.__name__)
    elif "__enter__" in dir(runner) and "__exit__" in dir(runner):
        logger.info("Registering context-manager capable runner object [%s] for [%s]." % (str(runner), str(operation_type)))
        __RUNNERS[operation_type] = SingleClusterDelegatingRunner(runner, str(runner), context_manager_enabled=True)
    else:
        logger.info("Registering runner object [%s] for [%s]." % (str(runner), str(operation_type)))
        __RUNNERS[operation_type] = SingleClusterDelegatingRunner(runner, str(runner))


# Only intended for unit-testing!
def remove_runner(operation_type):
    del __RUNNERS[operation_type]


class Runner:
    """
    Base class for all operations against Elasticsearch.
    """

    def __enter__(self):
        return self

    def __call__(self, *args):
        """
        Runs the actual method that should be benchmarked.

        :param args: All arguments that are needed to call this method.
        :return: A pair of (int, String). The first component indicates the "weight" of this call. it is typically 1 but for bulk operations
                 it should be the actual bulk size. The second component is the "unit" of weight which should be "ops" (short for
                 "operations") by default. If applicable, the unit should always be in plural form. It is used in metrics records
                 for throughput and reports. A value will then be shown as e.g. "111 ops/s".
        """
        raise NotImplementedError("abstract operation")

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class SingleClusterDelegatingRunner(Runner):
    def __init__(self, runnable, name, context_manager_enabled=False):
        self.runnable = runnable
        self.name = name
        self.context_manager_enabled = context_manager_enabled

    def __call__(self, *args):
        # Single cluster mode: es parameter passed in runner is a client object for the "default" cluster
        es = args[0]
        return self.runnable(es['default'], *args[1:])

    def __repr__(self, *args, **kwargs):
        if self.context_manager_enabled:
            return "user-defined context-manager enabled runner for [%s]" % self.name
        else:
            return "user-defined runner for [%s]" % self.name

    def __enter__(self):
        if self.context_manager_enabled:
            self.runnable.__enter__()
            return self
        else:
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.context_manager_enabled:
            return self.runnable.__exit__(exc_type, exc_val, exc_tb)
        else:
            return False


class MultiClusterDelegatingRunner(Runner):
    def __init__(self, runnable, name, context_manager_enabled=False):
        self.runnable = runnable
        self.name = name
        self.context_manager_enabled = context_manager_enabled

    def __call__(self, *args):
        # Multi cluster mode: pass the entire es dict and let runner code handle connections to different clusters
        return self.runnable(*args)

    def __repr__(self, *args, **kwargs):
        if self.context_manager_enabled:
            return "user-defined multi-cluster context-manager enabled runner for [%s]" % self.name
        else:
            return "user-defined multi-cluster enabled runner for [%s]" % self.name

    def __enter__(self):
        if self.context_manager_enabled:
            self.runnable.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.context_manager_enabled:
            return self.runnable.__exit__(exc_type, exc_val, exc_tb)
        else:
            return False


def mandatory(params, key, op):
    try:
        return params[key]
    except KeyError:
        raise exceptions.DataError("Parameter source for operation '%s' did not provide the mandatory parameter '%s'. Please add it to your"
                                   " parameter source." % (str(op), key))


class BulkIndex(Runner):
    """
    Bulk indexes the given documents.
    """

    def __init__(self):
        super().__init__()

    def __call__(self, es, params):
        """
        Runs one bulk indexing operation.

        :param es: The Elasticsearch client.
        :param params: A hash with all parameters. See below for details.
        :return: A hash with meta data for this bulk operation. See below for details.

        It expects a parameter dict with the following mandatory keys:

        * ``body``: containing all documents for the current bulk request.
        * ``bulk-size``: the number of documents in this bulk.
        * ``action_metadata_present``: if ``True``, assume that an action and metadata line is present (meaning only half of the lines
        contain actual documents to index)
        * ``index``: The name of the affected index in case ``action_metadata_present`` is ``False``.
        * ``type``: The name of the affected type in case ``action_metadata_present`` is ``False``.

        The following keys are optional:

        * ``pipeline``: If present, runs the the specified ingest pipeline for this bulk.
        * ``detailed-results``: If ``True``, the runner will analyze the response and add detailed meta-data. Defaults to ``False``. Note
        that this has a very significant impact on performance and will very likely cause a bottleneck in the benchmark driver so please
        be very cautious enabling this feature. Our own measurements have shown a median overhead of several thousand times (execution time
         is in the single digit microsecond range when this feature is disabled and in the single digit millisecond range when this feature
         is enabled; numbers based on a bulk size of 500 elements and no errors). For details please refer to the respective benchmarks
         in ``benchmarks/driver``.


        Returned meta data
        `
        The following meta data are always returned:

        * ``index``: name of the affected index. May be `None` if it could not be derived.
        * ``bulk-size``: bulk size, e.g. 5.000.
        * ``bulk-request-size-bytes``: size of the full bulk requset in bytes
        * ``total-document-size-bytes``: size of all documents contained in the bulk request in bytes
        * ``weight``: operation-agnostic representation of the bulk size (used internally by Rally for throughput calculation).
        * ``unit``: The unit in which to interpret ``bulk-size`` and ``weight``. Always "docs".
        * ``success``: A boolean indicating whether the bulk request has succeeded.
        * ``success-count``: Number of successfully processed items for this request (denoted in ``unit``).
        * ``error-count``: Number of failed items for this request (denoted in ``unit``).
        * ``took``` Value of the the ``took`` property in the bulk response.

        If ``detailed-results`` is ``True`` the following meta data are returned in addition:

        * ``ops``: A hash with the operation name as key (e.g. index, update, delete) and various counts as values. ``item-count`` contains
          the total number of items for this key. Additionally, we return a separate counter each result (indicating e.g. the number of created
          items, the number of deleted items etc.).
        * ``shards_histogram``: An array of hashes where each hash has two keys: ``item-count`` contains the number of items to which a shard
          distribution applies and ``shards`` contains another hash with the actual distribution of ``total``, ``successful`` and ``failed``
          shards (see examples below).
        * ``bulk-request-size-bytes``: Total size of the bulk request body in bytes.
        * ``total-document-size-bytes``: Total size of all documents within the bulk request body in bytes.

        Here are a few examples:

        If ``detailed-results`` is ``False`` a typical return value is::

            {
                "index": "my_index",
                "weight": 5000,
                "unit": "docs",
                "bulk-size": 5000,
                "success": True,
                "success-count": 5000,
                "error-count": 0,
                "took": 20
            }

        Whereas the response will look as follow if there are bulk errors::

            {
                "index": "my_index",
                "weight": 5000,
                "unit": "docs",
                "bulk-size": 5000,
                "success": False,
                "success-count": 4000,
                "error-count": 1000,
                "took": 20
            }

        If ``detailed-results`` is ``True`` a typical return value is::


            {
                "index": "my_index",
                "weight": 5000,
                "unit": "docs",
                "bulk-size": 5000,
                "bulk-request-size-bytes": 2250000,
                "total-document-size-bytes": 2000000,
                "success": True,
                "success-count": 5000,
                "error-count": 0,
                "took": 20,
                "ops": {
                    "index": {
                        "item-count": 5000,
                        "created": 5000
                    }
                },
                "shards_histogram": [
                    {
                        "item-count": 5000,
                        "shards": {
                            "total": 2,
                            "successful": 2,
                            "failed": 0
                        }
                    }
                ]
            }

        An example error response may look like this::


            {
                "index": "my_index",
                "weight": 5000,
                "unit": "docs",
                "bulk-size": 5000,
                "bulk-request-size-bytes": 2250000,
                "total-document-size-bytes": 2000000,
                "success": False,
                "success-count": 4000,
                "error-count": 1000,
                "took": 20,
                "ops": {
                    "index": {
                        "item-count": 5000,
                        "created": 4000,
                        "noop": 1000
                    }
                },
                "shards_histogram": [
                    {
                        "item-count": 4000,
                        "shards": {
                            "total": 2,
                            "successful": 2,
                            "failed": 0
                        }
                    },
                    {
                        "item-count": 500,
                        "shards": {
                            "total": 2,
                            "successful": 1,
                            "failed": 1
                        }
                    },
                    {
                        "item-count": 500,
                        "shards": {
                            "total": 2,
                            "successful": 0,
                            "failed": 2
                        }
                    }
                ]
            }
        """
        detailed_results = params.get("detailed-results", False)
        index = params.get("index")

        bulk_params = {}
        if "pipeline" in params:
            bulk_params["pipeline"] = params["pipeline"]

        # TODO: Remove this fallback logic with Rally 1.0
        if "action-metadata-present" in params:
            action_meta_data_key = "action-metadata-present"
        else:
            if "action_metadata_present" in params:
                logger.warning("Your parameter source uses the deprecated name [action_metadata_present]. Please change it to "
                               "[action-metadata-present].")
            action_meta_data_key = "action_metadata_present"

        with_action_metadata = mandatory(params, action_meta_data_key, self)
        bulk_size = mandatory(params, "bulk-size", self)

        if with_action_metadata:
            # only half of the lines are documents
            response = es.bulk(body=params["body"], params=bulk_params)
        else:
            response = es.bulk(body=params["body"], index=index, doc_type=params["type"], params=bulk_params)

        stats = self.detailed_stats(params, bulk_size, response) if detailed_results else self.simple_stats(bulk_size, response)

        meta_data = {
            "index": str(index) if index else None,
            "weight": bulk_size,
            "unit": "docs",
            "bulk-size": bulk_size
        }
        meta_data.update(stats)
        if not stats["success"]:
            meta_data["error-type"] = "bulk"
        return meta_data

    def detailed_stats(self, params, bulk_size, response):
        ops = {}
        shards_histogram = OrderedDict()
        bulk_error_count = 0
        error_details = set()
        bulk_request_size_bytes = 0
        total_document_size_bytes = 0

        for line_number, data in enumerate(params["body"]):

            line_size = len(data.encode('utf-8'))

            # TODO: Remove this fallback logic with Rally 1.0
            if "action-metadata-present" in params:
                action_meta_data_key = "action-metadata-present"
            else:
                if "action_metadata_present" in params:
                    logger.warning("Your parameter source uses the deprecated name [action_metadata_present]. Please change it to "
                                   "[action-metadata-present].")
                action_meta_data_key = "action_metadata_present"

            if params[action_meta_data_key]:
                if line_number % 2 == 1:
                    total_document_size_bytes += line_size
            else:
                total_document_size_bytes += line_size

            bulk_request_size_bytes += line_size

        for idx, item in enumerate(response["items"]):
            # there is only one (top-level) item
            op, data = next(iter(item.items()))
            if op not in ops:
                ops[op] = Counter()
            ops[op]["item-count"] += 1
            if "result" in data:
                ops[op][data["result"]] += 1

            if "_shards" in data:
                s = data["_shards"]
                sk = "%d-%d-%d" % (s["total"], s["successful"], s["failed"])
                if sk not in shards_histogram:
                    shards_histogram[sk] = {
                        "item-count": 0,
                        "shards": s
                    }
                shards_histogram[sk]["item-count"] += 1
            if data["status"] > 299 or ("_shards" in data and data["_shards"]["failed"] > 0):
                bulk_error_count += 1
                self.extract_error_details(error_details, data)
        stats = {
            "took": response.get("took"),
            "success": bulk_error_count == 0,
            "success-count": bulk_size - bulk_error_count,
            "error-count": bulk_error_count,
            "ops": ops,
            "shards_histogram": list(shards_histogram.values()),
            "bulk-request-size-bytes": bulk_request_size_bytes,
            "total-document-size-bytes": total_document_size_bytes
        }
        if bulk_error_count > 0:
            stats["error-type"] = "bulk"
            stats["error-description"] = self.error_description(error_details)
        return stats

    def simple_stats(self, bulk_size, response):
        bulk_error_count = 0
        error_details = set()
        if response["errors"]:
            for idx, item in enumerate(response["items"]):
                data = next(iter(item.values()))
                if data["status"] > 299 or data["_shards"]["failed"] > 0:
                    bulk_error_count += 1
                    self.extract_error_details(error_details, data)
        stats = {
            "took": response.get("took"),
            "success": bulk_error_count == 0,
            "success-count": bulk_size - bulk_error_count,
            "error-count": bulk_error_count
        }
        if bulk_error_count > 0:
            stats["error-type"] = "bulk"
            stats["error-description"] = self.error_description(error_details)
        return stats

    def extract_error_details(self, error_details, data):
        if data.get("error") and data["error"].get("reason"):
            error_details.add((data["status"], data["error"]["reason"]))
        else:
            error_details.add((data["status"], None))

    def error_description(self, error_details):
        error_description = ""
        for status, reason in error_details:
            if reason:
                error_description += "HTTP status: %s, message: %s" % (str(status), reason)
            else:
                error_description += "HTTP status: %s" % str(status)
        return error_description

    def __repr__(self, *args, **kwargs):
        return "bulk-index"


class ForceMerge(Runner):
    """
    Runs a force merge operation against Elasticsearch.
    """

    def __call__(self, es, params):
        logger.info("Force merging all indices.")
        import elasticsearch
        try:
            if "max-num-segments" in params:
                max_num_segments = params["max-num-segments"]
            elif "max_num_segments" in params:
                logger.warning("Your parameter source uses the deprecated name [max_num_segments]. Please change it to [max-num-segments].")
                max_num_segments = params["max_num_segments"]
            else:
                max_num_segments = None

            if max_num_segments:
                es.indices.forcemerge(index="_all", max_num_segments=max_num_segments)
            else:
                es.indices.forcemerge(index="_all")
        except elasticsearch.TransportError as e:
            # this is caused by older versions of Elasticsearch (< 2.1), fall back to optimize
            if e.status_code == 400:
                if max_num_segments:
                    es.transport.perform_request("POST", "/_optimize?max_num_segments={}".format(max_num_segments))
                else:
                    es.transport.perform_request("POST", "/_optimize")
            else:
                raise e

    def __repr__(self, *args, **kwargs):
        return "force-merge"


class IndicesStats(Runner):
    """
    Gather index stats for all indices.
    """

    def __call__(self, es, params):
        es.indices.stats(metric="_all")

    def __repr__(self, *args, **kwargs):
        return "indices-stats"


class NodeStats(Runner):
    """
    Gather node stats for all nodes.
    """

    def __call__(self, es, params):
        es.nodes.stats(metric="_all")

    def __repr__(self, *args, **kwargs):
        return "node-stats"


class Query(Runner):
    """
    Runs a request body search against Elasticsearch.

    It expects at least the following keys in the `params` hash:

    * `index`: The index or indices against which to issue the query.
    * `type`: See `index`
    * `cache`: True iff the request cache should be used.
    * `body`: Query body

    If the following parameters are present in addition, a scroll query will be issued:

    * `pages`: Number of pages to retrieve at most for this scroll. If a scroll query does yield less results than the specified number of
               pages we will terminate earlier.
    * `results-per-page`: Number of results to retrieve per page.

    Returned meta data

    The following meta data are always returned:

    * ``weight``: operation-agnostic representation of the "weight" of an operation (used internally by Rally for throughput calculation).
                  Always 1 for normal queries and the number of retrieved pages for scroll queries.
    * ``unit``: The unit in which to interpret ``weight``. Always "ops".
    * ``hits``: Total number of hits for this operation.
    * ``timed_out``: Whether the search has timed out. For scroll queries, this flag is ``True`` if the flag was ``True`` for any of the
                     queries issued.

    For scroll queries we also return:

    * ``pages``: Total number of pages that have been retrieved.
    """

    def __init__(self):
        self.scroll_id = None
        self.es = None

    def __call__(self, es, params):
        # TODO: Remove items_per_page with Rally 1.0.
        if "pages" in params and ("results-per-page" in params or "items_per_page" in params):
            return self.scroll_query(es, params)
        else:
            return self.request_body_query(es, params)

    def request_body_query(self, es, params):
        if "request-params" in params:
            request_params = params["request-params"]
        elif "request_params" in params:
            # TODO: Remove with Rally 1.0.
            logger.warning("Your parameter source uses the deprecated name [request_params]. Please change it to [request-params].")
            request_params = params["request_params"]
        else:
            request_params = {}
        if "cache" in params:
            request_params["request_cache"] = params["cache"]
        elif "use_request_cache" in params:
            # TODO: Remove with Rally 1.0.
            logger.warning("Your parameter source uses the deprecated name [use_request_cache]. Please change it to [cache].")
            request_params["request_cache"] = params["use_request_cache"]
        r = es.search(
            index=params.get("index", "_all"),
            doc_type=params.get("type"),
            body=mandatory(params, "body", self),
            **request_params)
        hits = r["hits"]["total"]
        return {
            "weight": 1,
            "unit": "ops",
            "hits": hits,
            "timed_out": r["timed_out"],
            "took": r["took"]
        }

    def scroll_query(self, es, params):
        if "request-params" in params:
            request_params = params["request-params"]
        elif "request_params" in params:
            # TODO: Remove with Rally 1.0.
            logger.warning("Your parameter source uses the deprecated name [request_params]. Please change it to [request-params].")
            request_params = params["request_params"]
        else:
            request_params = {}
        hits = 0
        retrieved_pages = 0
        timed_out = False
        took = 0
        self.es = es
        # explicitly convert to int to provoke an error otherwise
        total_pages = sys.maxsize if params["pages"] == "all" else int(params["pages"])
        if "cache" in params:
            cache = params["cache"]
        elif "use_request_cache" in params:
            # TODO: Remove with Rally 1.0.
            logger.warning("Your parameter source uses the deprecated name [use_request_cache]. Please change it to [cache].")
            cache = params["use_request_cache"]
        else:
            cache = None
        if "results-per-page" in params:
            size = params["results-per-page"]
        elif "items_per_page" in params:
            # TODO: Remove with Rally 1.0.
            logger.warning("Your parameter source uses the deprecated name [items_per_page]. Please change it to [results-per-page].")
            size = params["items_per_page"]
        else:
            size = None

        for page in range(total_pages):
            if page == 0:
                r = es.search(
                    index=params.get("index", "_all"),
                    doc_type=params.get("type"),
                    body=mandatory(params, "body", self),
                    sort="_doc",
                    scroll="10s",
                    size=size,
                    request_cache=cache,
                    **request_params
                )
                # This should only happen if we concurrently create an index and start searching
                self.scroll_id = r.get("_scroll_id", None)
            else:
                # This does only work for ES 2.x and above
                # r = es.scroll(body={"scroll_id": self.scroll_id, "scroll": "10s"})
                # This is the most compatible version to perform a scroll across all supported versions of Elasticsearch
                # (1.x does not support a proper JSON body in search scroll requests).
                r = self.es.transport.perform_request("GET", "/_search/scroll", params={"scroll_id": self.scroll_id, "scroll": "10s"})
            hit_count = len(r["hits"]["hits"])
            timed_out = timed_out or r["timed_out"]
            took += r["took"]
            hits += hit_count
            retrieved_pages += 1
            if hit_count == 0:
                # We're done prematurely. Even if we are on page index zero, we still made one call.
                break

        return {
            "weight": retrieved_pages,
            "pages": retrieved_pages,
            "hits": hits,
            "unit": "pages",
            "timed_out": timed_out,
            "took": took
        }

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.scroll_id and self.es:
            try:
                # This does only work for ES 2.x and above
                # self.es.clear_scroll(body={"scroll_id": [self.scroll_id]})

                # This is the most compatible version to clear one scroll id across all supported versions of Elasticsearch
                # (1.x does not support a proper JSON body in clear scroll requests).
                self.es.transport.perform_request("DELETE", "/_search/scroll/%s" % self.scroll_id)
            except BaseException:
                logger.exception("Could not clear scroll [%s]. This will lead to excessive resource usage in Elasticsearch and "
                                 "will skew your benchmark results." % self.scroll_id)
        self.scroll_id = None
        self.es = None
        return False

    def __repr__(self, *args, **kwargs):
        return "query"


class ClusterHealth(Runner):
    """
    Get cluster health
    """

    def __call__(self, es, params):
        from enum import Enum
        from functools import total_ordering
        from elasticsearch.client import _make_path

        @total_ordering
        class ClusterHealthStatus(Enum):
            UNKNOWN = 0
            RED = 1
            YELLOW = 2
            GREEN = 3

            def __lt__(self, other):
                if self.__class__ is other.__class__:
                    return self.value < other.value
                return NotImplemented

        def status(v):
            try:
                return ClusterHealthStatus[v.upper()]
            except (KeyError, AttributeError):
                return ClusterHealthStatus.UNKNOWN

        index = params.get("index")
        request_params = params.get("request-params", {})
        # by default, Elasticsearch will not wait and thus we treat this as success
        expected_cluster_status = request_params.get("wait_for_status", str(ClusterHealthStatus.UNKNOWN))
        # newer ES versions >= 5.0
        if "wait_for_no_relocating_shards" in request_params:
            expected_relocating_shards = 0
        else:
            # older ES versions
            # either the user has defined something or we're good with any count of relocating shards.
            expected_relocating_shards = int(request_params.get("wait_for_relocating_shards", sys.maxsize))

        # This would not work if the request parameter is not a proper method parameter for the ES client...
        # result = es.cluster.health(**request_params)
        result = es.transport.perform_request("GET", _make_path("_cluster", "health", index), params=request_params)
        cluster_status = result["status"]
        relocating_shards = result["relocating_shards"]

        return {
            "weight": 1,
            "unit": "ops",
            "success": status(cluster_status) >= status(expected_cluster_status) and relocating_shards <= expected_relocating_shards,
            "cluster-status": cluster_status,
            "relocating-shards": relocating_shards
        }

    def __repr__(self, *args, **kwargs):
        return "cluster-health"


class PutPipeline(Runner):
    """
    Execute the `put pipeline API <https://www.elastic.co/guide/en/elasticsearch/reference/current/put-pipeline-api.html>`_. Note that this
    API is only available from Elasticsearch 5.0 onwards.
    """

    def __call__(self, es, params):
        es.ingest.put_pipeline(id=mandatory(params, "id", self),
                               body=mandatory(params, "body", self),
                               master_timeout=params.get("master-timeout"),
                               timeout=params.get("timeout"),
                               )

    def __repr__(self, *args, **kwargs):
        return "put-pipeline"


class Refresh(Runner):
    """
    Execute the `refresh API <https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-refresh.html>`_.
    """

    def __call__(self, es, params):
        es.indices.refresh(index=params.get("index", "_all"))

    def __repr__(self, *args, **kwargs):
        return "refresh"


class CreateIndex(Runner):
    """
    Execute the `create index API <https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html>`_.
    """

    def __call__(self, es, params):
        indices = mandatory(params, "indices", self)
        request_params = params.get("request-params", {})
        for index, body in indices:
            es.indices.create(index=index, body=body, **request_params)
        return len(indices), "ops"

    def __repr__(self, *args, **kwargs):
        return "create-index"


class DeleteIndex(Runner):
    """
    Execute the `delete index API <https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-delete-index.html>`_.
    """

    def __call__(self, es, params):
        ops = 0

        indices = mandatory(params, "indices", self)
        only_if_exists = params.get("only-if-exists", False)
        request_params = params.get("request-params", {})

        for index_name in indices:
            if not only_if_exists:
                es.indices.delete(index=index_name, **request_params)
                ops += 1
            elif only_if_exists and es.indices.exists(index=index_name):
                logger.info("Index [%s] already exists. Deleting it." % index_name)
                es.indices.delete(index=index_name, **request_params)
                ops += 1

        return ops, "ops"

    def __repr__(self, *args, **kwargs):
        return "delete-index"


class CreateIndexTemplate(Runner):
    """
    Execute the `PUT index template API <https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html>`_.
    """

    def __call__(self, es, params):
        templates = mandatory(params, "templates", self)
        request_params = params.get("request-params", {})
        for template, body in templates:
            es.indices.put_template(name=template,
                                    body=body,
                                    **request_params)
        return len(templates), "ops"

    def __repr__(self, *args, **kwargs):
        return "create-index-template"


class DeleteIndexTemplate(Runner):
    """
    Execute the `delete index template API <https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html#delete>`_.
    """

    def __call__(self, es, params):
        template_names = mandatory(params, "templates", self)
        only_if_exists = params.get("only-if-exists", False)
        request_params = params.get("request-params", {})
        ops_count = 0

        for template_name, delete_matching_indices, index_pattern in template_names:
            if not only_if_exists:
                es.indices.delete_template(name=template_name, **request_params)
                ops_count += 1
            elif only_if_exists and es.indices.exists_template(template_name):
                logger.info("Index template [%s] already exists. Deleting it." % template_name)
                es.indices.delete_template(name=template_name, **request_params)
                ops_count += 1
            # ensure that we do not provide an empty index pattern by accident
            if delete_matching_indices and index_pattern:
                es.indices.delete(index=index_pattern)
                ops_count += 1

        return ops_count, "ops"

    def __repr__(self, *args, **kwargs):
        return "delete-index-template"


class RawRequest(Runner):
    def __call__(self, es, params):
        request_params = {}
        if "ignore" in params:
            request_params["ignore"] = params["ignore"]
        request_params.update(params.get("request-params", {}))

        es.transport.perform_request(method=params.get("method", "GET"),
                                     url=mandatory(params, "path", self),
                                     headers=params.get("headers"),
                                     body=params.get("body"),
                                     params=request_params)

    def __repr__(self, *args, **kwargs):
        return "raw-request"


# TODO: Allow to use this from (selected) regular runners and add user documentation.
# TODO: It would maybe be interesting to add meta-data on how many retries there were.
class Retry(Runner):
    """
    This runner can be used as a wrapper around regular runners to retry operations.

    It defines the following parameters:

    * ``retries`` (optional, default 0): The number of times the operation is retried.
    * ``retry-wait-period`` (optional, default 0.5): The time in seconds to wait after an error.
    * ``retry-on-timeout`` (optional, default True): Whether to retry on connection timeout.
    * ``retry-on-error`` (optional, default False): Whether to retry on failure (i.e. the delegate returns ``success == False``)
    """

    def __init__(self, delegate):
        self.delegate = delegate

    def __enter__(self):
        self.delegate.__enter__()
        return self

    def __call__(self, es, params):
        import elasticsearch
        import socket

        max_attempts = params.get("retries", 0) + 1
        sleep_time = params.get("retry-wait-period", 0.5)
        retry_on_timeout = params.get("retry-on-timeout", True)
        retry_on_error = params.get("retry-on-error", False)

        for attempt in range(max_attempts):
            last_attempt = attempt + 1 == max_attempts
            try:
                return_value = self.delegate(es, params)
                if last_attempt or not retry_on_error:
                    return return_value
                # we can determine success if and only if the runner returns a dict. Otherwise, we have to assume it was fine.
                elif isinstance(return_value, dict):
                    if return_value.get("success", True):
                        return return_value
                    else:
                        time.sleep(sleep_time)
                else:
                    return return_value
            except (socket.timeout, elasticsearch.exceptions.ConnectionError):
                if last_attempt or not retry_on_timeout:
                    raise
                else:
                    time.sleep(sleep_time)
            except elasticsearch.exceptions.TransportError as e:
                if last_attempt or not retry_on_timeout:
                    raise e
                elif e.status_code == 408:
                    logger.debug("%s has timed out." % repr(self.delegate))
                    time.sleep(sleep_time)
                else:
                    raise e

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.delegate.__exit__(exc_type, exc_val, exc_tb)

    def __repr__(self, *args, **kwargs):
        return "retryable %s" % repr(self.delegate)


register_runner(track.OperationType.Bulk.name, BulkIndex())
register_runner(track.OperationType.ForceMerge.name, ForceMerge())
register_runner(track.OperationType.IndicesStats.name, IndicesStats())
register_runner(track.OperationType.NodesStats.name, NodeStats())
register_runner(track.OperationType.Search.name, Query())
register_runner(track.OperationType.RawRequest.name, RawRequest())

# We treat the following as administrative commands and thus already start to wrap them in a retry.
register_runner(track.OperationType.ClusterHealth.name, Retry(ClusterHealth()))
register_runner(track.OperationType.PutPipeline.name, Retry(PutPipeline()))
register_runner(track.OperationType.Refresh.name, Retry(Refresh()))
register_runner(track.OperationType.CreateIndex.name, Retry(CreateIndex()))
register_runner(track.OperationType.DeleteIndex.name, Retry(DeleteIndex()))
register_runner(track.OperationType.CreateIndexTemplate.name, Retry(CreateIndexTemplate()))
register_runner(track.OperationType.DeleteIndexTemplate.name, Retry(DeleteIndexTemplate()))
