import types
import logging

import elasticsearch

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
    # we'd rather use callable() but this will erroneously also classify a class as callable...
    if isinstance(runner, types.FunctionType):
        logger.debug("Registering function [%s] for [%s]." % (str(runner), str(operation_type)))
        __RUNNERS[operation_type] = DelegatingRunner(runner)
    else:
        logger.debug("Registering object [%s] for [%s]." % (str(runner), str(operation_type)))
        __RUNNERS[operation_type] = runner


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


class DelegatingRunner(Runner):
    def __init__(self, runnable):
        self.runnable = runnable

    def __call__(self, *args):
        return self.runnable(*args)

    def __repr__(self, *args, **kwargs):
        return "user-defined runner for [%s]" % self.runnable.__name__


class BulkIndex(Runner):
    """
    Bulk indexes the given documents.

    It expects the parameter hash to contain a key "body" containing all documents for the current bulk request.

    """
    def __init__(self):
        super().__init__()

    def __call__(self, es, params):
        bulk_params = {}
        if "pipeline" in params:
            bulk_params["pipeline"] = params["pipeline"]

        with_action_metadata = params["action_metadata_present"]

        if with_action_metadata:
            # only half of the lines are documents
            bulk_size = len(params["body"]) // 2
            response = es.bulk(body=params["body"], params=bulk_params)
        else:
            bulk_size = len(params["body"])
            response = es.bulk(body=params["body"], index=params["index"], doc_type=params["type"], params=bulk_params)

        bulk_error_count = 0
        if response["errors"]:
            for idx, item in enumerate(response["items"]):
                if item["index"]["status"] > 299:
                    bulk_error_count += 1

        return {
            "weight": bulk_size,
            "unit": "docs",
            "bulk-size": bulk_size,
            "success": bulk_error_count == 0,
            "success-count": bulk_size - bulk_error_count,
            "error-count": bulk_error_count
        }

    def __repr__(self, *args, **kwargs):
        return "bulk-index"


class ForceMerge(Runner):
    """
    Runs a force merge operation against Elasticsearch.
    """
    def __call__(self, es, params):
        logger.info("Force merging all indices.")
        try:
            es.indices.forcemerge(index="_all")
        except elasticsearch.TransportError as e:
            # this is caused by older versions of Elasticsearch (< 2.1), fall back to optimize
            if e.status_code == 400:
                es.indices.optimize(index="_all")
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
    * `use_request_cache`: True iff the request cache should be used.
    * `body`: Query body

    If the following parameters are present in addition, a scroll query will be issued:

    * `pages`: Number of pages to retrieve at most for this scroll. If a scroll query does yield less results than the specified number of
               pages we will terminate earlier.
    * `items_per_page`: Number of items to retrieve per page.

    """

    def __init__(self):
        self.scroll_id = None
        self.es = None

    def __call__(self, es, params):
        if "pages" in params and "items_per_page" in params:
            return self.scroll_query(es, params)
        else:
            return self.request_body_query(es, params)

    def request_body_query(self, es, params):
        es.search(index=params["index"], doc_type=params["type"], request_cache=params["use_request_cache"], body=params["body"])
        return 1, "ops"

    def scroll_query(self, es, params):
        self.es = es
        r = es.search(
            index=params["index"],
            doc_type=params["type"],
            body=params["body"],
            sort="_doc",
            scroll="10s",
            size=params["items_per_page"],
            request_cache=params["use_request_cache"])
        self.scroll_id = r["_scroll_id"]
        total_pages = params["pages"]
        # Note that starting with ES 2.0, the initial call to search() returns already the first result page
        # so we have to retrieve one page less
        for page in range(total_pages - 1):
            hit_count = len(r["hits"]["hits"])
            if hit_count == 0:
                # We're done prematurely. Even if we are on page index zero, we still made one call.
                return page + 1, "ops"
            r = es.scroll(scroll_id=self.scroll_id, scroll="10s")
        return total_pages, "ops"

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.scroll_id and self.es:
            self.es.clear_scroll(scroll_id=self.scroll_id)
            self.scroll_id = None
            self.es = None
        return False

    def __repr__(self, *args, **kwargs):
        return "query"


register_runner(track.OperationType.Index.name, BulkIndex())
register_runner(track.OperationType.ForceMerge.name, ForceMerge())
register_runner(track.OperationType.IndicesStats.name, IndicesStats())
register_runner(track.OperationType.NodesStats.name, NodeStats())
register_runner(track.OperationType.Search.name, Query())


