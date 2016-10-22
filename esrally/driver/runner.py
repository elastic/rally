import logging

import elasticsearch

from esrally import track, exceptions

logger = logging.getLogger("rally.driver")

# Mapping from operation type to specific runner
runners = {}


def runner_for(operation_type):
    try:
        return runners[operation_type]
    except KeyError:
        raise exceptions.RallyError("No runner available for operation type [%s]" % operation_type)


class Runner:
    """
    Base class for all operations against Elasticsearch.
    """

    def __init__(self, operation_type):
        # self register
        global runners
        runners[operation_type] = self

    def __enter__(self):
        return self

    def __call__(self, *args):
        """
        Runs the actual method that should be benchmarked.

        :param args: All arguments that are needed to call this method.
        :return: An int indicating the "weight" of this call. This is typically 1 but for bulk operations it should be the actual bulk size.
        """
        raise NotImplementedError("abstract operation")

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class BulkIndex(Runner):
    """
    Bulk indexes the given documents.

    It expects the parameter hash to contain a key "body" containing all documents for the current bulk request.

    """
    def __init__(self):
        super().__init__(track.OperationType.Index)

    def __call__(self, es, params):
        bulk_params = {}
        if "pipeline" in params:
            bulk_params["pipeline"] = params["pipeline"]

        response = es.bulk(body=params["body"], params=bulk_params)
        if response["errors"]:
            for idx, item in enumerate(response["items"]):
                if item["index"]["status"] != 201:
                    msg = "Could not bulk index. "
                    msg += "Error in line [%d]\n" % (idx + 1)
                    msg += "Bulk item: [%s]\n" % item
                    msg += "Buffer size is [%d]\n" % idx
                    raise exceptions.DataError(msg)
        # at this point, the bulk will always contain a separate meta data line
        return len(params["body"]) // 2


class ForceMerge(Runner):
    """
    Runs a force merge operation against Elasticsearch.
    """
    def __init__(self):
        super().__init__(track.OperationType.ForceMerge)

    def __call__(self, es, params):
        indices = ",".join(params["indices"])
        logger.info("Force merging indices [%s]." % indices)
        try:
            es.indices.forcemerge(index=indices)
        except elasticsearch.TransportError as e:
            # this is caused by older versions of Elasticsearch (< 2.1), fall back to optimize
            if e.status_code == 400:
                es.indices.optimize(index=indices)
            else:
                raise e
        return 1


class IndicesStats(Runner):
    """
    Gather index stats for all indices.
    """
    def __init__(self):
        super().__init__(track.OperationType.IndicesStats)

    def __call__(self, es, params):
        es.indices.stats(metric="_all")
        return 1


class NodeStats(Runner):
    """
    Gather node stats for all nodes.
    """
    def __init__(self):
        super().__init__(track.OperationType.NodesStats)

    def __call__(self, es, params):
        es.nodes.stats(metric="_all")
        return 1


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
        super().__init__(track.OperationType.Search)
        self.scroll_id = None
        self.es = None

    def __call__(self, es, params):
        if "pages" in params and "items_per_page" in params:
            return self.scroll_query(es, params)
        else:
            return self.request_body_query(es, params)

    def request_body_query(self, es, params):
        es.search(index=params["index"], doc_type=params["type"], request_cache=params["use_request_cache"], body=params["body"])
        return 1

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
                return page + 1
            r = es.scroll(scroll_id=self.scroll_id, scroll="10s")
        return total_pages

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.scroll_id and self.es:
            self.es.clear_scroll(scroll_id=self.scroll_id)
            self.scroll_id = None
            self.es = None
        return False


# Initialize all runners - they'll then self-register
BulkIndex()
ForceMerge()
IndicesStats()
NodeStats()
Query()
