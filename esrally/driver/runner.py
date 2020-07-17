# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import asyncio
import json
import logging
import random
import sys
import types
from collections import Counter, OrderedDict
from copy import deepcopy

import ijson

from esrally import exceptions, track

# Mapping from operation type to specific runner
__RUNNERS = {}


def register_default_runners():
    register_runner(track.OperationType.Bulk.name, BulkIndex(), async_runner=True)
    register_runner(track.OperationType.ForceMerge.name, ForceMerge(), async_runner=True)
    register_runner(track.OperationType.IndicesStats.name, Retry(IndicesStats()), async_runner=True)
    register_runner(track.OperationType.NodesStats.name, NodeStats(), async_runner=True)
    register_runner(track.OperationType.Search.name, Query(), async_runner=True)
    register_runner(track.OperationType.RawRequest.name, RawRequest(), async_runner=True)
    # This is an administrative operation but there is no need for a retry here as we don't issue a request
    register_runner(track.OperationType.Sleep.name, Sleep(), async_runner=True)
    # these requests should not be retried as they are not idempotent
    register_runner(track.OperationType.CreateSnapshot.name, CreateSnapshot(), async_runner=True)
    register_runner(track.OperationType.RestoreSnapshot.name, RestoreSnapshot(), async_runner=True)
    # We treat the following as administrative commands and thus already start to wrap them in a retry.
    register_runner(track.OperationType.ClusterHealth.name, Retry(ClusterHealth()), async_runner=True)
    register_runner(track.OperationType.PutPipeline.name, Retry(PutPipeline()), async_runner=True)
    register_runner(track.OperationType.Refresh.name, Retry(Refresh()), async_runner=True)
    register_runner(track.OperationType.CreateIndex.name, Retry(CreateIndex()), async_runner=True)
    register_runner(track.OperationType.DeleteIndex.name, Retry(DeleteIndex()), async_runner=True)
    register_runner(track.OperationType.CreateIndexTemplate.name, Retry(CreateIndexTemplate()), async_runner=True)
    register_runner(track.OperationType.DeleteIndexTemplate.name, Retry(DeleteIndexTemplate()), async_runner=True)
    register_runner(track.OperationType.ShrinkIndex.name, Retry(ShrinkIndex()), async_runner=True)
    register_runner(track.OperationType.CreateMlDatafeed.name, Retry(CreateMlDatafeed()), async_runner=True)
    register_runner(track.OperationType.DeleteMlDatafeed.name, Retry(DeleteMlDatafeed()), async_runner=True)
    register_runner(track.OperationType.StartMlDatafeed.name, Retry(StartMlDatafeed()), async_runner=True)
    register_runner(track.OperationType.StopMlDatafeed.name, Retry(StopMlDatafeed()), async_runner=True)
    register_runner(track.OperationType.CreateMlJob.name, Retry(CreateMlJob()), async_runner=True)
    register_runner(track.OperationType.DeleteMlJob.name, Retry(DeleteMlJob()), async_runner=True)
    register_runner(track.OperationType.OpenMlJob.name, Retry(OpenMlJob()), async_runner=True)
    register_runner(track.OperationType.CloseMlJob.name, Retry(CloseMlJob()), async_runner=True)
    register_runner(track.OperationType.DeleteSnapshotRepository.name, Retry(DeleteSnapshotRepository()), async_runner=True)
    register_runner(track.OperationType.CreateSnapshotRepository.name, Retry(CreateSnapshotRepository()), async_runner=True)
    register_runner(track.OperationType.WaitForRecovery.name, Retry(IndicesRecovery()), async_runner=True)
    register_runner(track.OperationType.PutSettings.name, Retry(PutSettings()), async_runner=True)
    register_runner(track.OperationType.CreateTransform.name, Retry(CreateTransform()), async_runner=True)
    register_runner(track.OperationType.StartTransform.name, Retry(StartTransform()), async_runner=True)
    register_runner(track.OperationType.WaitForTransform.name, Retry(WaitForTransform()), async_runner=True)
    register_runner(track.OperationType.DeleteTransform.name, Retry(DeleteTransform()), async_runner=True)


def runner_for(operation_type):
    try:
        return __RUNNERS[operation_type]
    except KeyError:
        raise exceptions.RallyError("No runner available for operation type [%s]" % operation_type)


def register_runner(operation_type, runner, **kwargs):
    logger = logging.getLogger(__name__)
    async_runner = kwargs.get("async_runner", False)
    if not async_runner:
        raise exceptions.RallyAssertionError(
            "Runner [{}] must be implemented as async runner and registered with async_runner=True.".format(str(runner)))

    if getattr(runner, "multi_cluster", False):
        if "__aenter__" in dir(runner) and "__aexit__" in dir(runner):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Registering runner object [%s] for [%s].", str(runner), str(operation_type))
            __RUNNERS[operation_type] = _multi_cluster_runner(runner, str(runner), context_manager_enabled=True)
        else:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Registering context-manager capable runner object [%s] for [%s].", str(runner), str(operation_type))
            __RUNNERS[operation_type] = _multi_cluster_runner(runner, str(runner))
    # we'd rather use callable() but this will erroneously also classify a class as callable...
    elif isinstance(runner, types.FunctionType):
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Registering runner function [%s] for [%s].", str(runner), str(operation_type))
        __RUNNERS[operation_type] = _single_cluster_runner(runner, runner.__name__)
    elif "__aenter__" in dir(runner) and "__aexit__" in dir(runner):
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Registering context-manager capable runner object [%s] for [%s].", str(runner), str(operation_type))
        __RUNNERS[operation_type] = _single_cluster_runner(runner, str(runner), context_manager_enabled=True)
    else:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Registering runner object [%s] for [%s].", str(runner), str(operation_type))
        __RUNNERS[operation_type] = _single_cluster_runner(runner, str(runner))


# Only intended for unit-testing!
def remove_runner(operation_type):
    del __RUNNERS[operation_type]


class Runner:
    """
    Base class for all operations against Elasticsearch.
    """

    def __init__(self, *args, **kwargs):
        super(Runner, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)

    async def __aenter__(self):
        return self

    async def __call__(self, *args):
        """
        Runs the actual method that should be benchmarked.

        :param args: All arguments that are needed to call this method.
        :return: A pair of (int, String). The first component indicates the "weight" of this call. it is typically 1 but for bulk operations
                 it should be the actual bulk size. The second component is the "unit" of weight which should be "ops" (short for
                 "operations") by default. If applicable, the unit should always be in plural form. It is used in metrics records
                 for throughput and reports. A value will then be shown as e.g. "111 ops/s".
        """
        raise NotImplementedError("abstract operation")

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False


class Delegator:
    """
    Mixin to unify delegate handling
    """
    def __init__(self, delegate, *args, **kwargs):
        super(Delegator, self).__init__(*args, **kwargs)
        self.delegate = delegate


def unwrap(runner):
    """
    Unwraps all delegators until the actual runner.

    :param runner: An arbitrarily nested chain of delegators around a runner.
    :return: The innermost runner.
    """
    delegate = getattr(runner, "delegate", None)
    if delegate:
        return unwrap(delegate)
    else:
        return runner


def _single_cluster_runner(runnable, name, context_manager_enabled=False):
    # only pass the default ES client
    delegate = MultiClientRunner(runnable, name, lambda es: es["default"], context_manager_enabled)
    return _with_completion(delegate)


def _multi_cluster_runner(runnable, name, context_manager_enabled=False):
    # pass all ES clients
    delegate = MultiClientRunner(runnable, name, lambda es: es, context_manager_enabled)
    return _with_completion(delegate)


def _with_completion(delegate):
    unwrapped_runner = unwrap(delegate)
    if hasattr(unwrapped_runner, "completed") and hasattr(unwrapped_runner, "percent_completed"):
        return WithCompletion(delegate, unwrapped_runner)
    else:
        return NoCompletion(delegate)


class NoCompletion(Runner, Delegator):
    def __init__(self, delegate):
        super().__init__(delegate=delegate)

    @property
    def completed(self):
        return None

    @property
    def percent_completed(self):
        return None

    async def __call__(self, *args):
        return await self.delegate(*args)

    def __repr__(self, *args, **kwargs):
        return repr(self.delegate)

    async def __aenter__(self):
        await self.delegate.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self.delegate.__aexit__(exc_type, exc_val, exc_tb)


class WithCompletion(Runner, Delegator):
    def __init__(self, delegate, progressable):
        super().__init__(delegate=delegate)
        self.progressable = progressable

    @property
    def completed(self):
        return self.progressable.completed

    @property
    def percent_completed(self):
        return self.progressable.percent_completed

    async def __call__(self, *args):
        return await self.delegate(*args)

    def __repr__(self, *args, **kwargs):
        return repr(self.delegate)

    async def __aenter__(self):
        await self.delegate.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self.delegate.__aexit__(exc_type, exc_val, exc_tb)


class MultiClientRunner(Runner, Delegator):
    def __init__(self, runnable, name, client_extractor, context_manager_enabled=False):
        super().__init__(delegate=runnable)
        self.name = name
        self.client_extractor = client_extractor
        self.context_manager_enabled = context_manager_enabled

    async def __call__(self, *args):
        return await self.delegate(self.client_extractor(args[0]), *args[1:])

    def __repr__(self, *args, **kwargs):
        if self.context_manager_enabled:
            return "user-defined context-manager enabled runner for [%s]" % self.name
        else:
            return "user-defined runner for [%s]" % self.name

    async def __aenter__(self):
        if self.context_manager_enabled:
            await self.delegate.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.context_manager_enabled:
            return await self.delegate.__aexit__(exc_type, exc_val, exc_tb)
        else:
            return False


def mandatory(params, key, op):
    try:
        return params[key]
    except KeyError:
        raise exceptions.DataError("Parameter source for operation '%s' did not provide the mandatory parameter '%s'. Please add it to your"
                                   " parameter source." % (str(op), key))


def escape(v):
    """
    Escapes values so they can be used as query parameters

    :param v: The raw value. May be None.
    :return: The escaped value.
    """
    if v is None:
        return None
    elif isinstance(v, bool):
        return str(v).lower()
    else:
        return str(v)


class BulkIndex(Runner):
    """
    Bulk indexes the given documents.
    """

    def __init__(self):
        super().__init__()

    async def __call__(self, es, params):
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
        * ``bulk-request-size-bytes``: size of the full bulk request in bytes
        * ``total-document-size-bytes``: size of all documents contained in the bulk request in bytes
        * ``weight``: operation-agnostic representation of the bulk size (used internally by Rally for throughput calculation).
        * ``unit``: The unit in which to interpret ``bulk-size`` and ``weight``. Always "docs".
        * ``success``: A boolean indicating whether the bulk request has succeeded.
        * ``success-count``: Number of successfully processed items for this request (denoted in ``unit``).
        * ``error-count``: Number of failed items for this request (denoted in ``unit``).
        * ``took``` Value of the the ``took`` property in the bulk response.

        If ``detailed-results`` is ``True`` the following meta data are returned in addition:

        * ``ops``: A hash with the operation name as key (e.g. index, update, delete) and various counts as values. ``item-count`` contains
          the total number of items for this key. Additionally, we return a separate counter each result (indicating e.g. the number of
          created items, the number of deleted items etc.).
        * ``shards_histogram``: An array of hashes where each hash has two keys: ``item-count`` contains the number of items to which a
          shard distribution applies and ``shards`` contains another hash with the actual distribution of ``total``, ``successful`` and
          ``failed`` shards (see examples below).
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

        with_action_metadata = mandatory(params, "action-metadata-present", self)
        bulk_size = mandatory(params, "bulk-size", self)

        # parse responses lazily in the standard case - responses might be large thus parsing skews results and if no
        # errors have occurred we only need a small amount of information from the potentially large response.
        if not detailed_results:
            es.return_raw_response()

        if with_action_metadata:
            # only half of the lines are documents
            response = await es.bulk(body=params["body"], params=bulk_params)
        else:
            response = await es.bulk(body=params["body"], index=index, doc_type=params.get("type"), params=bulk_params)

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
        with_action_metadata = mandatory(params, "action-metadata-present", self)

        if isinstance(params["body"], str):
            bulk_lines = params["body"].split("\n")
        elif isinstance(params["body"], list):
            bulk_lines = params["body"]
        else:
            raise exceptions.DataError("bulk body is neither string nor list")

        for line_number, data in enumerate(bulk_lines):
            line_size = len(data.encode('utf-8'))
            if with_action_metadata:
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
        if "ingest_took" in response:
            stats["ingest_took"] = response["ingest_took"]

        return stats

    def simple_stats(self, bulk_size, response):
        bulk_error_count = 0
        error_details = set()
        # parse lazily on the fast path
        props = parse(response, ["errors", "took"])

        if props.get("errors", False):
            # Reparse fully in case of errors - this will be slower
            parsed_response = json.loads(response.getvalue())
            for idx, item in enumerate(parsed_response["items"]):
                data = next(iter(item.values()))
                if data["status"] > 299 or ('_shards' in data and data["_shards"]["failed"] > 0):
                    bulk_error_count += 1
                    self.extract_error_details(error_details, data)
        stats = {
            "took": props.get("took"),
            "success": bulk_error_count == 0,
            "success-count": bulk_size - bulk_error_count,
            "error-count": bulk_error_count
        }
        if bulk_error_count > 0:
            stats["error-type"] = "bulk"
            stats["error-description"] = self.error_description(error_details)
        return stats

    def extract_error_details(self, error_details, data):
        error_data = data.get("error", {})
        error_reason = error_data.get("reason") if isinstance(error_data, dict) else str(error_data)
        if error_data:
            error_details.add((data["status"], error_reason))
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

    async def __call__(self, es, params):
        import elasticsearch
        max_num_segments = params.get("max-num-segments")
        # preliminary support for overriding the global request timeout (see #567). As force-merge falls back to
        # the raw transport API (where the keyword argument is called `timeout`) in some cases we will always need
        # a special handling for the force-merge API.
        request_timeout = params.get("request-timeout")
        try:
            if max_num_segments:
                await es.indices.forcemerge(index=params.get("index"), max_num_segments=max_num_segments, request_timeout=request_timeout)
            else:
                await es.indices.forcemerge(index=params.get("index"), request_timeout=request_timeout)
        except elasticsearch.TransportError as e:
            # this is caused by older versions of Elasticsearch (< 2.1), fall back to optimize
            if e.status_code == 400:
                if max_num_segments:
                    await es.transport.perform_request("POST", f"/_optimize?max_num_segments={max_num_segments}",
                                                       timeout=request_timeout)
                else:
                    await es.transport.perform_request("POST", "/_optimize", timeout=request_timeout)
            else:
                raise e

    def __repr__(self, *args, **kwargs):
        return "force-merge"


class IndicesStats(Runner):
    """
    Gather index stats for all indices.
    """

    def _get(self, v, path):
        if v is None:
            return None
        elif len(path) == 1:
            return v.get(path[0])
        else:
            return self._get(v.get(path[0]), path[1:])

    def _safe_string(self, v):
        return str(v) if v is not None else None

    async def __call__(self, es, params):
        index = params.get("index", "_all")
        condition = params.get("condition")

        response = await es.indices.stats(index=index, metric="_all")
        if condition:
            path = mandatory(condition, "path", repr(self))
            expected_value = mandatory(condition, "expected-value", repr(self))
            actual_value = self._get(response, path.split("."))
            return {
                "weight": 1,
                "unit": "ops",
                "condition": {
                    "path": path,
                    # avoid mapping issues in the ES metrics store by always rendering values as strings
                    "actual-value": self._safe_string(actual_value),
                    "expected-value": self._safe_string(expected_value)
                },
                # currently we only support "==" as a predicate but that might change in the future
                "success": actual_value == expected_value
            }
        else:
            return {
                "weight": 1,
                "unit": "ops",
                "success": True
            }

    def __repr__(self, *args, **kwargs):
        return "indices-stats"


class NodeStats(Runner):
    """
    Gather node stats for all nodes.
    """

    async def __call__(self, es, params):
        await es.nodes.stats(metric="_all")

    def __repr__(self, *args, **kwargs):
        return "node-stats"


def parse(text, props, lists=None):
    """
    Selectively parsed the provided text as JSON extracting only the properties provided in ``props``. If ``lists`` is
    specified, this function determines whether the provided lists are empty (respective value will be ``True``) or
    contain elements (respective key will be ``False``).

    :param text: A text to parse.
    :param props: A mandatory list of property paths (separated by a dot character) for which to extract values.
    :param lists: An optional list of property paths to JSON lists in the provided text.
    :return: A dict containing all properties and lists that have been found in the provided text.
    """
    text.seek(0)
    parser = ijson.parse(text)
    parsed = {}
    parsed_lists = {}
    current_list = None
    expect_end_array = False
    try:
        for prefix, event, value in parser:
            if expect_end_array:
                # True if the list is empty, False otherwise
                parsed_lists[current_list] = event == "end_array"
                expect_end_array = False
            if prefix in props:
                parsed[prefix] = value
            elif lists is not None and prefix in lists and event == "start_array":
                current_list = prefix
                expect_end_array = True
            # found all necessary properties
            if len(parsed) == len(props) and (lists is None or len(parsed_lists) == len(lists)):
                break
    except ijson.IncompleteJSONError:
        # did not find all properties
        pass

    parsed.update(parsed_lists)
    return parsed


class Query(Runner):
    """
    Runs a request body search against Elasticsearch.

    It expects at least the following keys in the `params` hash:

    * `index`: The index or indices against which to issue the query.
    * `type`: See `index`
    * `cache`: True iff the request cache should be used.
    * `body`: Query body

    The following parameters are optional:

    * `detailed-results` (default: ``False``): Records more detailed meta-data about queries. As it analyzes the
                                               corresponding response in more detail, this might incur additional
                                               overhead which can skew measurement results. This flag is ineffective
                                               for scroll queries (detailed meta-data are always returned).

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
    * ``hits_relation``: whether ``hits`` is accurate (``eq``) or a lower bound of the actual hit count (``gte``).
    * ``timed_out``: Whether the search has timed out. For scroll queries, this flag is ``True`` if the flag was ``True`` for any of the
                     queries issued.

    For scroll queries we also return:

    * ``pages``: Total number of pages that have been retrieved.
    """

    def __init__(self):
        super().__init__()

    async def __call__(self, es, params):
        if "pages" in params and "results-per-page" in params:
            return await self.scroll_query(es, params)
        else:
            return await self.request_body_query(es, params)

    async def request_body_query(self, es, params):
        request_params = self._default_request_params(params)
        index = params.get("index", "_all")
        body = mandatory(params, "body", self)
        doc_type = params.get("type")
        detailed_results = params.get("detailed-results", False)
        headers = self._headers(params)

        # disable eager response parsing - responses might be huge thus skewing results
        es.return_raw_response()

        r = await self._raw_search(es, doc_type, index, body, request_params, headers)

        if detailed_results:
            props = parse(r, ["hits.total", "hits.total.value", "hits.total.relation", "timed_out", "took"])
            hits_total = props.get("hits.total.value", props.get("hits.total", 0))
            hits_relation = props.get("hits.total.relation", "eq")
            timed_out = props.get("timed_out", False)
            took = props.get("took", 0)

            return {
                "weight": 1,
                "unit": "ops",
                "success": True,
                "hits": hits_total,
                "hits_relation": hits_relation,
                "timed_out": timed_out,
                "took": took
            }
        else:
            return {
                "weight": 1,
                "unit": "ops",
                "success": True
            }

    async def scroll_query(self, es, params):
        request_params = self._default_request_params(params)
        hits = 0
        hits_relation = None
        retrieved_pages = 0
        timed_out = False
        took = 0
        # explicitly convert to int to provoke an error otherwise
        total_pages = sys.maxsize if params["pages"] == "all" else int(params["pages"])
        size = params.get("results-per-page")
        headers = self._headers(params)
        scroll_id = None

        # disable eager response parsing - responses might be huge thus skewing results
        es.return_raw_response()

        try:
            for page in range(total_pages):
                if page == 0:
                    index = params.get("index", "_all")
                    body = mandatory(params, "body", self)
                    sort = "_doc"
                    scroll = "10s"
                    doc_type = params.get("type")
                    params = request_params
                    params["sort"] = sort
                    params["scroll"] = scroll
                    params["size"] = size
                    r = await self._raw_search(es, doc_type, index, body, params, headers=headers)

                    props = parse(r,
                                  ["_scroll_id", "hits.total", "hits.total.value", "hits.total.relation", "timed_out", "took"],
                                  ["hits.hits"])
                    scroll_id = props.get("_scroll_id")
                    hits = props.get("hits.total.value", props.get("hits.total", 0))
                    hits_relation = props.get("hits.total.relation", "eq")
                    timed_out = props.get("timed_out", False)
                    took = props.get("took", 0)
                    all_results_collected = (size is not None and hits < size) or hits == 0
                else:
                    r = await es.transport.perform_request("GET", "/_search/scroll",
                                                           body={"scroll_id": scroll_id, "scroll": "10s"},
                                                           headers=headers)
                    props = parse(r, ["hits.total", "hits.total.value", "hits.total.relation", "timed_out", "took"], ["hits.hits"])
                    timed_out = timed_out or props.get("timed_out", False)
                    took += props.get("took", 0)
                    # is the list of hits empty?
                    all_results_collected = props.get("hits.hits", False)
                retrieved_pages += 1
                if all_results_collected:
                    break
        finally:
            if scroll_id:
                # noinspection PyBroadException
                try:
                    await es.clear_scroll(body={"scroll_id": [scroll_id]})
                except BaseException:
                    self.logger.exception("Could not clear scroll [%s]. This will lead to excessive resource usage in "
                                          "Elasticsearch and will skew your benchmark results.", scroll_id)

        return {
            "weight": retrieved_pages,
            "pages": retrieved_pages,
            "hits": hits,
            "hits_relation": hits_relation,
            "unit": "pages",
            "timed_out": timed_out,
            "took": took
        }

    async def _raw_search(self, es, doc_type, index, body, params, headers=None):
        components = []
        if index:
            components.append(index)
        if doc_type:
            components.append(doc_type)
        components.append("_search")
        path = "/".join(components)
        return await es.transport.perform_request("GET", "/" + path, params=params, body=body, headers=headers)

    def _default_request_params(self, params):
        request_params = params.get("request-params", {})
        cache = params.get("cache")
        if cache is not None:
            request_params["request_cache"] = str(cache).lower()
        return request_params

    def _headers(self, params):
        # reduces overhead due to decompression of very large responses
        if params.get("response-compression-enabled", True):
            return None
        else:
            return {"Accept-Encoding": "identity"}

    def __repr__(self, *args, **kwargs):
        return "query"


class ClusterHealth(Runner):
    """
    Get cluster health
    """

    async def __call__(self, es, params):
        from enum import Enum
        from functools import total_ordering

        @total_ordering
        class ClusterHealthStatus(Enum):
            UNKNOWN = 0
            RED = 1
            YELLOW = 2
            GREEN = 3

            def __lt__(self, other):
                if self.__class__ is other.__class__:
                    # pylint: disable=comparison-with-callable
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

        result = await es.cluster.health(index=index, params=request_params)
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

    async def __call__(self, es, params):
        await es.ingest.put_pipeline(id=mandatory(params, "id", self),
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

    async def __call__(self, es, params):
        await es.indices.refresh(index=params.get("index", "_all"))

    def __repr__(self, *args, **kwargs):
        return "refresh"


class CreateIndex(Runner):
    """
    Execute the `create index API <https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html>`_.
    """

    async def __call__(self, es, params):
        indices = mandatory(params, "indices", self)
        request_params = params.get("request-params", {})
        for index, body in indices:
            await es.indices.create(index=index, body=body, params=request_params)
        return len(indices), "ops"

    def __repr__(self, *args, **kwargs):
        return "create-index"


class DeleteIndex(Runner):
    """
    Execute the `delete index API <https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-delete-index.html>`_.
    """

    async def __call__(self, es, params):
        ops = 0

        indices = mandatory(params, "indices", self)
        only_if_exists = params.get("only-if-exists", False)
        request_params = params.get("request-params", {})

        for index_name in indices:
            if not only_if_exists:
                await es.indices.delete(index=index_name, params=request_params)
                ops += 1
            elif only_if_exists and await es.indices.exists(index=index_name):
                self.logger.info("Index [%s] already exists. Deleting it.", index_name)
                await es.indices.delete(index=index_name, params=request_params)
                ops += 1

        return ops, "ops"

    def __repr__(self, *args, **kwargs):
        return "delete-index"


class CreateIndexTemplate(Runner):
    """
    Execute the `PUT index template API <https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html>`_.
    """

    async def __call__(self, es, params):
        templates = mandatory(params, "templates", self)
        request_params = params.get("request-params", {})
        for template, body in templates:
            await es.indices.put_template(name=template,
                                          body=body,
                                          params=request_params)
        return len(templates), "ops"

    def __repr__(self, *args, **kwargs):
        return "create-index-template"


class DeleteIndexTemplate(Runner):
    """
    Execute the `delete index template API
    <https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html#delete>`_.
    """

    async def __call__(self, es, params):
        template_names = mandatory(params, "templates", self)
        only_if_exists = params.get("only-if-exists", False)
        request_params = params.get("request-params", {})
        ops_count = 0

        for template_name, delete_matching_indices, index_pattern in template_names:
            if not only_if_exists:
                await es.indices.delete_template(name=template_name, params=request_params)
                ops_count += 1
            elif only_if_exists and await es.indices.exists_template(template_name):
                self.logger.info("Index template [%s] already exists. Deleting it.", template_name)
                await es.indices.delete_template(name=template_name, params=request_params)
                ops_count += 1
            # ensure that we do not provide an empty index pattern by accident
            if delete_matching_indices and index_pattern:
                await es.indices.delete(index=index_pattern)
                ops_count += 1

        return ops_count, "ops"

    def __repr__(self, *args, **kwargs):
        return "delete-index-template"


class ShrinkIndex(Runner):
    """
    Execute the `shrink index API <https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-shrink-index.html>`_.

    This is a high-level runner that actually executes multiple low-level operations under the hood.
    """

    def __init__(self):
        super().__init__()
        self.cluster_health = Retry(ClusterHealth())

    async def _wait_for(self, es, idx, description):
        # wait a little bit before the first check
        await asyncio.sleep(3)
        result = await self.cluster_health(es, params={
            "index": idx,
            "retries": sys.maxsize,
            "request-params": {
                "wait_for_no_relocating_shards": "true"
            }
        })
        if not result["success"]:
            raise exceptions.RallyAssertionError("Failed to wait for [{}].".format(description))

    async def __call__(self, es, params):
        source_index = mandatory(params, "source-index", self)
        target_index = mandatory(params, "target-index", self)
        # we need to inject additional settings so we better copy the body
        target_body = deepcopy(mandatory(params, "target-body", self))
        shrink_node = params.get("shrink-node")
        # Choose a random data node if none is specified
        if not shrink_node:
            node_names = []
            # choose a random data node
            node_info = await es.nodes.info()
            for node in node_info["nodes"].values():
                if "data" in node["roles"]:
                    node_names.append(node["name"])
            if not node_names:
                raise exceptions.RallyAssertionError("Could not choose a suitable shrink-node automatically. Please specify it explicitly.")
            shrink_node = random.choice(node_names)
        self.logger.info("Using [%s] as shrink node.", shrink_node)
        self.logger.info("Preparing [%s] for shrinking.", source_index)
        # prepare index for shrinking
        await es.indices.put_settings(index=source_index,
                                      body={
                                          "settings": {
                                              "index.routing.allocation.require._name": shrink_node,
                                              "index.blocks.write": "true"
                                          }
                                      },
                                      preserve_existing=True)

        self.logger.info("Waiting for relocation to finish for index [%s]...", source_index)
        await self._wait_for(es, source_index, "shard relocation for index [{}]".format(source_index))
        self.logger.info("Shrinking [%s] to [%s].", source_index, target_index)
        if "settings" not in target_body:
            target_body["settings"] = {}
        target_body["settings"]["index.routing.allocation.require._name"] = None
        target_body["settings"]["index.blocks.write"] = None
        # kick off the shrink operation
        await es.indices.shrink(index=source_index, target=target_index, body=target_body)

        self.logger.info("Waiting for shrink to finish for index [%s]...", source_index)
        await self._wait_for(es, target_index, "shrink for index [{}]".format(target_index))
        self.logger.info("Shrinking [%s] to [%s] has finished.", source_index, target_index)
        # ops_count is not really important for this operation...
        return 1, "ops"

    def __repr__(self, *args, **kwargs):
        return "shrink-index"


class CreateMlDatafeed(Runner):
    """
    Execute the `create datafeed API <https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-put-datafeed.html>`_.
    """

    async def __call__(self, es, params):
        import elasticsearch
        datafeed_id = mandatory(params, "datafeed-id", self)
        body = mandatory(params, "body", self)
        try:
            await es.xpack.ml.put_datafeed(datafeed_id=datafeed_id, body=body)
        except elasticsearch.TransportError as e:
            # fallback to old path
            if e.status_code == 400:
                await es.transport.perform_request(
                    "PUT",
                    f"/_xpack/ml/datafeeds/{datafeed_id}",
                    body=body,
                )
            else:
                raise e

    def __repr__(self, *args, **kwargs):
        return "create-ml-datafeed"


class DeleteMlDatafeed(Runner):
    """
    Execute the `delete datafeed API <https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-datafeed.html>`_.
    """

    async def __call__(self, es, params):
        import elasticsearch
        datafeed_id = mandatory(params, "datafeed-id", self)
        force = params.get("force", False)
        try:
            # we don't want to fail if a datafeed does not exist, thus we ignore 404s.
            await es.xpack.ml.delete_datafeed(datafeed_id=datafeed_id, force=force, ignore=[404])
        except elasticsearch.TransportError as e:
            # fallback to old path (ES < 7)
            if e.status_code == 400:
                await es.transport.perform_request(
                    "DELETE",
                    f"/_xpack/ml/datafeeds/{datafeed_id}",
                    params={
                        "force": escape(force),
                        "ignore": 404
                    },
                )
            else:
                raise e

    def __repr__(self, *args, **kwargs):
        return "delete-ml-datafeed"


class StartMlDatafeed(Runner):
    """
    Execute the `start datafeed API <https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-start-datafeed.html>`_.
    """

    async def __call__(self, es, params):
        import elasticsearch
        datafeed_id = mandatory(params, "datafeed-id", self)
        body = params.get("body")
        start = params.get("start")
        end = params.get("end")
        timeout = params.get("timeout")
        try:
            await es.xpack.ml.start_datafeed(datafeed_id=datafeed_id, body=body, start=start, end=end, timeout=timeout)
        except elasticsearch.TransportError as e:
            # fallback to old path (ES < 7)
            if e.status_code == 400:
                await es.transport.perform_request(
                    "POST",
                    f"/_xpack/ml/datafeeds/{datafeed_id}/_start",
                    body=body,
                )
            else:
                raise e

    def __repr__(self, *args, **kwargs):
        return "start-ml-datafeed"


class StopMlDatafeed(Runner):
    """
    Execute the `stop datafeed API <https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-stop-datafeed.html>`_.
    """

    async def __call__(self, es, params):
        import elasticsearch
        datafeed_id = mandatory(params, "datafeed-id", self)
        force = params.get("force", False)
        timeout = params.get("timeout")
        try:
            await es.xpack.ml.stop_datafeed(datafeed_id=datafeed_id, force=force, timeout=timeout)
        except elasticsearch.TransportError as e:
            # fallback to old path (ES < 7)
            if e.status_code == 400:
                request_params = {
                    "force": escape(force),
                }
                if timeout:
                    request_params["timeout"] = escape(timeout)
                await es.transport.perform_request(
                    "POST",
                    f"/_xpack/ml/datafeeds/{datafeed_id}/_stop",
                    params=request_params
                )
            else:
                raise e

    def __repr__(self, *args, **kwargs):
        return "stop-ml-datafeed"


class CreateMlJob(Runner):
    """
    Execute the `create job API <https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-put-job.html>`_.
    """

    async def __call__(self, es, params):
        import elasticsearch
        job_id = mandatory(params, "job-id", self)
        body = mandatory(params, "body", self)
        try:
            await es.xpack.ml.put_job(job_id=job_id, body=body)
        except elasticsearch.TransportError as e:
            # fallback to old path (ES < 7)
            if e.status_code == 400:
                await es.transport.perform_request(
                    "PUT",
                    f"/_xpack/ml/anomaly_detectors/{job_id}",
                    body=body,
                )
            else:
                raise e

    def __repr__(self, *args, **kwargs):
        return "create-ml-job"


class DeleteMlJob(Runner):
    """
    Execute the `delete job API <https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-delete-job.html>`_.
    """

    async def __call__(self, es, params):
        import elasticsearch
        job_id = mandatory(params, "job-id", self)
        force = params.get("force", False)
        # we don't want to fail if a job does not exist, thus we ignore 404s.
        try:
            await es.xpack.ml.delete_job(job_id=job_id, force=force, ignore=[404])
        except elasticsearch.TransportError as e:
            # fallback to old path (ES < 7)
            if e.status_code == 400:
                await es.transport.perform_request(
                    "DELETE",
                    f"/_xpack/ml/anomaly_detectors/{job_id}",
                    params={
                        "force": escape(force),
                        "ignore": 404
                    },
                )
            else:
                raise e

    def __repr__(self, *args, **kwargs):
        return "delete-ml-job"


class OpenMlJob(Runner):
    """
    Execute the `open job API <https://www.elastic.co/guide/en/elasticsearch/reference/current/ml-open-job.html>`_.
    """

    async def __call__(self, es, params):
        import elasticsearch
        job_id = mandatory(params, "job-id", self)
        try:
            await es.xpack.ml.open_job(job_id=job_id)
        except elasticsearch.TransportError as e:
            # fallback to old path (ES < 7)
            if e.status_code == 400:
                await es.transport.perform_request(
                    "POST",
                    f"/_xpack/ml/anomaly_detectors/{job_id}/_open",
                )
            else:
                raise e

    def __repr__(self, *args, **kwargs):
        return "open-ml-job"


class CloseMlJob(Runner):
    """
    Execute the `close job API <http://www.elastic.co/guide/en/elasticsearch/reference/current/ml-close-job.html>`_.
    """

    async def __call__(self, es, params):
        import elasticsearch
        job_id = mandatory(params, "job-id", self)
        force = params.get("force", False)
        timeout = params.get("timeout")
        try:
            await es.xpack.ml.close_job(job_id=job_id, force=force, timeout=timeout)
        except elasticsearch.TransportError as e:
            # fallback to old path (ES < 7)
            if e.status_code == 400:
                request_params = {
                    "force": escape(force),
                }
                if timeout:
                    request_params["timeout"] = escape(timeout)

                await es.transport.perform_request(
                    "POST",
                    f"/_xpack/ml/anomaly_detectors/{job_id}/_close",
                    params=request_params,
                )
            else:
                raise e

    def __repr__(self, *args, **kwargs):
        return "close-ml-job"


class RawRequest(Runner):
    async def __call__(self, es, params):
        request_params = {}
        if "ignore" in params:
            request_params["ignore"] = params["ignore"]
        request_params.update(params.get("request-params", {}))

        await es.transport.perform_request(method=params.get("method", "GET"),
                                           url=mandatory(params, "path", self),
                                           headers=params.get("headers"),
                                           body=params.get("body"),
                                           params=request_params)

    def __repr__(self, *args, **kwargs):
        return "raw-request"


class Sleep(Runner):
    """
    Sleeps for the specified duration not issuing any request.
    """

    async def __call__(self, es, params):
        await asyncio.sleep(mandatory(params, "duration", "sleep"))

    def __repr__(self, *args, **kwargs):
        return "sleep"


class DeleteSnapshotRepository(Runner):
    """
    Deletes a snapshot repository
    """
    async def __call__(self, es, params):
        await es.snapshot.delete_repository(repository=mandatory(params, "repository", repr(self)))

    def __repr__(self, *args, **kwargs):
        return "delete-snapshot-repository"


class CreateSnapshotRepository(Runner):
    """
    Creates a new snapshot repository
    """
    async def __call__(self, es, params):
        request_params = params.get("request-params", {})
        await es.snapshot.create_repository(repository=mandatory(params, "repository", repr(self)),
                                            body=mandatory(params, "body", repr(self)),
                                            params=request_params)

    def __repr__(self, *args, **kwargs):
        return "create-snapshot-repository"


class CreateSnapshot(Runner):
    """
    Creates a new snapshot repository
    """
    async def __call__(self, es, params):
        request_params = params.get("request-params", {})
        wait_for_completion = params.get("wait-for-completion", False)
        repository = mandatory(params, "repository", repr(self))
        snapshot = mandatory(params, "snapshot", repr(self))
        body = mandatory(params, "body", repr(self))
        response = await es.snapshot.create(repository=repository,
                                            snapshot=snapshot,
                                            body=body,
                                            params=request_params,
                                            wait_for_completion=wait_for_completion)

        # We can derive a more useful throughput metric if the snapshot has successfully completed
        if wait_for_completion and response.get("snapshot", {}).get("state") == "SUCCESS":
            stats_response = await es.snapshot.status(repository=repository,
                                                      snapshot=snapshot)
            size = stats_response["snapshots"][0]["stats"]["total"]["size_in_bytes"]
            # while the actual service time as determined by Rally should be pretty accurate, the actual time it took
            # to restore allows for a correct calculation of achieved throughput.
            time_in_millis = stats_response["snapshots"][0]["stats"]["time_in_millis"]
            time_in_seconds = time_in_millis / 1000
            return {
                "weight": size,
                "unit": "byte",
                "success": True,
                "service_time": time_in_seconds,
                "time_period": time_in_seconds
            }

    def __repr__(self, *args, **kwargs):
        return "create-snapshot"


class RestoreSnapshot(Runner):
    """
    Restores a snapshot from an already registered repository
    """
    async def __call__(self, es, params):
        request_params = params.get("request-params", {})
        await es.snapshot.restore(repository=mandatory(params, "repository", repr(self)),
                                  snapshot=mandatory(params, "snapshot", repr(self)),
                                  body=params.get("body"),
                                  wait_for_completion=params.get("wait-for-completion", False),
                                  params=request_params)

    def __repr__(self, *args, **kwargs):
        return "restore-snapshot"


class IndicesRecovery(Runner):
    async def __call__(self, es, params):
        index = mandatory(params, "index", repr(self))
        wait_period = params.get("completion-recheck-wait-period", 1)

        all_shards_done = False
        total_recovered = 0
        total_start_millis = sys.maxsize
        total_end_millis = 0

        # wait until recovery is done
        # The nesting level is ok here given the structure of the API response
        # pylint: disable=too-many-nested-blocks
        while not all_shards_done:
            response = await es.indices.recovery(index=index)
            # This might happen if we happen to call the API before the next recovery is scheduled.
            if not response:
                self.logger.debug("Empty index recovery response for [%s].", index)
            else:
                # check whether all shards are done
                all_shards_done = True
                total_recovered = 0
                total_start_millis = sys.maxsize
                total_end_millis = 0
                for _, idx_data in response.items():
                    for _, shard_data in idx_data.items():
                        for shard in shard_data:
                            current_shard_done = shard["stage"] == "DONE"
                            all_shards_done = all_shards_done and current_shard_done
                            if current_shard_done:
                                total_start_millis = min(total_start_millis, shard["start_time_in_millis"])
                                total_end_millis = max(total_end_millis, shard["stop_time_in_millis"])
                                idx_size = shard["index"]["size"]
                                total_recovered += idx_size["recovered_in_bytes"]
                self.logger.debug("All shards done for [%s]: [%s].", index, all_shards_done)

            if not all_shards_done:
                await asyncio.sleep(wait_period)

        response_time_in_seconds = (total_end_millis - total_start_millis) / 1000
        return {
            "weight": total_recovered,
            "unit": "byte",
            "success": True,
            "throughput": total_recovered / response_time_in_seconds,
            "start_time_millis": total_start_millis,
            "stop_time_millis": total_end_millis
        }

    def __repr__(self, *args, **kwargs):
        return "wait-for-recovery"


class PutSettings(Runner):
    """
    Updates cluster settings with the
    `cluster settings API <http://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-update-settings.html>_.
    """
    async def __call__(self, es, params):
        await es.cluster.put_settings(body=mandatory(params, "body", repr(self)))

    def __repr__(self, *args, **kwargs):
        return "put-settings"


class CreateTransform(Runner):
    """
    Execute the `create transform API https://www.elastic.co/guide/en/elasticsearch/reference/current/put-transform.html`_.
    """

    async def __call__(self, es, params):
        transform_id = mandatory(params, "transform-id", self)
        body = mandatory(params, "body", self)
        defer_validation = params.get("defer-validation", False)
        await es.transform.put_transform(transform_id=transform_id, body=body, defer_validation=defer_validation)

    def __repr__(self, *args, **kwargs):
        return "create-transform"


class StartTransform(Runner):
    """
    Execute the `start transform API
    https://www.elastic.co/guide/en/elasticsearch/reference/current/start-transform.html`_.
    """

    async def __call__(self, es, params):
        transform_id = mandatory(params, "transform-id", self)
        timeout = params.get("timeout")

        await es.transform.start_transform(transform_id=transform_id, timeout=timeout)

    def __repr__(self, *args, **kwargs):
        return "start-transform"


class WaitForTransform(Runner):
    """
    Wait for the transform until it reaches a certain checkpoint.
    """

    def __init__(self):
        super().__init__()
        self._completed = False
        self._percent_completed = 0.0
        self._start_time = None
        self._last_documents_processed = 0
        self._last_processing_time = 0

    @property
    def completed(self):
        return self._completed

    @property
    def percent_completed(self):
        return self._percent_completed

    async def __call__(self, es, params):
        """
        stop the transform and wait until transform has finished return stats

        :param es: The Elasticsearch client.
        :param params: A hash with all parameters. See below for details.
        :return: A hash with stats from the run.

        Different to the `stop transform API
        https://www.elastic.co/guide/en/elasticsearch/reference/current/stop-transform.html`_ this command will wait
        until the transform is stopped and a checkpoint has been reached.

        It expects a parameter dict with the following mandatory keys:

        * ``transform-id``: the transform id to start, the transform must have been created upfront.

        The following keys are optional:
        * ``force``: forcefully stop a transform, default false
        * ``wait-for-checkpoint``: whether to wait until all data has been processed till the next checkpoint, default true
        * ``wait-for-completion``: whether to block until the transform has stopped, default true
        * ``transform-timeout``: overall runtime timeout of the transform in seconds, default 3600 (1h)
        * ``poll-interval``: how often transform stats are polled, used to set progress and check the state, default 0.5.
        """
        import time

        transform_id = mandatory(params, "transform-id", self)
        force = params.get("force", False)
        timeout = params.get("timeout")
        wait_for_completion = params.get("wait-for-completion", True)
        wait_for_checkpoint = params.get("wait-for-checkpoint", True)
        transform_timeout = params.get("transform-timeout", 60.0 * 60.0)
        poll_interval = params.get("poll-interval", 0.5)

        if not self._start_time:
            self._start_time = time.monotonic()
            await es.transform.stop_transform(transform_id=transform_id,
                                              force=force,
                                              timeout=timeout,
                                              wait_for_completion=False,
                                              wait_for_checkpoint=wait_for_checkpoint)

        while True:
            stats_response = await es.transform.get_transform_stats(transform_id=transform_id)
            state = stats_response["transforms"][0].get("state")
            transform_stats = stats_response["transforms"][0].get("stats", {})

            if (time.monotonic() - self._start_time) > transform_timeout:
                raise exceptions.RallyAssertionError(
                    f"Transform [{transform_id}] timed out after [{transform_timeout}] seconds. "
                    "Please consider increasing the timeout in the track.")

            if state == "failed":
                failure_reason = stats_response["transforms"][0].get("reason", "unknown")
                raise exceptions.RallyAssertionError(
                    f"Transform [{transform_id}] failed with [{failure_reason}].")
            elif state == "stopped" or wait_for_completion is False:
                self._completed = True
                self._percent_completed = 1.0
            else:
                self._percent_completed = stats_response["transforms"][0].get("checkpointing", {}).get("next", {}).get(
                    "checkpoint_progress", {}).get("percent_complete", 0.0) / 100.0

            documents_processed = transform_stats.get("documents_processed", 0)
            processing_time = transform_stats.get("search_time_in_ms", 0)
            processing_time += transform_stats.get("processing_time_in_ms", 0)
            processing_time += transform_stats.get("index_time_in_ms", 0)
            documents_processed_delta = documents_processed - self._last_documents_processed
            processing_time_delta = processing_time - self._last_processing_time

            # only report if we have enough data or transform has completed
            if self._completed or (documents_processed_delta > 5000 and processing_time_delta > 500):
                stats = {
                    "transform-id": transform_id,
                    "weight": transform_stats.get("documents_processed", 0),
                    "unit": "docs",
                }

                throughput = 0
                if self._completed:
                    # take the overall throughput
                    if processing_time > 0:
                        throughput = documents_processed / processing_time * 1000
                elif processing_time_delta > 0:
                    throughput = documents_processed_delta / processing_time_delta * 1000

                stats["throughput"] = throughput

                self._last_documents_processed = documents_processed
                self._last_processing_time = processing_time
                return stats
            else:
                # sleep for a while, so stats is not called to often
                await asyncio.sleep(poll_interval)

    def __repr__(self, *args, **kwargs):
        return "wait-for-transform"


class DeleteTransform(Runner):
    """
    Execute the `delete transform API
    https://www.elastic.co/guide/en/elasticsearch/reference/current/delete-transform.html`_.
    """

    async def __call__(self, es, params):
        transform_id = mandatory(params, "transform-id", self)
        force = params.get("force", False)
        # we don't want to fail if a job does not exist, thus we ignore 404s.
        await es.transform.delete_transform(transform_id=transform_id, force=force, ignore=[404])

    def __repr__(self, *args, **kwargs):
        return "delete-transform"


# TODO: Allow to use this from (selected) regular runners and add user documentation.
# TODO: It would maybe be interesting to add meta-data on how many retries there were.
class Retry(Runner, Delegator):
    """
    This runner can be used as a wrapper around regular runners to retry operations.

    It defines the following parameters:

    * ``retries`` (optional, default 0): The number of times the operation is retried.
    * ``retry-until-success`` (optional, default False): Retries until the delegate returns a success. This will also
                              forcibly set ``retry-on-error`` to ``True``.
    * ``retry-wait-period`` (optional, default 0.5): The time in seconds to wait after an error.
    * ``retry-on-timeout`` (optional, default True): Whether to retry on connection timeout.
    * ``retry-on-error`` (optional, default False): Whether to retry on failure (i.e. the delegate
                         returns ``success == False``)
    """

    def __init__(self, delegate):
        super().__init__(delegate=delegate)

    async def __aenter__(self):
        await self.delegate.__aenter__()
        return self

    async def __call__(self, es, params):
        import elasticsearch
        import socket

        retry_until_success = params.get("retry-until-success", False)
        if retry_until_success:
            max_attempts = sys.maxsize
            retry_on_error = True
        else:
            max_attempts = params.get("retries", 0) + 1
            retry_on_error = params.get("retry-on-error", False)
        sleep_time = params.get("retry-wait-period", 0.5)
        retry_on_timeout = params.get("retry-on-timeout", True)

        for attempt in range(max_attempts):
            last_attempt = attempt + 1 == max_attempts
            try:
                return_value = await self.delegate(es, params)
                if last_attempt or not retry_on_error:
                    return return_value
                # we can determine success if and only if the runner returns a dict. Otherwise, we have to assume it was fine.
                elif isinstance(return_value, dict):
                    if return_value.get("success", True):
                        self.logger.debug("%s has returned successfully", repr(self.delegate))
                        return return_value
                    else:
                        self.logger.debug("%s has returned with an error: %s.", repr(self.delegate), return_value)
                        await asyncio.sleep(sleep_time)
                else:
                    return return_value
            except (socket.timeout, elasticsearch.exceptions.ConnectionError):
                if last_attempt or not retry_on_timeout:
                    raise
                else:
                    await asyncio.sleep(sleep_time)
            except elasticsearch.exceptions.TransportError as e:
                if last_attempt or not retry_on_timeout:
                    raise e
                elif e.status_code == 408:
                    self.logger.debug("%s has timed out.", repr(self.delegate))
                    await asyncio.sleep(sleep_time)
                else:
                    raise e

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self.delegate.__aexit__(exc_type, exc_val, exc_tb)

    def __repr__(self, *args, **kwargs):
        return "retryable %s" % repr(self.delegate)
