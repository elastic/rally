# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import collections
import inspect
import logging
import math
import numbers
import operator
import random
import time
from abc import ABC
from enum import Enum
from typing import Callable, Deque

from esrally import exceptions
from esrally.track import track
from esrally.utils import io

__PARAM_SOURCES_BY_OP: dict[track.OperationType, ParamSource] = {}
__PARAM_SOURCES_BY_NAME: dict[str, ParamSource] = {}


def param_source_for_operation(op_type, track, params, task_name):
    try:
        # we know that this can only be a Rally core parameter source
        return __PARAM_SOURCES_BY_OP[op_type](track, params, operation_name=task_name)
    except KeyError:
        return ParamSource(track, params, operation_name=task_name)


def param_source_for_name(name, track, params):
    param_source = __PARAM_SOURCES_BY_NAME[name]

    if inspect.isfunction(param_source):
        return DelegatingParamSource(track, params, param_source)
    else:
        return param_source(track, params)


def ensure_valid_param_source(param_source):
    if not inspect.isfunction(param_source) and not inspect.isclass(param_source):
        raise exceptions.RallyAssertionError(f"Parameter source [{param_source}] must be either a function or a class.")


def register_param_source_for_operation(op_type, param_source_class):
    ensure_valid_param_source(param_source_class)
    __PARAM_SOURCES_BY_OP[op_type.to_hyphenated_string()] = param_source_class


def register_param_source_for_name(name, param_source_class):
    ensure_valid_param_source(param_source_class)
    __PARAM_SOURCES_BY_NAME[name] = param_source_class


# only intended for tests
def _unregister_param_source_for_name(name):
    # We intentionally do not specify a default value if the key does not exist. If we try to remove a key that we didn't insert then
    # something is fishy with the test and we'd rather know early.
    __PARAM_SOURCES_BY_NAME.pop(name)


# Default
class ParamSource:
    """
    A `ParamSource` captures the parameters for a given operation. Rally will create one global ParamSource for each operation and will then
     invoke `#partition()` to get a `ParamSource` instance for each client. During the benchmark, `#params()` will be called repeatedly
     before Rally invokes the corresponding runner (that will actually execute the operation against Elasticsearch).
    """

    def __init__(self, track, params, **kwargs):
        """
        Creates a new ParamSource instance.

        :param track:  The current track definition
        :param params: A hash of all parameters that have been extracted for this operation.
        """
        self.track = track
        self._params = params
        self.kwargs = kwargs

    def partition(self, partition_index, total_partitions):
        """
        This method will be invoked by Rally at the beginning of the lifecycle. It splits a parameter source per client. If the
        corresponding operation is idempotent, return `self` (e.g. for queries). If the corresponding operation has side-effects and it
        matters which client executes which part (e.g. an index operation from a source file), return the relevant part.

        Do NOT assume that you can share state between ParamSource objects in different partitions (technical explanation: each client
        will be a dedicated process, so each object of a `ParamSource` lives in its own process and hence cannot share state with other
        instances).

        :param partition_index: The current partition for which a parameter source is needed. It is in the range [0, `total_partitions`).
        :param total_partitions: The total number of partitions (i.e. clients).
        :return: A parameter source for the current partition.
        """
        return self

    @property
    def infinite(self):
        # for bwc
        return self.size() is None

    # Deprecated
    def size(self):
        """
        Rally has two modes in which it can run:

        * It will either run an operation for a pre-determined number of times or
        * It can run until the parameter source is exhausted.

        In the former case, you should determine the number of times that `#params()` will be invoked. With that number, Rally can show
        the progress made so far to the user. In the latter case, return ``None``.

        :return:  The "size" of this parameter source or ``None`` if should run eternally.
        """
        return None

    def params(self):
        """
        :return: A hash containing the parameters that will be provided to the corresponding operation runner (key: parameter name,
        value: parameter value).
        """
        return self._params

    def _client_params(self):
        """
        For use when a ParamSource does not propagate self._params but does use elasticsearch client under the hood

        :return: all applicable parameters that are global to Rally and apply to the elasticsearch-py client
        """
        return {
            "request-timeout": self._params.get("request-timeout"),
            "headers": self._params.get("headers"),
            "opaque-id": self._params.get("opaque-id"),
        }


class DelegatingParamSource(ParamSource):
    def __init__(self, track, params, delegate, **kwargs):
        super().__init__(track, params, **kwargs)
        self.delegate = delegate

    def params(self):
        return self.delegate(self.track, self._params, **self.kwargs)


class SleepParamSource(ParamSource):
    def __init__(self, track, params, **kwargs):
        super().__init__(track, params, **kwargs)
        try:
            duration = params["duration"]
        except KeyError:
            raise exceptions.InvalidSyntax("parameter 'duration' is mandatory for sleep operation")

        if not isinstance(duration, numbers.Number):
            raise exceptions.InvalidSyntax("parameter 'duration' for sleep operation must be a number")
        if duration < 0:
            raise exceptions.InvalidSyntax(f"parameter 'duration' must be non-negative but was {duration}")

    def params(self):
        return dict(self._params)


class CreateIndexParamSource(ParamSource):
    def __init__(self, track, params, **kwargs):
        super().__init__(track, params, **kwargs)
        self.request_params = params.get("request-params", {})
        self.index_definitions = []
        if track.indices:
            filter_idx = params.get("index")
            if isinstance(filter_idx, str):
                filter_idx = [filter_idx]
            settings = params.get("settings")
            for idx in track.indices:
                if not filter_idx or idx.name in filter_idx:
                    body = idx.body
                    if body and settings:
                        if "settings" in body:
                            # merge (and potentially override)
                            body["settings"].update(settings)
                        else:
                            body["settings"] = settings
                    elif not body and settings:
                        body = {"settings": settings}

                    self.index_definitions.append((idx.name, body))
        else:
            try:
                # only 'index' is mandatory, the body is optional (may be ok to create an index without a body)
                idx = params["index"]
                body = params.get("body")
                if isinstance(idx, str):
                    idx = [idx]
                for i in idx:
                    self.index_definitions.append((i, body))
            except KeyError:
                raise exceptions.InvalidSyntax("Please set the property 'index' for the create-index operation")

    def params(self):
        p = {}
        # ensure we pass all parameters...
        p.update(self._params)
        p.update(
            {
                "indices": self.index_definitions,
                "request-params": self.request_params,
            }
        )
        return p


class CreateDataStreamParamSource(ParamSource):
    def __init__(self, track, params, **kwargs):
        super().__init__(track, params, **kwargs)
        self.request_params = params.get("request-params", {})
        self.data_stream_definitions = []
        if track.data_streams:
            filter_ds = params.get("data-stream")
            if isinstance(filter_ds, str):
                filter_ds = [filter_ds]
            for ds in track.data_streams:
                if not filter_ds or ds.name in filter_ds:
                    self.data_stream_definitions.append(ds.name)
        else:
            try:
                data_stream = params["data-stream"]
                data_streams = [data_stream] if isinstance(data_stream, str) else data_stream
                for ds in data_streams:
                    self.data_stream_definitions.append(ds)
            except KeyError:
                raise exceptions.InvalidSyntax("Please set the property 'data-stream' for the create-data-stream operation")

    def params(self):
        p = {}
        # ensure we pass all parameters...
        p.update(self._params)
        p.update(
            {
                "data-streams": self.data_stream_definitions,
                "request-params": self.request_params,
            }
        )
        return p


class DeleteDataStreamParamSource(ParamSource):
    def __init__(self, track, params, **kwargs):
        super().__init__(track, params, **kwargs)
        self.request_params = params.get("request-params", {})
        self.only_if_exists = params.get("only-if-exists", True)

        self.data_stream_definitions = []
        target_data_stream = params.get("data-stream")
        if target_data_stream:
            target_data_stream = [target_data_stream] if isinstance(target_data_stream, str) else target_data_stream
            for ds in target_data_stream:
                self.data_stream_definitions.append(ds)
        elif track.data_streams:
            for ds in track.data_streams:
                self.data_stream_definitions.append(ds.name)
        else:
            raise exceptions.InvalidSyntax("delete-data-stream operation targets no data stream")

    def params(self):
        p = {}
        # ensure we pass all parameters...
        p.update(self._params)
        p.update(
            {"data-streams": self.data_stream_definitions, "request-params": self.request_params, "only-if-exists": self.only_if_exists}
        )
        return p


class DeleteIndexParamSource(ParamSource):
    def __init__(self, track, params, **kwargs):
        super().__init__(track, params, **kwargs)
        self.request_params = params.get("request-params", {})
        self.only_if_exists = params.get("only-if-exists", True)

        self.index_definitions = []
        target_index = params.get("index")
        if target_index:
            if isinstance(target_index, str):
                target_index = [target_index]
            for idx in target_index:
                self.index_definitions.append(idx)
        elif track.indices:
            for idx in track.indices:
                self.index_definitions.append(idx.name)
        else:
            raise exceptions.InvalidSyntax("delete-index operation targets no index")

    def params(self):
        p = {}
        # ensure we pass all parameters...
        p.update(self._params)
        p.update(
            {
                "indices": self.index_definitions,
                "request-params": self.request_params,
                "only-if-exists": self.only_if_exists,
            }
        )
        return p


class CreateIndexTemplateParamSource(ParamSource):
    def __init__(self, track, params, **kwargs):
        super().__init__(track, params, **kwargs)
        self.request_params = params.get("request-params", {})
        self.template_definitions = []
        if track.templates:
            filter_template = params.get("template")
            settings = params.get("settings")
            for template in track.templates:
                if not filter_template or template.name == filter_template:
                    body = template.content
                    if body and settings:
                        if "settings" in body:
                            # merge (and potentially override)
                            body["settings"].update(settings)
                        else:
                            body["settings"] = settings

                    self.template_definitions.append((template.name, body))
        else:
            try:
                self.template_definitions.append((params["template"], params["body"]))
            except KeyError:
                raise exceptions.InvalidSyntax("Please set the properties 'template' and 'body' for the create-index-template operation")

    def params(self):
        p = {}
        # ensure we pass all parameters...
        p.update(self._params)
        p.update(
            {
                "templates": self.template_definitions,
                "request-params": self.request_params,
            }
        )
        return p


class DeleteTemplateParamSource(ABC, ParamSource):
    def __init__(self, track, params, templates, **kwargs):
        super().__init__(track, params, **kwargs)
        self.only_if_exists = params.get("only-if-exists", True)
        self.request_params = params.get("request-params", {})
        self.template_definitions = []
        if templates:
            filter_template = params.get("template")
            for template in templates:
                if not filter_template or template.name == filter_template:
                    self.template_definitions.append((template.name, template.delete_matching_indices, template.pattern))
        else:
            try:
                template = params["template"]
            except KeyError:
                raise exceptions.InvalidSyntax(f"Please set the property 'template' for the {params.get('operation-type')} operation")

            delete_matching = params.get("delete-matching-indices", False)
            try:
                index_pattern = params["index-pattern"] if delete_matching else None
            except KeyError:
                raise exceptions.InvalidSyntax(
                    "The property 'index-pattern' is required for delete-index-template if 'delete-matching-indices' is true."
                )
            self.template_definitions.append((template, delete_matching, index_pattern))

    def params(self):
        p = {}
        # ensure we pass all parameters...
        p.update(self._params)
        p.update(
            {
                "templates": self.template_definitions,
                "only-if-exists": self.only_if_exists,
                "request-params": self.request_params,
            }
        )
        return p


class DeleteIndexTemplateParamSource(DeleteTemplateParamSource):
    def __init__(self, track, params, **kwargs):
        super().__init__(track, params, track.templates, **kwargs)


class DeleteComposableTemplateParamSource(DeleteTemplateParamSource):
    def __init__(self, track, params, **kwargs):
        super().__init__(track, params, track.composable_templates, **kwargs)


class DeleteComponentTemplateParamSource(ParamSource):
    def __init__(self, track, params, **kwargs):
        super().__init__(track, params, **kwargs)
        self.only_if_exists = params.get("only-if-exists", True)
        self.request_params = params.get("request-params", {})
        self.template_definitions = []
        if track.component_templates:
            filter_template = params.get("template")
            for template in track.component_templates:
                if not filter_template or template.name == filter_template:
                    self.template_definitions.append(template.name)
        else:
            try:
                template = params["template"]
                self.template_definitions.append(template)
            except KeyError:
                raise exceptions.InvalidSyntax(f"Please set the property 'template' for the {params.get('operation-type')} operation.")

    def params(self):
        return {
            "templates": self.template_definitions,
            "only-if-exists": self.only_if_exists,
            "request-params": self.request_params,
        }


class CreateTemplateParamSource(ABC, ParamSource):
    def __init__(self, track, params, templates, **kwargs):
        super().__init__(track, params, **kwargs)
        self.request_params = params.get("request-params", {})
        self.template_definitions = []
        if "template" in params and "body" in params:
            self.template_definitions.append((params["template"], params["body"]))
        elif templates:
            filter_template = params.get("template")
            settings = params.get("settings")
            template_definitions = []
            for template in templates:
                if not filter_template or template.name == filter_template:
                    body = self._create_or_merge(template.content, ["template", "settings"], settings)
                    template_definitions.append((template.name, body))
            if filter_template and not template_definitions:
                template_names = ", ".join([template.name for template in templates])
                raise exceptions.InvalidSyntax(f"Unknown template: {filter_template}. Available templates: {template_names}.")
            self.template_definitions.extend(template_definitions)
        else:
            raise exceptions.InvalidSyntax(
                "Please set the properties 'template' and 'body' for the "
                f"{params.get('operation-type')} operation or declare composable and/or component "
                "templates in the track"
            )

    @staticmethod
    def _create_or_merge(content, path, new_content):
        original_content = content
        if new_content:
            for sub_path in path:
                if sub_path not in content:
                    content[sub_path] = {}
                content = content[sub_path]
            CreateTemplateParamSource.__merge(content, new_content)
        return original_content

    @staticmethod
    def __merge(dct, merge_dct):
        for k in merge_dct.keys():
            if k in dct and isinstance(dct[k], dict) and isinstance(merge_dct[k], collections.abc.Mapping):
                CreateTemplateParamSource.__merge(dct[k], merge_dct[k])
            else:
                dct[k] = merge_dct[k]

    def params(self):
        return {
            "templates": self.template_definitions,
            "request-params": self.request_params,
        }


class CreateComposableTemplateParamSource(CreateTemplateParamSource):
    def __init__(self, track, params, **kwargs):
        super().__init__(track, params, track.composable_templates, **kwargs)


class CreateComponentTemplateParamSource(CreateTemplateParamSource):
    def __init__(self, track, params, **kwargs):
        super().__init__(track, params, track.component_templates, **kwargs)


class SearchParamSource(ParamSource):
    def __init__(self, track, params, **kwargs):
        super().__init__(track, params, **kwargs)
        target_name = get_target(track, params)
        type_name = params.get("type")
        if params.get("data-stream") and type_name:
            raise exceptions.InvalidSyntax(f"'type' not supported with 'data-stream' for operation '{kwargs.get('operation_name')}'")
        request_cache = params.get("cache", None)
        detailed_results = params.get("detailed-results", False)
        query_body = params.get("body", None)
        pages = params.get("pages", None)
        results_per_page = params.get("results-per-page", None)
        request_params = params.get("request-params", {})
        response_compression_enabled = params.get("response-compression-enabled", True)
        with_point_in_time_from = params.get("with-point-in-time-from", None)

        self.query_params = {
            "index": target_name,
            "type": type_name,
            "cache": request_cache,
            "detailed-results": detailed_results,
            "request-params": request_params,
            "response-compression-enabled": response_compression_enabled,
            "body": query_body,
        }

        if not target_name:
            raise exceptions.InvalidSyntax(
                f"'index' or 'data-stream' is mandatory and is missing for operation '{kwargs.get('operation_name')}'"
            )

        if pages:
            self.query_params["pages"] = pages
        if results_per_page:
            self.query_params["results-per-page"] = results_per_page
        if with_point_in_time_from:
            self.query_params["with-point-in-time-from"] = with_point_in_time_from
        if "assertions" in params:
            if not detailed_results:
                # for paginated queries the value does not matter because detailed results are always retrieved.
                is_paginated = bool(pages)
                if not is_paginated:
                    raise exceptions.InvalidSyntax("The property [detailed-results] must be [true] if assertions are defined")
            self.query_params["assertions"] = params["assertions"]

        # Ensure we pass global parameters
        self.query_params.update(self._client_params())

    def params(self):
        return self.query_params


class IndexIdConflict(Enum):
    """
    Determines which id conflicts to simulate during indexing.

    * NoConflicts: Produce no id conflicts
    * SequentialConflicts: A document id is replaced with a document id with a sequentially increasing id
    * RandomConflicts: A document id is replaced with a document id with a random other id

    Note that this assumes that each document in the benchmark corpus has an id between [1, size_of(corpus)]
    """

    NoConflicts = 0
    SequentialConflicts = 1
    RandomConflicts = 2


class BulkIndexParamSource(ParamSource):
    def __init__(self, track, params, **kwargs):
        super().__init__(track, params, **kwargs)
        id_conflicts = params.get("conflicts", None)
        if not id_conflicts:
            self.id_conflicts = IndexIdConflict.NoConflicts
        elif id_conflicts == "sequential":
            self.id_conflicts = IndexIdConflict.SequentialConflicts
        elif id_conflicts == "random":
            self.id_conflicts = IndexIdConflict.RandomConflicts
        else:
            raise exceptions.InvalidSyntax("Unknown 'conflicts' setting [%s]" % id_conflicts)

        if "data-streams" in params and self.id_conflicts != IndexIdConflict.NoConflicts:
            raise exceptions.InvalidSyntax("'conflicts' cannot be used with 'data-streams'")

        if self.id_conflicts != IndexIdConflict.NoConflicts:
            self.conflict_probability = self.float_param(
                params, name="conflict-probability", default_value=25, min_value=0, max_value=100, min_operator=operator.lt
            )
            self.on_conflict = params.get("on-conflict", "index")
            if self.on_conflict not in ["index", "update"]:
                raise exceptions.InvalidSyntax(f"Unknown 'on-conflict' setting [{self.on_conflict}]")
            self.recency = self.float_param(params, name="recency", default_value=0, min_value=0, max_value=1, min_operator=operator.lt)

        else:
            self.conflict_probability = None
            self.on_conflict = None
            self.recency = None

        self.corpora = self.used_corpora(track, params)

        if len(self.corpora) == 0:
            raise exceptions.InvalidSyntax(
                f"There is no document corpus definition for track {track}. You must add at "
                f"least one before making bulk requests to Elasticsearch."
            )

        for corpus in self.corpora:
            for document_set in corpus.documents:
                if document_set.includes_action_and_meta_data and self.id_conflicts != IndexIdConflict.NoConflicts:
                    file_name = document_set.document_archive if document_set.has_compressed_corpus() else document_set.document_file

                    raise exceptions.InvalidSyntax(
                        "Cannot generate id conflicts [%s] as [%s] in document corpus [%s] already contains an "
                        "action and meta-data line." % (id_conflicts, file_name, corpus)
                    )

        self.pipeline = params.get("pipeline", None)
        try:
            self.bulk_size = int(params["bulk-size"])
            if self.bulk_size <= 0:
                raise exceptions.InvalidSyntax("'bulk-size' must be positive but was %d" % self.bulk_size)
        except KeyError:
            raise exceptions.InvalidSyntax("Mandatory parameter 'bulk-size' is missing")
        except ValueError:
            raise exceptions.InvalidSyntax("'bulk-size' must be numeric")

        try:
            self.batch_size = int(params.get("batch-size", self.bulk_size))
            if self.batch_size <= 0:
                raise exceptions.InvalidSyntax("'batch-size' must be positive but was %d" % self.batch_size)
            if self.batch_size < self.bulk_size:
                raise exceptions.InvalidSyntax("'batch-size' must be greater than or equal to 'bulk-size'")
            if self.batch_size % self.bulk_size != 0:
                raise exceptions.InvalidSyntax("'batch-size' must be a multiple of 'bulk-size'")
        except ValueError:
            raise exceptions.InvalidSyntax("'batch-size' must be numeric")

        self.ingest_percentage = self.float_param(params, name="ingest-percentage", default_value=100, min_value=0, max_value=100)
        self.param_source = PartitionBulkIndexParamSource(
            self.corpora,
            self.batch_size,
            self.bulk_size,
            self.ingest_percentage,
            self.id_conflicts,
            self.conflict_probability,
            self.on_conflict,
            self.recency,
            self.pipeline,
            self._params,
        )

    def float_param(self, params, name, default_value, min_value, max_value, min_operator=operator.le):
        try:
            value = float(params.get(name, default_value))
            if min_operator(value, min_value) or value > max_value:
                interval_min = "(" if min_operator is operator.le else "["
                raise exceptions.InvalidSyntax(
                    f"'{name}' must be in the range {interval_min}{min_value:.1f}, {max_value:.1f}] but was {value:.1f}"
                )
            return value
        except ValueError:
            raise exceptions.InvalidSyntax(f"'{name}' must be numeric")

    def used_corpora(self, t, params):
        corpora = []
        track_corpora_names = [corpus.name for corpus in t.corpora]
        corpora_names = params.get("corpora", track_corpora_names)
        if isinstance(corpora_names, str):
            corpora_names = [corpora_names]

        for corpus in t.corpora:
            if corpus.name in corpora_names:
                filtered_corpus = corpus.filter(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    target_indices=params.get("indices"),
                    target_data_streams=params.get("data-streams"),
                )
                if filtered_corpus.number_of_documents(source_format=track.Documents.SOURCE_FORMAT_BULK) > 0:
                    corpora.append(filtered_corpus)

        # the track has corpora but none of them match
        if t.corpora and not corpora:
            raise exceptions.RallyAssertionError(
                "The provided corpus %s does not match any of the corpora %s." % (corpora_names, track_corpora_names)
            )

        return corpora

    def partition(self, partition_index, total_partitions):
        # register the new partition internally
        self.param_source.partition(partition_index, total_partitions)
        return self.param_source

    def params(self):
        raise exceptions.RallyError("Do not use a BulkIndexParamSource without partitioning")


class PartitionBulkIndexParamSource:
    def __init__(
        self,
        corpora,
        batch_size,
        bulk_size,
        ingest_percentage,
        id_conflicts,
        conflict_probability,
        on_conflict,
        recency,
        pipeline=None,
        original_params=None,
    ):
        """

        :param corpora: Specification of affected document corpora.
        :param batch_size: The number of documents to read in one go.
        :param bulk_size: The size of bulk index operations (number of documents per bulk).
        :param ingest_percentage: A number between (0.0, 100.0] that defines how much of the whole corpus should be ingested.
        :param id_conflicts: The type of id conflicts.
        :param conflict_probability: A number between (0.0, 100.0] that defines the probability that a document is replaced by another one.
        :param on_conflict: A string indicating which action should be taken on id conflicts (either "index" or "update").
        :param recency: A number between [0.0, 1.0] indicating whether to bias generation of conflicting ids towards more recent ones.
                        May be None.
        :param pipeline: The name of the ingest pipeline to run.
        :param original_params: The original dict passed to the parent parameter source.
        """
        self.corpora = corpora
        self.partitions = []
        self.total_partitions = None
        self.batch_size = batch_size
        self.bulk_size = bulk_size
        self.ingest_percentage = ingest_percentage
        self.id_conflicts = id_conflicts
        self.conflict_probability = conflict_probability
        self.on_conflict = on_conflict
        self.recency = recency
        self.pipeline = pipeline
        self.original_params = original_params
        # this is only intended for unit-testing
        self.create_reader = original_params.pop("__create_reader", create_default_reader)
        self.current_bulk = 0
        # use a value > 0 so percent_completed returns a sensible value
        self.total_bulks = 1
        self.infinite = False

    def partition(self, partition_index, total_partitions):
        if self.total_partitions is None:
            self.total_partitions = total_partitions
        elif self.total_partitions != total_partitions:
            raise exceptions.RallyAssertionError(
                f"Total partitions is expected to be [{self.total_partitions}] but was [{total_partitions}]"
            )
        self.partitions.append(partition_index)

    def params(self):
        if self.current_bulk == 0:
            self._init_internal_params()
        # self.internal_params always reads all files. This is necessary to ensure we terminate early in case
        # the user has specified ingest percentage.
        if self.current_bulk == self.total_bulks:
            raise StopIteration()
        self.current_bulk += 1
        return next(self.internal_params)

    def _init_internal_params(self):
        # contains a continuous range of client ids
        self.partitions = sorted(self.partitions)
        start_index = self.partitions[0]
        end_index = self.partitions[-1]

        self.internal_params = bulk_data_based(
            self.total_partitions,
            start_index,
            end_index,
            self.corpora,
            self.batch_size,
            self.bulk_size,
            self.id_conflicts,
            self.conflict_probability,
            self.on_conflict,
            self.recency,
            self.pipeline,
            self.original_params,
            self.create_reader,
        )

        all_bulks = number_of_bulks(self.corpora, start_index, end_index, self.total_partitions, self.bulk_size)
        self.total_bulks = math.ceil((all_bulks * self.ingest_percentage) / 100)

    @property
    def percent_completed(self):
        return self.current_bulk / self.total_bulks


class OpenPointInTimeParamSource(ParamSource):
    def __init__(self, track, params, **kwargs):
        super().__init__(track, params, **kwargs)
        target_name = get_target(track, params)
        self._index_name = target_name
        self._keep_alive = params.get("keep-alive")

    def params(self):
        parsed_params = {"index": self._index_name, "keep-alive": self._keep_alive}
        parsed_params.update(self._client_params())
        return parsed_params


class ClosePointInTimeParamSource(ParamSource):
    def __init__(self, track, params, **kwargs):
        super().__init__(track, params, **kwargs)
        self._pit_task_name = params.get("with-point-in-time-from")

    def params(self):
        parsed_params = {"with-point-in-time-from": self._pit_task_name}
        parsed_params.update(self._client_params())
        return parsed_params


class ForceMergeParamSource(ParamSource):
    def __init__(self, track, params, **kwargs):
        super().__init__(track, params, **kwargs)
        if len(track.indices) > 0 or len(track.data_streams) > 0:
            # force merge data streams and indices - API call is the same so treat as indices
            default_target = ",".join(map(str, track.indices + track.data_streams))
        else:
            default_target = "_all"

        self._target_name = params.get("index")
        if not self._target_name:
            self._target_name = params.get("data-stream", default_target)

        self._max_num_segments = params.get("max-num-segments")
        self._poll_period = params.get("poll-period", 10)
        self._mode = params.get("mode", "blocking")

    def params(self):
        parsed_params = {
            "index": self._target_name,
            "max-num-segments": self._max_num_segments,
            "mode": self._mode,
            "poll-period": self._poll_period,
        }
        parsed_params.update(self._client_params())
        return parsed_params


class DownsampleParamSource(ParamSource):
    def __init__(self, track, params, **kwargs):
        super().__init__(track, params, **kwargs)
        self._fixed_interval = params.get("fixed-interval", "1h")
        params["index"] = params.get("source-index")
        self._source_index = get_target(track, params)
        self._target_index = params.get("target-index", f"{self._source_index}-{self._fixed_interval}")

    def params(self):
        parsed_params = {"fixed-interval": self._fixed_interval, "source-index": self._source_index, "target-index": self._target_index}
        parsed_params.update(self._client_params())
        return parsed_params


def get_target(track, params):
    if len(track.indices) == 1:
        default_target = track.indices[0].name
    elif len(track.data_streams) == 1:
        default_target = track.data_streams[0].name
    else:
        default_target = None
    # indices are preferred but data streams can also be queried the same way
    target_name = params.get("index")
    if not target_name:
        target_name = params.get("data-stream", default_target)
    return target_name


def number_of_bulks(corpora, start_partition_index, end_partition_index, total_partitions, bulk_size):
    """
    :return: The number of bulk operations that the given client will issue.
    """
    bulks = 0
    for corpus in corpora:
        for docs in corpus.documents:
            _, num_docs, _ = bounds(
                docs.number_of_documents, start_partition_index, end_partition_index, total_partitions, docs.includes_action_and_meta_data
            )
            complete_bulks, rest = (num_docs // bulk_size, num_docs % bulk_size)
            bulks += complete_bulks
            if rest > 0:
                bulks += 1
    return bulks


def build_conflicting_ids(conflicts, docs_to_index, offset, shuffle=random.shuffle):
    if conflicts is None or conflicts == IndexIdConflict.NoConflicts:
        return None
    all_ids = [0] * docs_to_index
    for i in range(docs_to_index):
        # always consider the offset as each client will index its own range and we don't want uncontrolled conflicts across clients
        all_ids[i] = "%010d" % (offset + i)
    if conflicts == IndexIdConflict.RandomConflicts:
        shuffle(all_ids)
    return all_ids


def chain(*iterables):
    """
    Chains the given iterables similar to `itertools.chain` except that it also respects the context manager contract.

    :param iterables: A number of iterable that should be chained.
    :return: An iterable that will delegate to all provided iterables in turn.
    """
    for it in filter(lambda x: x is not None, iterables):
        # execute within a context
        with it:
            yield from it


def create_default_reader(
    docs, offset, num_lines, num_docs, batch_size, bulk_size, id_conflicts, conflict_probability, on_conflict, recency
):
    source = Slice(io.MmapSource, offset, num_lines)
    target = None
    use_create = False
    if docs.target_index:
        target = docs.target_index
    elif docs.target_data_stream:
        target = docs.target_data_stream
        use_create = True
        if id_conflicts != IndexIdConflict.NoConflicts:
            # can only create docs in data streams
            raise exceptions.RallyError("Conflicts cannot be generated with append only data streams")

    if docs.includes_action_and_meta_data:
        return SourceOnlyIndexDataReader(docs.document_file, batch_size, bulk_size, source, target, docs.target_type)
    else:
        am_handler = GenerateActionMetaData(
            target,
            docs.target_type,
            build_conflicting_ids(id_conflicts, num_docs, offset),
            conflict_probability,
            on_conflict,
            recency,
            use_create=use_create,
        )
        return MetadataIndexDataReader(docs.document_file, batch_size, bulk_size, source, am_handler, target, docs.target_type)


def create_readers(
    num_clients: int,
    start_client_index: int,
    end_client_index: int,
    corpora: list[track.DocumentCorpus],
    batch_size: int,
    bulk_size: int,
    id_conflicts: IndexIdConflict,
    conflict_probability: float,
    on_conflict: str,
    recency: str,
    create_reader: Callable[..., IndexDataReader],
) -> list[IndexDataReader]:
    """
    Return a list of IndexDataReader instances to allow a range of clients to read their share of corpora.

    We're looking for better parallelism between corpora in indexing tasks in two ways:

    1. By giving each client its own starting point in the list of corpora (using a
       modulus of the number of corpora listed and the number of the client). In a track
       with 2 corpora and 5 clients, clients 1, 3, and 5 would start with the first corpus
       and clients 2 and 4 would start with the second corpus.
    2. By generating the IndexDataReader list round-robin among all files, instead of in
       order. If I'm the first client, I start with the first partition of the first file
       of the first corpus. Then I move on to the first partition of the first file of the
       second corpus, and so on.
    """
    corpora_readers: list[Deque[IndexDataReader]] = []
    total_readers = 0
    # stagger which corpus each client starts with for better parallelism (see 1. above)
    start_corpora_id = start_client_index % len(corpora)
    reordered_corpora = corpora[start_corpora_id:] + corpora[:start_corpora_id]

    for corpus in reordered_corpora:
        reader_queue: Deque[IndexDataReader] = collections.deque()
        for docs in corpus.documents:
            offset, num_docs, num_lines = bounds(
                docs.number_of_documents, start_client_index, end_client_index, num_clients, docs.includes_action_and_meta_data
            )
            if num_docs > 0:
                reader: IndexDataReader = create_reader(
                    docs, offset, num_lines, num_docs, batch_size, bulk_size, id_conflicts, conflict_probability, on_conflict, recency
                )
                reader_queue.append(reader)
                total_readers += 1
        corpora_readers.append(reader_queue)

    # Stagger which files will be read (see 2. above)
    staggered_readers: list[IndexDataReader] = []
    while total_readers > 0:
        for reader_queue in corpora_readers:
            # Since corpora don't necessarily contain the same number of documents, we
            # ignore already consumed queues
            if reader_queue:
                staggered_readers.append(reader_queue.popleft())
                total_readers -= 1
    return staggered_readers


def bounds(total_docs, start_client_index, end_client_index, num_clients, includes_action_and_meta_data):
    """

    Calculates the start offset and number of documents for a range of clients.

    :param total_docs: The total number of documents to index.
    :param start_client_index: The first client index.  Must be in the range [0, `num_clients').
    :param end_client_index: The last client index.  Must be in the range [0, `num_clients').
    :param num_clients: The total number of clients that will run bulk index operations.
    :param includes_action_and_meta_data: Whether the source file already includes the action and meta-data line.
    :return: A tuple containing: the start offset (in lines) for the document corpus, the number documents that the
             clients should index, and the number of lines that the clients should read.
    """
    source_lines_per_doc = 2 if includes_action_and_meta_data else 1

    docs_per_client = total_docs / num_clients

    start_offset_docs = round(docs_per_client * start_client_index)
    end_offset_docs = round(docs_per_client * (end_client_index + 1))

    offset_lines = start_offset_docs * source_lines_per_doc
    docs = end_offset_docs - start_offset_docs
    lines = docs * source_lines_per_doc

    return offset_lines, docs, lines


def bulk_generator(readers, pipeline, original_params):
    bulk_id = 0
    for index, type, batch in readers:
        # each batch can contain of one or more bulks
        for docs_in_bulk, bulk in batch:
            bulk_id += 1
            bulk_params = {
                "index": index,
                "type": type,
                # For our implementation it's always present. Either the original source file already contains this line or the generator
                # has added it.
                "action-metadata-present": True,
                "body": bulk,
                # This is not always equal to the bulk_size we get as parameter. The last bulk may be less than the bulk size.
                "bulk-size": docs_in_bulk,
                "unit": "docs",
            }
            if pipeline:
                bulk_params["pipeline"] = pipeline

            params = original_params.copy()
            params.update(bulk_params)
            yield params


def bulk_data_based(
    num_clients,
    start_client_index,
    end_client_index,
    corpora,
    batch_size,
    bulk_size,
    id_conflicts,
    conflict_probability,
    on_conflict,
    recency,
    pipeline,
    original_params,
    create_reader=create_default_reader,
):
    """
    Calculates the necessary schedule for bulk operations.

    :param num_clients: The total number of clients that will run the bulk operation.
    :param start_client_index: The first client for which we calculated the schedule. Must be in the range [0, `num_clients').
    :param end_client_index: The last client for which we calculated the schedule. Must be in the range [0, `num_clients').
    :param corpora: Specification of affected document corpora.
    :param batch_size: The number of documents to read in one go.
    :param bulk_size: The size of bulk index operations (number of documents per bulk).
    :param id_conflicts: The type of id conflicts to simulate.
    :param conflict_probability: A number between (0.0, 100.0] that defines the probability that a document is replaced by another one.
    :param on_conflict: A string indicating which action should be taken on id conflicts (either "index" or "update").
    :param recency: A number between [0.0, 1.0] indicating whether to bias generation of conflicting ids towards more recent ones.
                    May be None.
    :param pipeline: Name of the ingest pipeline to use. May be None.
    :param original_params: A dict of original parameters that were passed from the track. They will be merged into the returned parameters.
    :param create_reader: A function to create the index reader. By default a file based index reader will be created. This parameter is
                      intended for testing only.
    :return: A generator for the bulk operations of the given client.
    """
    readers = create_readers(
        num_clients,
        start_client_index,
        end_client_index,
        corpora,
        batch_size,
        bulk_size,
        id_conflicts,
        conflict_probability,
        on_conflict,
        recency,
        create_reader,
    )
    return bulk_generator(chain(*readers), pipeline, original_params)


class GenerateActionMetaData:
    RECENCY_SLOPE = 30

    def __init__(
        self,
        index_name,
        type_name,
        conflicting_ids=None,
        conflict_probability=None,
        on_conflict=None,
        recency=None,
        rand=random.random,
        randint=random.randint,
        randexp=random.expovariate,
        use_create=False,
    ):
        if type_name:
            self.meta_data_index_with_id = '{"index": {"_index": "%s", "_type": "%s", "_id": "%s"}}\n' % (index_name, type_name, "%s")
            self.meta_data_update_with_id = '{"update": {"_index": "%s", "_type": "%s", "_id": "%s"}}\n' % (index_name, type_name, "%s")
            self.meta_data_index_no_id = '{"index": {"_index": "%s", "_type": "%s"}}\n' % (index_name, type_name)
        else:
            self.meta_data_index_with_id = '{"index": {"_index": "%s", "_id": "%s"}}\n' % (index_name, "%s")
            self.meta_data_update_with_id = '{"update": {"_index": "%s", "_id": "%s"}}\n' % (index_name, "%s")
            self.meta_data_index_no_id = '{"index": {"_index": "%s"}}\n' % index_name
            self.meta_data_create_no_id = '{"create": {"_index": "%s"}}\n' % index_name
        if use_create and conflicting_ids:
            raise exceptions.RallyError("Index mode '_create' cannot be used with conflicting ids")
        self.conflicting_ids = conflicting_ids
        self.on_conflict = on_conflict
        self.use_create = use_create
        # random() produces numbers between 0 and 1 and the user denotes the probability in percentage between 0 and 100
        self.conflict_probability = conflict_probability / 100.0 if conflict_probability is not None else 0
        self.recency = recency if recency is not None else 0

        self.rand = rand
        self.randint = randint
        self.randexp = randexp
        self.id_up_to = 0

    @property
    def is_constant(self):
        """
        :return: True iff the iterator will always return the same value.
        """
        return self.conflicting_ids is None

    def __iter__(self):
        return self

    def __next__(self):
        if self.conflicting_ids is not None:
            if self.conflict_probability and self.id_up_to > 0 and self.rand() <= self.conflict_probability:
                # a recency of zero means that we don't care about recency and just take a random number
                # within the whole interval.
                if self.recency == 0:
                    idx = self.randint(0, self.id_up_to - 1)
                else:
                    # A recency > 0 biases id selection towards more recent ids. The recency parameter decides
                    # by how much we bias. See docs for the resulting curve.
                    #
                    # idx_range is in the interval [0, 1].
                    idx_range = min(self.randexp(GenerateActionMetaData.RECENCY_SLOPE * self.recency), 1)
                    # the resulting index is in the range [0, self.id_up_to). Note that a smaller idx_range
                    # biases towards more recently used ids (higher indexes).
                    idx = round((self.id_up_to - 1) * (1 - idx_range))

                doc_id = self.conflicting_ids[idx]
                action = self.on_conflict
            else:
                if self.id_up_to >= len(self.conflicting_ids):
                    raise StopIteration()
                doc_id = self.conflicting_ids[self.id_up_to]
                self.id_up_to += 1
                action = "index"

            if action == "index":
                return "index", self.meta_data_index_with_id % doc_id
            elif action == "update":
                return "update", self.meta_data_update_with_id % doc_id
            else:
                raise exceptions.RallyAssertionError(f"Unknown action [{action}]")
        else:
            if self.use_create:
                return "create", self.meta_data_create_no_id
            return "index", self.meta_data_index_no_id


class Slice:
    def __init__(self, source_class, offset, number_of_lines):
        self.source_class = source_class
        self.source = None
        self.offset = offset
        self.number_of_lines = number_of_lines
        self.current_line = 0
        self.bulk_size = None
        self.logger = logging.getLogger(__name__)

    def open(self, file_name, mode, bulk_size):
        self.bulk_size = bulk_size
        self.source = self.source_class(file_name, mode).open()
        self.logger.info(
            "Will read [%d] lines from [%s] starting from line [%d] with bulk size [%d].",
            self.number_of_lines,
            file_name,
            self.offset,
            self.bulk_size,
        )
        start = time.perf_counter()
        io.skip_lines(file_name, self.source, self.offset)
        end = time.perf_counter()
        self.logger.debug("Skipping [%d] lines took [%f] s.", self.offset, end - start)
        return self

    def close(self):
        self.source.close()
        self.source = None

    def __iter__(self):
        return self

    def __next__(self):
        if self.current_line >= self.number_of_lines:
            raise StopIteration()

        # ensure we don't read past the allowed number of lines.
        lines = self.source.readlines(min(self.bulk_size, self.number_of_lines - self.current_line))
        self.current_line += len(lines)
        if len(lines) == 0:
            raise StopIteration()
        return lines

    def __str__(self):
        return "%s[%d;%d]" % (self.source, self.offset, self.offset + self.number_of_lines)


class IndexDataReader:
    """
    Reads a file in bulks into an array and also adds a meta-data line before each document if necessary.

    This implementation also supports batching. This means that you can specify batch_size = N * bulk_size, where N
    is any natural number >= 1. This makes file reading more efficient for small bulk sizes.
    """

    def __init__(self, data_file, batch_size, bulk_size, file_source, index_name, type_name):
        self.data_file = data_file
        self.batch_size = batch_size
        self.bulk_size = bulk_size
        self.file_source = file_source
        self.index_name = index_name
        self.type_name = type_name

    def __enter__(self):
        self.file_source.open(self.data_file, "rt", self.bulk_size)
        return self

    def __iter__(self):
        return self

    def __next__(self):
        """
        Returns lines for N bulk requests (where N is bulk_size / batch_size)
        """
        batch = []
        try:
            docs_in_batch = 0
            while docs_in_batch < self.batch_size:
                try:
                    docs_in_bulk, bulk = self.read_bulk()
                except StopIteration:
                    break
                if docs_in_bulk == 0:
                    break
                docs_in_batch += docs_in_bulk
                batch.append((docs_in_bulk, b"".join(bulk)))
            if docs_in_batch == 0:
                raise StopIteration()
            return self.index_name, self.type_name, batch
        except OSError:
            logging.getLogger(__name__).exception("Could not read [%s]", self.data_file)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file_source.close()
        return False


class MetadataIndexDataReader(IndexDataReader):
    def __init__(self, data_file, batch_size, bulk_size, file_source, action_metadata, index_name, type_name):
        super().__init__(data_file, batch_size, bulk_size, file_source, index_name, type_name)
        self.action_metadata = action_metadata
        self.action_metadata_line = None

    def __enter__(self):
        super().__enter__()
        if self.action_metadata.is_constant:
            _, self.action_metadata_line = next(self.action_metadata)
            self.read_bulk = self._read_bulk_fast
        else:
            self.read_bulk = self._read_bulk_regular
        return self

    def _read_bulk_fast(self):
        """
        Special-case implementation for bulk data files where the action and meta-data line is always identical.
        """
        current_bulk = []
        # hoist
        action_metadata_line = self.action_metadata_line.encode("utf-8")
        docs = next(self.file_source)

        for doc in docs:
            current_bulk.append(action_metadata_line)
            current_bulk.append(doc)
        return len(docs), current_bulk

    def _read_bulk_regular(self):
        """
        General case implementation for bulk files. This implementation can cover all cases but is slower when the
        action and meta-data line is always identical.
        """
        current_bulk = []
        docs = next(self.file_source)
        for doc in docs:
            action_metadata_item = next(self.action_metadata)
            if action_metadata_item:
                action_type, action_metadata_line = action_metadata_item
                current_bulk.append(action_metadata_line.encode("utf-8"))
                if action_type == "update":
                    # remove the trailing "\n" as the doc needs to fit on one line
                    doc = doc.strip()
                    current_bulk.append(b'{"doc":%s}\n' % doc)
                else:
                    current_bulk.append(doc)
            else:
                current_bulk.append(doc)
        return len(docs), current_bulk


class SourceOnlyIndexDataReader(IndexDataReader):
    def __init__(self, data_file, batch_size, bulk_size, file_source, index_name, type_name):
        # keep batch size as it only considers documents read, not lines read but increase the bulk size as
        # documents are only on every other line.
        super().__init__(data_file, batch_size, bulk_size * 2, file_source, index_name, type_name)

    def read_bulk(self):
        bulk_items = next(self.file_source)
        return len(bulk_items) // 2, bulk_items


register_param_source_for_operation(track.OperationType.Bulk, BulkIndexParamSource)
register_param_source_for_operation(track.OperationType.Search, SearchParamSource)
register_param_source_for_operation(track.OperationType.ScrollSearch, SearchParamSource)
register_param_source_for_operation(track.OperationType.PaginatedSearch, SearchParamSource)
register_param_source_for_operation(track.OperationType.CompositeAgg, SearchParamSource)
register_param_source_for_operation(track.OperationType.CreateIndex, CreateIndexParamSource)
register_param_source_for_operation(track.OperationType.DeleteIndex, DeleteIndexParamSource)
register_param_source_for_operation(track.OperationType.CreateDataStream, CreateDataStreamParamSource)
register_param_source_for_operation(track.OperationType.DeleteDataStream, DeleteDataStreamParamSource)
register_param_source_for_operation(track.OperationType.CreateIndexTemplate, CreateIndexTemplateParamSource)
register_param_source_for_operation(track.OperationType.DeleteIndexTemplate, DeleteIndexTemplateParamSource)
register_param_source_for_operation(track.OperationType.CreateComponentTemplate, CreateComponentTemplateParamSource)
register_param_source_for_operation(track.OperationType.DeleteComponentTemplate, DeleteComponentTemplateParamSource)
register_param_source_for_operation(track.OperationType.CreateComposableTemplate, CreateComposableTemplateParamSource)
register_param_source_for_operation(track.OperationType.DeleteComposableTemplate, DeleteComposableTemplateParamSource)
register_param_source_for_operation(track.OperationType.Sleep, SleepParamSource)
register_param_source_for_operation(track.OperationType.ForceMerge, ForceMergeParamSource)
register_param_source_for_operation(track.OperationType.Downsample, DownsampleParamSource)

# Also register by name, so users can use it too
register_param_source_for_name("file-reader", BulkIndexParamSource)
