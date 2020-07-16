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

from enum import Enum, unique

from esrally import exceptions


class Index:
    """
    Defines an index in Elasticsearch.
    """

    def __init__(self, name, body=None, types=None):
        """

        Creates a new index.

        :param name: The index name. Mandatory.
        :param body: A dict representation of the index body. Optional.
        :param types: A list of types. Should contain at least one type.
        """
        if types is None:
            types = []
        if body is None:
            body = {}
        self.name = name
        self.body = body
        self.types = types

    def matches(self, pattern):
        if pattern is None:
            return True
        elif pattern in ["_all", "*"]:
            return True
        elif self.name == pattern:
            return True
        else:
            return False

    def __str__(self):
        return self.name

    def __repr__(self):
        r = []
        for prop, value in vars(self).items():
            r.append("%s = [%s]" % (prop, repr(value)))
        return ", ".join(r)

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == other.name


class IndexTemplate:
    """
    Defines an index template in Elasticsearch.
    """

    def __init__(self, name, pattern, content, delete_matching_indices=False):
        """

        Creates a new index template.

        :param name: Name of the index template. Mandatory.
        :param pattern: The index pattern to which the index template applies. Mandatory.
        :param content: The content of the corresponding template. Mandatory.
        :param delete_matching_indices: Delete all indices that match the pattern before the benchmark iff True.
        """
        self.name = name
        self.pattern = pattern
        self.content = content
        self.delete_matching_indices = delete_matching_indices

    def __str__(self, *args, **kwargs):
        return self.name

    def __repr__(self):
        r = []
        for prop, value in vars(self).items():
            r.append("%s = [%s]" % (prop, repr(value)))
        return ", ".join(r)

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == other.name


class Documents:
    SOURCE_FORMAT_BULK = "bulk"

    def __init__(self, source_format, document_file=None, document_archive=None, base_url=None, includes_action_and_meta_data=False,
                 number_of_documents=0, compressed_size_in_bytes=0, uncompressed_size_in_bytes=0, target_index=None, target_type=None):
        """

        :param source_format: The format of these documents. Mandatory.
        :param document_file: The file name of benchmark documents after decompression. Optional (e.g. for percolation we
        just need a mapping but no documents)
        :param document_archive: The file name of the compressed benchmark document name on the remote server. Optional (e.g. for
        percolation we just need a mapping but no documents)
        :param base_url: The URL from which to load data if they are not available locally. Optional.
        :param includes_action_and_meta_data: True, if the source file already includes the action and meta-data line. False, if it only
        contains documents.
        :param number_of_documents: The number of documents in the benchmark document. Needed for proper progress reporting. Only needed if
         a document_archive is given.
        :param compressed_size_in_bytes: The compressed size in bytes of the benchmark document. Needed for verification of the download and
         user reporting. Only useful if a document_archive is given (optional but recommended to be set).
        :param uncompressed_size_in_bytes: The size in bytes of the benchmark document after decompressing it. Only useful if a
        document_archive is given (optional but recommended to be set).
        :param target_index: The index to target for bulk operations. May be ``None`` if ``includes_action_and_meta_data`` is ``False``.
        :param target_type: The document type to target for bulk operations. May be ``None`` if ``includes_action_and_meta_data``
        is ``False``.
        """

        self.source_format = source_format
        self.document_file = document_file
        self.document_archive = document_archive
        self.base_url = base_url
        self.includes_action_and_meta_data = includes_action_and_meta_data
        self.number_of_documents = number_of_documents
        self.compressed_size_in_bytes = compressed_size_in_bytes
        self.uncompressed_size_in_bytes = uncompressed_size_in_bytes
        self.target_index = target_index
        self.target_type = target_type

    def has_compressed_corpus(self):
        return self.document_archive is not None

    def has_uncompressed_corpus(self):
        return self.document_file is not None

    @property
    def number_of_lines(self):
        if self.includes_action_and_meta_data:
            return self.number_of_documents * 2
        else:
            return self.number_of_documents

    @property
    def is_bulk(self):
        return self.source_format == Documents.SOURCE_FORMAT_BULK

    def __str__(self):
        return "%s documents from %s" % (self.source_format, self.document_file)

    def __repr__(self):
        r = []
        for prop, value in vars(self).items():
            r.append("%s = [%s]" % (prop, repr(value)))
        return ", ".join(r)

    def __hash__(self):
        return hash(self.source_format) ^ hash(self.document_file) ^ hash(self.document_archive) ^ hash(self.base_url) ^ \
               hash(self.includes_action_and_meta_data) ^ hash(self.number_of_documents) ^ hash(self.compressed_size_in_bytes) ^ \
               hash(self.uncompressed_size_in_bytes) ^ hash(self.target_index) ^ hash(self.target_type)

    def __eq__(self, othr):
        return (isinstance(othr, type(self)) and
                (self.source_format, self.document_file, self.document_archive, self.base_url, self.includes_action_and_meta_data,
                 self.number_of_documents, self.compressed_size_in_bytes, self.uncompressed_size_in_bytes,
                 self.target_type, self.target_type) ==
                (othr.source_format, othr.document_file, othr.document_archive, othr.base_url, othr.includes_action_and_meta_data,
                 othr.number_of_documents, othr.compressed_size_in_bytes, othr.uncompressed_size_in_bytes,
                 othr.target_type, othr.target_type))


class DocumentCorpus:
    def __init__(self, name, documents=None):
        """

        :param name: The name of this document corpus. Mandatory.
        :param documents: A list of ``Documents`` instances that belong to this corpus.
        """
        if documents is None:
            documents = []
        self.name = name
        self.documents = documents

    def number_of_documents(self, source_format):
        num = 0
        for doc in self.documents:
            if doc.source_format == source_format:
                num += doc.number_of_documents
        return num

    def compressed_size_in_bytes(self, source_format):
        num = 0
        for doc in self.documents:
            if doc.source_format == source_format and doc.compressed_size_in_bytes is not None:
                num += doc.compressed_size_in_bytes
            else:
                return None
        return num

    def uncompressed_size_in_bytes(self, source_format):
        num = 0
        for doc in self.documents:
            if doc.source_format == source_format and doc.uncompressed_size_in_bytes is not None:
                num += doc.uncompressed_size_in_bytes
            else:
                return None
        return num

    def filter(self, source_format=None, target_indices=None):
        filtered = []
        for d in self.documents:
            # skip if source format or target index does not match
            if source_format and d.source_format != source_format:
                continue
            if target_indices and d.target_index not in target_indices:
                continue

            filtered.append(d)
        return DocumentCorpus(self.name, filtered)

    def union(self, other):
        if self.name != other.name:
            raise exceptions.RallyAssertionError("Both document corpora must have the same name")
        if self is other:
            return self
        else:
            return DocumentCorpus(self.name, list(set(self.documents).union(other.documents)))

    def __str__(self):
        return self.name

    def __repr__(self):
        r = []
        for prop, value in vars(self).items():
            r.append("%s = [%s]" % (prop, repr(value)))
        return ", ".join(r)

    def __hash__(self):
        return hash(self.name) ^ hash(self.documents)

    def __eq__(self, othr):
        return (isinstance(othr, type(self)) and
                (self.name, self.documents) ==
                (othr.name, othr.documents))


class Track:
    """
    A track defines the data set that is used. It corresponds loosely to a use case (e.g. logging, event processing, analytics, ...)
    """

    def __init__(self, name, description=None, meta_data=None, challenges=None, indices=None, templates=None, corpora=None,
                 has_plugins=False):
        """

        Creates a new track.

        :param name: A short, descriptive name for this track. As per convention, this name should be in lower-case without spaces.
        :param description: A description for this track (should be less than 80 characters).
        :param meta_data: An optional dict of meta-data elements to attach to each metrics record. Default: {}.
        :param challenges: A list of one or more challenges to use. Precondition: If the list is non-empty it contains exactly one element
        with its ``default`` property set to ``True``.
        :param indices: A list of indices for this track. May be None.
        :param templates: A list of index templates for this track. May be None.
        :param corpora: A list of document corpus definitions for this track. May be None.
        :param has_plugins: True iff the track also defines plugins (e.g. custom runners or parameter sources).
        """
        self.name = name
        self.meta_data = meta_data if meta_data else {}
        self.description = description if description is not None else ""
        self.challenges = challenges if challenges else []
        self.indices = indices if indices else []
        self.corpora = corpora if corpora else []
        self.templates = templates if templates else []
        self.has_plugins = has_plugins

    @property
    def default_challenge(self):
        for challenge in self.challenges:
            if challenge.default:
                return challenge
        # This should only happen if we don't have any challenges
        return None

    def find_challenge_or_default(self, name):
        """
        :param name: The name of the challenge to find.
        :return: The challenge with the given name. The default challenge, if the name is "" or ``None``.
        """
        if name in [None, ""]:
            return self.default_challenge
        else:
            return self.find_challenge(name)

    def find_challenge(self, name):
        for challenge in self.challenges:
            if challenge.name == name:
                return challenge
        raise exceptions.InvalidName("Unknown challenge [%s] for track [%s]" % (name, self.name))

    @property
    def number_of_documents(self):
        num_docs = 0
        for corpus in self.corpora:
            # TODO #341: Improve API to let users define what they want (everything, just specific types, ...)
            num_docs += corpus.number_of_documents(Documents.SOURCE_FORMAT_BULK)
        return num_docs

    @property
    def compressed_size_in_bytes(self):
        size = 0
        for corpus in self.corpora:
            # TODO #341: Improve API to let users define what they want (everything, just specific types, ...)
            curr_size = corpus.compressed_size_in_bytes(Documents.SOURCE_FORMAT_BULK)
            if curr_size is not None:
                size += curr_size
            else:
                return None
        return size

    @property
    def uncompressed_size_in_bytes(self):
        size = 0
        for corpus in self.corpora:
            # TODO #341: Improve API to let users define what they want (everything, just specific types, ...)
            curr_size = corpus.uncompressed_size_in_bytes(Documents.SOURCE_FORMAT_BULK)
            if curr_size is not None:
                size += curr_size
            else:
                return None
        return size

    def __str__(self):
        return self.name

    def __repr__(self):
        r = []
        for prop, value in vars(self).items():
            r.append("%s = [%s]" % (prop, repr(value)))
        return ", ".join(r)

    def __hash__(self):
        return hash(self.name) ^ hash(self.meta_data) ^ hash(self.description) ^ hash(self.challenges) ^ \
               hash(self.indices) ^ hash(self.templates) ^ hash(self.corpora)

    def __eq__(self, othr):
        return (isinstance(othr, type(self)) and
                (self.name, self.meta_data, self.description, self.challenges, self.indices, self.templates, self.corpora) ==
                (othr.name, othr.meta_data, othr.description, othr.challenges, othr.indices, othr.templates, othr.corpora))


class Challenge:
    """
    A challenge defines the concrete operations that will be done.
    """

    def __init__(self,
                 name,
                 description=None,
                 user_info=None,
                 cluster_settings=None,
                 default=False,
                 auto_generated=False,
                 meta_data=None,
                 schedule=None):
        self.name = name
        self.meta_data = meta_data if meta_data else {}
        self.description = description
        self.user_info = user_info
        self.cluster_settings = cluster_settings if cluster_settings else {}
        self.default = default
        self.auto_generated = auto_generated
        self.schedule = schedule if schedule else []

    def prepend_tasks(self, tasks):
        self.schedule = tasks + self.schedule

    def remove_task(self, task):
        self.schedule.remove(task)

    def __str__(self):
        return self.name

    def __repr__(self):
        r = []
        for prop, value in vars(self).items():
            r.append("%s = [%s]" % (prop, repr(value)))
        return ", ".join(r)

    def __hash__(self):
        return hash(self.name) ^ hash(self.description) ^ hash(self.cluster_settings) ^ hash(self.default) ^ \
               hash(self.auto_generated) ^ hash(self.meta_data) ^ hash(self.schedule)

    def __eq__(self, othr):
        return (isinstance(othr, type(self)) and
                (self.name, self.description, self.cluster_settings, self.default, self.auto_generated, self.meta_data, self.schedule) ==
                (othr.name, othr.description, othr.cluster_settings, othr.default, othr.auto_generated, othr.meta_data, othr.schedule))


@unique
class OperationType(Enum):
    # for the time being we are not considering this action as administrative
    IndicesStats = 1
    NodesStats = 2
    Search = 3
    Bulk = 4
    RawRequest = 5
    WaitForRecovery = 6
    CreateSnapshot = 7


    # administrative actions
    ForceMerge = 1001
    ClusterHealth = 1002
    PutPipeline = 1003
    Refresh = 1004
    CreateIndex = 1005
    DeleteIndex = 1006
    CreateIndexTemplate = 1007
    DeleteIndexTemplate = 1008
    ShrinkIndex = 1009
    CreateMlDatafeed = 1010
    DeleteMlDatafeed = 1011
    StartMlDatafeed = 1012
    StopMlDatafeed = 1013
    CreateMlJob = 1014
    DeleteMlJob = 1015
    OpenMlJob = 1016
    CloseMlJob = 1017
    Sleep = 1018
    DeleteSnapshotRepository = 1019
    CreateSnapshotRepository = 1020
    RestoreSnapshot = 1021
    PutSettings = 1022
    CreateTransform = 1023
    StartTransform = 1024
    WaitForTransform = 1025
    DeleteTransform = 1026

    @property
    def admin_op(self):
        # pylint: disable=comparison-with-callable
        return self.value > 1000

    # pylint: disable=too-many-return-statements
    @classmethod
    def from_hyphenated_string(cls, v):
        if v == "force-merge":
            return OperationType.ForceMerge
        elif v == "index-stats":
            return OperationType.IndicesStats
        elif v == "node-stats":
            return OperationType.NodesStats
        elif v == "search":
            return OperationType.Search
        elif v == "cluster-health":
            return OperationType.ClusterHealth
        elif v == "bulk":
            return OperationType.Bulk
        elif v == "raw-request":
            return OperationType.RawRequest
        elif v == "put-pipeline":
            return OperationType.PutPipeline
        elif v == "refresh":
            return OperationType.Refresh
        elif v == "create-index":
            return OperationType.CreateIndex
        elif v == "delete-index":
            return OperationType.DeleteIndex
        elif v == "create-index-template":
            return OperationType.CreateIndexTemplate
        elif v == "delete-index-template":
            return OperationType.DeleteIndexTemplate
        elif v == "shrink-index":
            return OperationType.ShrinkIndex
        elif v == "create-ml-datafeed":
            return OperationType.CreateMlDatafeed
        elif v == "delete-ml-datafeed":
            return OperationType.DeleteMlDatafeed
        elif v == "start-ml-datafeed":
            return OperationType.StartMlDatafeed
        elif v == "stop-ml-datafeed":
            return OperationType.StopMlDatafeed
        elif v == "create-ml-job":
            return OperationType.CreateMlJob
        elif v == "delete-ml-job":
            return OperationType.DeleteMlJob
        elif v == "open-ml-job":
            return OperationType.OpenMlJob
        elif v == "close-ml-job":
            return OperationType.CloseMlJob
        elif v == "sleep":
            return OperationType.Sleep
        elif v == "delete-snapshot-repository":
            return OperationType.DeleteSnapshotRepository
        elif v == "create-snapshot-repository":
            return OperationType.CreateSnapshotRepository
        elif v == "create-snapshot":
            return OperationType.CreateSnapshot
        elif v == "restore-snapshot":
            return OperationType.RestoreSnapshot
        elif v == "wait-for-recovery":
            return OperationType.WaitForRecovery
        elif v == "put-settings":
            return OperationType.PutSettings
        elif v == "create-transform":
            return OperationType.CreateTransform
        elif v == "start-transform":
            return OperationType.StartTransform
        elif v == "wait-for-transform":
            return OperationType.WaitForTransform
        elif v == "delete-transform":
            return OperationType.DeleteTransform
        else:
            raise KeyError("No enum value for [%s]" % v)


class TaskNameFilter:
    def __init__(self, name):
        self.name = name

    def matches(self, task):
        return self.name == task.name

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return isinstance(other, type(self)) and self.name == other.name

    def __str__(self, *args, **kwargs):
        return "filter for task name [%s]" % self.name


class TaskOpTypeFilter:
    def __init__(self, op_type_name):
        # we need to use the string representation because users may define their own operation types via a custom runners.
        try:
            self.op_type = OperationType.from_hyphenated_string(op_type_name).name
        except KeyError:
            self.op_type = op_type_name

    def matches(self, task):
        return self.op_type == task.operation.type

    def __hash__(self):
        return hash(self.op_type)

    def __eq__(self, other):
        return isinstance(other, type(self)) and self.op_type == other.op_type

    def __str__(self, *args, **kwargs):
        return "filter for operation type [%s]" % self.op_type


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class AdminTaskFilter(metaclass=Singleton):
    """
    Matches all tasks that execute administrative actions.
    """
    def matches(self, task):
        try:
            return OperationType.from_hyphenated_string(task.operation.type).admin_op
        except KeyError:
            return False

    def __str__(self, *args, **kwargs):
        return "filter for admin tasks"


# Schedule elements
class Parallel:
    def __init__(self, tasks, clients=None):
        self.tasks = tasks
        self._clients = clients
        self.nested = True

    @property
    def clients(self):
        if self._clients is not None:
            return self._clients
        else:
            num_clients = 0
            for task in self.tasks:
                num_clients += task.clients
            return num_clients

    def matches(self, task_filter):
        # a parallel element matches if any of its elements match
        for task in self.tasks:
            if task.matches(task_filter):
                return True
        return False

    def remove_task(self, task):
        self.tasks.remove(task)

    def __iter__(self):
        return iter(self.tasks)

    def __str__(self, *args, **kwargs):
        return "%d parallel tasks" % len(self.tasks)

    def __repr__(self):
        r = []
        for prop, value in vars(self).items():
            r.append("%s = [%s]" % (prop, repr(value)))
        return ", ".join(r)

    def __hash__(self):
        return hash(self.tasks)

    def __eq__(self, other):
        return isinstance(other, type(self)) and self.tasks == other.tasks


class Task:
    def __init__(self, name, operation, meta_data=None, warmup_iterations=None, iterations=None, warmup_time_period=None, time_period=None,
                 clients=1,
                 completes_parent=False, schedule="deterministic", params=None):
        self.name = name
        self.operation = operation
        self.meta_data = meta_data if meta_data else {}
        self.warmup_iterations = warmup_iterations
        self.iterations = iterations
        self.warmup_time_period = warmup_time_period
        self.time_period = time_period
        self.clients = clients
        self.completes_parent = completes_parent
        self.schedule = schedule
        self.params = params if params else {}
        self.nested = False

    def matches(self, task_filter):
        return task_filter.matches(self)

    def __hash__(self):
        # Note that we do not include `params` in __hash__ and __eq__ (the other attributes suffice to uniquely define a task)
        return hash(self.name) ^ hash(self.operation) ^ hash(self.warmup_iterations) ^ hash(self.iterations) ^ \
               hash(self.warmup_time_period) ^ hash(self.time_period) ^ hash(self.clients) ^ hash(self.schedule) ^ \
               hash(self.completes_parent)

    def __eq__(self, other):
        # Note that we do not include `params` in __hash__ and __eq__ (the other attributes suffice to uniquely define a task)
        return isinstance(other, type(self)) and (self.name, self.operation, self.warmup_iterations, self.iterations,
                                                  self.warmup_time_period, self.time_period, self.clients, self.schedule,
                                                  self.completes_parent) == (other.name, other.operation, other.warmup_iterations,
                                                                             other.iterations, other.warmup_time_period, other.time_period,
                                                                             other.clients, other.schedule, other.completes_parent)

    def __iter__(self):
        return iter([self])

    def __str__(self, *args, **kwargs):
        return self.name

    def __repr__(self):
        r = []
        for prop, value in vars(self).items():
            r.append("%s = [%s]" % (prop, repr(value)))
        return ", ".join(r)


class Operation:
    def __init__(self, name, operation_type, meta_data=None, params=None, param_source=None):
        if params is None:
            params = {}
        self.name = name
        self.meta_data = meta_data if meta_data else {}
        self.type = operation_type
        self.params = params
        self.param_source = param_source

    @property
    def include_in_reporting(self):
        return self.params.get("include-in-reporting", True)

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return isinstance(other, type(self)) and self.name == other.name

    def __str__(self, *args, **kwargs):
        return self.name

    def __repr__(self):
        r = []
        for prop, value in vars(self).items():
            r.append("%s = [%s]" % (prop, repr(value)))
        return ", ".join(r)
