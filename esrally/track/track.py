import logging
from enum import Enum

logger = logging.getLogger("rally.track")


class Index:
    """
    Defines an index in Elasticsearch.
    """

    def __init__(self, name, auto_managed, types):
        """

        Creates a new index.

        :param name: The index name. Mandatory.
        :param auto_managed: True iff Rally should automatically manage this index (i.e. it can create and delete it at will).
        :param types: A list of types. Should contain at least one type.
        """
        self.name = name
        self.auto_managed = auto_managed
        self.types = types

    @property
    def number_of_documents(self):
        num_docs = 0
        for t in self.types:
            num_docs += t.number_of_documents
        return num_docs

    def __str__(self, *args, **kwargs):
        return self.name


class IndexTemplate:
    """
    Defines an index template in Elasticsearch.
    """

    def __init__(self, name, pattern, template_file):
        """

        Creates a new index template.

        :param name: Name of the index template. Mandatory.
        :param pattern: The index pattern to which the index template applies. Mandatory.
        :param template_file: The name of the corresponding template file. Mandatory.
        """
        self.name = name
        self.pattern = pattern
        self.template_file = template_file

    def __str__(self, *args, **kwargs):
        return self.name


class Type:
    """
    Defines a type in Elasticsearch.
    """

    def __init__(self, name, mapping_file, document_file=None, document_archive=None, number_of_documents=0,
                 compressed_size_in_bytes=0,
                 uncompressed_size_in_bytes=0):
        """

        Creates a new type. Mappings are mandatory but the document_archive (and associated properties) are optional.

        :param name: The name of this type. Mandatory.
        :param mapping_file: The file name of the mapping file on the remote server. Mandatory.
        :param document_file: The file name of benchmark documents after decompression. Optional (e.g. for percolation we
        just need a mapping but no documents)
        :param document_file: The file name of the compressed benchmark document name on the remote server. Optional (e.g. for percolation we
        just need a mapping but no documents)
        :param number_of_documents: The number of documents in the benchmark document. Needed for proper progress reporting. Only needed if
         a document_archive is given.
        :param compressed_size_in_bytes: The compressed size in bytes of the benchmark document. Needed for verification of the download and
         user reporting. Only needed if a document_archive is given.
        :param uncompressed_size_in_bytes: The size in bytes of the benchmark document after decompressing it. Only needed if a
        document_archive is given.
        """
        self.name = name
        self.mapping_file = mapping_file
        self.document_file = document_file
        self.document_archive = document_archive
        self.number_of_documents = number_of_documents
        self.compressed_size_in_bytes = compressed_size_in_bytes
        self.uncompressed_size_in_bytes = uncompressed_size_in_bytes

    def has_valid_document_data(self):
        return self.document_file is not None and \
               self.number_of_documents > 0 and \
               self.compressed_size_in_bytes > 0 and \
               self.uncompressed_size_in_bytes > 0

    def __str__(self, *args, **kwargs):
        return self.name


class Track:
    """
    A track defines the data set that is used. It corresponds loosely to a use case (e.g. logging, event processing, analytics, ...)
    """

    def __init__(self, name, short_description, description, source_root_url, challenges, indices=None, templates=None):
        """

        Creates a new track.

        :param name: A short, descriptive name for this track. As per convention, this name should be in lower-case without spaces.
        :param short_description: A short description for this track (should be less than 80 characters).
        :param description: A longer description for this track.
        :param source_root_url: The publicly reachable http URL of the root folder for this track (without a trailing slash). Directly
        below this URL the benchmark document files have to be located.
        :param challenges: A list of one or more challenges to use.
        :param indices: A list of indices for this track. May be None. One of `indices` or `templates` must be set though.
        :param templates: A list of index templates for this track. May be None. One of `indices` or `templates` must be set though.
        """
        self.name = name
        self.short_description = short_description
        self.description = description
        self.source_root_url = source_root_url
        self.challenges = challenges
        self.indices = indices
        self.templates = templates

    @property
    def number_of_documents(self):
        num_docs = 0
        if self.indices:
            for index in self.indices:
                num_docs += index.number_of_documents
        return num_docs

    def __str__(self):
        return self.name


class Challenge:
    """
    A challenge defines the concrete operations that will be done.
    """

    def __init__(self,
                 name,
                 description,
                 index_settings,
                 schedule=None):
        if schedule is None:
            schedule = {}
        self.name = name
        self.description = description
        self.index_settings = index_settings
        self.schedule = schedule

    def __str__(self):
        return self.name


class OperationType(Enum):
    Index = 0,
    ForceMerge = 1,
    IndicesStats = 2,
    NodesStats = 3,
    Search = 4

    @classmethod
    def from_hyphenated_string(cls, v):
        if v == "index":
            return OperationType.Index
        elif v == "force-merge":
            return OperationType.ForceMerge
        elif v == "index-stats":
            return OperationType.IndicesStats
        elif v == "node-stats":
            return OperationType.NodesStats
        elif v == "search":
            return OperationType.Search
        else:
            raise KeyError("No enum value for [%s]" % v)


# Schedule elements
class Parallel:
    def __init__(self, tasks, clients=None):
        self.tasks = tasks
        self._clients = clients

    @property
    def clients(self):
        if self._clients is not None:
            return self._clients
        else:
            num_clients = 0
            for task in self.tasks:
                num_clients += task.clients
            return num_clients

    def __iter__(self):
        return iter(self.tasks)

    def __repr__(self, *args, **kwargs):
        return "%d parallel tasks" % len(self.tasks)


class Task:
    def __init__(self, operation, warmup_iterations=0, iterations=1, warmup_time_period=None, time_period=None, clients=1, target_throughput=None):
        self.operation = operation
        self.warmup_iterations = warmup_iterations
        self.iterations = iterations
        self.warmup_time_period = warmup_time_period
        self.time_period = time_period
        self.clients = clients
        self.target_throughput = target_throughput

    def __iter__(self):
        return iter([self])

    def __repr__(self, *args, **kwargs):
        return "Task for [%s]" % self.operation.name


class Operation:
    def __init__(self, name, operation_type, params=None, param_source=None):
        if params is None:
            params = {}
        self.name = name
        self.type = operation_type
        self.params = params
        self.param_source = param_source

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == other.name

    def __repr__(self, *args, **kwargs):
        return self.name
