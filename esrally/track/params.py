import logging
import random
import time
from enum import Enum

from esrally import exceptions
from esrally.track import track
from esrally.utils import io

logger = logging.getLogger("rally.track")

PARAM_SOURCES = {}


def param_source(op_type, indices, params):
    try:
        return PARAM_SOURCES[op_type](indices, params)
    except KeyError:
        logger.info("No specific parameter source registered for [%s]. Creating default parameter source." % op_type)
        return ParamSource(indices, params)


# Default
class ParamSource:
    def __init__(self, indices, params):
        self.indices = indices
        self._params = params

    def partition(self, partition_index, total_partitions):
        return self

    def variation_count(self):
        return 1

    def params(self):
        return self._params


class SearchParamSource(ParamSource):
    def __init__(self, indices, params):
        super().__init__(indices, params)
        if len(indices) == 1 and len(indices[0].types) == 1:
            default_index = indices[0].name
            default_type = indices[0].types[0].name
        else:
            default_index = None
            default_type = None

        index_name = params.get("index", default_index)
        type_name = params.get("type", default_type)
        request_cache = params.get("cache", False)
        query_body = params.get("body", None)
        pages = params.get("pages", None)
        items_per_page = params.get("results-per-page", None)

        self.query_params = {
            "index": index_name,
            "type": type_name,
            "use_request_cache": request_cache,
            "body": query_body
        }

        if not index_name:
            raise exceptions.InvalidSyntax("'index' is mandatory")

        if pages:
            self.query_params["pages"] = pages
        if items_per_page:
            self.query_params["items_per_page"] = items_per_page

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
    NoConflicts = 0,
    SequentialConflicts = 1,
    RandomConflicts = 2


class BulkIndexParamSource(ParamSource):
    def __init__(self, indices, params):
        super().__init__(indices, params)

        id_conflicts = params.get("conflicts", None)
        if not id_conflicts:
            self.id_conflicts = IndexIdConflict.NoConflicts
        elif id_conflicts == "sequential":
            self.id_conflicts = IndexIdConflict.SequentialConflicts
        elif id_conflicts == "random":
            self.id_conflicts = IndexIdConflict.RandomConflicts
        else:
            raise exceptions.InvalidSyntax("Unknown index id conflict type [%s]." % id_conflicts)
        self.pipeline = params.get("pipeline", None)
        try:
            self.bulk_size = int(params["bulk-size"])
            if self.bulk_size <= 0:
                raise exceptions.InvalidSyntax("'bulk-size' must be positive but was %d" % self.bulk_size)
        except KeyError:
            raise exceptions.InvalidSyntax("Mandatory parameter 'bulk-size' is missing")
        except ValueError:
            raise exceptions.InvalidSyntax("'bulk-size' must be numeric")

    def partition(self, partition_index, total_partitions):
        return PartitionBulkIndexParamSource(self.indices, partition_index, total_partitions, self.bulk_size, self.id_conflicts, self.pipeline)

    def params(self):
        raise exceptions.RallyError("Do not use a BulkIndexParamSource without partitioning")

    def variation_count(self):
        raise exceptions.RallyError("Do not use a BulkIndexParamSource without partitioning")


class PartitionBulkIndexParamSource(ParamSource):
    def __init__(self, indices, partition_index, total_partitions, bulk_size, id_conflicts=None, pipeline=None):
        """

        :param indices: Specification of affected indices.
        :param partition_index: The current partition index.  Must be in the range [0, `total_partitions`).
        :param total_partitions: The total number of partitions (i.e. clietns) for bulk index operations.
        :param bulk_size: The size of bulk index operations (number of documents per bulk).
        :param id_conflicts: The type of id conflicts.
        :param pipeline: The name of the ingest pipeline to run.
        """
        super().__init__(indices, {})
        self.partition_index = partition_index
        self.total_partitions = total_partitions
        self.bulk_size = bulk_size
        self.id_conflicts = id_conflicts
        self.pipeline = pipeline
        self.internal_params = bulk_data_based(total_partitions, partition_index, indices, bulk_size, id_conflicts, pipeline)

    def partition(self, partition_index, total_partitions):
        raise exceptions.RallyError("Cannot partition a PartitionBulkIndexParamSource further")

    def params(self):
        return next(self.internal_params)

    def variation_count(self):
        return self.number_of_bulks()

    def number_of_bulks(self):
        """
        :return: The number of bulk operations that the given client will issue.
        """
        bulks = 0
        for index in self.indices:
            for type in index.types:
                offset, num_docs = bounds(type.number_of_documents, self.partition_index, self.total_partitions)
                complete_bulks, rest = (num_docs // self.bulk_size, num_docs % self.bulk_size)
                bulks += complete_bulks
                if rest > 0:
                    bulks += 1
        return bulks


def build_conflicting_ids(conflicts, docs_to_index, offset):
    if conflicts is None or conflicts == IndexIdConflict.NoConflicts:
        return None
    logger.info("building ids with id conflicts of type [%s]" % conflicts)
    all_ids = [0] * docs_to_index
    for i in range(docs_to_index):
        # always consider the offset as each client will index its own range and we don't want uncontrolled conflicts across clients
        if conflicts == IndexIdConflict.SequentialConflicts:
            all_ids[i] = "%10d" % (offset + i)
        else:  # RandomConflicts
            all_ids[i] = "%10d" % random.randint(offset, offset + docs_to_index)
    return all_ids


def chain(*iterables):
    """
    Chains the given iterables similar to `itertools.chain` except that it also respects the context manager contract.

    :param iterables: A number of iterable that should be chained.
    :return: An iterable that will delegate to all provided iterables in turn.
    """
    for it in iterables:
        # execute within a context
        with it:
            for element in it:
                yield element


def create_default_reader(index, type, offset, num_docs, bulk_size, id_conflicts):
    return IndexDataReader(type.document_file, num_docs,
                           build_conflicting_ids(id_conflicts, num_docs, offset), index.name, type.name, bulk_size, offset)


def bounds(total_docs, client_index, num_clients):
    """

    Calculates the start offset and number of documents for each client.

    :param total_docs: The total number of documents to index.
    :param client_index: The current client index.  Must be in the range [0, `num_clients').
    :param num_clients: The total number of clients that will run bulk index operations.
    :return: A tuple containing the start offset for the document corpus and the number documents that the client should index.
    """
    # last client gets one less if the number is uneven
    if client_index + 1 == num_clients and total_docs % num_clients > 0:
        correction = 1
    else:
        correction = 0
    docs_per_client = round(total_docs / num_clients) - correction
    # don't consider the correction for the other clients because it just applies to the last one
    offset = client_index * (docs_per_client + correction)
    return offset, docs_per_client


def bulk_data_based(num_clients, client_index, indices, bulk_size, id_conflicts, pipeline, create_reader=create_default_reader):
    """
    Calculates the necessary schedule for bulk operations.

    :param num_clients: The total number of clients that will run the bulk operation.
    :param client_index: The current client for which we calculated the schedule. Must be in the range [0, `num_clients').
    :param indices: Specification of affected indices.
    :param bulk_size: The size of bulk index operations (number of documents per bulk).
    :param id_conflicts: The type of id conflicts to simulate.
    :param pipeline: Name of the ingest pipeline to use. May be None.
    :param create_reader: A function to create the index reader. By default a file based index reader will be created. This parameter is
                          intended for testing only.
    :return: A generator for the bulk operations of the given client.
    """

    readers = []
    for index in indices:
        for type in index.types:
            offset, num_docs = bounds(type.number_of_documents, client_index, num_clients)
            if num_docs > 0:
                logger.info("Client [%d] will index [%d] docs starting from offset [%d] for [%s/%s]" %
                            (client_index, num_docs, offset, index, type))
                readers.append(create_reader(index, type, offset, num_docs, bulk_size, id_conflicts))
            else:
                logger.info("Client [%d] skips [%s/%s] (no documents to read)." % (client_index, index, type))
    reader = chain(*readers)
    for bulk in reader:
        params = {"body": bulk}
        if pipeline:
            params["pipeline"] = pipeline
        yield params


class FileSource:
    @staticmethod
    def open(file_name, mode):
        return FileSource(file_name, mode)

    def __init__(self, file_name, mode):
        self.f = open(file_name, mode)

    def seek(self, offset):
        self.f.seek(offset)

    def readline(self):
        return self.f.readline()

    def close(self):
        self.f.close()
        self.f = None


class IndexDataReader:
    """
    Reads an index file in bulks into an array and also adds the necessary meta-data line before each document.
    """

    def __init__(self, data_file, docs_to_index, conflicting_ids, index_name, type_name, bulk_size, offset=0, file_source=FileSource):
        self.data_file = data_file
        self.docs_to_index = docs_to_index
        self.conflicting_ids = conflicting_ids
        self.index_name = index_name
        self.type_name = type_name
        self.bulk_size = bulk_size
        self.id_up_to = 0
        self.current_bulk = 0
        self.offset = offset
        self.file_source = file_source
        self.f = None

    def __enter__(self):
        self.f = self.file_source.open(self.data_file, 'rt')
        # skip offset number of lines
        logger.info("Skipping %d lines in [%s]." % (self.offset, self.data_file))
        start = time.perf_counter()
        io.skip_lines(self.data_file, self.f, self.offset)
        end = time.perf_counter()
        logger.info("Skipping %d lines took %f s." % (self.offset, end - start))
        return self

    def __iter__(self):
        return self

    def __next__(self):
        """
        Returns lines for one bulk request.
        """
        buffer = []
        try:
            docs_indexed = 0
            docs_left = self.docs_to_index - (self.current_bulk * self.bulk_size)
            if self.f is None or docs_left <= 0:
                raise StopIteration()

            this_bulk_size = min(self.bulk_size, docs_left)
            while docs_indexed < this_bulk_size:
                line = self.f.readline()
                if len(line) == 0:
                    break
                line = line.strip()
                if self.conflicting_ids is not None:
                    # 25% of the time we replace a doc:
                    if self.id_up_to > 0 and random.randint(0, 3) == 3:
                        doc_id = self.conflicting_ids[random.randint(0, self.id_up_to - 1)]
                    else:
                        doc_id = self.conflicting_ids[self.id_up_to]
                        self.id_up_to += 1
                    cmd = '{"index": {"_index": "%s", "_type": "%s", "_id": "%s"}}' % (self.index_name, self.type_name, doc_id)
                else:
                    cmd = '{"index": {"_index": "%s", "_type": "%s"}}' % (self.index_name, self.type_name)

                buffer.append(cmd)
                buffer.append(line)

                docs_indexed += 1

            self.current_bulk += 1
            return buffer
        except IOError:
            logger.exception("Could not read [%s]" % self.data_file)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.f:
            self.f.close()
            self.f = None
        return False


PARAM_SOURCES[track.OperationType.Index] = BulkIndexParamSource
PARAM_SOURCES[track.OperationType.Search] = SearchParamSource
