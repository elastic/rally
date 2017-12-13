import logging
import random
import time
import types
import inspect
from enum import Enum

from esrally import exceptions
from esrally.track import track
from esrally.utils import io, console

logger = logging.getLogger("rally.track")

__PARAM_SOURCES_BY_OP = {}
__PARAM_SOURCES_BY_NAME = {}


def param_source_for_operation(op_type, track, params):
    try:
        # we know that this can only be a Rally core parameter source
        return __PARAM_SOURCES_BY_OP[op_type](track, params)
    except KeyError:
        logger.debug("No specific parameter source registered for operation type [%s]. Creating default parameter source." % op_type)
        return ParamSource(track, params)


def param_source_for_name(name, track, params):
    param_source = __PARAM_SOURCES_BY_NAME[name]

    # we'd rather use callable() but this will erroneously also classify a class as callable...
    if isinstance(param_source, types.FunctionType):
        # TODO: Remove me after some grace period
        try:
            s = inspect.signature(param_source, follow_wrapped=False)
        except TypeError:
            # follow_wrapped has been introduced in Python 3.5
            s = inspect.signature(param_source)
        if len(s.parameters) == 2 and s.parameters.get("indices"):
            console.warn("Parameter source '%s' is using deprecated method signature (indices, params). Please change it "
                         "to (track, params, **kwargs). For details please see the migration guide in the docs." % name, logger=logger)
            return LegacyDelegatingParamSource(track, params, param_source)
        else:
            return DelegatingParamSource(track, params, param_source)
    else:
        try:
            s = inspect.signature(param_source.__init__, follow_wrapped=False)
        except TypeError:
            # follow_wrapped has been introduced in Python 3.5
            s = inspect.signature(param_source)
        # self, indices, params
        if len(s.parameters) == 3 and s.parameters.get("indices"):
            console.warn("Parameter source '%s' is using deprecated method signature (indices, params). Please change it "
                         "to (track, params, **kwargs). For details please see the migration guide in the docs." % name, logger=logger)
            return param_source(track.indices, params)
        else:
            return param_source(track, params)


def register_param_source_for_operation(op_type, param_source_class):
    __PARAM_SOURCES_BY_OP[op_type.name] = param_source_class


def register_param_source_for_name(name, param_source_class):
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

    def size(self):
        """
        Rally has two modes in which it can run:

        * It will either run an operation for a pre-determined number of times or
        * It can run until the parameter source is exhausted.

        In the former case, return just 1. In the latter case, you should determine the number of times that `#params()` will be invoked.
        With that number, Rally can show the progress made so far to the user.

        :return:  The "size" of this parameter source or ``None`` if should run eternally.
        """
        return None

    def params(self):
        """
        :return: A hash containing the parameters that will be provided to the corresponding operation runner (key: parameter name,
        value: parameter value).
        """
        return self._params


class DelegatingParamSource(ParamSource):
    def __init__(self, track, params, delegate, **kwargs):
        super().__init__(track, params, **kwargs)
        self.delegate = delegate

    def params(self):
        return self.delegate(self.track, self._params, **self.kwargs)


class LegacyDelegatingParamSource(ParamSource):
    def __init__(self, track, params, delegate, **kwargs):
        super().__init__(track, params, **kwargs)
        self.delegate = delegate

    def params(self):
        return self.delegate(self.track.indices, self._params)


class CreateIndexParamSource(ParamSource):
    def __init__(self, track, params, **kwargs):
        super().__init__(track, params, **kwargs)
        self.request_params = params.get("request-params", {})
        self.index_definitions = []
        if track.indices:
            filter_index = params.get("index")
            settings = params.get("settings")
            for idx in track.indices:
                if not filter_index or idx.name == filter_index:
                    body = idx.body
                    if body and settings:
                        if "settings" in body:
                            # merge (and potentially override)
                            body["settings"].update(settings)
                        else:
                            body["settings"] = settings
                    elif not body:
                        # this is just needed because we will output this in the middle of the benchmark and will thus write
                        # this on the same line as the progress message.
                        console.println("")
                        console.warn("Creating index %s based on deprecated type mappings. Please specify an index body instead. "
                                     "For details please see the migration guide in the docs." % idx.name, logger=logger)
                        # TODO #366: Deprecate this syntax. We should only specify all mappings in the body property.
                        # check all types and merge their mappings
                        body = {
                            "mappings": {}
                        }
                        if settings:
                            body["settings"] = settings
                        for t in idx.types:
                            body["mappings"].update(t.mapping)

                    self.index_definitions.append((idx.name, body))
        else:
            # TODO: Should we allow to create multiple indices at once?
            try:
                # only 'index' is mandatory, the body is optional (may be ok to create an index without a body)
                self.index_definitions.append((params["index"], params.get("body")))
            except KeyError:
                raise exceptions.InvalidSyntax("Please set the property 'index' for the create-index operation")

    def params(self):
        p = {}
        # ensure we pass all parameters...
        p.update(self._params)
        p.update({
            "indices": self.index_definitions,
            "request-params": self.request_params
        })
        return p


class DeleteIndexParamSource(ParamSource):
    def __init__(self, track, params, **kwargs):
        super().__init__(track, params, **kwargs)
        self.request_params = params.get("request-params", {})
        self.only_if_exists = params.get("only-if-exists", True)

        self.index_definitions = []
        target_index = params.get("index")
        if target_index:
            # TODO: Should we allow to delete multiple indices at once?
            self.index_definitions.append(target_index)
        elif track.indices:
            for idx in track.indices:
                self.index_definitions.append(idx.name)
        else:
            raise exceptions.InvalidSyntax("delete-index operation targets no index")

    def params(self):
        p = {}
        # ensure we pass all parameters...
        p.update(self._params)
        p.update({
            "indices": self.index_definitions,
            "request-params": self.request_params,
            "only-if-exists": self.only_if_exists
        })
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
            # TODO: Should we allow to create multiple index templates at once?
            try:
                self.template_definitions.append((params["template"], params["body"]))
            except KeyError:
                raise exceptions.InvalidSyntax("Please set the properties 'template' and 'body' for the create-index-template operation")

    def params(self):
        p = {}
        # ensure we pass all parameters...
        p.update(self._params)
        p.update({
            "templates": self.template_definitions,
            "request-params": self.request_params
        })
        return p


class DeleteIndexTemplateParamSource(ParamSource):
    def __init__(self, track, params, **kwargs):
        super().__init__(track, params, **kwargs)
        self.only_if_exists = params.get("only-if-exists", True)
        self.request_params = params.get("request-params", {})
        self.template_definitions = []
        if track.templates:
            filter_template = params.get("template")
            for template in track.templates:
                if not filter_template or template.name == filter_template:
                    self.template_definitions.append((template.name, template.delete_matching_indices, template.pattern))
        else:
            # TODO: Should we allow to delete multiple templates at once?
            try:
                template = params["template"]
            except KeyError:
                raise exceptions.InvalidSyntax("Please set the property 'template' for the delete-index-template operation")

            delete_matching = params.get("delete-matching-indices", False)
            try:
                index_pattern = params["index-pattern"] if delete_matching else None
            except KeyError:
                raise exceptions.InvalidSyntax("The property 'index-pattern' is required for delete-index-template if "
                                               "'delete-matching-indices' is true.")
            self.template_definitions.append((template, delete_matching, index_pattern))

    def params(self):
        p = {}
        # ensure we pass all parameters...
        p.update(self._params)
        p.update({
            "templates": self.template_definitions,
            "only-if-exists": self.only_if_exists,
            "request-params": self.request_params
        })
        return p


# TODO #365: This contains "body-params" as an undocumented feature. Get more experience and expand it to make it actually usable.
class SearchParamSource(ParamSource):
    def __init__(self, track, params, **kwargs):
        super().__init__(track, params, **kwargs)
        if len(track.indices) == 1:
            default_index = track.indices[0].name
            if len(track.indices[0].types) == 1:
                default_type = track.indices[0].types[0].name
            else:
                default_type = None
        else:
            default_index = None
            default_type = None

        index_name = params.get("index", default_index)
        type_name = params.get("type", default_type)
        request_cache = params.get("cache", False)
        query_body = params.get("body", None)
        query_body_params = params.get("body-params", None)
        pages = params.get("pages", None)
        items_per_page = params.get("results-per-page", None)
        request_params = params.get("request-params", {})

        self.query_params = {
            "index": index_name,
            "type": type_name,
            "use_request_cache": request_cache,
            "request_params": request_params,
            "body": query_body
        }

        if not index_name:
            raise exceptions.InvalidSyntax("'index' is mandatory")

        if pages:
            self.query_params["pages"] = pages
        if items_per_page:
            self.query_params["items_per_page"] = items_per_page

        self.query_body_params = []
        if query_body_params:
            for param, data in query_body_params.items():
                # TODO #365: Stricly check for allowed syntax. Be lenient in the pre-release and only interpret what's safely possible.
                # build path based on param
                # if not isinstance(data, list):
                #    raise exceptions.RallyError("%s in body-params defines %s but only lists are allowed. This may be a new syntax "
                #                                "that is not recognized by this version. Please upgrade Rally." % (param, data))
                if isinstance(data, list):
                    query_body_path = param.split(".")
                    b = self.query_params["body"]
                    # check early to ensure this path is actually contained in the body
                    try:
                        self.get_from_dict(b, query_body_path)
                    except KeyError:
                        raise exceptions.RallyError("The path %s could not be found within the query body %s." % (param, b))

                    self.query_body_params.append((query_body_path, data))

    def get_from_dict(self, d, path):
        v = d
        for k in path:
            v = v[k]
        return v

    def set_in_dict(self, d, path, val):
        v = d
        # navigate to the next to last path
        for k in path[:-1]:
            v = v[k]
        # the value is now the inner-most dictionary and the last path element is its key
        v[path[-1]] = val

    def params(self, choice=random.choice):
        if self.query_body_params:
            # needs to replace params first
            for path, data in self.query_body_params:
                self.set_in_dict(self.query_params["body"], path, choice(data))
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
    def __init__(self, track, params, **kwargs):
        super().__init__(track, params, **kwargs)

        self.indices = track.indices
        id_conflicts = params.get("conflicts", None)
        if not id_conflicts:
            self.id_conflicts = IndexIdConflict.NoConflicts
        elif id_conflicts == "sequential":
            self.id_conflicts = IndexIdConflict.SequentialConflicts
        elif id_conflicts == "random":
            self.id_conflicts = IndexIdConflict.RandomConflicts
        else:
            raise exceptions.InvalidSyntax("Unknown 'conflicts' setting [%s]" % id_conflicts)

        for index in self.indices:
            for t in index.types:
                if t.includes_action_and_meta_data and self.id_conflicts != IndexIdConflict.NoConflicts:
                    raise exceptions.InvalidSyntax("Cannot generate id conflicts [%s] as type [%s] in index [%s] already contains an "
                                                   "action and meta-data line." % (id_conflicts, index, t))

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
        if len(self.indices) == 1 and len(self.indices[0].types) == 1:
            default_index = self.indices[0].name
        else:
            default_index = None
        self.index_name = params.get("index", default_index)

    def partition(self, partition_index, total_partitions):
        chosen_indices = [idx for idx in self.indices if idx.matches(self.index_name)]
        if not chosen_indices:
            raise exceptions.RallyAssertionError("The provided index [%s] does not match any of the indices [%s]." %
                                                 (self.index_name, ",".join([str(i) for i in self.indices])))

        logger.info("Choosing indices [%s] for partition [%d] of [%d]." %
                    (",".join([str(i) for i in chosen_indices]), partition_index, total_partitions))
        return PartitionBulkIndexParamSource(chosen_indices, partition_index, total_partitions, self.batch_size, self.bulk_size,
                                             self.id_conflicts, self.pipeline, self._params)

    def params(self):
        raise exceptions.RallyError("Do not use a BulkIndexParamSource without partitioning")

    def size(self):
        raise exceptions.RallyError("Do not use a BulkIndexParamSource without partitioning")


class PartitionBulkIndexParamSource:
    def __init__(self, indices, partition_index, total_partitions, batch_size, bulk_size, id_conflicts=None,
                 pipeline=None, original_params=None):
        """

        :param indices: Specification of affected indices.
        :param partition_index: The current partition index.  Must be in the range [0, `total_partitions`).
        :param total_partitions: The total number of partitions (i.e. clients) for bulk index operations.
        :param batch_size: The number of documents to read in one go.
        :param bulk_size: The size of bulk index operations (number of documents per bulk).
        :param id_conflicts: The type of id conflicts.
        :param pipeline: The name of the ingest pipeline to run.
        """
        self.indices = indices
        self.partition_index = partition_index
        self.total_partitions = total_partitions
        self.batch_size = batch_size
        self.bulk_size = bulk_size
        self.id_conflicts = id_conflicts
        self.pipeline = pipeline
        self.internal_params = bulk_data_based(total_partitions, partition_index, indices, batch_size,
                                               bulk_size, id_conflicts, pipeline, original_params)

    def partition(self, partition_index, total_partitions):
        raise exceptions.RallyError("Cannot partition a PartitionBulkIndexParamSource further")

    def params(self):
        return next(self.internal_params)

    def size(self):
        return number_of_bulks(self.indices, self.partition_index, self.total_partitions, self.bulk_size)


def number_of_bulks(indices, partition_index, total_partitions, bulk_size):
    """
    :return: The number of bulk operations that the given client will issue.
    """
    bulks = 0
    for index in indices:
        for type in index.types:
            _, num_docs, _ = bounds(type.number_of_documents, partition_index, total_partitions, type.includes_action_and_meta_data)
            complete_bulks, rest = (num_docs // bulk_size, num_docs % bulk_size)
            bulks += complete_bulks
            if rest > 0:
                bulks += 1
    return bulks


def build_conflicting_ids(conflicts, docs_to_index, offset, rand=random.randint):
    if conflicts is None or conflicts == IndexIdConflict.NoConflicts:
        return None
    logger.info("building ids with id conflicts of type [%s]" % conflicts)
    all_ids = [0] * docs_to_index
    for i in range(docs_to_index):
        # always consider the offset as each client will index its own range and we don't want uncontrolled conflicts across clients
        if conflicts == IndexIdConflict.SequentialConflicts:
            all_ids[i] = "%10d" % (offset + i)
        else:  # RandomConflicts
            all_ids[i] = "%10d" % rand(offset, offset + docs_to_index)
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


def create_default_reader(index, type, offset, num_lines, num_docs, batch_size, bulk_size, id_conflicts):
    source = Slice(io.FileSource, offset, num_lines)

    if type.includes_action_and_meta_data:
        am_handler = SourceActionMetaData(source)
    else:
        am_handler = GenerateActionMetaData(index, type, build_conflicting_ids(id_conflicts, num_docs, offset))

    return IndexDataReader(type.document_file, batch_size, bulk_size, source, am_handler, index, type)


def create_readers(num_clients, client_index, indices, batch_size, bulk_size, id_conflicts, create_reader):
    readers = []
    for index in indices:
        for type in index.types:
            offset, num_docs, num_lines = bounds(type.number_of_documents, client_index, num_clients, type.includes_action_and_meta_data)
            if num_docs > 0:
                logger.info("Task-relative client at index [%d] will bulk index [%d] docs starting from line offset [%d] for [%s/%s]" %
                            (client_index, num_docs, offset, index, type))
                readers.append(create_reader(index, type, offset, num_lines, num_docs, batch_size, bulk_size, id_conflicts))
            else:
                logger.info("Task-relative client at index [%d] skips [%s/%s] (no documents to read)." % (client_index, index, type))
    return readers


def bounds(total_docs, client_index, num_clients, includes_action_and_meta_data):
    """

    Calculates the start offset and number of documents for each client.

    :param total_docs: The total number of documents to index.
    :param client_index: The current client index.  Must be in the range [0, `num_clients').
    :param num_clients: The total number of clients that will run bulk index operations.
    :param includes_action_and_meta_data: Whether the source file already includes the action and meta-data line.
    :return: A tuple containing: the start offset (in lines) for the document corpus, the number documents that the client should index,
    and the number of lines that the client should read.
    """
    source_lines_per_doc = 2 if includes_action_and_meta_data else 1

    docs_per_client = total_docs / num_clients

    start_offset_docs = round(docs_per_client * client_index)
    end_offset_docs = round(docs_per_client * (client_index + 1))

    offset_lines = start_offset_docs * source_lines_per_doc
    docs = end_offset_docs - start_offset_docs
    lines = docs * source_lines_per_doc

    return offset_lines, docs, lines


def bulk_generator(readers, client_index, pipeline, original_params):
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
                "action_metadata_present": True,
                "body": bulk,
                # This is not always equal to the bulk_size we get as parameter. The last bulk may be less than the bulk size.
                "bulk-size": docs_in_bulk,
                # a globally unique id for this bulk
                "bulk-id": "%d-%d" % (client_index, bulk_id)
            }
            if pipeline:
                bulk_params["pipeline"] = pipeline

            params = original_params.copy()
            params.update(bulk_params)
            yield params


def bulk_data_based(num_clients, client_index, indices, batch_size, bulk_size, id_conflicts, pipeline, original_params,
                    create_reader=create_default_reader):
    """
    Calculates the necessary schedule for bulk operations.

    :param num_clients: The total number of clients that will run the bulk operation.
    :param client_index: The current client for which we calculated the schedule. Must be in the range [0, `num_clients').
    :param indices: Specification of affected indices.
    :param batch_size: The number of documents to read in one go.
    :param bulk_size: The size of bulk index operations (number of documents per bulk).
    :param id_conflicts: The type of id conflicts to simulate.
    :param pipeline: Name of the ingest pipeline to use. May be None.
    :param original_params: A dict of original parameters that were passed from the track. They will be merged into the returned parameters.
    :param create_reader: A function to create the index reader. By default a file based index reader will be created. This parameter is
                      intended for testing only.
    :return: A generator for the bulk operations of the given client.
    """
    readers = create_readers(num_clients, client_index, indices, batch_size, bulk_size, id_conflicts, create_reader)
    return bulk_generator(chain(*readers), client_index, pipeline, original_params)


class GenerateActionMetaData:
    def __init__(self, index_name, type_name, conflicting_ids, rand=random.randint):
        self.index_name = index_name
        self.type_name = type_name
        self.conflicting_ids = conflicting_ids
        self.rand = rand
        self.id_up_to = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.conflicting_ids is not None:
            # 25% of the time we replace a doc:
            if self.id_up_to > 0 and self.rand(0, 3) == 3:
                doc_id = self.conflicting_ids[self.rand(0, self.id_up_to - 1)]
            else:
                doc_id = self.conflicting_ids[self.id_up_to]
                self.id_up_to += 1
            return '{"index": {"_index": "%s", "_type": "%s", "_id": "%s"}}' % (self.index_name, self.type_name, doc_id)
        else:
            return '{"index": {"_index": "%s", "_type": "%s"}}' % (self.index_name, self.type_name)


class SourceActionMetaData:
    def __init__(self, source):
        self.source = source

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.source)


class Slice:
    def __init__(self, source_class, offset, number_of_lines):
        self.source_class = source_class
        self.source = None
        self.offset = offset
        self.number_of_lines = number_of_lines
        self.current_line = 0

    def open(self, file_name, mode):
        self.source = self.source_class(file_name, mode).open()
        # skip offset number of lines
        logger.info("Skipping %d lines in [%s]." % (self.offset, file_name))
        start = time.perf_counter()
        io.skip_lines(file_name, self.source, self.offset)
        end = time.perf_counter()
        logger.info("Skipping %d lines took %f s." % (self.offset, end - start))
        return self

    def close(self):
        self.source.close()
        self.source = None

    def __iter__(self):
        return self

    def __next__(self):
        if self.current_line >= self.number_of_lines:
            raise StopIteration()
        else:
            self.current_line += 1
            line = self.source.readline()
            if len(line) == 0:
                raise StopIteration()
            return line.strip()

    def __str__(self):
        return "%s[%d;%d]" % (self.source, self.offset, self.offset + self.number_of_lines)


class IndexDataReader:
    """
    Reads a file in bulks into an array and also adds a meta-data line before each document if necessary.

    This implementation also supports batching. This means that you can specify batch_size = N * bulk_size, where N is any natural
    number >= 1. This makes file reading more efficient for small bulk sizes.
    """

    def __init__(self, data_file, batch_size, bulk_size, file_source, action_metadata, index_name, type_name):
        self.data_file = data_file
        self.batch_size = batch_size
        self.bulk_size = bulk_size
        self.file_source = file_source
        self.action_metadata = action_metadata
        self.index_name = index_name
        self.type_name = type_name

    def __enter__(self):
        self.file_source.open(self.data_file, 'rt')
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
                docs_in_bulk, bulk = self.read_bulk()
                if docs_in_bulk == 0:
                    break
                docs_in_batch += docs_in_bulk
                batch.append((docs_in_bulk, bulk))
            if docs_in_batch == 0:
                raise StopIteration()
            logger.debug("Returning a batch with %d bulks." % len(batch))
            return self.index_name, self.type_name, batch
        except IOError:
            logger.exception("Could not read [%s]" % self.data_file)

    def read_bulk(self):
        docs_in_bulk = 0
        current_bulk = []

        for action_metadata_line, document in zip(self.action_metadata, self.file_source):
            if action_metadata_line:
                current_bulk.append(action_metadata_line)
            current_bulk.append(document)
            docs_in_bulk += 1
            if docs_in_bulk == self.bulk_size:
                break
        return docs_in_bulk, current_bulk

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file_source.close()
        return False


# TODO #370: Remove this registration.
# Old name - deprecated
register_param_source_for_operation(track.OperationType.Index, BulkIndexParamSource)
# New name
register_param_source_for_operation(track.OperationType.Bulk, BulkIndexParamSource)
register_param_source_for_operation(track.OperationType.Search, SearchParamSource)
register_param_source_for_operation(track.OperationType.CreateIndex, CreateIndexParamSource)
register_param_source_for_operation(track.OperationType.DeleteIndex, DeleteIndexParamSource)
register_param_source_for_operation(track.OperationType.CreateIndexTemplate, CreateIndexTemplateParamSource)
register_param_source_for_operation(track.OperationType.DeleteIndexTemplate, DeleteIndexTemplateParamSource)

# Also register by name, so users can use it too
register_param_source_for_name("file-reader", BulkIndexParamSource)
