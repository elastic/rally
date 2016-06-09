import os
import logging
import json
import urllib.error
from enum import Enum

import jinja2
import jinja2.exceptions
import jsonschema

from esrally import exceptions, time
from esrally.utils import io, convert, net, git, versions

logger = logging.getLogger("rally.track")


class Index:
    """
    Defines an index in Elasticsearch.
    """

    def __init__(self, name, types):
        """

        Creates a new index.

        :param name: The index name. Mandatory.
        :param types: A list of types. Should contain at least one type.
        """
        self.name = name
        self.types = types

    @property
    def number_of_documents(self):
        num_docs = 0
        for t in self.types:
            num_docs += t.number_of_documents
        return num_docs


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


class Track:
    """
    A track defines the data set that is used. It corresponds loosely to a use case (e.g. logging, event processing, analytics, ...)
    """

    def __init__(self, name, short_description, description, source_root_url, challenges, indices=None):
        """

        Creates a new track.

        :param name: A short, descriptive name for this track. As per convention, this name should be in lower-case without spaces.
        :param short_description: A short description for this track (should be less than 80 characters).
        :param description: A longer description for this track.
        :param source_root_url: The publicly reachable http URL of the root folder for this track (without a trailing slash). Directly
        below this URL the benchmark document files have to be located.
        :param challenges: A list of one or more challenges to use.
        :param indices: A list of indices for this track.
        """
        self.name = name
        self.short_description = short_description
        self.description = description
        self.source_root_url = source_root_url
        self.challenges = challenges
        self.indices = indices

    @property
    def number_of_documents(self):
        num_docs = 0
        for index in self.indices:
            num_docs += index.number_of_documents
        return num_docs

    def __str__(self):
        return self.name


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


class BenchmarkPhase(Enum):
    index = 0,
    stats = 1
    search = 2,


class LatencyBenchmarkSettings:
    def __init__(self, queries=None, warmup_iteration_count=1000, iteration_count=1000):
        """
        Creates new LatencyBenchmarkSettings.

        :param queries: A list of queries to run. Optional.
        :param warmup_iteration_count: The number of times each query should be run for warmup. Defaults to 1000 iterations.
        :param iteration_count: The number of times each query should be run. Defaults to 1000 iterations.
        """
        if queries is None:
            queries = []
        self.queries = queries
        self.warmup_iteration_count = warmup_iteration_count
        self.iteration_count = iteration_count


class IndexBenchmarkSettings:
    def __init__(self, index_settings=None, clients=8, bulk_size=5000, id_conflicts=IndexIdConflict.NoConflicts,
                 force_merge=True):
        """
        :param index_settings: A hash of index-level settings that will be set when the index is created.
        :param clients: Number of concurrent clients that should index data.
        :param bulk_size: The number of documents to submit in a single bulk (Default: 5000).
        :param id_conflicts: Whether to produce index id conflicts during indexing (Default: NoConflicts).
        :param force_merge: Whether to do a force merge after the index benchmark (Default: True).
        """
        if index_settings is None:
            index_settings = {}
        self.index_settings = index_settings
        self.clients = clients
        self.bulk_size = bulk_size
        self.id_conflicts = id_conflicts
        self.force_merge = force_merge


class Challenge:
    """
    A challenge defines the concrete operations that will be done.
    """

    def __init__(self,
                 name,
                 description,
                 benchmark=None):
        if benchmark is None:
            benchmark = {}
        self.name = name
        self.description = description
        self.benchmark = benchmark

    def __str__(self):
        return self.name


class TrackSyntaxError(exceptions.InvalidSyntax):
    """
    Raised whenever a syntax problem is encountered when loading the track specification.
    """
    pass


def list_tracks(cfg):
    """

    Lists all known tracks. Note that users can specify a distribution version so if different tracks are available for
    different versions, this will be reflected in the output.

    :param cfg: The config object.
    :return: A list of tracks that are available for the provided distribution version or else for the master version.
    """
    repo = TrackRepository(cfg)
    reader = TrackFileReader(cfg)
    distribution_version = cfg.opts("source", "distribution.version", mandatory=False)
    data_root = cfg.opts("benchmarks", "local.dataset.cache")
    return [reader.read(track_name,
                        repo.track_file(distribution_version, track_name),
                        repo.track_dir(track_name),
                        "%s/%s" % (data_root, track_name.lower())
                        )
            for track_name in repo.track_names(distribution_version)]


def load_track(cfg, track_name):
    """

    Loads a track with the given name.

    :param cfg: The config object.
    :param track_name: The name of a track to load.
    :return: The loaded track.
    """
    repo = TrackRepository(cfg)
    reader = TrackFileReader(cfg)
    distribution_version = cfg.opts("source", "distribution.version", mandatory=False)
    data_root = cfg.opts("benchmarks", "local.dataset.cache")
    return reader.read(track_name, repo.track_file(distribution_version, track_name), repo.track_dir(track_name),
                       "%s/%s" % (data_root, track_name.lower()))


def prepare_track(track, cfg):
    """
    Ensures that all track data are available for running the benchmark.

    :param track: A track that is about to be run.
    :param cfg: The config object.
    """

    def download(cfg, url, local_path, size_in_bytes):
        offline = cfg.opts("system", "offline.mode")
        file_exists = os.path.isfile(local_path)

        if file_exists:
            logger.info("[%s] already exists locally. Skipping download." % local_path)
            return

        if not offline:
            logger.info("Downloading from [%s] to [%s]." % (url, local_path))
            try:
                io.ensure_dir(os.path.dirname(local_path))
                size_in_mb = round(convert.bytes_to_mb(size_in_bytes))
                # ensure output appears immediately
                print("Downloading data from %s (%s MB) ... " % (url, size_in_mb), end='', flush=True)
                net.download(url, local_path, size_in_bytes)
                print("Done")
            except urllib.error.URLError:
                logger.exception("Could not download [%s] to [%s]." % (url, local_path))

        # file must exist at this point -> verify
        if not os.path.isfile(local_path):
            if offline:
                raise exceptions.SystemSetupError(
                        "Cannot find %s. Please disable offline mode and retry again." % local_path)
            else:
                raise exceptions.SystemSetupError(
                        "Could not download from %s to %s. Please verify that data are available at %s and "
                        "check your internet connection." % (url, local_path, url))

    def decompress(data_set_path, expected_size_in_bytes):
        # we assume that track data are always compressed and try to decompress them before running the benchmark
        basename, extension = io.splitext(data_set_path)
        if not os.path.isfile(basename) or os.path.getsize(basename) != expected_size_in_bytes:
            logger.info("Unzipping track data from [%s] to [%s]." % (data_set_path, basename))
            io.decompress(data_set_path, io.dirname(data_set_path))
            extracted_bytes = os.path.getsize(basename)
            if extracted_bytes != expected_size_in_bytes:
                raise exceptions.DataError("[%s] is corrupt. Extracted [%d] bytes but [%d] bytes are expected." %
                                           (basename, extracted_bytes, expected_size_in_bytes))

    for index in track.indices:
        for type in index.types:
            if type.document_archive:
                data_url = "%s/%s" % (track.source_root_url, os.path.basename(type.document_archive))
                download(cfg, data_url, type.document_archive, type.compressed_size_in_bytes)
                decompress(type.document_archive, type.uncompressed_size_in_bytes)


class TrackRepository:
    """
    Manages track specifications.
    """

    def __init__(self, cfg):
        self.cfg = cfg
        self.name = cfg.opts("system", "track.repository")
        # If no URL is found, we consider this a local only repo (but still require that it is a git repo)
        self.url = cfg.opts("tracks", "%s.url" % self.name, mandatory=False)
        self.remote = self.url is not None and self.url.strip() != ""
        root = cfg.opts("system", "root.dir")
        track_repositories = cfg.opts("benchmarks", "track.repository.dir")
        self.tracks_dir = "%s/%s/%s" % (root, track_repositories, self.name)
        if self.remote:
            # a normal git repo with a remote
            if not git.is_working_copy(self.tracks_dir):
                git.clone(src=self.tracks_dir, remote=self.url)
            else:
                git.fetch(src=self.tracks_dir, remote=self.url)
        else:
            if not git.is_working_copy(self.tracks_dir):
                raise exceptions.SystemSetupError("'{src}' must be a git repository.\n\nPlease run:\ngit -C {src} init"
                                                  .format(src=self.tracks_dir))

    def track_names(self, distribution_version):
        self._update(distribution_version)
        return filter(lambda d: not d.startswith("."), next(os.walk(self.tracks_dir))[1])

    def track_dir(self, track_name):
        return "%s/%s" % (self.tracks_dir, track_name)

    def track_file(self, distribution_version, track_name):
        self._update(distribution_version)
        return "%s/track.json" % self.track_dir(track_name)

    def _update(self, distribution_version):
        try:
            if self.remote:
                branch = versions.best_match(git.branches(self.tracks_dir, remote=self.remote), distribution_version)
                if branch:
                    logger.info("Rebasing on '%s' in '%s' for distribution version '%s'." % (branch, self.tracks_dir, distribution_version))
                    git.rebase(self.tracks_dir, branch=branch)
                    return
                else:
                    msg = "Could not find track data remotely for distribution version %s. " \
                          "Trying to find track data locally." % distribution_version
                    logger.warn(msg)
            branch = versions.best_match(git.branches(self.tracks_dir, remote=False), distribution_version)
            if branch:
                logger.info("Checking out '%s' in '%s' for distribution version '%s'." % (branch, self.tracks_dir, distribution_version))
                git.checkout(self.tracks_dir, branch=branch)
            else:
                raise exceptions.SystemSetupError("Cannot find track data for distribution version %s" % distribution_version)
        except exceptions.SupplyError as e:
            raise exceptions.DataError("Cannot update track data in '%s': %s" % (self.tracks_dir, e))


def render_template(loader, template_name, clock=time.Clock):
    env = jinja2.Environment(loader=loader)
    env.globals["now"] = clock.now()
    env.filters["days_ago"] = time.days_ago
    template = env.get_template(template_name)

    return template.render()


def render_template_from_file(template_file_name):
    return render_template(loader=jinja2.FileSystemLoader(io.dirname(template_file_name)), template_name=io.basename(template_file_name))


class TrackFileReader:
    """
    Creates a track from a track file.
    """

    def __init__(self, cfg):
        self.cfg = cfg
        track_schema_file = "%s/resources/track-schema.json" % (self.cfg.opts("system", "rally.root"))
        self.track_schema = json.loads(open(track_schema_file).read())
        self.read_track = TrackReader()

    def read(self, track_name, track_file, mapping_dir, data_dir):
        """
        Reads a track file, verifies it against the JSON schema and if valid, creates a track.

        :param track_name: The name of the track.
        :param track_file: The complete path to the track file.
        :param mapping_dir: The directory where the mapping files for this track are stored locally.
        :param data_dir: The directory where the data file for this track are stored locally.
        :return: A corresponding track instance if the track file is valid.
        """

        logger.info("Reading track file %s." % track_file)
        try:
            rendered = render_template_from_file(track_file)
            logger.info("Final rendered track for '%s': %s" % (track_file, rendered))
            track_spec = json.loads(rendered)
        except (json.JSONDecodeError, jinja2.exceptions.TemplateError) as e:
            logger.exception("Could not load [%s]." % track_file)
            raise TrackSyntaxError("Could not load '%s'" % track_file, e)
        try:
            jsonschema.validate(track_spec, self.track_schema)
        except jsonschema.exceptions.ValidationError as ve:
            raise TrackSyntaxError(
                "Track '%s' is invalid.\n\nError details: %s\nInstance: %s\nPath: %s\nSchema path: %s"
                % (track_name, ve.message,
                   json.dumps(ve.instance, indent=4, sort_keys=True), ve.absolute_path, ve.absolute_schema_path))
        return self.read_track(track_name, track_spec, mapping_dir, data_dir)


class TrackReader:
    """
    Creates a track instances based on its parsed JSON description.
    """

    def __init__(self):
        self.name = None

    def __call__(self, track_name, track_specification, mapping_dir, data_dir):
        self.name = track_name
        short_description = self._r(track_specification, ["meta", "short-description"])
        description = self._r(track_specification, ["meta", "description"])
        source_root_url = self._r(track_specification, ["meta", "data-url"])
        indices = [self._create_index(idx, mapping_dir, data_dir) for idx in self._r(track_specification, "indices")]
        challenges = self._create_challenges(track_specification, indices)

        return Track(name=self.name, short_description=short_description, description=description,
                     source_root_url=source_root_url,
                     challenges=challenges, indices=indices)

    def _error(self, msg):
        raise TrackSyntaxError("Track '%s' is invalid. %s" % (self.name, msg))

    def _r(self, root, path, error_ctx=None, mandatory=True, default_value=None):
        if isinstance(path, str):
            path = [path]

        structure = root
        try:
            for k in path:
                structure = structure[k]
            return structure
        except KeyError:
            if mandatory:
                if error_ctx:
                    self._error("Mandatory element '%s' is missing in '%s'." % (".".join(path), error_ctx))
                else:
                    self._error("Mandatory element '%s' is missing." % ".".join(path))
            else:
                return default_value

    def _create_index(self, index_spec, mapping_dir, data_dir):
        index_name = self._r(index_spec, "name")
        types = [self._create_type(type_spec, mapping_dir, data_dir) for type_spec in self._r(index_spec, "types")]
        valid_document_data = False
        for type in types:
            if type.has_valid_document_data():
                valid_document_data = True
                break
        if not valid_document_data:
            self._error("Index '%s' is invalid. At least one of its types needs to define documents." % index_name)

        return Index(name=index_name, types=types)

    def _create_type(self, type_spec, mapping_dir, data_dir):
        compressed_docs = self._r(type_spec, "documents", mandatory=False)
        if compressed_docs:
            document_archive = "%s/%s" % (data_dir, compressed_docs)
            document_file = "%s/%s" % (data_dir, io.splitext(compressed_docs)[0])
        else:
            document_archive = None
            document_file = None

        return Type(name=self._r(type_spec, "name"),
                    mapping_file="%s/%s" % (mapping_dir, self._r(type_spec, "mapping")),
                    document_file=document_file,
                    document_archive=document_archive,
                    number_of_documents=self._r(type_spec, "document-count", mandatory=False, default_value=0),
                    compressed_size_in_bytes=self._r(type_spec, "compressed-bytes", mandatory=False),
                    uncompressed_size_in_bytes=self._r(type_spec, "uncompressed-bytes", mandatory=False)
                    )

    def _create_challenges(self, track_spec, indices):
        ops = self._parse_operations(self._r(track_spec, "operations"), indices)
        challenges = []
        for challenge in self._r(track_spec, "challenges"):
            challenge_name = self._r(challenge, "name", error_ctx="challenges")
            challenge_description = self._r(challenge, "description", error_ctx=challenge_name)
            benchmarks = {}

            operations_per_type = {}
            for op in self._r(challenge, "schedule", error_ctx=challenge_name):
                if op not in ops:
                    self._error("'schedule' for challenge '%s' contains a non-existing operation '%s'. "
                                "Please add an operation '%s' to the 'operations' block." % (challenge_name, op, op))

                benchmark_type, benchmark_spec = ops[op]
                if benchmark_type in benchmarks:
                    new_op_name = op
                    old_op_name = operations_per_type[benchmark_type]
                    self._error(
                            "'schedule' for challenge '%s' contains multiple operations of type '%s' which is currently "
                            "unsupported. Please remove one of these operations: '%s', '%s'" %
                            (challenge_name, benchmark_type, old_op_name, new_op_name))
                benchmarks[benchmark_type] = benchmark_spec
                operations_per_type[benchmark_type] = op
            challenges.append(Challenge(name=challenge_name,
                                        description=challenge_description,
                                        benchmark=benchmarks))

        return challenges

    def _parse_operations(self, ops_specs, indices):
        # key = name, value = (BenchmarkPhase instance, BenchmarkSettings instance)
        ops = {}
        for op_spec in ops_specs:
            ops_spec_name = self._r(op_spec, "name", error_ctx="operations")
            ops[ops_spec_name] = self._create_op(ops_spec_name, op_spec, indices)

        return ops

    def _create_op(self, ops_spec_name, ops_spec, indices):
        benchmark_type = self._r(ops_spec, "type")
        if benchmark_type == "index":
            id_conflicts = self._r(ops_spec, "conflicts", mandatory=False)
            if not id_conflicts:
                id_conflicts = IndexIdConflict.NoConflicts
            elif id_conflicts == "sequential":
                id_conflicts = IndexIdConflict.SequentialConflicts
            elif id_conflicts == "random":
                id_conflicts = IndexIdConflict.RandomConflicts
            else:
                raise TrackSyntaxError("Unknown conflict type '%s' for operation '%s'" % (id_conflicts, ops_spec))

            return (BenchmarkPhase.index,
                    IndexBenchmarkSettings(index_settings=self._r(ops_spec, "index-settings", error_ctx=ops_spec_name),
                                           clients=self._r(ops_spec, ["clients", "count"], error_ctx=ops_spec_name),
                                           bulk_size=self._r(ops_spec, "bulk-size", error_ctx=ops_spec_name),
                                           force_merge=self._r(ops_spec, "force-merge", error_ctx=ops_spec_name),
                                           id_conflicts=id_conflicts))
        elif benchmark_type == "search":
            # TODO: Honor clients settings
            return (BenchmarkPhase.search,
                    LatencyBenchmarkSettings(
                            queries=self._create_queries(self._r(ops_spec, "queries", error_ctx=ops_spec_name),
                                                         indices),
                            warmup_iteration_count=self._r(ops_spec, "warmup-iterations", error_ctx=ops_spec_name),
                            iteration_count=self._r(ops_spec, "iterations")))
        elif benchmark_type == "stats":
            # TODO: Honor clients settings
            return (BenchmarkPhase.stats,
                    LatencyBenchmarkSettings(
                            warmup_iteration_count=self._r(ops_spec, "warmup-iterations", error_ctx=ops_spec_name),
                            iteration_count=self._r(ops_spec, "iterations", error_ctx=ops_spec_name)))
        else:
            raise TrackSyntaxError("Unknown benchmark type '%s' for operation '%s'" % (benchmark_type, ops_spec_name))

    def _create_queries(self, queries_spec, indices):
        if len(indices) == 1 and len(indices[0].types) == 1:
            default_index = indices[0].name
            default_type = indices[0].types[0].name
        else:
            default_index = None
            default_type = None
        queries = []
        for query_spec in queries_spec:
            query_name = self._r(query_spec, "name")
            query_type = self._r(query_spec, "query-type", mandatory=False, default_value="default",
                                 error_ctx=query_name)

            index_name = self._r(query_spec, "index", mandatory=False, default_value=default_index,
                                 error_ctx=query_name)
            type_name = self._r(query_spec, "type", mandatory=False, default_value=default_type, error_ctx=query_name)

            if not index_name or not type_name:
                raise TrackSyntaxError("Query '%s' requires an index and a type." % query_name)
            request_cache = self._r(query_spec, "cache", mandatory=False, default_value=False, error_ctx=query_name)
            if query_type == "default":
                query_body = self._r(query_spec, "body", error_ctx=query_name)
                queries.append(DefaultQuery(index=index_name, type=type_name, name=query_name,
                                            body=query_body, use_request_cache=request_cache))
            elif query_type == "scroll":
                query_body = self._r(query_spec, "body", mandatory=False, error_ctx=query_name)
                pages = self._r(query_spec, "pages", error_ctx=query_name)
                items_per_page = self._r(query_spec, "results-per-page", error_ctx=query_name)
                queries.append(ScrollQuery(index=index_name, type=type_name, name=query_name, body=query_body,
                                           use_request_cache=request_cache, pages=pages, items_per_page=items_per_page))
            else:
                raise TrackSyntaxError("Unknown query type '%s' in query '%s'" % (query_type, query_name))
        return queries


class Query:
    def __init__(self, name, normalization_factor=1):
        self.name = name
        self.normalization_factor = normalization_factor

    def run(self, es):
        """
        Runs a query.

        :param es: Elasticsearch client object
        """
        raise NotImplementedError("abstract method")

    def close(self, es):
        """
        Subclasses can override this method for any custom cleanup logic

        :param es: Elasticsearch client object
        """
        pass

    def __str__(self):
        return self.name


class DefaultQuery:
    def __init__(self, index, type, name, body, use_request_cache=False):
        self.name = name
        self.normalization_factor = 1
        self.index = index
        self.type = type
        self.body = body
        self.use_request_cache = use_request_cache

    def __enter__(self):
        return self

    def __call__(self, es):
        return es.search(index=self.index, doc_type=self.type, request_cache=self.use_request_cache, body=self.body)

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def __str__(self):
        return self.name


class ScrollQuery:
    def __init__(self, index, type, name, body, use_request_cache, pages, items_per_page):
        self.name = name
        self.normalization_factor = pages
        self.index = index
        self.type = type
        self.pages = pages
        self.items_per_page = items_per_page
        self.body = body
        self.use_request_cache = use_request_cache
        self.scroll_id = None
        self.es = None

    def __enter__(self):
        return self

    def __call__(self, es):
        self.es = es
        r = es.search(
                index=self.index,
                doc_type=self.type,
                body=self.body,
                sort="_doc",
                scroll="10s",
                size=self.items_per_page,
                request_cache=self.use_request_cache)
        self.scroll_id = r["_scroll_id"]
        # Note that starting with ES 2.0, the initial call to search() returns already the first result page
        # so we have to retrieve one page less
        for i in range(self.pages - 1):
            hit_count = len(r["hits"]["hits"])
            if hit_count == 0:
                # done
                break
            r = es.scroll(scroll_id=self.scroll_id, scroll="10s")

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.scroll_id and self.es:
            self.es.clear_scroll(scroll_id=self.scroll_id)
            self.scroll_id = None
            self.es = None
        return False

    def __str__(self):
        return self.name
