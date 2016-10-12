import json
import logging
import os

import urllib.error
from enum import Enum

import jinja2
import jinja2.exceptions
import jsonschema
import tabulate

from esrally import exceptions, time, PROGRAM_NAME
from esrally.utils import io, convert, net, git, versions, console

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


class TrackSyntaxError(exceptions.InvalidSyntax):
    """
    Raised whenever a syntax problem is encountered when loading the track specification.
    """
    pass


def tracks(cfg):
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


def list_tracks(cfg):
    console.println("Available tracks:\n")
    console.println(tabulate.tabulate(
        tabular_data=[[t.name, t.short_description, ",".join(map(str, t.challenges))] for t in tracks(cfg)],
        headers=["Name", "Description", "Challenges"]))


def load_track(cfg):
    """

    Loads a track

    :param cfg: The config object. It contains the name of the track to load.
    :return: The loaded track.
    """
    track_name = cfg.opts("benchmarks", "track")
    try:
        repo = TrackRepository(cfg)
        reader = TrackFileReader(cfg)
        distribution_version = cfg.opts("source", "distribution.version", mandatory=False)
        data_root = cfg.opts("benchmarks", "local.dataset.cache")
        return reader.read(track_name, repo.track_file(distribution_version, track_name), repo.track_dir(track_name),
                           "%s/%s" % (data_root, track_name.lower()))
    except FileNotFoundError:
        logger.exception("Cannot load track [%s]" % track_name)
        raise exceptions.SystemSetupError("Cannot load track %s. You can list the available tracks with %s list tracks." %
                                          (track_name, PROGRAM_NAME))


def prepare_track(track, cfg):
    """
    Ensures that all track data are available for running the benchmark.

    :param track: A track that is about to be run.
    :param cfg: The config object.
    """

    def download(cfg, url, local_path, size_in_bytes):
        offline = cfg.opts("system", "offline.mode")
        file_exists = os.path.isfile(local_path)

        # ensure we only skip the download if the file size also matches our expectation
        if file_exists and os.path.getsize(local_path) == size_in_bytes:
            logger.info("[%s] already exists locally. Skipping download." % local_path)
            return False

        if not offline:
            try:
                io.ensure_dir(os.path.dirname(local_path))
                size_in_mb = round(convert.bytes_to_mb(size_in_bytes))
                # ensure output appears immediately
                console.println("Downloading data from [%s] (%s MB) to [%s] ... "
                                % (url, size_in_mb, local_path), end='', flush=True, logger=logger.info)
                net.download(url, local_path, size_in_bytes)
                console.println("Done")
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

        actual_size = os.path.getsize(local_path)
        if actual_size != size_in_bytes:
            raise exceptions.DataError("[%s] is corrupt. Downloaded [%d] bytes but [%d] bytes are expected." %
                                       (local_path, actual_size, size_in_bytes))

        return True

    def decompress(data_set_path, expected_size_in_bytes):
        # we assume that track data are always compressed and try to decompress them before running the benchmark
        basename, extension = io.splitext(data_set_path)
        decompressed = False
        if not os.path.isfile(basename) or os.path.getsize(basename) != expected_size_in_bytes:
            decompressed = True
            console.println("Decompressing track data from [%s] to [%s] (resulting size: %.2f GB) ... " %
                  (data_set_path, basename, convert.bytes_to_gb(type.uncompressed_size_in_bytes)), end='', flush=True, logger=logger.info)
            io.decompress(data_set_path, io.dirname(data_set_path))
            console.println("Done")
            extracted_bytes = os.path.getsize(basename)
            if extracted_bytes != expected_size_in_bytes:
                raise exceptions.DataError("[%s] is corrupt. Extracted [%d] bytes but [%d] bytes are expected." %
                                           (basename, extracted_bytes, expected_size_in_bytes))
        return basename, decompressed

    for index in track.indices:
        for type in index.types:
            if type.document_archive:
                data_url = "%s/%s" % (track.source_root_url, os.path.basename(type.document_archive))
                download(cfg, data_url, type.document_archive, type.compressed_size_in_bytes)
                decompressed_file_path, was_decompressed = decompress(type.document_archive, type.uncompressed_size_in_bytes)
                # just rebuild the file every time for the time being. Later on, we might check the data file fingerprint to avoid it
                io.prepare_file_offset_table(decompressed_file_path)


class TrackRepository:
    """
    Manages track specifications.
    """

    def __init__(self, cfg):
        self.cfg = cfg
        self.name = cfg.opts("system", "track.repository")
        self.offline = cfg.opts("system", "offline.mode")
        # If no URL is found, we consider this a local only repo (but still require that it is a git repo)
        self.url = cfg.opts("tracks", "%s.url" % self.name, mandatory=False)
        self.remote = self.url is not None and self.url.strip() != ""
        root = cfg.opts("system", "root.dir")
        track_repositories = cfg.opts("benchmarks", "track.repository.dir")
        self.tracks_dir = "%s/%s/%s" % (root, track_repositories, self.name)
        if self.remote and not self.offline:
            # a normal git repo with a remote
            if not git.is_working_copy(self.tracks_dir):
                git.clone(src=self.tracks_dir, remote=self.url)
            else:
                git.fetch(src=self.tracks_dir)
        else:
            if not git.is_working_copy(self.tracks_dir):
                raise exceptions.SystemSetupError("[{src}] must be a git repository.\n\nPlease run:\ngit -C {src} init"
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
            if self.remote and not self.offline:
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
            index_settings = self._r(challenge, "index-settings", error_ctx=challenge_name, mandatory=False)

            schedule = []

            for op in self._r(challenge, "schedule", error_ctx=challenge_name):
                if "parallel" in op:
                    task = self.parse_parallel(op["parallel"], ops, challenge_name)
                else:
                    task = self.parse_task(op, ops, challenge_name)
                schedule.append(task)

            challenges.append(Challenge(name=challenge_name,
                                        description=challenge_description,
                                        index_settings=index_settings,
                                        schedule=schedule))
        return challenges

    def parse_parallel(self, ops_spec, ops, challenge_name):
        default_warmup_iterations = self._r(ops_spec, "warmup-iterations", error_ctx="parallel", mandatory=False)
        default_iterations = self._r(ops_spec, "iterations", error_ctx="parallel", mandatory=False)
        clients = self._r(ops_spec, "clients", error_ctx="parallel", mandatory=False)

        # now descent to each operation
        tasks = []
        for task in self._r(ops_spec, "tasks", error_ctx="parallel"):
            tasks.append(self.parse_task(task, ops, challenge_name, default_warmup_iterations, default_iterations))
        return Parallel(tasks, clients)

    def parse_task(self, task_spec, ops, challenge_name, default_warmup_iterations=0, default_iterations=1):
        op_name = task_spec["operation"]
        if op_name not in ops:
            self._error("'schedule' for challenge '%s' contains a non-existing operation '%s'. "
                        "Please add an operation '%s' to the 'operations' block." % (challenge_name, op_name, op_name))
        return Task(operation=ops[op_name],
                    warmup_iterations=self._r(task_spec, "warmup-iterations", error_ctx=op_name, mandatory=False,
                                              default_value=default_warmup_iterations),
                    warmup_time_period=self._r(task_spec, "warmup-time-period", error_ctx=op_name, mandatory=False,
                                               default_value=0),
                    iterations=self._r(task_spec, "iterations", error_ctx=op_name, mandatory=False, default_value=default_iterations),
                    clients=self._r(task_spec, "clients", error_ctx=op_name, mandatory=False, default_value=1),
                    target_throughput=self._r(task_spec, "target-throughput", error_ctx=op_name, mandatory=False))

    def _parse_operations(self, ops_specs, indices):
        # key = name, value = operation
        ops = {}
        for op_spec in ops_specs:
            ops_spec_name = self._r(op_spec, "name", error_ctx="operations")
            ops[ops_spec_name] = self._create_op(ops_spec_name, op_spec, indices)

        return ops

    def _create_op(self, ops_spec_name, ops_spec, indices):
        benchmark_type = self._r(ops_spec, "operation-type")
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

            bulk_size = self._r(ops_spec, "bulk-size", error_ctx=ops_spec_name)
            pipeline = self._r(ops_spec, "pipeline", error_ctx=ops_spec_name, mandatory=False)

            params = {
                "bulk-size": bulk_size,
                "id-conflicts": id_conflicts,
                "pipeline": pipeline
            }

            return Operation(name=ops_spec_name,
                             operation_type=OperationType.Index,
                             params=params,
                             granularity_unit="docs/s")
        elif benchmark_type == "force-merge":
            return Operation(name=ops_spec_name, operation_type=OperationType.ForceMerge,
                             params={"indices": [index.name for index in indices]})
        elif benchmark_type == "search":
            return self._create_query(ops_spec, ops_spec_name, indices)
        elif benchmark_type == "index-stats":
            return Operation(name=ops_spec_name, operation_type=OperationType.IndicesStats)
        elif benchmark_type == "node-stats":
            return Operation(name=ops_spec_name, operation_type=OperationType.NodesStats)
        else:
            raise TrackSyntaxError("Unknown benchmark type '%s' for operation '%s'" % (benchmark_type, ops_spec_name))

    def _create_query(self, query_spec, ops_spec_name, indices):
        if len(indices) == 1 and len(indices[0].types) == 1:
            default_index = indices[0].name
            default_type = indices[0].types[0].name
        else:
            default_index = None
            default_type = None

        index_name = self._r(query_spec, "index", mandatory=False, default_value=default_index, error_ctx=ops_spec_name)
        type_name = self._r(query_spec, "type", mandatory=False, default_value=default_type, error_ctx=ops_spec_name)
        request_cache = self._r(query_spec, "cache", mandatory=False, default_value=False, error_ctx=ops_spec_name)

        query_body = self._r(query_spec, "body", mandatory=False, error_ctx=ops_spec_name)
        pages = self._r(query_spec, "pages", mandatory=False, error_ctx=ops_spec_name)
        items_per_page = self._r(query_spec, "results-per-page", mandatory=False, error_ctx=ops_spec_name)

        params = {
            "index": index_name,
            "type": type_name,
            "name": ops_spec_name,
            "use_request_cache": request_cache,
            "body": query_body
        }

        if not index_name:
            raise TrackSyntaxError("Query '%s' requires an index." % ops_spec_name)

        if pages:
            params["pages"] = pages
        if items_per_page:
            params["items_per_page"] = items_per_page

        return Operation(name=ops_spec_name,
                         operation_type=OperationType.Search,
                         params=params)


class OperationType(Enum):
    Index = 0,
    ForceMerge = 1,
    IndicesStats = 2,
    NodesStats = 3,
    Search = 4


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
    def __init__(self, operation, warmup_iterations=0, warmup_time_period=0, iterations=1, clients=1, target_throughput=None):
        self.operation = operation
        self.warmup_iterations = warmup_iterations
        self.warmup_time_period = warmup_time_period
        self.iterations = iterations
        self.clients = clients
        self.target_throughput = target_throughput

    def __iter__(self):
        return iter([self])

    def __repr__(self, *args, **kwargs):
        return "Task for [%s]" % self.operation.name


class Operation:
    def __init__(self, name, operation_type, granularity_unit="ops/s", params=None):
        if params is None:
            params = {}
        self.name = name
        self.type = operation_type
        self.granularity_unit = granularity_unit
        self.params = params

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == other.name

    def __repr__(self, *args, **kwargs):
        return self.name
