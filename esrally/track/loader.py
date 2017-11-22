import json
import logging
import os
import glob
import urllib.error

import jinja2
import jinja2.exceptions
import jsonschema
import tabulate
from esrally import exceptions, time, PROGRAM_NAME
from esrally.track import params, track
from esrally.utils import io, convert, net, console, modules, repo

logger = logging.getLogger("rally.track")


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
    repo = track_repo(cfg)
    reader = TrackFileReader(cfg)
    return [reader.read(track_name,
                        repo.track_file(track_name),
                        repo.track_dir(track_name))
            for track_name in repo.track_names]


def list_tracks(cfg):
    console.println("Available tracks:\n")
    console.println(tabulate.tabulate(
        tabular_data=[
            [t.name, t.description, t.number_of_documents, convert.bytes_to_human_string(t.compressed_size_in_bytes),
             convert.bytes_to_human_string(t.uncompressed_size_in_bytes), t.default_challenge,
             ",".join(map(str, t.challenges))] for t in tracks(cfg)
        ],
        headers=["Name", "Description", "Documents", "Compressed Size", "Uncompressed Size", "Default Challenge", "All Challenges"]))


def load_track(cfg):
    """

    Loads a track

    :param cfg: The config object. It contains the name of the track to load.
    :return: The loaded track.
    """
    track_name = None
    try:
        repo = track_repo(cfg)
        track_name = repo.track_name
        track_dir = repo.track_dir(track_name)
        reader = TrackFileReader(cfg)
        included_tasks = cfg.opts("track", "include.tasks")

        current_track = reader.read(track_name, repo.track_file(track_name), track_dir)
        current_track = filter_included_tasks(current_track, filters_from_included_tasks(included_tasks))
        plugin_reader = TrackPluginReader(track_dir)
        current_track.has_plugins = plugin_reader.can_load()

        if cfg.opts("track", "test.mode.enabled"):
            return post_process_for_test_mode(current_track)
        else:
            return current_track
    except FileNotFoundError:
        logger.exception("Cannot load track [%s]" % track_name)
        raise exceptions.SystemSetupError("Cannot load track %s. List the available tracks with %s list tracks." %
                                          (track_name, PROGRAM_NAME))


def load_track_plugins(cfg, register_runner, register_scheduler):
    repo = track_repo(cfg, fetch=False, update=False)
    track_name = repo.track_name
    track_plugin_path = repo.track_dir(track_name)

    plugin_reader = TrackPluginReader(track_plugin_path, register_runner, register_scheduler)

    if plugin_reader.can_load():
        plugin_reader.load()
    else:
        logger.debug("Track [%s] in path [%s] does not define any track plugins." % (track_name, track_plugin_path))


def set_absolute_data_path(cfg, t):
    """
    Sets an absolute data path on all document files in this track. Internally we store only relative paths in the track as long as possible
    as the data root directory may be different on each host. In the end we need to have an absolute path though when we want to read the
    file on the target host.

    :param cfg: The config object.
    :param t: The track to modify.
    """
    data_root = data_dir(cfg, t.name)
    for index in t.indices:
        for t in index.types:
            if t.document_archive:
                t.document_archive = os.path.join(data_root, t.document_archive)
            if t.document_file:
                t.document_file = os.path.join(data_root, t.document_file)


def is_simple_track_mode(cfg):
    return cfg.exists("track", "track.path")


def track_repo(cfg, fetch=True, update=True):
    if is_simple_track_mode(cfg):
        track_path = cfg.opts("track", "track.path")
        return SimpleTrackRepository(track_path)
    else:
        return GitTrackRepository(cfg, fetch, update)


def data_dir(cfg, track_name):
    if is_simple_track_mode(cfg):
        track_path = cfg.opts("track", "track.path")
        r = SimpleTrackRepository(track_path)
        # data should always be stored in the track's directory. If the user uses the same directory on all machines this will even work
        # in the distributed case. However, the user is responsible to ensure that this is actually the case.
        return r.track_dir(track_name)
    else:
        return os.path.join(cfg.opts("benchmarks", "local.dataset.cache"), track_name)


class GitTrackRepository:
    def __init__(self, cfg, fetch, update, repo_class=repo.RallyRepository):
        # current track name (if any)
        self.track_name = cfg.opts("track", "track.name", mandatory=False)
        distribution_version = cfg.opts("mechanic", "distribution.version", mandatory=False)
        repo_name = cfg.opts("track", "repository.name")
        offline = cfg.opts("system", "offline.mode")
        remote_url = cfg.opts("tracks", "%s.url" % repo_name, mandatory=False)
        root = cfg.opts("node", "root.dir")
        track_repositories = cfg.opts("benchmarks", "track.repository.dir")
        tracks_dir = os.path.join(root, track_repositories)

        self.repo = repo_class(remote_url, tracks_dir, repo_name, "tracks", offline, fetch)
        if update:
            self.repo.update(distribution_version)

    @property
    def track_names(self):
        return filter(lambda p: os.path.exists(self.track_file(p)), next(os.walk(self.repo.repo_dir))[1])

    def track_dir(self, track_name):
        return os.path.join(self.repo.repo_dir, track_name)

    def track_file(self, track_name):
        return os.path.join(self.track_dir(track_name), "track.json")


class SimpleTrackRepository:
    def __init__(self, track_path):
        if not os.path.exists(track_path):
            raise exceptions.SystemSetupError("Track path %s does not exist" % track_path)

        if os.path.isdir(track_path):
            self.track_name = io.basename(track_path)
            self._track_dir = track_path
            self._track_file = os.path.join(track_path, "track.json")
            if not os.path.exists(self._track_file):
                raise exceptions.SystemSetupError("Could not find track.json in %s" % track_path)
        elif os.path.isfile(track_path):
            if io.has_extension(track_path, ".json"):
                self._track_dir = io.dirname(track_path)
                self._track_file = track_path
                self.track_name = io.splitext(io.basename(track_path))[0]
            else:
                raise exceptions.SystemSetupError("%s has to be a JSON file" % track_path)
        else:
            raise exceptions.SystemSetupError("%s is neither a file nor a directory" % track_path)

    @property
    def track_names(self):
        return [self.track_name]

    def track_dir(self, track_name):
        assert track_name == self.track_name
        return self._track_dir

    def track_file(self, track_name):
        assert track_name == self.track_name
        return self._track_file


def operation_parameters(t, op):
    if op.param_source:
        logger.debug("Creating parameter source with name [%s]" % op.param_source)
        return params.param_source_for_name(op.param_source, t.indices, op.params)
    else:
        logger.debug("Creating parameter source for operation type [%s]" % op.type)
        return params.param_source_for_operation(op.type, t.indices, op.params)


def prepare_track(track, cfg):
    """
    Ensures that all track data are available for running the benchmark.

    :param track: A track that is about to be run.
    :param cfg: The config object.
    """
    if not track.source_root_url:
        logger.info("Track [%s] does not specify a source root URL. Assuming data are available locally." % track.name)

    data_root = data_dir(cfg, track.name)
    logger.info("Resolved data root directory for track [%s] to [%s]." % (track.name, data_root))
    for index in track.indices:
        for type in index.types:
            if type.has_valid_document_data():
                offline = cfg.opts("system", "offline.mode")
                test_mode = cfg.opts("track", "test.mode.enabled")
                prepare_corpus(track.name, track.source_root_url, data_root, type, offline, test_mode)
            else:
                logger.info("Type [%s] in index [%s] does not define documents. No data are indexed from a file for this type." %
                            (type.name, index.name))


def prepare_corpus(track_name, source_root_url, data_root, type, offline, test_mode):
    def is_locally_available(file_name, expected_size):
        return os.path.isfile(file_name) and (expected_size is None or os.path.getsize(file_name) == expected_size)

    def decompress_corpus(archive_path, documents_path, uncompressed_size):
        if uncompressed_size:
            console.info("Decompressing track data from [%s] to [%s] (resulting size: %.2f GB) ... " %
                         (archive_path, documents_path, convert.bytes_to_gb(uncompressed_size)),
                         end='', flush=True, logger=logger)
        else:
            console.info("Decompressing track data from [%s] to [%s] ... " % (archive_path, documents_path), end='',
                         flush=True, logger=logger)

        io.decompress(archive_path, io.dirname(archive_path))
        console.println("[OK]")
        if not os.path.isfile(documents_path):
            raise exceptions.DataError("Decompressing [%s] did not create [%s]. Please check with the track author if the compressed "
                                       "archive has been created correctly." % (archive_path, documents_path))

        extracted_bytes = os.path.getsize(documents_path)
        if uncompressed_size is not None and extracted_bytes != uncompressed_size:
            raise exceptions.DataError("[%s] is corrupt. Extracted [%d] bytes but [%d] bytes are expected." %
                                       (documents_path, extracted_bytes, uncompressed_size))

    def download_corpus(root_url, target_path, size_in_bytes, track_name, offline, test_mode):
        file_name = os.path.basename(target_path)

        if not root_url:
            raise exceptions.DataError("%s is missing and it cannot be downloaded because no source URL is provided in the track."
                                       % target_path)
        if offline:
            raise exceptions.SystemSetupError("Cannot find %s. Please disable offline mode and retry again." % target_path)

        data_url = "%s/%s" % (source_root_url, file_name)
        try:
            io.ensure_dir(os.path.dirname(target_path))
            if size_in_bytes:
                size_in_mb = round(convert.bytes_to_mb(size_in_bytes))
                logger.info("Downloading data from [%s] (%s MB) to [%s]." % (data_url, size_in_mb, target_path))
            else:
                logger.info("Downloading data from [%s] to [%s]." % (data_url, target_path))

            # we want to have a bit more accurate download progress as these files are typically very large
            progress = net.Progress("[INFO] Downloading data for track %s" % track_name, accuracy=1)
            net.download(data_url, target_path, size_in_bytes, progress_indicator=progress)
            progress.finish()
            logger.info("Downloaded data from [%s] to [%s]." % (data_url, target_path))
        except urllib.error.HTTPError as e:
            if e.code == 404 and test_mode:
                raise exceptions.DataError("Track [%s] does not support test mode. Please ask the track author to add it or "
                                           "disable test mode and retry." % track_name)
            else:
                msg = "Could not download [%s] to [%s]" % (data_url, target_path)
                if e.reason:
                    msg += " (HTTP status: %s, reason: %s)" % (str(e.code), e.reason)
                else:
                    msg += " (HTTP status: %s)" % str(e.code)
                raise exceptions.DataError(msg)
        except urllib.error.URLError:
            logger.exception("Could not download [%s] to [%s]." % (data_url, target_path))
            raise exceptions.DataError("Could not download [%s] to [%s]." % (data_url, target_path))

        if not os.path.isfile(target_path):
            raise exceptions.SystemSetupError(
                "Cannot download from %s to %s. Please verify that data are available at %s and "
                "check your internet connection." % (data_url, target_path, data_url))

        actual_size = os.path.getsize(target_path)
        if size_in_bytes is not None and actual_size != size_in_bytes:
            raise exceptions.DataError("[%s] is corrupt. Downloaded [%d] bytes but [%d] bytes are expected." %
                                       (target_path, actual_size, size_in_bytes))

    def create_file_offset_table(document_file_path, expected_number_of_lines):
        # just rebuild the file every time for the time being. Later on, we might check the data file fingerprint to avoid it
        lines_read = io.prepare_file_offset_table(document_file_path)
        if lines_read and lines_read != expected_number_of_lines:
            io.remove_file_offset_table(document_file_path)
            raise exceptions.DataError("Data in [%s] for track [%s] are invalid. Expected [%d] lines but got [%d]."
                                       % (document_file_path, track, expected_number_of_lines, lines_read))

    full_document_path = os.path.join(data_root, type.document_file)
    full_archive_path = os.path.join(data_root, type.document_archive) if type.has_compressed_corpus() else None
    while True:
        if is_locally_available(full_document_path, type.uncompressed_size_in_bytes):
            break
        elif type.has_compressed_corpus() and is_locally_available(full_archive_path, type.compressed_size_in_bytes):
            decompress_corpus(full_archive_path, full_document_path, type.uncompressed_size_in_bytes)
        else:
            if type.has_compressed_corpus():
                target_path = full_archive_path
                expected_size = type.compressed_size_in_bytes
            elif type.has_uncompressed_corpus():
                target_path = full_document_path
                expected_size = type.uncompressed_size_in_bytes
            else:
                # this should not happen in practice as the JSON schema should take care of this
                raise exceptions.RallyAssertionError("Track %s specifies documents but neither a compressed nor an uncompressed corpus" %
                                                     track_name)
            download_corpus(source_root_url, target_path, expected_size, track_name, offline, test_mode)

    create_file_offset_table(full_document_path, type.number_of_lines)


def render_template(loader, template_name, template_vars=None, glob_helper=lambda f: [], clock=time.Clock):
    macros = """
        {% macro collect(parts) -%}
            {% set comma = joiner() %}
            {% for part in glob(parts) %}
                {{ comma() }}
                {% include part %}
            {% endfor %}
        {%- endmacro %}
    """

    # place helpers dict loader first to prevent users from overriding our macros.
    env = jinja2.Environment(
        loader=jinja2.ChoiceLoader([jinja2.DictLoader({"rally.helpers": macros}), loader])
    )
    if template_vars:
        for k, v in template_vars.items():
            env.globals[k] = v
    # ensure that user variables never override our internal variables
    env.globals["now"] = clock.now()
    env.globals["glob"] = glob_helper
    env.filters["days_ago"] = time.days_ago
    template = env.get_template(template_name)

    return template.render()


def render_template_from_file(template_file_name, template_vars):
    def relative_glob(start, f):
        result = glob.glob(os.path.join(start, f))
        if result:
            return [os.path.relpath(p, start) for p in result]
        else:
            return []

    base_path = io.dirname(template_file_name)
    return render_template(loader=jinja2.FileSystemLoader(base_path),
                           template_name=io.basename(template_file_name),
                           template_vars=template_vars,
                           glob_helper=lambda f: relative_glob(base_path, f))


def filter_included_tasks(t, filters):
    def match(task, filters):
        for f in filters:
            if task.matches(f):
                return True
        return False

    if not filters:
        return t
    else:
        for challenge in t.challenges:
            # don't modify the schedule while iterating over it
            tasks_to_remove = []
            for task in challenge.schedule:
                if not match(task, filters):
                    tasks_to_remove.append(task)
                else:
                    leafs_to_remove = []
                    for leaf_task in task:
                        if not match(leaf_task, filters):
                            leafs_to_remove.append(leaf_task)
                    for leaf_task in leafs_to_remove:
                        logger.info("Removing sub-task [%s] from challenge [%s] due to task filter." % (leaf_task, challenge))
                        task.remove_task(leaf_task)
            for task in tasks_to_remove:
                logger.info("Removing task [%s] from challenge [%s] due to task filter." % (task, challenge))
                challenge.remove_task(task)

    return t


def filters_from_included_tasks(included_tasks):
    filters = []
    if included_tasks:
        for t in included_tasks:
            spec = t.split(":")
            if len(spec) == 1:
                filters.append(track.TaskNameFilter(spec[0]))
            elif len(spec) == 2:
                if spec[0] == "type":
                    filters.append(track.TaskOpTypeFilter(spec[1]))
                else:
                    raise exceptions.SystemSetupError("Invalid format for included tasks: [%s]. Expected [type] but got [%s]." % (t, spec[0]))
            else:
                raise exceptions.SystemSetupError("Invalid format for included tasks: [%s]" % t)
    return filters


def post_process_for_test_mode(t):
    logger.info("Preparing track [%s] for test mode." % str(t))
    for index in t.indices:
        for type in index.types:
            if type.has_valid_document_data():
                logger.info("Reducing corpus size to 1000 documents for [%s/%s]" % (index, type))
                type.number_of_documents = 1000

                path, ext = io.splitext(type.document_archive)
                path_2, ext_2 = io.splitext(path)

                type.document_archive = "%s-1k%s%s" % (path_2, ext_2, ext)
                type.document_file = "%s-1k%s" % (path_2, ext_2)
                # we don't want to check sizes
                type.compressed_size_in_bytes = None
                type.uncompressed_size_in_bytes = None

    for challenge in t.challenges:
        for task in challenge.schedule:
            # we need iterate over leaf tasks and await iterating over possible intermediate 'parallel' elements
            for leaf_task in task:
                # iteration-based schedules are divided among all clients and we should provide at least one iteration for each client.
                if leaf_task.warmup_iterations > leaf_task.clients:
                    count = leaf_task.clients
                    logger.info("Resetting warmup iterations to %d for [%s]" % (count, str(leaf_task)))
                    leaf_task.warmup_iterations = count
                if leaf_task.iterations > leaf_task.clients:
                    count = leaf_task.clients
                    logger.info("Resetting measurement iterations to %d for [%s]" % (count, str(leaf_task)))
                    leaf_task.iterations = count
                if leaf_task.warmup_time_period is not None and leaf_task.warmup_time_period > 0:
                    leaf_task.warmup_time_period = 0
                    logger.info("Resetting warmup time period for [%s] to [%d] seconds." % (str(leaf_task), leaf_task.warmup_time_period))
                if leaf_task.time_period is not None and leaf_task.time_period > 10:
                    leaf_task.time_period = 10
                    logger.info("Resetting measurement time period for [%s] to [%d] seconds." % (str(leaf_task), leaf_task.time_period))
    return t


class TrackFileReader:
    MAXIMUM_SUPPORTED_TRACK_VERSION = 1
    """
    Creates a track from a track file.
    """

    def __init__(self, cfg):
        track_schema_file = "%s/resources/track-schema.json" % (cfg.opts("node", "rally.root"))
        self.track_schema = json.loads(open(track_schema_file).read())
        override_auto_manage_indices = cfg.opts("track", "auto_manage_indices")
        self.track_params = cfg.opts("track", "params")
        self.read_track = TrackSpecificationReader(override_auto_manage_indices)

    def read(self, track_name, track_spec_file, mapping_dir):
        """
        Reads a track file, verifies it against the JSON schema and if valid, creates a track.

        :param track_name: The name of the track.
        :param track_spec_file: The complete path to the track specification file.
        :param mapping_dir: The directory where the mapping files for this track are stored locally.
        :return: A corresponding track instance if the track file is valid.
        """

        logger.info("Reading track specification file [%s]." % track_spec_file)
        try:
            rendered = render_template_from_file(track_spec_file, self.track_params)
            logger.info("Final rendered track for '%s': %s" % (track_spec_file, rendered))
            track_spec = json.loads(rendered)
        except (json.JSONDecodeError, jinja2.exceptions.TemplateError) as e:
            logger.exception("Could not load [%s]." % track_spec_file)
            raise TrackSyntaxError("Could not load '%s'" % track_spec_file, e)
        # check the track version before even attempting to validate the JSON format to avoid bogus errors.
        raw_version = track_spec.get("version", TrackFileReader.MAXIMUM_SUPPORTED_TRACK_VERSION)
        try:
            track_version = int(raw_version)
        except ValueError:
            raise exceptions.InvalidSyntax("version identifier for track %s must be numeric but was [%s]" % (track_name, str(raw_version)))
        if TrackFileReader.MAXIMUM_SUPPORTED_TRACK_VERSION < track_version:
            raise exceptions.RallyError("Track %s requires a newer version of Rally. Please upgrade Rally (supported track version: %d, "
                                        "required track version: %d)" %
                                        (track_name, TrackFileReader.MAXIMUM_SUPPORTED_TRACK_VERSION, track_version))
        try:
            jsonschema.validate(track_spec, self.track_schema)
        except jsonschema.exceptions.ValidationError as ve:
            raise TrackSyntaxError(
                "Track '%s' is invalid.\n\nError details: %s\nInstance: %s\nPath: %s\nSchema path: %s"
                % (track_name, ve.message,
                   json.dumps(ve.instance, indent=4, sort_keys=True), ve.absolute_path, ve.absolute_schema_path))
        return self.read_track(track_name, track_spec, mapping_dir)


class TrackPluginReader:
    """
    Loads track plugins
    """
    def __init__(self, track_plugin_path, runner_registry=None, scheduler_registry=None):
        self.runner_registry = runner_registry
        self.scheduler_registry = scheduler_registry
        self.loader = modules.ComponentLoader(root_path=track_plugin_path, component_entry_point="track")

    def can_load(self):
        return self.loader.can_load()

    def load(self):
        root_module = self.loader.load()
        try:
            # every module needs to have a register() method
            root_module.register(self)
        except BaseException:
            msg = "Could not register track plugin at [%s]" % self.loader.root_path
            logger.exception(msg)
            raise exceptions.SystemSetupError(msg)

    def register_param_source(self, name, param_source):
        params.register_param_source_for_name(name, param_source)

    def register_runner(self, name, runner):
        self.runner_registry(name, runner)

    def register_scheduler(self, name, scheduler):
        self.scheduler_registry(name, scheduler)


class TrackSpecificationReader:
    """
    Creates a track instances based on its parsed JSON description.
    """

    def __init__(self, override_auto_manage_indices=None, source=io.FileSource):
        self.name = None
        self.override_auto_manage_indices = override_auto_manage_indices
        self.source = source
        self.index_op_type_warning_issued = False

    def __call__(self, track_name, track_specification, mapping_dir):
        self.name = track_name
        description = self._r(track_specification, "description", mandatory=False, default_value="")
        source_root_url = self._r(track_specification, "data-url", mandatory=False)
        meta_data = self._r(track_specification, "meta", mandatory=False)
        indices = [self._create_index(idx, mapping_dir)
                   for idx in self._r(track_specification, "indices", mandatory=False, default_value=[])]
        templates = [self._create_template(tpl, mapping_dir)
                     for tpl in self._r(track_specification, "templates", mandatory=False, default_value=[])]
        challenges = self._create_challenges(track_specification)

        # This can be valid, e.g. for a search only benchmark
        # if len(indices) == 0 and len(templates) == 0:
        #    self._error("Specify at least one index or one template.")

        return track.Track(name=self.name, meta_data=meta_data, description=description, source_root_url=source_root_url,
                           challenges=challenges, indices=indices, templates=templates)

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

    def _create_index(self, index_spec, mapping_dir):
        index_name = self._r(index_spec, "name")
        if self.override_auto_manage_indices is not None:
            auto_managed = self.override_auto_manage_indices
            logger.info("User explicitly forced auto-managed indices to [%s] on the command line." % str(auto_managed))
        else:
            auto_managed = self._r(index_spec, "auto-managed", mandatory=False, default_value=True)
            logger.info("Using index auto-management setting from track which is set to [%s]." % str(auto_managed))

        types = [self._create_type(type_spec, mapping_dir)
                 for type_spec in self._r(index_spec, "types", mandatory=auto_managed, default_value=[])]
        valid_document_data = False
        for type in types:
            if type.has_valid_document_data():
                valid_document_data = True
                break
        if not valid_document_data:
            console.warn("None of the types for index [%s] defines documents. Please check that you either don't want to index data or "
                         "parameter sources are defined for indexing." % index_name, logger=logger)

        return track.Index(name=index_name, auto_managed=auto_managed, types=types)

    def _create_template(self, tpl_spec, mapping_dir):
        name = self._r(tpl_spec, "name")
        index_pattern = self._r(tpl_spec, "index-pattern")
        delete_matching_indices = self._r(tpl_spec, "delete-matching-indices", mandatory=False, default_value=True)
        template_file = os.path.join(mapping_dir, self._r(tpl_spec, "template"))
        with self.source(template_file, "rt") as f:
            template_content = json.load(f)
        return track.IndexTemplate(name, index_pattern, template_content, delete_matching_indices)

    def _create_type(self, type_spec, mapping_dir):
        docs = self._r(type_spec, "documents", mandatory=False)
        if docs:
            if io.is_archive(docs):
                document_archive = docs
                document_file = io.splitext(docs)[0]
            else:
                document_archive = None
                document_file = docs
            number_of_documents = self._r(type_spec, "document-count")
            compressed_bytes = self._r(type_spec, "compressed-bytes", mandatory=False)
            uncompressed_bytes = self._r(type_spec, "uncompressed-bytes", mandatory=False)
        else:
            document_archive = None
            document_file = None
            number_of_documents = 0
            compressed_bytes = 0
            uncompressed_bytes = 0

        mapping_file = os.path.join(mapping_dir, self._r(type_spec, "mapping"))
        with self.source(mapping_file, "rt") as f:
            mapping = json.load(f)

        return track.Type(name=self._r(type_spec, "name"),
                          mapping=mapping,
                          document_file=document_file,
                          document_archive=document_archive,
                          includes_action_and_meta_data=self._r(type_spec, "includes-action-and-meta-data", mandatory=False,
                                                                default_value=False),
                          number_of_documents=number_of_documents,
                          compressed_size_in_bytes=compressed_bytes,
                          uncompressed_size_in_bytes=uncompressed_bytes)

    def _create_challenges(self, track_spec):
        ops = self.parse_operations(self._r(track_spec, "operations", mandatory=False, default_value=[]))
        challenges = []
        known_challenge_names = set()
        default_challenge = None
        challenge_specs = self._get_challenge_specs(track_spec)
        number_of_challenges = len(challenge_specs)
        for challenge_spec in challenge_specs:
            name = self._r(challenge_spec, "name", error_ctx="challenges")
            description = self._r(challenge_spec, "description", error_ctx=name, mandatory=False)
            user_info = self._r(challenge_spec, "user-info", error_ctx=name, mandatory=False)
            meta_data = self._r(challenge_spec, "meta", error_ctx=name, mandatory=False)
            # if we only have one challenge it is treated as default challenge, no matter what the user has specified
            default = number_of_challenges == 1 or self._r(challenge_spec, "default", error_ctx=name, mandatory=False)
            index_settings = self._r(challenge_spec, "index-settings", error_ctx=name, mandatory=False)
            cluster_settings = self._r(challenge_spec, "cluster-settings", error_ctx=name, mandatory=False)

            if default and default_challenge is not None:
                self._error("Both '%s' and '%s' are defined as default challenges. Please define only one of them as default."
                            % (default_challenge.name, name))
            if name in known_challenge_names:
                self._error("Duplicate challenge with name '%s'." % name)
            known_challenge_names.add(name)

            schedule = []

            for op in self._r(challenge_spec, "schedule", error_ctx=name):
                if "parallel" in op:
                    task = self.parse_parallel(op["parallel"], ops, name)
                else:
                    task = self.parse_task(op, ops, name)
                schedule.append(task)

            # verify we don't have any duplicate task names (which can be confusing / misleading in reporting).
            known_task_names = set()
            for task in schedule:
                for sub_task in task:
                    if sub_task.name in known_task_names:
                        self._error("Challenge '%s' contains multiple tasks with the name '%s'. Please use the task's name property to "
                                    "assign a unique name for each task." % (name, sub_task.name))
                    else:
                        known_task_names.add(sub_task.name)

            challenge = track.Challenge(name=name,
                                        meta_data=meta_data,
                                        description=description,
                                        user_info=user_info,
                                        index_settings=index_settings,
                                        cluster_settings=cluster_settings,
                                        default=default,
                                        schedule=schedule)
            if default:
                default_challenge = challenge

            challenges.append(challenge)

        if challenges and default_challenge is None:
            self._error("No default challenge specified. Please edit the track and add \"default\": true to one of the challenges %s."
                        % ", ".join([c.name for c in challenges]))
        return challenges

    def _get_challenge_specs(self, track_spec):
        challenge = self._r(track_spec, "challenge", mandatory=False)
        challenges = self._r(track_spec, "challenges", mandatory=False)

        if challenge is not None and challenges is not None:
            self._error("'challenge' and 'challenges' are defined but only one of them is allowed.")
        elif challenge is not None:
            return [challenge]
        elif challenges is not None:
            return challenges
        else:
            self._error("You must define either 'challenge' or 'challenges' but none is specified.")

    def parse_parallel(self, ops_spec, ops, challenge_name):
        # use same default values as #parseTask() in case the 'parallel' element did not specify anything
        default_warmup_iterations = self._r(ops_spec, "warmup-iterations", error_ctx="parallel", mandatory=False, default_value=0)
        default_iterations = self._r(ops_spec, "iterations", error_ctx="parallel", mandatory=False, default_value=1)
        default_warmup_time_period = self._r(ops_spec, "warmup-time-period", error_ctx="parallel", mandatory=False)
        default_time_period = self._r(ops_spec, "time-period", error_ctx="parallel", mandatory=False)
        clients = self._r(ops_spec, "clients", error_ctx="parallel", mandatory=False)
        completed_by = self._r(ops_spec, "completed-by", error_ctx="parallel", mandatory=False)

        # now descent to each operation
        tasks = []
        for task in self._r(ops_spec, "tasks", error_ctx="parallel"):
            tasks.append(self.parse_task(task, ops, challenge_name, default_warmup_iterations, default_iterations,
                                         default_warmup_time_period, default_time_period, completed_by))
        if completed_by:
            completion_task = None
            for task in tasks:
                if task.completes_parent and not completion_task:
                    completion_task = task
                elif task.completes_parent:
                    self._error("'parallel' element for challenge '%s' contains multiple tasks with the name '%s' which are marked with "
                                "'completed-by' but only task is allowed to match." % (challenge_name, completed_by))
            if not completion_task:
                self._error("'parallel' element for challenge '%s' is marked with 'completed-by' with task name '%s' but no task with "
                            "this name exists." % (challenge_name, completed_by))
        return track.Parallel(tasks, clients)

    def parse_task(self, task_spec, ops, challenge_name, default_warmup_iterations=0, default_iterations=1,
                   default_warmup_time_period=None, default_time_period=None, completed_by_name=None):

        op_spec = task_spec["operation"]
        if isinstance(op_spec, str) and op_spec in ops:
            op = ops[op_spec]
        else:
            # may as well an inline operation
            op = self.parse_operation(op_spec, error_ctx="inline operation in challenge %s" % challenge_name)

        schedule = self._r(task_spec, "schedule", error_ctx=op.name, mandatory=False, default_value="deterministic")
        task = track.Task(name=self._r(task_spec, "name", error_ctx=op.name, mandatory=False, default_value=op.name),
                          operation=op,
                          meta_data=self._r(task_spec, "meta", error_ctx=op.name, mandatory=False),
                          warmup_iterations=self._r(task_spec, "warmup-iterations", error_ctx=op.name, mandatory=False,
                                                    default_value=default_warmup_iterations),
                          iterations=self._r(task_spec, "iterations", error_ctx=op.name, mandatory=False, default_value=default_iterations),
                          warmup_time_period=self._r(task_spec, "warmup-time-period", error_ctx=op.name, mandatory=False,
                                                     default_value=default_warmup_time_period),
                          time_period=self._r(task_spec, "time-period", error_ctx=op.name, mandatory=False,
                                              default_value=default_time_period),
                          clients=self._r(task_spec, "clients", error_ctx=op.name, mandatory=False, default_value=1),
                          # this will work because op_name must always be set, i.e. it is never `None`.
                          completes_parent=(op.name == completed_by_name),
                          schedule=schedule,
                          params=task_spec)
        if task.warmup_iterations != default_warmup_iterations and task.time_period is not None:
            self._error("Operation '%s' in challenge '%s' defines '%d' warmup iterations and a time period of '%d' seconds. Please do not "
                        "mix time periods and iterations." % (op.name, challenge_name, task.warmup_iterations, task.time_period))
        elif task.warmup_time_period is not None and task.iterations != default_iterations:
            self._error("Operation '%s' in challenge '%s' defines a warmup time period of '%d' seconds and '%d' iterations. Please do not "
                        "mix time periods and iterations." % (op.name, challenge_name, task.warmup_time_period, task.iterations))

        return task

    def parse_operations(self, ops_specs):
        # key = name, value = operation
        ops = {}
        for op_spec in ops_specs:
            op = self.parse_operation(op_spec)
            if op.name in ops:
                self._error("Duplicate operation with name '%s'." % op.name)
            else:
                ops[op.name] = op
        return ops

    def parse_operation(self, op_spec, error_ctx="operations"):
        # just a name, let's assume it is a simple operation like force-merge and create a full operation
        if isinstance(op_spec, str):
            op_name = op_spec
            meta_data = None
            op_type_name = op_spec
            param_source = None
        else:
            meta_data = self._r(op_spec, "meta", error_ctx=error_ctx, mandatory=False)
            # Rally's core operations will still use enums then but we'll allow users to define arbitrary operations
            op_type_name = self._r(op_spec, "operation-type", error_ctx=error_ctx)
            # fallback to use the operation type as the operation name
            op_name = self._r(op_spec, "name", error_ctx=error_ctx, mandatory=False, default_value=op_type_name)
            param_source = self._r(op_spec, "param-source", error_ctx=error_ctx, mandatory=False)

        try:
            # TODO #370: Remove this warning.
            # Add a deprecation warning but not for built-in tracks (they need to keep the name for backwards compatibility in the meantime)
            if op_type_name == "index" and \
                            self.name not in ["geonames", "geopoint", "noaa", "logging", "nyc_taxis", "pmc", "percolator", "nested"] and \
                            not self.index_op_type_warning_issued:
                console.warn("The track %s uses the deprecated operation-type [index] for bulk index operations. Please rename this "
                             "operation type to [bulk]." % self.name)
                # Don't spam the console...
                self.index_op_type_warning_issued = True

            op_type = track.OperationType.from_hyphenated_string(op_type_name).name
            logger.debug("Using built-in operation type [%s] for operation [%s]." % (op_type, op_name))
        except KeyError:
            logger.info("Using user-provided operation type [%s] for operation [%s]." % (op_type_name, op_name))
            op_type = op_type_name

        try:
            return track.Operation(name=op_name, meta_data=meta_data, operation_type=op_type, params=op_spec, param_source=param_source)
        except exceptions.InvalidSyntax as e:
            raise TrackSyntaxError("Invalid operation [%s]: %s" % (op_name, str(e)))

