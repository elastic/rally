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

    def track_names(repo):
        return filter(lambda p: os.path.exists(track_file(repo, p)), next(os.walk(repo.repo_dir))[1])

    repo = track_repo(cfg)
    reader = TrackFileReader(cfg)
    return [reader.read(track_name,
                        track_file(repo, track_name),
                        track_dir(repo, track_name))
            for track_name in track_names(repo)]


def list_tracks(cfg):
    console.println("Available tracks:\n")
    console.println(tabulate.tabulate(
        tabular_data=[
            [t.name, t.short_description, t.number_of_documents, convert.bytes_to_human_string(t.compressed_size_in_bytes),
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
    track_name = cfg.opts("track", "track.name")
    try:
        repo = track_repo(cfg)
        reader = TrackFileReader(cfg)
        included_tasks = cfg.opts("track", "include.tasks")

        current_track = reader.read(track_name, track_file(repo, track_name), track_dir(repo, track_name))
        current_track = filter_included_tasks(current_track, filters_from_included_tasks(included_tasks))

        if cfg.opts("track", "test.mode.enabled"):
            return post_process_for_test_mode(current_track)
        else:
            return current_track
    except FileNotFoundError:
        logger.exception("Cannot load track [%s]" % track_name)
        raise exceptions.SystemSetupError("Cannot load track %s. List the available tracks with %s list tracks." %
                                          (track_name, PROGRAM_NAME))


def load_track_plugins(cfg, register_runner, register_scheduler):
    track_name = cfg.opts("track", "track.name")
    repo = track_repo(cfg, fetch=False, update=False)
    track_plugin_path = track_dir(repo, track_name)

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
    data_root = cfg.opts("benchmarks", "local.dataset.cache")
    for index in t.indices:
        for t in index.types:
            if t.document_archive:
                t.document_archive = os.path.join(data_root, t.document_archive)
            if t.document_file:
                t.document_file = os.path.join(data_root, t.document_file)


def track_repo(cfg, fetch=True, update=True):
    distribution_version = cfg.opts("mechanic", "distribution.version", mandatory=False)
    repo_name = cfg.opts("track", "repository.name")
    offline = cfg.opts("system", "offline.mode")
    remote_url = cfg.opts("tracks", "%s.url" % repo_name, mandatory=False)
    root = cfg.opts("node", "root.dir")
    track_repositories = cfg.opts("benchmarks", "track.repository.dir")
    tracks_dir = os.path.join(root, track_repositories)

    current_track_repo = repo.RallyRepository(remote_url, tracks_dir, repo_name, "tracks", offline, fetch)
    if update:
        current_track_repo.update(distribution_version)
    return current_track_repo


def track_dir(repo, track_name):
    return os.path.join(repo.repo_dir, track_name)


def track_file(repo, track_name):
    return os.path.join(track_dir(repo, track_name), "track.json")


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

    def download(cfg, url, local_path, size_in_bytes):
        offline = cfg.opts("system", "offline.mode")
        file_exists = os.path.isfile(local_path)

        # ensure we only skip the download if the file size also matches our expectation
        if file_exists and (size_in_bytes is None or os.path.getsize(local_path) == size_in_bytes):
            logger.info("[%s] already exists locally. Skipping download." % local_path)
            return False

        if not offline:
            try:
                io.ensure_dir(os.path.dirname(local_path))
                if size_in_bytes:
                    size_in_mb = round(convert.bytes_to_mb(size_in_bytes))
                    # ensure output appears immediately
                    logger.info("Downloading data from [%s] (%s MB) to [%s]." % (url, size_in_mb, local_path))
                else:
                    logger.info("Downloading data from [%s] to [%s]." % (url, local_path))

                # we want to have a bit more accurate download progress as these files are typically very large
                progress = net.Progress("[INFO] Downloading data for track %s" % track.name, accuracy=1)
                net.download(url, local_path, size_in_bytes, progress_indicator=progress)
                progress.finish()
                logger.info("Downloaded data from [%s] to [%s]." % (url, local_path))
            except urllib.error.URLError:
                logger.exception("Could not download [%s] to [%s]." % (url, local_path))

        # file must exist at this point -> verify
        if not os.path.isfile(local_path):
            if offline:
                raise exceptions.SystemSetupError(
                    "Cannot find %s. Please disable offline mode and retry again." % local_path)
            else:
                raise exceptions.SystemSetupError(
                    "Cannot download from %s to %s. Please verify that data are available at %s and "
                    "check your internet connection." % (url, local_path, url))

        actual_size = os.path.getsize(local_path)
        if size_in_bytes is not None and actual_size != size_in_bytes:
            raise exceptions.DataError("[%s] is corrupt. Downloaded [%d] bytes but [%d] bytes are expected." %
                                       (local_path, actual_size, size_in_bytes))

        return True

    def decompress(data_set_path, expected_size_in_bytes):
        # we assume that track data are always compressed and try to decompress them before running the benchmark
        basename, extension = io.splitext(data_set_path)
        decompressed = False
        if not os.path.isfile(basename) or os.path.getsize(basename) != expected_size_in_bytes:
            decompressed = True
            if type.uncompressed_size_in_bytes:
                console.info("Decompressing track data from [%s] to [%s] (resulting size: %.2f GB) ... " %
                             (data_set_path, basename, convert.bytes_to_gb(type.uncompressed_size_in_bytes)),
                             end='', flush=True, logger=logger)
            else:
                console.info("Decompressing track data from [%s] to [%s] ... " % (data_set_path, basename), end='',
                             flush=True, logger=logger)

            io.decompress(data_set_path, io.dirname(data_set_path))
            console.println("[OK]")
            extracted_bytes = os.path.getsize(basename)
            if expected_size_in_bytes is not None and extracted_bytes != expected_size_in_bytes:
                raise exceptions.DataError("[%s] is corrupt. Extracted [%d] bytes but [%d] bytes are expected." %
                                           (basename, extracted_bytes, expected_size_in_bytes))
        return basename, decompressed

    if not track.source_root_url:
        logger.info("Track [%s] does not specify a source root URL. Assuming data are available locally." % track.name)

    data_root = cfg.opts("benchmarks", "local.dataset.cache")
    for index in track.indices:
        for type in index.types:
            if type.document_archive:
                absolute_archive_path = os.path.join(data_root, type.document_archive)
                if track.source_root_url:
                    data_url = "%s/%s" % (track.source_root_url, os.path.basename(absolute_archive_path))
                    download(cfg, data_url, absolute_archive_path, type.compressed_size_in_bytes)
                if not os.path.exists(absolute_archive_path):
                    if cfg.opts("track", "test.mode.enabled"):
                        logger.error("[%s] does not exist so assuming that track [%s] does not support test mode." %
                                     (absolute_archive_path, track))
                        raise exceptions.DataError("Track [%s] does not support test mode. Please ask the track author to add it or "
                                                   "disable test mode and retry." % track)
                    else:
                        logger.error("[%s] does not exist." % absolute_archive_path)
                        raise exceptions.DataError("Track data file [%s] is missing." % absolute_archive_path)
                decompressed_file_path, was_decompressed = decompress(absolute_archive_path, type.uncompressed_size_in_bytes)
                # just rebuild the file every time for the time being. Later on, we might check the data file fingerprint to avoid it
                lines_read = io.prepare_file_offset_table(decompressed_file_path)
                if lines_read and lines_read != type.number_of_lines:
                    io.remove_file_offset_table(decompressed_file_path)
                    raise exceptions.DataError("Data in [%s] for track [%s] are invalid. Expected [%d] lines but got [%d]."
                                               % (decompressed_file_path, track, type.number_of_lines, lines_read))
            else:
                logger.info("Type [%s] in index [%s] does not define a document archive. No data are indexed from a file for this type." %
                            (type.name, index.name))


def render_template(loader, template_name, glob_helper=lambda f: [], clock=time.Clock):
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
    env.globals["now"] = clock.now()
    env.globals["glob"] = glob_helper
    env.filters["days_ago"] = time.days_ago
    template = env.get_template(template_name)

    return template.render()


def render_template_from_file(template_file_name):
    def relative_glob(start, f):
        result = glob.glob(os.path.join(start, f))
        if result:
            return [os.path.relpath(p, start) for p in result]
        else:
            return []

    base_path = io.dirname(template_file_name)
    return render_template(loader=jinja2.FileSystemLoader(base_path),
                           template_name=io.basename(template_file_name),
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
                filters.append(track.TaskOpNameFilter(spec[0]))
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
    """
    Creates a track from a track file.
    """

    def __init__(self, cfg):
        track_schema_file = "%s/resources/track-schema.json" % (cfg.opts("node", "rally.root"))
        self.track_schema = json.loads(open(track_schema_file).read())
        override_auto_manage_indices = cfg.opts("track", "auto_manage_indices")
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
            rendered = render_template_from_file(track_spec_file)
            logger.info("Final rendered track for '%s': %s" % (track_spec_file, rendered))
            track_spec = json.loads(rendered)
        except (json.JSONDecodeError, jinja2.exceptions.TemplateError) as e:
            logger.exception("Could not load [%s]." % track_spec_file)
            raise TrackSyntaxError("Could not load '%s'" % track_spec_file, e)
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
    def __init__(self, track_plugin_path, runner_registry, scheduler_registry):
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

    def __call__(self, track_name, track_specification, mapping_dir):
        self.name = track_name
        short_description = self._r(track_specification, "short-description")
        description = self._r(track_specification, "description")
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

        return track.Track(name=self.name, meta_data=meta_data, short_description=short_description, description=description,
                           source_root_url=source_root_url,
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
        compressed_docs = self._r(type_spec, "documents", mandatory=False)
        if compressed_docs:
            relative_data_dir = self.name.lower()
            document_archive = os.path.join(relative_data_dir, compressed_docs)
            document_file = os.path.join(relative_data_dir, io.splitext(compressed_docs)[0])
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
        ops = self.parse_operations(self._r(track_spec, "operations"))
        challenges = []
        known_challenge_names = set()
        default_challenge = None
        number_of_challenges = len(self._r(track_spec, "challenges"))
        for challenge in self._r(track_spec, "challenges"):
            name = self._r(challenge, "name", error_ctx="challenges")
            description = self._r(challenge, "description", error_ctx=name)
            user_info = self._r(challenge, "user-info", error_ctx=name, mandatory=False)
            meta_data = self._r(challenge, "meta", error_ctx=name, mandatory=False)
            # if we only have one challenge it is treated as default challenge, no matter what the user has specified
            default = number_of_challenges == 1 or self._r(challenge, "default", error_ctx=name, mandatory=False)
            index_settings = self._r(challenge, "index-settings", error_ctx=name, mandatory=False)
            cluster_settings = self._r(challenge, "cluster-settings", error_ctx=name, mandatory=False)

            if default and default_challenge is not None:
                self._error("Both '%s' and '%s' are defined as default challenges. Please define only one of them as default."
                            % (default_challenge.name, name))
            if name in known_challenge_names:
                self._error("Duplicate challenge with name '%s'." % name)
            known_challenge_names.add(name)

            schedule = []

            for op in self._r(challenge, "schedule", error_ctx=name):
                if "parallel" in op:
                    task = self.parse_parallel(op["parallel"], ops, name)
                else:
                    task = self.parse_task(op, ops, name)
                schedule.append(task)

            new_challenge = track.Challenge(name=name,
                                            meta_data=meta_data,
                                            description=description,
                                            user_info=user_info,
                                            index_settings=index_settings,
                                            cluster_settings=cluster_settings,
                                            default=default,
                                            schedule=schedule)
            if default:
                default_challenge = new_challenge

            challenges.append(new_challenge)

        if challenges and default_challenge is None:
            self._error("No default challenge specified. Please edit the track and add \"default\": true to one of the challenges %s."
                        % ", ".join([c.name for c in challenges]))
        return challenges

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
        op_name = task_spec["operation"]
        if op_name not in ops:
            self._error("'schedule' for challenge '%s' contains a non-existing operation '%s'. "
                        "Please add an operation '%s' to the 'operations' block." % (challenge_name, op_name, op_name))

        schedule = self._r(task_spec, "schedule", error_ctx=op_name, mandatory=False, default_value="deterministic")
        task = track.Task(operation=ops[op_name],
                          meta_data=self._r(task_spec, "meta", error_ctx=op_name, mandatory=False),
                          warmup_iterations=self._r(task_spec, "warmup-iterations", error_ctx=op_name, mandatory=False,
                                                    default_value=default_warmup_iterations),
                          iterations=self._r(task_spec, "iterations", error_ctx=op_name, mandatory=False, default_value=default_iterations),
                          warmup_time_period=self._r(task_spec, "warmup-time-period", error_ctx=op_name, mandatory=False,
                                                     default_value=default_warmup_time_period),
                          time_period=self._r(task_spec, "time-period", error_ctx=op_name, mandatory=False,
                                              default_value=default_time_period),
                          clients=self._r(task_spec, "clients", error_ctx=op_name, mandatory=False, default_value=1),
                          # this will work because op_name must always be set, i.e. it is never `None`.
                          completes_parent=(op_name == completed_by_name),
                          schedule=schedule,
                          params=task_spec)
        if task.warmup_iterations != default_warmup_iterations and task.time_period is not None:
            self._error("Operation '%s' in challenge '%s' defines '%d' warmup iterations and a time period of '%d' seconds. Please do not "
                        "mix time periods and iterations." % (op_name, challenge_name, task.warmup_iterations, task.time_period))
        elif task.warmup_time_period is not None and task.iterations != default_iterations:
            self._error("Operation '%s' in challenge '%s' defines a warmup time period of '%d' seconds and '%d' iterations. Please do not "
                        "mix time periods and iterations." % (op_name, challenge_name, task.warmup_time_period, task.iterations))

        return task

    def parse_operations(self, ops_specs):
        # key = name, value = operation
        ops = {}
        for op_spec in ops_specs:
            op_name = self._r(op_spec, "name", error_ctx="operations")
            meta_data = self._r(op_spec, "meta", error_ctx="operations", mandatory=False)
            # Rally's core operations will still use enums then but we'll allow users to define arbitrary operations
            op_type_name = self._r(op_spec, "operation-type", error_ctx="operations")
            try:
                op_type = track.OperationType.from_hyphenated_string(op_type_name).name
                logger.debug("Using built-in operation type [%s] for operation [%s]." % (op_type, op_name))
            except KeyError:
                logger.info("Using user-provided operation type [%s] for operation [%s]." % (op_type_name, op_name))
                op_type = op_type_name
            param_source = self._r(op_spec, "param-source", error_ctx="operations", mandatory=False)
            if op_name in ops:
                self._error("Duplicate operation with name '%s'." % op_name)
            try:
                ops[op_name] = track.Operation(name=op_name, meta_data=meta_data, operation_type=op_type, params=op_spec,
                                               param_source=param_source)
            except exceptions.InvalidSyntax as e:
                raise TrackSyntaxError("Invalid operation [%s]: %s" % (op_name, str(e)))
        return ops
