import importlib.machinery
import json
import logging
import os
import sys
import glob
import urllib.error

import jinja2
import jinja2.exceptions
import jsonschema
import tabulate
from esrally import exceptions, time, PROGRAM_NAME
from esrally.track import params, track
from esrally.utils import io, convert, net, git, versions, console

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
    repo = TrackRepository(cfg)
    reader = TrackFileReader(cfg)
    distribution_version = cfg.opts("mechanic", "distribution.version", mandatory=False)
    data_root = cfg.opts("benchmarks", "local.dataset.cache")
    return [reader.read(track_name,
                        # avoid excessive fetch from remote repo
                        repo.track_file(distribution_version, track_name, needs_update=False),
                        repo.track_dir(track_name),
                        "%s/%s" % (data_root, track_name.lower())
                        )
            for track_name in repo.track_names(distribution_version)]


def list_tracks(cfg):
    console.println("Available tracks:\n")
    console.println(tabulate.tabulate(
        tabular_data=[[t.name, t.short_description, t.default_challenge, ",".join(map(str, t.challenges))] for t in tracks(cfg)],
        headers=["Name", "Description", "Default Challenge", "All Challenges"]))


def load_track(cfg):
    """

    Loads a track

    :param cfg: The config object. It contains the name of the track to load.
    :return: The loaded track.
    """
    track_name = cfg.opts("track", "track.name")
    try:
        repo = TrackRepository(cfg)
        reader = TrackFileReader(cfg)
        distribution_version = cfg.opts("mechanic", "distribution.version", mandatory=False)
        data_root = cfg.opts("benchmarks", "local.dataset.cache")
        full_track = reader.read(track_name, repo.track_file(distribution_version, track_name), repo.track_dir(track_name),
                                 "%s/%s" % (data_root, track_name.lower()))
        if cfg.opts("track", "test.mode.enabled"):
            return post_process_for_test_mode(full_track)
        else:
            return full_track
    except FileNotFoundError:
        logger.exception("Cannot load track [%s]" % track_name)
        raise exceptions.SystemSetupError("Cannot load track %s. List the available tracks with %s list tracks." %
                                          (track_name, PROGRAM_NAME))


def load_track_plugins(cfg, register_runner, register_scheduler):
    track_name = cfg.opts("track", "track.name")
    # TODO #257: If we distribute drivers we need to ensure that the correct branch in the track repo is checked out
    repo = TrackRepository(cfg, fetch=False)
    plugin_reader = TrackPluginReader(register_runner, register_scheduler)

    track_plugin_path = repo.track_dir(track_name)
    if plugin_reader.can_load(track_plugin_path):
        plugin_reader.load(track_plugin_path)
    else:
        logger.debug("Track [%s] in path [%s] does not define any track plugins." % (track_name, track_plugin_path))


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

    for index in track.indices:
        for type in index.types:
            if type.document_archive:
                if track.source_root_url:
                    data_url = "%s/%s" % (track.source_root_url, os.path.basename(type.document_archive))
                    download(cfg, data_url, type.document_archive, type.compressed_size_in_bytes)
                if not os.path.exists(type.document_archive):
                    if cfg.opts("track", "test.mode.enabled"):
                        logger.error("[%s] does not exist so assuming that track [%s] does not support test mode." %
                                     (type.document_archive, track))
                        raise exceptions.DataError("Track [%s] does not support test mode. Please ask the track author to add it or "
                                                   "disable test mode and retry." % track)
                    else:
                        logger.error("[%s] does not exist." % type.document_archive)
                        raise exceptions.DataError("Track data file [%s] is missing." % type.document_archive)
                decompressed_file_path, was_decompressed = decompress(type.document_archive, type.uncompressed_size_in_bytes)
                # just rebuild the file every time for the time being. Later on, we might check the data file fingerprint to avoid it
                io.prepare_file_offset_table(decompressed_file_path)
            else:
                logger.info("Type [%s] in index [%s] does not define a document archive. No data are indexed from a file for this type." %
                            (type.name, index.name))


class TrackRepository:
    """
    Manages track specifications.
    """

    def __init__(self, cfg, fetch=True):
        self.cfg = cfg
        self.name = cfg.opts("track", "repository.name")
        self.offline = cfg.opts("system", "offline.mode")
        # If no URL is found, we consider this a local only repo (but still require that it is a git repo)
        self.url = cfg.opts("tracks", "%s.url" % self.name, mandatory=False)
        self.remote = self.url is not None and self.url.strip() != ""
        root = cfg.opts("node", "root.dir")
        track_repositories = cfg.opts("benchmarks", "track.repository.dir")
        self.tracks_dir = "%s/%s/%s" % (root, track_repositories, self.name)
        if self.remote and not self.offline and fetch:
            # a normal git repo with a remote
            if not git.is_working_copy(self.tracks_dir):
                git.clone(src=self.tracks_dir, remote=self.url)
            else:
                try:
                    git.fetch(src=self.tracks_dir)
                except exceptions.SupplyError:
                    console.warn("Could not update tracks. Continuing with your locally available state.", logger=logger)
        else:
            if not git.is_working_copy(self.tracks_dir):
                raise exceptions.SystemSetupError("[{src}] must be a git repository.\n\nPlease run:\ngit -C {src} init"
                                                  .format(src=self.tracks_dir))

    def track_names(self, distribution_version):
        self._update(distribution_version)
        return filter(self._is_track, next(os.walk(self.tracks_dir))[1])

    def _is_track(self, path):
        return os.path.exists(self._track_file(path))

    def track_dir(self, track_name):
        return "%s/%s" % (self.tracks_dir, track_name)

    def _track_file(self, track_name):
        return "%s/track.json" % self.track_dir(track_name)

    def track_file(self, distribution_version, track_name, needs_update=True):
        if needs_update:
            self._update(distribution_version)
        return self._track_file(track_name)

    def _update(self, distribution_version):
        try:
            if self.remote and not self.offline:
                branch = versions.best_match(git.branches(self.tracks_dir, remote=self.remote), distribution_version)
                if branch:
                    # Allow uncommitted changes iff we do not have to change the branch
                    logger.info(
                        "Checking out [%s] in [%s] for distribution version [%s]." % (branch, self.tracks_dir, distribution_version))
                    git.checkout(self.tracks_dir, branch=branch)
                    logger.info("Rebasing on [%s] in [%s] for distribution version [%s]." % (branch, self.tracks_dir, distribution_version))
                    try:
                        git.rebase(self.tracks_dir, branch=branch)
                    except exceptions.SupplyError:
                        logger.exception("Cannot rebase due to local changes in [%s]" % self.tracks_dir)
                        console.warn(
                            "Local changes in [%s] prevent track update from remote. Please commit your changes." % self.tracks_dir)
                    return
                else:
                    msg = "Could not find track data remotely for distribution version [%s]. " \
                          "Trying to find track data locally." % distribution_version
                    logger.warning(msg)
            branch = versions.best_match(git.branches(self.tracks_dir, remote=False), distribution_version)
            if branch:
                logger.info("Checking out [%s] in [%s] for distribution version [%s]." % (branch, self.tracks_dir, distribution_version))
                git.checkout(self.tracks_dir, branch=branch)
            else:
                raise exceptions.SystemSetupError("Cannot find track data for distribution version %s" % distribution_version)
        except exceptions.SupplyError:
            tb = sys.exc_info()[2]
            raise exceptions.DataError("Cannot update track data in [%s]." % self.tracks_dir).with_traceback(tb)


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

    def read(self, track_name, track_spec_file, mapping_dir, data_dir):
        """
        Reads a track file, verifies it against the JSON schema and if valid, creates a track.

        :param track_name: The name of the track.
        :param track_spec_file: The complete path to the track specification file.
        :param mapping_dir: The directory where the mapping files for this track are stored locally.
        :param data_dir: The directory where the data file for this track are stored locally.
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
        return self.read_track(track_name, track_spec, mapping_dir, data_dir)


class TrackPluginReader:
    """
    Loads track plugins
    """
    def __init__(self, runner_registry, scheduler_registry):
        self.runner_registry = runner_registry
        self.scheduler_registry = scheduler_registry

    def _modules(self, plugins_dirs, plugin_name, plugin_root_path):
        for path in plugins_dirs:
            for filename in os.listdir(path):
                name, ext = os.path.splitext(filename)
                if ext.endswith(".py"):
                    root_relative_path = os.path.join(path, name)[len(plugin_root_path) + len(os.path.sep):]
                    module_name = "%s.%s" % (plugin_name, root_relative_path.replace(os.path.sep, "."))
                    yield module_name

    def _load_plugin(self, plugin_name, plugins_dirs, plugin_root_path):
        # precondition: A module with this name has to exist provided that the caller has called #can_load() before.
        root_module_name = "%s.track" % plugin_name

        for p in self._modules(plugins_dirs, plugin_name, plugin_root_path):
            logger.debug("Loading module [%s]" % p)
            m = importlib.import_module(p)
            importlib.reload(m)
            if p == root_module_name:
                root_module = m
        return root_module

    def can_load(self, track_plugin_path):
        return os.path.exists(os.path.join(track_plugin_path, "track.py"))

    def load(self, track_plugin_path):
        plugin_name = io.basename(track_plugin_path)
        logger.info("Loading track plugin [%s] from [%s]" % (plugin_name, track_plugin_path))
        # search all paths within this directory for modules but exclude all directories starting with "_"
        module_dirs = []
        for dirpath, dirs, _ in os.walk(track_plugin_path):
            module_dirs.append(dirpath)
            ignore = []
            for d in dirs:
                if d.startswith("_"):
                    logger.debug("Removing [%s] from load path." % d)
                    ignore.append(d)
            for d in ignore:
                dirs.remove(d)
        # load path is only the root of the package hierarchy
        plugin_root_path = os.path.abspath(os.path.join(track_plugin_path, os.pardir))
        logger.debug("Adding [%s] to Python load path." % plugin_root_path)
        # needs to be at the beginning of the system path, otherwise import machinery tries to load application-internal modules
        sys.path.insert(0, plugin_root_path)
        try:
            root_module = self._load_plugin(plugin_name, module_dirs, track_plugin_path)
            # every module needs to have a register() method
            root_module.register(self)
        except BaseException:
            msg = "Could not load track plugin [%s]" % plugin_name
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

    def __init__(self, override_auto_manage_indices=None):
        self.name = None
        self.override_auto_manage_indices = override_auto_manage_indices

    def __call__(self, track_name, track_specification, mapping_dir, data_dir):
        self.name = track_name
        short_description = self._r(track_specification, "short-description")
        description = self._r(track_specification, "description")
        source_root_url = self._r(track_specification, "data-url", mandatory=False)
        meta_data = self._r(track_specification, "meta", mandatory=False)
        indices = [self._create_index(idx, mapping_dir, data_dir)
                   for idx in self._r(track_specification, "indices", mandatory=False, default_value=[])]
        templates = [self._create_template(tpl, mapping_dir)
                     for tpl in self._r(track_specification, "templates", mandatory=False, default_value=[])]
        challenges = self._create_challenges(track_specification)

        if len(indices) == 0 and len(templates) == 0:
            self._error("Specify at least one index or one template.")

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

    def _create_index(self, index_spec, mapping_dir, data_dir):
        index_name = self._r(index_spec, "name")
        if self.override_auto_manage_indices is not None:
            auto_managed = self.override_auto_manage_indices
            logger.info("User explicitly forced auto-managed indices to [%s] on the command line." % str(auto_managed))
        else:
            auto_managed = self._r(index_spec, "auto-managed", mandatory=False, default_value=True)
            logger.info("Using index auto-management setting from track which is set to [%s]." % str(auto_managed))

        types = [self._create_type(type_spec, mapping_dir, data_dir)
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
        template_file = "%s/%s" % (mapping_dir, self._r(tpl_spec, "template"))
        return track.IndexTemplate(name, index_pattern, template_file, delete_matching_indices)

    def _create_type(self, type_spec, mapping_dir, data_dir):
        compressed_docs = self._r(type_spec, "documents", mandatory=False)
        if compressed_docs:
            document_archive = "%s/%s" % (data_dir, compressed_docs)
            document_file = "%s/%s" % (data_dir, io.splitext(compressed_docs)[0])
            number_of_documents = self._r(type_spec, "document-count")
            compressed_bytes = self._r(type_spec, "compressed-bytes", mandatory=False)
            uncompressed_bytes = self._r(type_spec, "uncompressed-bytes", mandatory=False)
        else:
            document_archive = None
            document_file = None
            number_of_documents = 0
            compressed_bytes = 0
            uncompressed_bytes = 0

        return track.Type(name=self._r(type_spec, "name"),
                          mapping_file="%s/%s" % (mapping_dir, self._r(type_spec, "mapping")),
                          document_file=document_file,
                          document_archive=document_archive,
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

        # now descent to each operation
        tasks = []
        for task in self._r(ops_spec, "tasks", error_ctx="parallel"):
            tasks.append(self.parse_task(task, ops, challenge_name, default_warmup_iterations, default_iterations,
                                         default_warmup_time_period, default_time_period))
        return track.Parallel(tasks, clients)

    def parse_task(self, task_spec, ops, challenge_name, default_warmup_iterations=0, default_iterations=1,
                   default_warmup_time_period=None, default_time_period=None):
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
                          schedule=schedule, params=task_spec)
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
