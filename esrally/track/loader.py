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

DEFAULT_TRACKS = ["geonames", "geopoint", "noaa", "http_logs", "nyc_taxis", "pmc", "percolator", "nested"]


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
        expected_cluster_health = cfg.opts("driver", "cluster.health")

        current_track = reader.read(track_name, repo.track_file(track_name), track_dir)
        current_track = filter_included_tasks(current_track, filters_from_included_tasks(included_tasks))
        current_track = post_process_for_index_auto_management(current_track, expected_cluster_health)
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

    def first_existing(root_dirs, f):
        for root_dir in root_dirs:
            p = os.path.join(root_dir, f)
            if os.path.exists(p):
                return p
        return None

    for corpus in t.corpora:
        data_root = data_dir(cfg, t.name, corpus.name)
        for document_set in corpus.documents:
            # At this point we can assume that the file is available locally. Check which path exists and set it.
            if document_set.document_archive:
                document_set.document_archive = first_existing(data_root, document_set.document_archive)
            if document_set.document_file:
                document_set.document_file = first_existing(data_root, document_set.document_file)


def is_simple_track_mode(cfg):
    return cfg.exists("track", "track.path")


def track_repo(cfg, fetch=True, update=True):
    if is_simple_track_mode(cfg):
        track_path = cfg.opts("track", "track.path")
        return SimpleTrackRepository(track_path)
    else:
        return GitTrackRepository(cfg, fetch, update)


def data_dir(cfg, track_name, corpus_name):
    """
    Determines potential data directories for the provided track and corpus name.

    :param cfg: The config object.
    :param track_name: Name of the current track.
    :param corpus_name: Name of the current corpus.
    :return: A list containing either one or two elements. Each element contains a path to a directory which may contain document files.
    """
    corpus_dir = os.path.join(cfg.opts("benchmarks", "local.dataset.cache"), corpus_name)
    if is_simple_track_mode(cfg):
        track_path = cfg.opts("track", "track.path")
        r = SimpleTrackRepository(track_path)
        # data should always be stored in the track's directory. If the user uses the same directory on all machines this will even work
        # in the distributed case. However, the user is responsible to ensure that this is actually the case.
        return [r.track_dir(track_name), corpus_dir]
    else:
        return [corpus_dir]


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
        assert track_name == self.track_name, "Expect provided track name [%s] to match [%s]" % (track_name, self.track_name)
        return self._track_dir

    def track_file(self, track_name):
        assert track_name == self.track_name
        return self._track_file


def operation_parameters(t, op):
    if op.param_source:
        logger.debug("Creating parameter source with name [%s]" % op.param_source)
        return params.param_source_for_name(op.param_source, t, op.params)
    else:
        logger.debug("Creating parameter source for operation type [%s]" % op.type)
        return params.param_source_for_operation(op.type, t, op.params)


def prepare_track(t, cfg):
    """
    Ensures that all track data are available for running the benchmark.

    :param t: A track that is about to be run.
    :param cfg: The config object.
    """
    offline = cfg.opts("system", "offline.mode")
    test_mode = cfg.opts("track", "test.mode.enabled")
    for corpus in t.corpora:
        data_root = data_dir(cfg, t.name, corpus.name)
        logger.info("Resolved data root directory for document corpus [%s] in track [%s] to %s." % (corpus.name, t.name, data_root))
        prep = DocumentSetPreparator(t.name, offline, test_mode)

        for document_set in corpus.documents:
            if document_set.is_bulk:
                if len(data_root) == 1:
                    prep.prepare_document_set(document_set, data_root[0])
                # attempt to prepare everything in the current directory and fallback to the corpus directory
                elif not prep.prepare_bundled_document_set(document_set, data_root[0]):
                    prep.prepare_document_set(document_set, data_root[1])


class DocumentSetPreparator:
    def __init__(self, track_name, offline, test_mode):
        self.track_name = track_name
        self.offline = offline
        self.test_mode = test_mode

    def is_locally_available(self, file_name):
        return os.path.isfile(file_name)

    def has_expected_size(self, file_name, expected_size):
        return expected_size is None or os.path.getsize(file_name) == expected_size

    def decompress(self, archive_path, documents_path, uncompressed_size):
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

    def download(self, base_url, target_path, size_in_bytes, detail_on_missing_root_url):
        file_name = os.path.basename(target_path)

        if not base_url:
            raise exceptions.DataError("%s and it cannot be downloaded because no base URL is provided."
                                       % detail_on_missing_root_url)
        if self.offline:
            raise exceptions.SystemSetupError("Cannot find %s. Please disable offline mode and retry again." % target_path)

        data_url = "%s/%s" % (base_url, file_name)
        try:
            io.ensure_dir(os.path.dirname(target_path))
            if size_in_bytes:
                size_in_mb = round(convert.bytes_to_mb(size_in_bytes))
                logger.info("Downloading data from [%s] (%s MB) to [%s]." % (data_url, size_in_mb, target_path))
            else:
                logger.info("Downloading data from [%s] to [%s]." % (data_url, target_path))

            # we want to have a bit more accurate download progress as these files are typically very large
            progress = net.Progress("[INFO] Downloading data for track %s" % self.track_name, accuracy=1)
            net.download(data_url, target_path, size_in_bytes, progress_indicator=progress)
            progress.finish()
            logger.info("Downloaded data from [%s] to [%s]." % (data_url, target_path))
        except urllib.error.HTTPError as e:
            if e.code == 404 and self.test_mode:
                raise exceptions.DataError("Track [%s] does not support test mode. Please ask the track author to add it or "
                                           "disable test mode and retry." % self.track_name)
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
                "check your Internet connection." % (data_url, target_path, data_url))

        actual_size = os.path.getsize(target_path)
        if size_in_bytes is not None and actual_size != size_in_bytes:
            raise exceptions.DataError("[%s] is corrupt. Downloaded [%d] bytes but [%d] bytes are expected." %
                                       (target_path, actual_size, size_in_bytes))

    def create_file_offset_table(self, document_file_path, expected_number_of_lines):
        # just rebuild the file every time for the time being. Later on, we might check the data file fingerprint to avoid it
        lines_read = io.prepare_file_offset_table(document_file_path)
        if lines_read and lines_read != expected_number_of_lines:
            io.remove_file_offset_table(document_file_path)
            raise exceptions.DataError("Data in [%s] for track [%s] are invalid. Expected [%d] lines but got [%d]."
                                       % (document_file_path, track, expected_number_of_lines, lines_read))

    def prepare_document_set(self, document_set, data_root):
        """
        Prepares a document set locally.

        Precondition: The document set contains either a compressed or an uncompressed document file reference.
        Postcondition: Either following files will be present locally:

            * The compressed document file (if specified originally in the corpus)
            * The uncompressed document file
            * A file offset table based on the document file

            Or this method will raise an appropriate Exception (download error, inappropriate specification of files, ...).

        :param document_set: A document set.
        :param data_root: The data root directory for this document set.
        """
        doc_path = os.path.join(data_root, document_set.document_file)
        archive_path = os.path.join(data_root, document_set.document_archive) if document_set.has_compressed_corpus() else None
        while True:
            if self.is_locally_available(doc_path) and \
                    self.has_expected_size(doc_path, document_set.uncompressed_size_in_bytes):
                break
            elif document_set.has_compressed_corpus() and \
                    self.is_locally_available(archive_path) and \
                    self.has_expected_size(archive_path, document_set.compressed_size_in_bytes):
                self.decompress(archive_path, doc_path, document_set.uncompressed_size_in_bytes)
            else:
                if document_set.has_compressed_corpus():
                    target_path = archive_path
                    expected_size = document_set.compressed_size_in_bytes
                elif document_set.has_uncompressed_corpus():
                    target_path = doc_path
                    expected_size = document_set.uncompressed_size_in_bytes
                else:
                    # this should not happen in practice as the JSON schema should take care of this
                    raise exceptions.RallyAssertionError("Track %s specifies documents but no corpus" % self.track_name)
                # provide a specific error message in case there is no download URL
                if self.is_locally_available(target_path):
                    # convert expected_size eagerly to a string as it might be None (but in that case we'll never see that error message)
                    msg = "%s is present but does not have the expected size of %s bytes" % (target_path, str(expected_size))
                else:
                    msg = "%s is missing" % target_path

                self.download(document_set.base_url, target_path, expected_size, msg)

        self.create_file_offset_table(doc_path, document_set.number_of_lines)

    def prepare_bundled_document_set(self, document_set, data_root):
        """
        Prepares a document set that comes "bundled" with the track, i.e. the data files are in the same directory as the track.
        This is a "lightweight" version of #prepare_document_set() which assumes that at least one file is already present in the
        current directory. It will attempt to find the appropriate files, decompress if necessary and create a file offset table.

        Precondition: The document set contains either a compressed or an uncompressed document file reference.
        Postcondition: If this method returns ``True``, the following files will be present locally:

            * The compressed document file (if specified originally in the corpus)
            * The uncompressed document file
            * A file offset table based on the document file

        If this method returns ``False`` either the document size is wrong or any files have not been found.

        :param document_set: A document set.
        :param data_root: The data root directory for this document set (should be the same as the track file).
        :return: See postcondition.
        """
        doc_path = os.path.join(data_root, document_set.document_file)
        archive_path = os.path.join(data_root, document_set.document_archive) if document_set.has_compressed_corpus() else None

        while True:
            if self.is_locally_available(doc_path):
                if self.has_expected_size(doc_path, document_set.uncompressed_size_in_bytes):
                    self.create_file_offset_table(doc_path, document_set.number_of_lines)
                    return True
                else:
                    raise exceptions.DataError("%s is present but does not have the expected size of %s bytes." %
                                               (doc_path, str(document_set.uncompressed_size_in_bytes)))

            if document_set.has_compressed_corpus() and self.is_locally_available(archive_path):
                if self.has_expected_size(archive_path, document_set.compressed_size_in_bytes):
                    self.decompress(archive_path, doc_path, document_set.uncompressed_size_in_bytes)
                else:
                    # treat this is an error because if the file is present but the size does not match, something is really fishy.
                    # It is likely that the user is currently creating a new track and did not specify the file size correctly.
                    raise exceptions.DataError("%s is present but does not have the expected size of %s bytes." %
                                               (archive_path, str(document_set.compressed_size_in_bytes)))
            else:
                return False


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
        # always include administrative tasks
        complete_filters = [track.AdminTaskFilter()] + filters

        for challenge in t.challenges:
            # don't modify the schedule while iterating over it
            tasks_to_remove = []
            for task in challenge.schedule:
                if not match(task, complete_filters):
                    tasks_to_remove.append(task)
                else:
                    leafs_to_remove = []
                    for leaf_task in task:
                        if not match(leaf_task, complete_filters):
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
                    raise exceptions.SystemSetupError(
                        "Invalid format for included tasks: [%s]. Expected [type] but got [%s]." % (t, spec[0]))
            else:
                raise exceptions.SystemSetupError("Invalid format for included tasks: [%s]" % t)
    return filters


def post_process_for_index_auto_management(t, expected_cluster_health):
    auto_managed_indices = any([index.auto_managed for index in t.indices])
    # spare users this warning for our default tracks
    if auto_managed_indices and t.name not in DEFAULT_TRACKS:
        console.warn("Track [%s] uses index auto-management which will be removed soon. Please add [delete-index] and [create-index] "
                     "tasks at the beginning of each relevant challenge and turn off index auto-management for each index. For details "
                     "please see the migration guide in the docs." % t.name)
    if auto_managed_indices or len(t.templates) > 0:
        for challenge in t.challenges:
            tasks = []
            # TODO: Remove the index settings element. We can do this much better now with the create-index operation.
            create_index_params = {"include-in-reporting": False}
            if challenge.index_settings:
                if t.name not in DEFAULT_TRACKS:
                    console.warn("Track [%s] defines the deprecated property 'index-settings'. Please create indices explicitly with "
                                 "[create-index] and define the respective index settings there instead." % t.name)
                create_index_params["settings"] = challenge.index_settings
            if len(t.templates) > 0:
                # check if the user has defined a create index template operation
                user_creates_templates = any(task.matches(track.TaskOpTypeFilter(track.OperationType.CreateIndexTemplate.name))
                                             for task in challenge.schedule)
                # We attempt to still do this automatically but issue a warning so that the user will create it themselves.
                if not user_creates_templates:
                    console.warn("Track [%s] defines %d index template(s) but soon Rally will not create them implicitly anymore. Please "
                                 "add [delete-index-template] and [create-index-template] tasks at the beginning of the challenge %s."
                                 % (t.name, len(t.templates), challenge.name), logger=logger)
                    tasks.append(track.Task(name="auto-delete-index-templates",
                                            operation=track.Operation(name="auto-delete-index-templates",
                                                                      operation_type=track.OperationType.DeleteIndexTemplate.name,
                                                                      params={
                                                                          "include-in-reporting": False,
                                                                          "only-if-exists": True
                                                                      })))
                    tasks.append(track.Task(name="auto-create-index-templates",
                                            operation=track.Operation(name="auto-create-index-templates",
                                                                      operation_type=track.OperationType.CreateIndexTemplate.name,
                                                                      params=create_index_params.copy())))

            if auto_managed_indices:
                tasks.append(track.Task(name="auto-delete-indices",
                                        operation=track.Operation(name="auto-delete-indices",
                                                                  operation_type=track.OperationType.DeleteIndex.name,
                                                                  params={
                                                                      "include-in-reporting": False,
                                                                      "only-if-exists": True
                                                                  })))
                tasks.append(track.Task(name="auto-create-indices",
                                        operation=track.Operation(name="auto-create-indices",
                                                                  operation_type=track.OperationType.CreateIndex.name,
                                                                  params=create_index_params.copy())))

            # check if the user has already defined a cluster-health operation
            user_checks_cluster_health = any(task.matches(track.TaskOpTypeFilter(track.OperationType.ClusterHealth.name))
                                             for task in challenge.schedule)

            if expected_cluster_health != "skip" and not user_checks_cluster_health:
                tasks.append(track.Task(name="auto-check-cluster-health",
                                        operation=track.Operation(name="auto-check-cluster-health",
                                                                  operation_type=track.OperationType.ClusterHealth.name,
                                                                  params={
                                                                      "include-in-reporting": False,
                                                                      "request-params": {
                                                                          "wait_for_status": expected_cluster_health
                                                                      }
                                                                  })))

            challenge.prepend_tasks(tasks)
        return t
    else:
        return t


def post_process_for_test_mode(t):
    logger.info("Preparing track [%s] for test mode." % str(t))
    for corpus in t.corpora:
        logger.info("Reducing corpus size to 1000 documents for [%s]" % corpus.name)
        for document_set in corpus.documents:
            # TODO #341: Should we allow this for snapshots too?
            if document_set.is_bulk:
                document_set.number_of_documents = 1000

                if document_set.has_compressed_corpus():
                    path, ext = io.splitext(document_set.document_archive)
                    path_2, ext_2 = io.splitext(path)

                    document_set.document_archive = "%s-1k%s%s" % (path_2, ext_2, ext)
                    document_set.document_file = "%s-1k%s" % (path_2, ext_2)
                elif document_set.has_uncompressed_corpus():
                    path, ext = io.splitext(corpus.document_file)
                    document_set.document_file = "%s-1k%s" % (path, ext)
                else:
                    raise exceptions.RallyAssertionError("Document corpus [%s] has neither compressed nor uncompressed corpus." %
                                                         corpus.name)

                # we don't want to check sizes
                document_set.compressed_size_in_bytes = None
                document_set.uncompressed_size_in_bytes = None

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
    # TODO #380: We will increase the version with 0.10.0 in our standard tracks but Rally 0.9.0 will already be prepared for this change.
    MAXIMUM_SUPPORTED_TRACK_VERSION = 2
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
        except jinja2.exceptions.TemplateNotFound:
            logger.exception("Could not load [%s]." % track_spec_file)
            raise exceptions.SystemSetupError("Track %s does not exist" % track_name)
        except (json.JSONDecodeError, jinja2.exceptions.TemplateError) as e:
            logger.exception("Could not load [%s]." % track_spec_file)
            # TODO: Check whether we can improve this.
            # Jinja classes cause serialization problems:
            #
            # File "/usr/local/lib/python3.6/site-packages/thespian-3.8.3-py3.6.egg/thespian/system/transport/TCPTransport.py", line 1431, in _addedDataToIncoming
            # rdata, extra = inc.data
            # File "/usr/local/lib/python3.6/site-packages/thespian-3.8.3-py3.6.egg/thespian/system/transport/TCPTransport.py", line 185, in data
            # def data(self): return self._rData.completed()
            # File "/usr/local/lib/python3.6/site-packages/thespian-3.8.3-py3.6.egg/thespian/system/transport/streamBuffer.py", line 80, in completed
            # return self._deserialize(self._buf), self._extra
            # TypeError: __init__() missing 1 required positional argument: 'lineno'
            # (rdata="", extra="")
            #
            # => Convert to string early on:
            raise TrackSyntaxError("Could not load '%s'" % track_spec_file, str(e))
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

    @property
    def meta_data(self):
        from esrally import version

        return {
            "rally_version": version.release_version()
        }


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

        meta_data = self._r(track_specification, "meta", mandatory=False)
        indices = [self._create_index(idx, mapping_dir)
                   for idx in self._r(track_specification, "indices", mandatory=False, default_value=[])]
        templates = [self._create_template(tpl, mapping_dir)
                     for tpl in self._r(track_specification, "templates", mandatory=False, default_value=[])]
        corpora = self._create_corpora(self._r(track_specification, "corpora", mandatory=False, default_value=[]), indices)
        # TODO: Remove this in Rally 0.10.0
        if corpora:
            logger.info("Track [%s] defines a 'corpora' block. Ignoring any legacy corpora definitions on document types." % self.name)
        else:
            logger.warning("Track [%s] does not define a 'corpora' block. Creating corpora definitions based on document types (will "
                           "be removed with the next minor release)." % self.name)
            corpora = self._create_legacy_corpora(track_specification)
            # Check whether we have legacy documents; otherwise there is no need for a warning...
            if corpora:
                console.warn("Track %s defines corpora together with document types. Please use a dedicated 'corpora' block now. "
                             "See the migration guide for details." % self.name, logger=logger)

        challenges = self._create_challenges(track_specification)

        return track.Track(name=self.name, meta_data=meta_data, description=description, challenges=challenges, indices=indices,
                           templates=templates, corpora=corpora)

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
        body_file = self._r(index_spec, "body", mandatory=False)
        if body_file:
            with self.source(os.path.join(mapping_dir, body_file), "rt") as f:
                body = json.load(f)
        else:
            body = None

        if self.override_auto_manage_indices is not None:
            auto_managed = self.override_auto_manage_indices
            logger.info("User explicitly forced auto-managed indices to [%s] on the command line." % str(auto_managed))
        else:
            auto_managed = self._r(index_spec, "auto-managed", mandatory=False, default_value=True)
            logger.info("Using index auto-management setting from track which is set to [%s]." % str(auto_managed))

        types = [self._create_type(type_spec, mapping_dir)
                 for type_spec in self._r(index_spec, "types", mandatory=auto_managed, default_value=[])]

        return track.Index(name=index_name, body=body, auto_managed=auto_managed, types=types)

    def _create_template(self, tpl_spec, mapping_dir):
        name = self._r(tpl_spec, "name")
        index_pattern = self._r(tpl_spec, "index-pattern")
        delete_matching_indices = self._r(tpl_spec, "delete-matching-indices", mandatory=False, default_value=True)
        template_file = os.path.join(mapping_dir, self._r(tpl_spec, "template"))
        with self.source(template_file, "rt") as f:
            template_content = json.load(f)
        return track.IndexTemplate(name, index_pattern, template_content, delete_matching_indices)

    def _create_corpora(self, corpora_specs, indices):
        document_corpora = []
        known_corpora_names = set()
        for corpus_spec in corpora_specs:
            name = self._r(corpus_spec, "name")

            if name in known_corpora_names:
                self._error("Duplicate document corpus name [%s]." % name)
            known_corpora_names.add(name)

            corpus = track.DocumentCorpus(name=name)
            # defaults on corpus level
            default_base_url = self._r(corpus_spec, "base-url", mandatory=False, default_value=None)
            default_source_format = self._r(corpus_spec, "source-format", mandatory=False, default_value=track.Documents.SOURCE_FORMAT_BULK)
            default_action_and_meta_data = self._r(corpus_spec, "includes-action-and-meta-data", mandatory=False, default_value=False)

            if len(indices) == 1:
                corpus_target_idx = self._r(corpus_spec, "target-index", mandatory=False, default_value=indices[0].name)
            else:
                corpus_target_idx = self._r(corpus_spec, "target-index", mandatory=False)

            if len(indices) == 1 and len(indices[0].types) == 1:
                corpus_target_type = self._r(corpus_spec, "target-type", mandatory=False, default_value=indices[0].types[0].name)
            else:
                corpus_target_type = self._r(corpus_spec, "target-type", mandatory=False)

            for doc_spec in self._r(corpus_spec, "documents"):
                base_url = self._r(doc_spec, "base-url", mandatory=False, default_value=default_base_url)
                source_format = self._r(doc_spec, "source-format", mandatory=False, default_value=default_source_format)

                if source_format == track.Documents.SOURCE_FORMAT_BULK:
                    docs = self._r(doc_spec, "source-file")
                    if io.is_archive(docs):
                        document_archive = docs
                        document_file = io.splitext(docs)[0]
                    else:
                        document_archive = None
                        document_file = docs
                    num_docs = self._r(doc_spec, "document-count")
                    compressed_bytes = self._r(doc_spec, "compressed-bytes", mandatory=False)
                    uncompressed_bytes = self._r(doc_spec, "uncompressed-bytes", mandatory=False)

                    includes_action_and_meta_data = self._r(doc_spec, "includes-action-and-meta-data", mandatory=False,
                                                            default_value=default_action_and_meta_data)
                    if includes_action_and_meta_data:
                        target_idx = None
                        target_type = None
                    else:
                        # we need an index and a type name if no meta-data are present
                        target_idx = self._r(doc_spec, "target-index", mandatory=corpus_target_idx is None,
                                             default_value=corpus_target_idx, error_ctx=docs)
                        target_type = self._r(doc_spec, "target-type", mandatory=corpus_target_type is None,
                                              default_value=corpus_target_type, error_ctx=docs)

                    docs = track.Documents(source_format=source_format,
                                           document_file=document_file,
                                           document_archive=document_archive,
                                           base_url=base_url,
                                           includes_action_and_meta_data=includes_action_and_meta_data,
                                           number_of_documents=num_docs,
                                           compressed_size_in_bytes=compressed_bytes,
                                           uncompressed_size_in_bytes=uncompressed_bytes,
                                           target_index=target_idx, target_type=target_type)
                    corpus.documents.append(docs)
                else:
                    self._error("Unknown source-format [%s] in document corpus [%s]." % (source_format, name))

            document_corpora.append(corpus)
        return document_corpora

    def _create_legacy_corpora(self, track_specification):
        base_url = self._r(track_specification, "data-url", mandatory=False)
        legacy_corpus = track.DocumentCorpus(name=self.name)
        for idx in self._r(track_specification, "indices", mandatory=False, default_value=[]):
            index_name = self._r(idx, "name")
            for type_spec in self._r(idx, "types", mandatory=False, default_value=[]):
                type_name = self._r(type_spec, "name")
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

                    docs = track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK,
                                           document_file=document_file,
                                           document_archive=document_archive,
                                           base_url=base_url,
                                           includes_action_and_meta_data=self._r(type_spec, "includes-action-and-meta-data",
                                                                                 mandatory=False,
                                                                                 default_value=False),
                                           number_of_documents=number_of_documents,
                                           compressed_size_in_bytes=compressed_bytes,
                                           uncompressed_size_in_bytes=uncompressed_bytes,
                                           target_index=index_name, target_type=type_name)
                    legacy_corpus.documents.append(docs)

        if legacy_corpus.documents:
            return [legacy_corpus]
        else:
            return []

    def _create_type(self, type_spec, mapping_dir):
        # TODO: Allow only strings in Rally 0.10.0 (we still needs this atm in order to allow users to define mapping files)
        if isinstance(type_spec, str):
            return track.Type(name=type_spec)
        else:
            mapping_file = self._r(type_spec, "mapping", mandatory=False)
            if mapping_file:
                with self.source(os.path.join(mapping_dir, mapping_file), "rt") as f:
                    mapping = json.load(f)
            else:
                mapping = None

            return track.Type(name=self._r(type_spec, "name"), mapping=mapping)

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
            # TODO #381: Remove this setting
            index_settings = self._r(challenge_spec, "index-settings", error_ctx=name, mandatory=False)
            cluster_settings = self._r(challenge_spec, "cluster-settings", error_ctx=name, mandatory=False)

            if index_settings and self.name not in DEFAULT_TRACKS:
                console.warn("Challenge [%s] in track [%s] defines the [index-settings] property which will be removed soon. For details "
                             "please see the migration guide in the docs." % (name, self.name))

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
                          # this is to provide scheduler-specific parameters for custom schedulers.
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
            # Cannot have parameters here
            params = {}
        else:
            meta_data = self._r(op_spec, "meta", error_ctx=error_ctx, mandatory=False)
            # Rally's core operations will still use enums then but we'll allow users to define arbitrary operations
            op_type_name = self._r(op_spec, "operation-type", error_ctx=error_ctx)
            # fallback to use the operation type as the operation name
            op_name = self._r(op_spec, "name", error_ctx=error_ctx, mandatory=False, default_value=op_type_name)
            param_source = self._r(op_spec, "param-source", error_ctx=error_ctx, mandatory=False)
            # just pass-through all parameters by default
            params = op_spec

        try:
            # TODO #370: Remove this warning.
            # Add a deprecation warning but not for built-in tracks (they need to keep the name for backwards compatibility in the meantime)
            if op_type_name == "index" and \
                    self.name not in DEFAULT_TRACKS and \
                    not self.index_op_type_warning_issued:
                console.warn("The track %s uses the deprecated operation-type [index] for bulk index operations. Please rename this "
                             "operation type to [bulk]." % self.name)
                # Don't spam the console...
                self.index_op_type_warning_issued = True

            op = track.OperationType.from_hyphenated_string(op_type_name)
            if "include-in-reporting" not in params:
                params["include-in-reporting"] = not op.admin_op
            op_type = op.name
            logger.debug("Using built-in operation type [%s] for operation [%s]." % (op_type, op_name))
        except KeyError:
            logger.info("Using user-provided operation type [%s] for operation [%s]." % (op_type_name, op_name))
            op_type = op_type_name

        try:
            return track.Operation(name=op_name, meta_data=meta_data, operation_type=op_type, params=params, param_source=param_source)
        except exceptions.InvalidSyntax as e:
            raise TrackSyntaxError("Invalid operation [%s]: %s" % (op_name, str(e)))
