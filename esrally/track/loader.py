# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import abc
import glob
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import urllib.error
from typing import Callable, Generator, Tuple

import jinja2
import jinja2.exceptions
import jsonschema
import tabulate
from jinja2 import meta

from esrally import PROGRAM_NAME, config, exceptions, paths, time, version
from esrally.track import params, track
from esrally.utils import collections, console, convert, io, modules, net, opts, repo


class TrackSyntaxError(exceptions.InvalidSyntax):
    """
    Raised whenever a syntax problem is encountered when loading the track specification.
    """


class TrackProcessor(abc.ABC):
    def on_after_load_track(self, track: track.Track) -> None:
        """
        This method is called by Rally after a track has been loaded. Implementations are expected to modify the
        provided track object in place.

        :param track: The current track.
        """

    @staticmethod
    def _noop():
        """
        To minimize complexity here, we use a no-op function to return a no-op result in the base class.
        Alternatively we could use an ABC with some refactoring. We def the function since lambdas cannot be
        pickled for Thespian's sake.
        """
        return

    def on_prepare_track(self, track: track.Track, data_root_dir: str) -> Generator[Tuple[Callable, dict], None, None]:
        """
        This method is called by Rally after the "after_load_track" phase. Here, any data that is necessary for
        benchmark execution should be prepared, e.g. by downloading data or generating it. Implementations should
        be aware that this method might be called on a different machine than "on_after_load_track" and they cannot
        share any state in between phases.

        :param track: The current track. This parameter should be treated as effectively immutable. Any modifications
                      will not be reflected in subsequent phases of the benchmark.
        :param data_root_dir: The data root directory on the current machine as configured by the user.
        :return: a Generator[Tuple[Callable, dict], None, None] of function/parameter pairs to be executed by the prepare track's executor
        actors.
        """
        yield TrackProcessor._noop, {}


class TrackProcessorRegistry:
    def __init__(self, cfg):
        self.required_processors = [TaskFilterTrackProcessor(cfg), TestModeTrackProcessor(cfg)]
        self.track_processors = []
        self.offline = cfg.opts("system", "offline.mode")
        self.test_mode = cfg.opts("track", "test.mode.enabled", mandatory=False, default_value=False)
        self.base_config = cfg
        self.custom_configuration = False

    def register_track_processor(self, processor):
        if not self.custom_configuration:
            # given processor should become the only element
            self.track_processors = []
        if not isinstance(processor, DefaultTrackPreparator):
            # stop resetting self.track_processors
            self.custom_configuration = True
        if hasattr(processor, "cfg"):
            processor.cfg = self.base_config
        if hasattr(processor, "downloader"):
            processor.downloader = Downloader(self.offline, self.test_mode)
        if hasattr(processor, "decompressor"):
            processor.decompressor = Decompressor()
        self.track_processors.append(processor)

    @property
    def processors(self):
        if not self.custom_configuration:
            self.register_track_processor(DefaultTrackPreparator())
        return [*self.required_processors, *self.track_processors]


def tracks(cfg):
    """

    Lists all known tracks. Note that users can specify a distribution version so if different tracks are available for
    different versions, this will be reflected in the output.

    :param cfg: The config object.
    :return: A list of tracks that are available for the provided distribution version or else for the main version.
    """
    repo = track_repo(cfg)
    return [_load_single_track(cfg, repo, track_name) for track_name in repo.track_names]


def list_tracks(cfg):
    available_tracks = tracks(cfg)
    only_auto_generated_challenges = all(t.default_challenge.auto_generated for t in available_tracks)

    data = []
    for t in available_tracks:
        line = [
            t.name,
            t.description,
            convert.number_to_human_string(t.number_of_documents),
            convert.bytes_to_human_string(t.compressed_size_in_bytes),
            convert.bytes_to_human_string(t.uncompressed_size_in_bytes),
        ]
        if not only_auto_generated_challenges:
            line.append(t.default_challenge)
            line.append(",".join(map(str, t.challenges)))
        data.append(line)

    headers = ["Name", "Description", "Documents", "Compressed Size", "Uncompressed Size"]
    if not only_auto_generated_challenges:
        headers.append("Default Challenge")
        headers.append("All Challenges")

    console.println("Available tracks:\n")
    console.println(tabulate.tabulate(tabular_data=data, headers=headers))


def track_info(cfg):
    def format_task(t, indent="", num="", suffix=""):
        msg = f"{indent}{num}{str(t)}"
        if t.clients > 1:
            msg += f" ({t.clients} clients)"
        msg += suffix
        return msg

    def challenge_info(c):
        if not c.auto_generated:
            msg = f"Challenge [{c.name}]"
            if c.default:
                msg += " (run by default)"
            console.println(msg, underline="=", overline="=")
            if c.description:
                console.println(f"\n{c.description}")

        console.println("\nSchedule:", underline="-")
        console.println("")
        for num, task in enumerate(c.schedule, start=1):
            if task.nested:
                console.println(format_task(task, suffix=":", num=f"{num}. "))
                for leaf_num, leaf_task in enumerate(task, start=1):
                    console.println(format_task(leaf_task, indent="\t", num=f"{num}.{leaf_num} "))
            else:
                console.println(format_task(task, num=f"{num}. "))

    t = load_track(cfg)
    console.println(f"Showing details for track [{t.name}]:\n")
    console.println(f"* Description: {t.description}")
    if t.number_of_documents:
        console.println(f"* Documents: {convert.number_to_human_string(t.number_of_documents)}")
        console.println(f"* Compressed Size: {convert.bytes_to_human_string(t.compressed_size_in_bytes)}")
        console.println(f"* Uncompressed Size: {convert.bytes_to_human_string(t.uncompressed_size_in_bytes)}")
    console.println("")

    if t.selected_challenge:
        challenge_info(t.selected_challenge)
    else:
        for challenge in t.challenges:
            challenge_info(challenge)
            console.println("")


def load_track(cfg, install_dependencies=False):
    """

    Loads a track

    :param cfg: The config object. It contains the name of the track to load.
    :return: The loaded track.
    """
    repo = track_repo(cfg)
    return _load_single_track(cfg, repo, repo.track_name, install_dependencies)


def _install_dependencies(dependencies):
    if dependencies:
        log_path = os.path.join(paths.logs(), "dependency.log")
        console.info(f"Installing track dependencies [{', '.join(dependencies)}]")
        try:
            with open(log_path, "ab") as install_log:
                subprocess.check_call(
                    [sys.executable, "-m", "pip", "install", *dependencies, "--upgrade", "--target", paths.libs()],
                    stdout=install_log,
                    stderr=install_log,
                )
        except subprocess.CalledProcessError:
            raise exceptions.SystemSetupError(f"Installation of track dependencies failed. See [{install_log.name}] for more information.")


def _load_single_track(cfg, track_repository, track_name, install_dependencies=False):
    try:
        track_dir = track_repository.track_dir(track_name)
        reader = TrackFileReader(cfg)
        current_track = reader.read(track_name, track_repository.track_file(track_name), track_dir)
        tpr = TrackProcessorRegistry(cfg)
        if install_dependencies:
            _install_dependencies(current_track.dependencies)
        has_plugins = load_track_plugins(cfg, track_name, register_track_processor=tpr.register_track_processor)
        current_track.has_plugins = has_plugins
        for processor in tpr.processors:
            processor.on_after_load_track(current_track)
        return current_track
    except FileNotFoundError as e:
        logging.getLogger(__name__).exception("Cannot load track [%s]", track_name)
        raise exceptions.SystemSetupError(
            f"Cannot load track [{track_name}]. List the available tracks with [{PROGRAM_NAME} list tracks]."
        ) from e
    except BaseException:
        logging.getLogger(__name__).exception("Cannot load track [%s]", track_name)
        raise


def load_track_plugins(
    cfg,
    track_name,
    register_runner=None,
    register_scheduler=None,
    register_track_processor=None,
    force_update=False,
):
    """
    Loads plugins that are defined for the current track (as specified by the configuration).

    :param cfg: The config object.
    :param track_name: Name of the track for which plugins should be loaded.
    :param register_runner: An optional function where runners can be registered.
    :param register_scheduler: An optional function where custom schedulers can be registered.
    :param register_track_processor: An optional function where track processors can be registered.
    :param force_update: If set to ``True`` this ensures that the track is first updated from the remote repository.
                         Defaults to ``False``.
    :return: True iff this track defines plugins and they have been loaded.
    """
    repo = track_repo(cfg, fetch=force_update, update=force_update)
    track_plugin_path = repo.track_dir(track_name)
    logging.getLogger(__name__).debug("Invoking plugin_reader with name [%s] resolved to path [%s]", track_name, track_plugin_path)
    plugin_reader = TrackPluginReader(track_plugin_path, register_runner, register_scheduler, register_track_processor)

    if plugin_reader.can_load():
        plugin_reader.load()
        return True
    else:
        return False


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


def track_path(cfg):
    repo = track_repo(cfg)
    track_name = repo.track_name
    track_dir = repo.track_dir(track_name)
    return track_dir


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
        repo_revision = cfg.opts("track", "repository.revision", mandatory=False)
        offline = cfg.opts("system", "offline.mode")
        remote_url = cfg.opts("tracks", "%s.url" % repo_name, mandatory=False)
        root = cfg.opts("node", "root.dir")
        track_repositories = cfg.opts("benchmarks", "track.repository.dir")
        tracks_dir = os.path.join(root, track_repositories)

        self.repo = repo_class(remote_url, tracks_dir, repo_name, "tracks", offline, fetch)
        if update:
            if repo_revision:
                # skip checkout if already on correct version.  this is helpful in case of multiple actors loading simultaneously.
                if not self.repo.correct_revision(repo_revision):
                    self.repo.checkout(repo_revision)
            else:
                self.repo.update(distribution_version)
                cfg.add(config.Scope.applicationOverride, "track", "repository.revision", self.repo.revision)

    @property
    def track_names(self):
        retval = []
        # + 1 to capture trailing slash
        fully_qualified_path_length = len(self.repo.repo_dir) + 1
        for root_dir, _, files in os.walk(self.repo.repo_dir):
            if "track.json" in files:
                retval.append(root_dir[fully_qualified_path_length:])
        return retval

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


def operation_parameters(t, task):
    op = task.operation
    if op.param_source:
        return params.param_source_for_name(op.param_source, t, op.params)
    else:
        return params.param_source_for_operation(op.type, t, op.params, task.name)


def used_corpora(t):
    corpora = {}
    if t.corpora:
        challenge = t.selected_challenge_or_default
        for task in challenge.schedule:
            for sub_task in task:
                param_source = operation_parameters(t, sub_task)
                if hasattr(param_source, "corpora"):
                    for c in param_source.corpora:
                        # We might have the same corpus *but* they contain different doc sets. Therefore also need to union over doc sets.
                        corpora[c.name] = corpora.get(c.name, c).union(c)
    return corpora.values()


class DefaultTrackPreparator(TrackProcessor):
    def __init__(self):
        super().__init__()
        # just declare here, will be injected later
        self.cfg = None
        self.downloader = None
        self.decompressor = None
        self.track = None

    @staticmethod
    def prepare_docs(cfg, track, corpus, preparator):
        for document_set in corpus.documents:
            if document_set.is_bulk:
                data_root = data_dir(cfg, track.name, corpus.name)
                logging.getLogger(__name__).info(
                    "Resolved data root directory for document corpus [%s] in track [%s] to [%s].", corpus.name, track.name, data_root
                )
                if len(data_root) == 1:
                    preparator.prepare_document_set(document_set, data_root[0])
                # attempt to prepare everything in the current directory and fallback to the corpus directory
                elif not preparator.prepare_bundled_document_set(document_set, data_root[0]):
                    preparator.prepare_document_set(document_set, data_root[1])

    def on_prepare_track(self, track, data_root_dir) -> Generator[Tuple[Callable, dict], None, None]:
        prep = DocumentSetPreparator(track.name, self.downloader, self.decompressor)
        for corpus in used_corpora(track):
            params = {"cfg": self.cfg, "track": track, "corpus": corpus, "preparator": prep}
            yield DefaultTrackPreparator.prepare_docs, params


class Decompressor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def decompress(self, archive_path, documents_path, uncompressed_size):
        if uncompressed_size:
            msg = (
                f"Decompressing track data from [{archive_path}] to [{documents_path}] (resulting size: "
                f"[{convert.bytes_to_gb(uncompressed_size):.2f}] GB) ... "
            )
        else:
            msg = f"Decompressing track data from [{archive_path}] to [{documents_path}] ... "

        console.info(msg, end="", flush=True, logger=self.logger)
        io.decompress(archive_path, io.dirname(archive_path))
        console.println("[OK]")
        if not os.path.isfile(documents_path):
            raise exceptions.DataError(
                f"Decompressing [{archive_path}] did not create [{documents_path}]. Please check with the track "
                f"author if the compressed archive has been created correctly."
            )

        extracted_bytes = os.path.getsize(documents_path)
        if uncompressed_size is not None and extracted_bytes != uncompressed_size:
            raise exceptions.DataError(
                f"[{documents_path}] is corrupt. Extracted [{extracted_bytes}] bytes but [{uncompressed_size}] bytes are expected."
            )


class Downloader:
    def __init__(self, offline, test_mode):
        self.offline = offline
        self.test_mode = test_mode
        self.logger = logging.getLogger(__name__)

    def download(self, base_url, target_path, size_in_bytes):
        file_name = os.path.basename(target_path)

        if not base_url:
            raise exceptions.DataError("Cannot download data because no base URL is provided.")
        if self.offline:
            raise exceptions.SystemSetupError(f"Cannot find [{target_path}]. Please disable offline mode and retry.")

        if base_url.endswith("/"):
            separator = ""
        else:
            separator = "/"
        # join manually as `urllib.parse.urljoin` does not work with S3 or GS URL schemes.
        data_url = f"{base_url}{separator}{file_name}"
        try:
            io.ensure_dir(os.path.dirname(target_path))
            if size_in_bytes:
                size_in_mb = round(convert.bytes_to_mb(size_in_bytes))
                self.logger.info("Downloading data from [%s] (%s MB) to [%s].", data_url, size_in_mb, target_path)
            else:
                self.logger.info("Downloading data from [%s] to [%s].", data_url, target_path)

            # we want to have a bit more accurate download progress as these files are typically very large
            progress = net.Progress("[INFO] Downloading track data", accuracy=1)
            net.download(data_url, target_path, size_in_bytes, progress_indicator=progress)
            progress.finish()
            self.logger.info("Downloaded data from [%s] to [%s].", data_url, target_path)
        except urllib.error.HTTPError as e:
            if e.code == 404 and self.test_mode:
                raise exceptions.DataError(
                    "This track does not support test mode. Ask the track author to add it or disable test mode and retry."
                ) from None

            msg = f"Could not download [{data_url}] to [{target_path}]"
            if e.reason:
                msg += f" (HTTP status: {e.code}, reason: {e.reason})"
            else:
                msg += f" (HTTP status: {e.code})"
            raise exceptions.DataError(msg) from e
        except urllib.error.URLError as e:
            raise exceptions.DataError(f"Could not download [{data_url}] to [{target_path}].") from e

        if not os.path.isfile(target_path):
            raise exceptions.SystemSetupError(
                f"Could not download [{data_url}] to [{target_path}]. Verify data "
                f"are available at [{data_url}] and check your Internet connection."
            )

        actual_size = os.path.getsize(target_path)
        if size_in_bytes is not None and actual_size != size_in_bytes:
            raise exceptions.DataError(
                f"[{target_path}] is corrupt. Downloaded [{actual_size}] bytes but [{size_in_bytes}] bytes are expected."
            )


class DocumentSetPreparator:
    def __init__(self, track_name, downloader, decompressor):
        self.track_name = track_name
        self.downloader = downloader
        self.decompressor = decompressor

    def is_locally_available(self, file_name):
        return os.path.isfile(file_name)

    def has_expected_size(self, file_name, expected_size):
        return expected_size is None or os.path.getsize(file_name) == expected_size

    def create_file_offset_table(self, document_file_path, expected_number_of_lines):
        # just rebuild the file every time for the time being. Later on, we might check the data file fingerprint to avoid it
        lines_read = io.prepare_file_offset_table(document_file_path)
        if lines_read and lines_read != expected_number_of_lines:
            io.remove_file_offset_table(document_file_path)
            raise exceptions.DataError(
                f"Data in [{document_file_path}] for track [{self.track_name}] are invalid. "
                f"Expected [{expected_number_of_lines}] lines but got [{lines_read}]."
            )

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
            if self.is_locally_available(doc_path) and self.has_expected_size(doc_path, document_set.uncompressed_size_in_bytes):
                break
            if (
                document_set.has_compressed_corpus()
                and self.is_locally_available(archive_path)
                and self.has_expected_size(archive_path, document_set.compressed_size_in_bytes)
            ):
                self.decompressor.decompress(archive_path, doc_path, document_set.uncompressed_size_in_bytes)
            else:
                if document_set.has_compressed_corpus():
                    target_path = archive_path
                    expected_size = document_set.compressed_size_in_bytes
                elif document_set.has_uncompressed_corpus():
                    target_path = doc_path
                    expected_size = document_set.uncompressed_size_in_bytes
                else:
                    # this should not happen in practice as the JSON schema should take care of this
                    raise exceptions.RallyAssertionError(f"Track {self.track_name} specifies documents but no corpus")

                try:
                    self.downloader.download(document_set.base_url, target_path, expected_size)
                except exceptions.DataError as e:
                    if e.message == "Cannot download data because no base URL is provided." and self.is_locally_available(target_path):
                        raise exceptions.DataError(
                            f"[{target_path}] is present but does not have the expected "
                            f"size of [{expected_size}] bytes and it cannot be downloaded "
                            f"because no base URL is provided."
                        ) from None
                    raise

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
                    raise exceptions.DataError(
                        f"[{doc_path}] is present but does not have the expected size "
                        f"of [{document_set.uncompressed_size_in_bytes}] bytes."
                    )

            if document_set.has_compressed_corpus() and self.is_locally_available(archive_path):
                if self.has_expected_size(archive_path, document_set.compressed_size_in_bytes):
                    self.decompressor.decompress(archive_path, doc_path, document_set.uncompressed_size_in_bytes)
                else:
                    # treat this is an error because if the file is present but the size does not match, something is
                    # really fishy. It is likely that the user is currently creating a new track and did not specify
                    # the file size correctly.
                    raise exceptions.DataError(
                        f"[{archive_path}] is present but does not have "
                        f"the expected size of [{document_set.compressed_size_in_bytes}] bytes."
                    )
            else:
                return False


class TemplateSource:
    """
    Prepares the fully assembled track file from file or string.
    Doesn't render using jinja2, but embeds track fragments referenced with
    rally.collect(parts=...
    """

    collect_parts_re = re.compile(r"{{\ +?rally\.collect\(parts=\"(.+?(?=\"))\"\)\ +?}}")

    def __init__(self, base_path, template_file_name, source=io.FileSource, fileglobber=glob.glob):
        self.base_path = base_path
        self.template_file_name = template_file_name
        self.source = source
        self.fileglobber = fileglobber
        self.assembled_source = None
        self.logger = logging.getLogger(__name__)

    def load_template_from_file(self):
        loader = jinja2.FileSystemLoader(self.base_path)
        try:
            base_track = loader.get_source(jinja2.Environment(), self.template_file_name)
        except jinja2.TemplateNotFound:
            self.logger.exception("Could not load track from [%s].", self.template_file_name)
            raise TrackSyntaxError(f"Could not load track from '{self.template_file_name}'")
        self.assembled_source = self.replace_includes(self.base_path, base_track[0])

    def load_template_from_string(self, template_source):
        self.assembled_source = self.replace_includes(self.base_path, template_source)

    def replace_includes(self, base_path, track_fragment):
        match = TemplateSource.collect_parts_re.findall(track_fragment)
        if match:
            # Construct replacement dict for matched captures
            repl = {}
            for glob_pattern in match:
                full_glob_path = os.path.join(base_path, glob_pattern)
                sub_source = self.read_glob_files(full_glob_path)
                repl[glob_pattern] = self.replace_includes(base_path=io.dirname(full_glob_path), track_fragment=sub_source)

            def replstring(matchobj):
                # matchobj.groups() is a tuple and first element contains the matched group id
                return repl[matchobj.groups()[0]]

            return TemplateSource.collect_parts_re.sub(replstring, track_fragment)
        return track_fragment

    def read_glob_files(self, pattern):
        source = []
        files = self.fileglobber(pattern)
        for fname in files:
            with self.source(fname, mode="rt", encoding="utf-8") as fp:
                source.append(fp.read())
        return ",\n".join(source)


def default_internal_template_vars(glob_helper=lambda f: [], clock=time.Clock):
    """
    Dict of internal global variables used by our jinja2 renderers
    """

    return {
        "globals": {
            "now": clock.now(),
            "glob": glob_helper,
        },
        "filters": {
            "days_ago": time.days_ago,
        },
    }


def render_template(template_source, template_vars=None, template_internal_vars=None, loader=None):
    macros = [
        """
        {% macro collect(parts) -%}
            {% set comma = joiner() %}
            {% for part in glob(parts) %}
                {{ comma() }}
                {% include part %}
            {% endfor %}
        {%- endmacro %}
        """,
        """
        {% macro exists_set_param(setting_name, value, default_value=None, comma=True) -%}
            {% if value is defined or default_value is not none %}
                {% if comma %} , {% endif %}
                {% if default_value is not none %}
                  "{{ setting_name }}": {{ value | default(default_value) | tojson }}
                {% else %}
                  "{{ setting_name }}": {{ value | tojson }}
                {% endif %}
              {% endif %}
        {%- endmacro %}
        """,
    ]

    # place helpers dict loader first to prevent users from overriding our macros.
    env = jinja2.Environment(
        loader=jinja2.ChoiceLoader([jinja2.DictLoader({"rally.helpers": "".join(macros)}), jinja2.BaseLoader(), loader])
    )

    if template_vars:
        for k, v in template_vars.items():
            env.globals[k] = v
    # ensure that user variables never override our internal variables
    if template_internal_vars:
        for macro_type in template_internal_vars:
            for env_global_key, env_global_value in template_internal_vars[macro_type].items():
                getattr(env, macro_type)[env_global_key] = env_global_value

    template = env.from_string(template_source)
    return template.render()


def register_all_params_in_track(assembled_source, complete_track_params=None):
    j2env = jinja2.Environment()

    # we don't need the following j2 filters/macros but we define them anyway to prevent parsing failures
    internal_template_vars = default_internal_template_vars()
    for macro_type in internal_template_vars:
        for env_global_key, env_global_value in internal_template_vars[macro_type].items():
            getattr(j2env, macro_type)[env_global_key] = env_global_value

    ast = j2env.parse(assembled_source)
    j2_variables = meta.find_undeclared_variables(ast)
    if complete_track_params:
        complete_track_params.populate_track_defined_params(j2_variables)


def render_template_from_file(template_file_name, template_vars, complete_track_params=None):
    def relative_glob(start, f):
        result = glob.glob(os.path.join(start, f))
        if result:
            return [os.path.relpath(p, start) for p in result]
        else:
            return []

    base_path = io.dirname(template_file_name)
    template_source = TemplateSource(base_path, io.basename(template_file_name))
    template_source.load_template_from_file()
    register_all_params_in_track(template_source.assembled_source, complete_track_params)

    return render_template(
        loader=jinja2.FileSystemLoader(base_path),
        template_source=template_source.assembled_source,
        template_vars=template_vars,
        template_internal_vars=default_internal_template_vars(glob_helper=lambda f: relative_glob(base_path, f)),
    )


class TaskFilterTrackProcessor(TrackProcessor):
    def __init__(self, cfg):
        self.logger = logging.getLogger(__name__)
        include_tasks = cfg.opts("track", "include.tasks", mandatory=False)
        exclude_tasks = cfg.opts("track", "exclude.tasks", mandatory=False)

        if include_tasks:
            filtered_tasks = include_tasks
            self.exclude = False
        else:
            filtered_tasks = exclude_tasks
            self.exclude = True
        self.filters = self._filters_from_filtered_tasks(filtered_tasks)

    def _filters_from_filtered_tasks(self, filtered_tasks):
        filters = []
        if filtered_tasks:
            for t in filtered_tasks:
                spec = t.split(":")
                if len(spec) == 1:
                    filters.append(track.TaskNameFilter(spec[0]))
                elif len(spec) == 2:
                    if spec[0] == "type":
                        filters.append(track.TaskOpTypeFilter(spec[1]))
                    elif spec[0] == "tag":
                        filters.append(track.TaskTagFilter(spec[1]))
                    else:
                        raise exceptions.SystemSetupError(f"Invalid format for filtered tasks: [{t}]. Expected [type] but got [{spec[0]}].")
                else:
                    raise exceptions.SystemSetupError(f"Invalid format for filtered tasks: [{t}]")
        return filters

    def _filter_out_match(self, task):
        for f in self.filters:
            if task.matches(f):
                if hasattr(task, "tasks") and self.exclude:
                    return False
                return self.exclude
        return not self.exclude

    def on_after_load_track(self, track):
        if not self.filters:
            return track

        for challenge in track.challenges:
            # don't modify the schedule while iterating over it
            tasks_to_remove = []
            for task in challenge.schedule:
                if self._filter_out_match(task):
                    tasks_to_remove.append(task)
                else:
                    leafs_to_remove = []
                    for leaf_task in task:
                        if self._filter_out_match(leaf_task):
                            leafs_to_remove.append(leaf_task)
                    for leaf_task in leafs_to_remove:
                        self.logger.info("Removing sub-task [%s] from challenge [%s] due to task filter.", leaf_task, challenge)
                        task.remove_task(leaf_task)
            for task in tasks_to_remove:
                self.logger.info("Removing task [%s] from challenge [%s] due to task filter.", task, challenge)
                challenge.remove_task(task)

        return track


class TestModeTrackProcessor(TrackProcessor):
    def __init__(self, cfg):
        self.test_mode_enabled = cfg.opts("track", "test.mode.enabled", mandatory=False, default_value=False)
        self.logger = logging.getLogger(__name__)

    def on_after_load_track(self, track):
        if not self.test_mode_enabled:
            return track
        self.logger.info("Preparing track [%s] for test mode.", str(track))
        for corpus in track.corpora:
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug("Reducing corpus size to 1000 documents for [%s]", corpus.name)
            for document_set in corpus.documents:
                # TODO #341: Should we allow this for snapshots too?
                if document_set.is_bulk:
                    document_set.number_of_documents = 1000

                    if document_set.has_compressed_corpus():
                        path, ext = io.splitext(document_set.document_archive)
                        path_2, ext_2 = io.splitext(path)

                        document_set.document_archive = f"{path_2}-1k{ext_2}{ext}"
                        document_set.document_file = f"{path_2}-1k{ext_2}"
                    elif document_set.has_uncompressed_corpus():
                        path, ext = io.splitext(document_set.document_file)
                        document_set.document_file = f"{path}-1k{ext}"
                    else:
                        raise exceptions.RallyAssertionError(
                            f"Document corpus [{corpus.name}] has neither compressed nor uncompressed corpus."
                        )

                    # we don't want to check sizes
                    document_set.compressed_size_in_bytes = None
                    document_set.uncompressed_size_in_bytes = None

        for challenge in track.challenges:
            for task in challenge.schedule:
                # we need iterate over leaf tasks and await iterating over possible intermediate 'parallel' elements
                for leaf_task in task:
                    # iteration-based schedules are divided among all clients and we should provide
                    # at least one iteration for each client.
                    if leaf_task.warmup_iterations is not None and leaf_task.warmup_iterations > leaf_task.clients:
                        count = leaf_task.clients
                        if self.logger.isEnabledFor(logging.DEBUG):
                            self.logger.debug("Resetting warmup iterations to %d for [%s]", count, str(leaf_task))
                        leaf_task.warmup_iterations = count
                    if leaf_task.iterations is not None and leaf_task.iterations > leaf_task.clients:
                        count = leaf_task.clients
                        if self.logger.isEnabledFor(logging.DEBUG):
                            self.logger.debug("Resetting measurement iterations to %d for [%s]", count, str(leaf_task))
                        leaf_task.iterations = count
                    if leaf_task.warmup_time_period is not None and leaf_task.warmup_time_period > 0:
                        leaf_task.warmup_time_period = 0
                        if self.logger.isEnabledFor(logging.DEBUG):
                            self.logger.debug(
                                "Resetting warmup time period for [%s] to [%d] seconds.", str(leaf_task), leaf_task.warmup_time_period
                            )
                    if leaf_task.time_period is not None and leaf_task.time_period > 10:
                        leaf_task.time_period = 10
                        if self.logger.isEnabledFor(logging.DEBUG):
                            self.logger.debug(
                                "Resetting measurement time period for [%s] to [%d] seconds.", str(leaf_task), leaf_task.time_period
                            )

                    # Keep throttled to expose any errors but increase the target throughput for short execution times.
                    if leaf_task.target_throughput:
                        original_throughput = leaf_task.target_throughput
                        leaf_task.params.pop("target-throughput", None)
                        leaf_task.params.pop("target-interval", None)
                        leaf_task.params["target-throughput"] = f"{sys.maxsize} {original_throughput.unit}"

        return track


class CompleteTrackParams:
    def __init__(self, user_specified_track_params=None):
        self.track_defined_params = set()
        self.user_specified_track_params = user_specified_track_params if user_specified_track_params else {}

    def populate_track_defined_params(self, list_of_track_params=None):
        self.track_defined_params.update(set(list_of_track_params))

    @property
    def sorted_track_defined_params(self):
        return sorted(self.track_defined_params)

    def unused_user_defined_track_params(self):
        set_user_params = set(list(self.user_specified_track_params.keys()))
        set_user_params.difference_update(self.track_defined_params)

        return list(set_user_params)


class TrackFileReader:
    MINIMUM_SUPPORTED_TRACK_VERSION = 2
    MAXIMUM_SUPPORTED_TRACK_VERSION = 2
    """
    Creates a track from a track file.
    """

    def __init__(self, cfg):
        track_schema_file = os.path.join(cfg.opts("node", "rally.root"), "resources", "track-schema.json")
        with open(track_schema_file, encoding="utf-8") as f:
            self.track_schema = json.loads(f.read())
        self.track_params = cfg.opts("track", "params", mandatory=False)
        self.complete_track_params = CompleteTrackParams(user_specified_track_params=self.track_params)
        self.read_track = TrackSpecificationReader(
            track_params=self.track_params,
            complete_track_params=self.complete_track_params,
            selected_challenge=cfg.opts("track", "challenge.name", mandatory=False),
        )
        self.logger = logging.getLogger(__name__)

    def read(self, track_name, track_spec_file, mapping_dir):
        """
        Reads a track file, verifies it against the JSON schema and if valid, creates a track.

        :param track_name: The name of the track.
        :param track_spec_file: The complete path to the track specification file.
        :param mapping_dir: The directory where the mapping files for this track are stored locally.
        :return: A corresponding track instance if the track file is valid.
        """

        self.logger.info("Reading track specification file [%s].", track_spec_file)
        # render the track to a temporary file instead of dumping it into the logs. It is easier to check for error messages
        # involving lines numbers and it also does not bloat Rally's log file so much.
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp:
            try:
                rendered = render_template_from_file(track_spec_file, self.track_params, complete_track_params=self.complete_track_params)
                with open(tmp.name, "w", encoding="utf-8") as f:
                    f.write(rendered)
                self.logger.info("Final rendered track for '%s' has been written to '%s'.", track_spec_file, tmp.name)
                track_spec = json.loads(rendered)
            except jinja2.exceptions.TemplateNotFound:
                self.logger.exception("Could not load [%s]", track_spec_file)
                raise exceptions.SystemSetupError(f"Track {track_name} does not exist")
            except json.JSONDecodeError as e:
                self.logger.exception("Could not load [%s].", track_spec_file)
                msg = f"Could not load '{track_spec_file}': {str(e)}."
                if e.doc and e.lineno > 0 and e.colno > 0:
                    line_idx = e.lineno - 1
                    lines = e.doc.split("\n")
                    ctx_line_count = 3
                    ctx_start = max(0, line_idx - ctx_line_count)
                    ctx_end = min(line_idx + ctx_line_count, len(lines))
                    erroneous_lines = lines[ctx_start:ctx_end]
                    erroneous_lines.insert(line_idx - ctx_start + 1, "-" * (e.colno - 1) + "^ Error is here")
                    msg += " Lines containing the error:\n\n{}\n\n".format("\n".join(erroneous_lines))
                msg += f"The complete track has been written to '{tmp.name}' for diagnosis."
                raise TrackSyntaxError(msg)
            except Exception as e:
                self.logger.exception("Could not load [%s].", track_spec_file)
                msg = f"Could not load '{track_spec_file}'. The complete track has been written to '{tmp.name}' for diagnosis."
                raise TrackSyntaxError(msg, e)

        # check the track version before even attempting to validate the JSON format to avoid bogus errors.
        raw_version = track_spec.get("version", TrackFileReader.MAXIMUM_SUPPORTED_TRACK_VERSION)
        try:
            track_version = int(raw_version)
        except ValueError:
            raise exceptions.InvalidSyntax("version identifier for track %s must be numeric but was [%s]" % (track_name, str(raw_version)))

        if TrackFileReader.MINIMUM_SUPPORTED_TRACK_VERSION > track_version:
            raise exceptions.RallyError(
                "Track {} is on version {} but needs to be updated at least to version {} to work with the "
                "current version of Rally.".format(track_name, track_version, TrackFileReader.MINIMUM_SUPPORTED_TRACK_VERSION)
            )
        if TrackFileReader.MAXIMUM_SUPPORTED_TRACK_VERSION < track_version:
            raise exceptions.RallyError(
                "Track {} requires a newer version of Rally. Please upgrade Rally (supported track version: {}, "
                "required track version: {}).".format(track_name, TrackFileReader.MAXIMUM_SUPPORTED_TRACK_VERSION, track_version)
            )

        try:
            jsonschema.validate(track_spec, self.track_schema)
        except jsonschema.exceptions.ValidationError as ve:
            raise TrackSyntaxError(
                "Track '{}' is invalid.\n\nError details: {}\nInstance: {}\nPath: {}\nSchema path: {}".format(
                    track_name, ve.message, json.dumps(ve.instance, indent=4, sort_keys=True), ve.absolute_path, ve.absolute_schema_path
                )
            )

        current_track = self.read_track(track_name, track_spec, mapping_dir, track_spec_file)

        unused_user_defined_track_params = self.complete_track_params.unused_user_defined_track_params()
        if len(unused_user_defined_track_params) > 0:
            err_msg = (
                "Some of your track parameter(s) {} are not used by this track; perhaps you intend to use {} instead.\n\n"
                "All track parameters you provided are:\n"
                "{}\n\n"
                "All parameters exposed by this track:\n"
                "{}".format(
                    ",".join(opts.double_quoted_list_of(sorted(unused_user_defined_track_params))),
                    ",".join(
                        opts.double_quoted_list_of(
                            sorted(
                                opts.make_list_of_close_matches(
                                    unused_user_defined_track_params, self.complete_track_params.track_defined_params
                                )
                            )
                        )
                    ),
                    "\n".join(opts.bulleted_list_of(sorted(list(self.track_params.keys())))),
                    "\n".join(opts.bulleted_list_of(self.complete_track_params.sorted_track_defined_params)),
                )
            )

            self.logger.critical(err_msg)
            # also dump the message on the console
            console.println(err_msg)
            raise exceptions.TrackConfigError(f"Unused track parameters {sorted(unused_user_defined_track_params)}.")
        return current_track


class TrackPluginReader:
    """
    Loads track plugins
    """

    def __init__(self, track_plugin_path, runner_registry=None, scheduler_registry=None, track_processor_registry=None):
        self.runner_registry = runner_registry
        self.scheduler_registry = scheduler_registry
        self.track_processor_registry = track_processor_registry
        self.loader = modules.ComponentLoader(root_path=track_plugin_path, component_entry_point="track")

    def can_load(self):
        return self.loader.can_load()

    def load(self):
        # get dependent libraries installed in a prior step. ensure dir exists to make sure loading works correctly.
        os.makedirs(paths.libs(), exist_ok=True)
        sys.path.insert(0, paths.libs())
        root_module = self.loader.load()
        try:
            # every module needs to have a register() method
            root_module.register(self)
        except BaseException:
            msg = "Could not register track plugin at [%s]" % self.loader.root_path
            logging.getLogger(__name__).exception(msg)
            raise exceptions.SystemSetupError(msg)

    def register_param_source(self, name, param_source):
        params.register_param_source_for_name(name, param_source)

    def register_runner(self, name, runner, **kwargs):
        if self.runner_registry:
            self.runner_registry(name, runner, **kwargs)

    def register_scheduler(self, name, scheduler):
        if self.scheduler_registry:
            self.scheduler_registry(name, scheduler)

    def register_track_processor(self, track_processor):
        if self.track_processor_registry:
            self.track_processor_registry(track_processor)

    @property
    def meta_data(self):
        return {
            "rally_version": version.release_version(),
            "async_runner": True,
        }


class TrackSpecificationReader:
    """
    Creates a track instances based on its parsed JSON description.
    """

    def __init__(self, track_params=None, complete_track_params=None, selected_challenge=None, source=io.FileSource):
        self.name = None
        self.base_path = None
        self.track_params = track_params if track_params else {}
        self.complete_track_params = complete_track_params
        self.selected_challenge = selected_challenge
        self.source = source
        self.logger = logging.getLogger(__name__)

    def __call__(self, track_name, track_specification, mapping_dir, spec_file=None):
        self.name = track_name
        self.base_path = os.path.dirname(os.path.abspath(spec_file)) if spec_file else None
        description = self._r(track_specification, "description", mandatory=False, default_value="")

        meta_data = self._r(track_specification, "meta", mandatory=False)
        indices = [
            self._create_index(idx, mapping_dir) for idx in self._r(track_specification, "indices", mandatory=False, default_value=[])
        ]
        data_streams = [
            self._create_data_stream(idx) for idx in self._r(track_specification, "data-streams", mandatory=False, default_value=[])
        ]
        if len(indices) > 0 and len(data_streams) > 0:
            # we guard against this early and support either or
            raise TrackSyntaxError("indices and data-streams cannot both be specified")
        templates = [
            self._create_index_template(tpl, mapping_dir)
            for tpl in self._r(track_specification, "templates", mandatory=False, default_value=[])
        ]
        composable_templates = [
            self._create_index_template(tpl, mapping_dir)
            for tpl in self._r(track_specification, "composable-templates", mandatory=False, default_value=[])
        ]
        component_templates = [
            self._create_component_template(tpl, mapping_dir)
            for tpl in self._r(track_specification, "component-templates", mandatory=False, default_value=[])
        ]
        corpora = self._create_corpora(self._r(track_specification, "corpora", mandatory=False, default_value=[]), indices, data_streams)
        challenges = self._create_challenges(track_specification)
        dependencies = self._r(track_specification, "dependencies", mandatory=False)
        # at this point, *all* track params must have been referenced in the templates
        return track.Track(
            name=self.name,
            meta_data=meta_data,
            description=description,
            challenges=challenges,
            indices=indices,
            data_streams=data_streams,
            templates=templates,
            composable_templates=composable_templates,
            component_templates=component_templates,
            corpora=corpora,
            dependencies=dependencies,
            root=self.base_path,
        )

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
            idx_body_tmpl_src = TemplateSource(mapping_dir, body_file, self.source)
            with self.source(os.path.join(mapping_dir, body_file), "rt") as f:
                idx_body_tmpl_src.load_template_from_string(f.read())
                body = self._load_template(idx_body_tmpl_src.assembled_source, f"definition for index {index_name} in {body_file}")
        else:
            body = None

        return track.Index(name=index_name, body=body, types=self._r(index_spec, "types", mandatory=False, default_value=[]))

    def _create_data_stream(self, data_stream_spec):
        return track.DataStream(name=self._r(data_stream_spec, "name"))

    def _create_component_template(self, tpl_spec, mapping_dir):
        name = self._r(tpl_spec, "name")
        template_file = self._r(tpl_spec, "template")
        template_path = self._r(tpl_spec, "template-path", mandatory=False)
        template_file = os.path.join(mapping_dir, template_file)
        idx_tmpl_src = TemplateSource(mapping_dir, template_file, self.source)
        with self.source(template_file, "rt") as f:
            idx_tmpl_src.load_template_from_string(f.read())
            template_content = self._load_template(
                idx_tmpl_src.assembled_source, f"definition for component template {name} in {template_file}"
            )
        if template_path:
            for field in template_path.split("."):
                template_content = template_content[field]
        return track.ComponentTemplate(name, template_content)

    def _create_index_template(self, tpl_spec, mapping_dir):
        name = self._r(tpl_spec, "name")
        template_file = self._r(tpl_spec, "template")
        template_path = self._r(tpl_spec, "template-path", mandatory=False)
        index_pattern = self._r(tpl_spec, "index-pattern")
        delete_matching_indices = self._r(tpl_spec, "delete-matching-indices", mandatory=False, default_value=True)
        template_file = os.path.join(mapping_dir, template_file)
        idx_tmpl_src = TemplateSource(mapping_dir, template_file, self.source)
        with self.source(template_file, "rt") as f:
            idx_tmpl_src.load_template_from_string(f.read())
            template_content = self._load_template(
                idx_tmpl_src.assembled_source, f"definition for index template {name} in {template_file}"
            )
        if template_path:
            for field in template_path.split("."):
                template_content = template_content[field]
        return track.IndexTemplate(name, index_pattern, template_content, delete_matching_indices)

    def _load_template(self, contents, description):
        self.logger.info("Loading template [%s].", description)
        register_all_params_in_track(contents, self.complete_track_params)
        try:
            rendered = render_template(template_source=contents, template_vars=self.track_params)
            return json.loads(rendered)
        except Exception as e:
            self.logger.exception("Could not load file template for %s.", description)
            raise TrackSyntaxError("Could not load file template for '%s'" % description, str(e))

    def _create_corpora(self, corpora_specs, indices, data_streams):
        if len(indices) > 0 and len(data_streams) > 0:
            raise TrackSyntaxError("indices and data-streams cannot both be specified")
        document_corpora = []
        known_corpora_names = set()
        for corpus_spec in corpora_specs:
            name = self._r(corpus_spec, "name")

            if name in known_corpora_names:
                self._error("Duplicate document corpus name [%s]." % name)
            known_corpora_names.add(name)

            meta_data = self._r(corpus_spec, "meta", error_ctx=name, mandatory=False)
            corpus = track.DocumentCorpus(name=name, meta_data=meta_data)
            # defaults on corpus level
            default_base_url = self._r(corpus_spec, "base-url", mandatory=False, default_value=None)
            default_source_format = self._r(corpus_spec, "source-format", mandatory=False, default_value=track.Documents.SOURCE_FORMAT_BULK)
            default_action_and_meta_data = self._r(corpus_spec, "includes-action-and-meta-data", mandatory=False, default_value=False)
            corpus_target_idx = None
            corpus_target_ds = None
            corpus_target_type = None

            if len(indices) == 1:
                corpus_target_idx = self._r(corpus_spec, "target-index", mandatory=False, default_value=indices[0].name)
            elif len(indices) > 0:
                corpus_target_idx = self._r(corpus_spec, "target-index", mandatory=False)

            if len(data_streams) == 1:
                corpus_target_ds = self._r(corpus_spec, "target-data-stream", mandatory=False, default_value=data_streams[0].name)
            elif len(data_streams) > 0:
                corpus_target_ds = self._r(corpus_spec, "target-data-stream", mandatory=False)

            if len(indices) == 1 and len(indices[0].types) == 1:
                corpus_target_type = self._r(corpus_spec, "target-type", mandatory=False, default_value=indices[0].types[0])
            elif len(indices) > 0:
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
                    doc_meta_data = self._r(doc_spec, "meta", error_ctx=name, mandatory=False)

                    includes_action_and_meta_data = self._r(
                        doc_spec, "includes-action-and-meta-data", mandatory=False, default_value=default_action_and_meta_data
                    )
                    if includes_action_and_meta_data:
                        target_idx = None
                        target_type = None
                        target_ds = None
                    else:
                        target_type = self._r(doc_spec, "target-type", mandatory=False, default_value=corpus_target_type, error_ctx=docs)

                        # require to be specified if we're using data streams and we have no default
                        target_ds = self._r(
                            doc_spec,
                            "target-data-stream",
                            mandatory=len(data_streams) > 0 and corpus_target_ds is None,
                            default_value=corpus_target_ds,
                            error_ctx=docs,
                        )
                        if target_ds and len(indices) > 0:
                            # if indices are in use we error
                            raise TrackSyntaxError("target-data-stream cannot be used when using indices")

                        if target_ds and target_type:
                            raise TrackSyntaxError("target-type cannot be used when using data-streams")

                        # need an index if we're using indices and no meta-data are present and we don't have a default
                        target_idx = self._r(
                            doc_spec,
                            "target-index",
                            mandatory=len(indices) > 0 and corpus_target_idx is None,
                            default_value=corpus_target_idx,
                            error_ctx=docs,
                        )
                        # either target_idx or target_ds
                        if target_idx and len(data_streams) > 0:
                            # if data streams are in use we error
                            raise TrackSyntaxError("target-index cannot be used when using data-streams")

                        # we need one or the other
                        if target_idx is None and target_ds is None:
                            raise TrackSyntaxError(
                                f"a {'target-index' if len(indices) > 0 else 'target-data-stream'} is required for {docs}"
                            )

                    docs = track.Documents(
                        source_format=source_format,
                        document_file=document_file,
                        document_archive=document_archive,
                        base_url=base_url,
                        includes_action_and_meta_data=includes_action_and_meta_data,
                        number_of_documents=num_docs,
                        compressed_size_in_bytes=compressed_bytes,
                        uncompressed_size_in_bytes=uncompressed_bytes,
                        target_index=target_idx,
                        target_type=target_type,
                        target_data_stream=target_ds,
                        meta_data=doc_meta_data,
                    )
                    corpus.documents.append(docs)
                else:
                    self._error("Unknown source-format [%s] in document corpus [%s]." % (source_format, name))
            document_corpora.append(corpus)
        return document_corpora

    def _create_challenges(self, track_spec):
        ops = self.parse_operations(self._r(track_spec, "operations", mandatory=False, default_value=[]))
        track_params = self._r(track_spec, "parameters", mandatory=False, default_value={})
        challenges = []
        known_challenge_names = set()
        default_challenge = None
        challenge_specs, auto_generated = self._get_challenge_specs(track_spec)
        number_of_challenges = len(challenge_specs)
        for challenge_spec in challenge_specs:
            name = self._r(challenge_spec, "name", error_ctx="challenges")
            description = self._r(challenge_spec, "description", error_ctx=name, mandatory=False)
            user_info = self._r(challenge_spec, "user-info", error_ctx=name, mandatory=False)
            challenge_params = self._r(challenge_spec, "parameters", error_ctx=name, mandatory=False, default_value={})
            meta_data = self._r(challenge_spec, "meta", error_ctx=name, mandatory=False)
            # if we only have one challenge it is treated as default challenge, no matter what the user has specified
            default = number_of_challenges == 1 or self._r(challenge_spec, "default", error_ctx=name, mandatory=False)
            selected = number_of_challenges == 1 or self.selected_challenge == name
            if default and default_challenge is not None:
                self._error(
                    "Both '%s' and '%s' are defined as default challenges. Please define only one of them as default."
                    % (default_challenge.name, name)
                )
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
                        self._error(
                            "Challenge '%s' contains multiple tasks with the name '%s'. Please use the task's name property to "
                            "assign a unique name for each task." % (name, sub_task.name)
                        )
                    else:
                        known_task_names.add(sub_task.name)

            # merge params
            final_challenge_params = dict(collections.merge_dicts(track_params, challenge_params))

            challenge = track.Challenge(
                name=name,
                parameters=final_challenge_params,
                meta_data=meta_data,
                description=description,
                user_info=user_info,
                default=default,
                selected=selected,
                auto_generated=auto_generated,
                schedule=schedule,
            )
            if default:
                default_challenge = challenge

            challenges.append(challenge)

        if challenges and default_challenge is None:
            self._error(
                'No default challenge specified. Please edit the track and add "default": true to one of the challenges %s.'
                % ", ".join([c.name for c in challenges])
            )
        return challenges

    def _get_challenge_specs(self, track_spec):
        schedule = self._r(track_spec, "schedule", mandatory=False)
        challenge = self._r(track_spec, "challenge", mandatory=False)
        challenges = self._r(track_spec, "challenges", mandatory=False)

        count_defined = len(list(filter(lambda e: e is not None, [schedule, challenge, challenges])))

        if count_defined == 0:
            self._error("You must define 'challenge', 'challenges' or 'schedule' but none is specified.")
        elif count_defined > 1:
            self._error("Multiple out of 'challenge', 'challenges' or 'schedule' are defined but only one of them is allowed.")
        elif challenge is not None:
            return [challenge], False
        elif challenges is not None:
            return challenges, False
        elif schedule is not None:
            return [{"name": "default", "schedule": schedule}], True
        else:
            raise AssertionError(f"Unexpected: schedule=[{schedule}], challenge=[{challenge}], challenges=[{challenges}]")

    def parse_parallel(self, ops_spec, ops, challenge_name):
        # use same default values as #parseTask() in case the 'parallel' element did not specify anything
        default_warmup_iterations = self._r(ops_spec, "warmup-iterations", error_ctx="parallel", mandatory=False)
        default_iterations = self._r(ops_spec, "iterations", error_ctx="parallel", mandatory=False)
        default_warmup_time_period = self._r(ops_spec, "warmup-time-period", error_ctx="parallel", mandatory=False)
        default_time_period = self._r(ops_spec, "time-period", error_ctx="parallel", mandatory=False)
        default_ramp_up_time_period = self._r(ops_spec, "ramp-up-time-period", error_ctx="parallel", mandatory=False)
        clients = self._r(ops_spec, "clients", error_ctx="parallel", mandatory=False)
        completed_by = self._r(ops_spec, "completed-by", error_ctx="parallel", mandatory=False)
        # now descent to each operation
        tasks = []
        for task in self._r(ops_spec, "tasks", error_ctx="parallel"):
            tasks.append(
                self.parse_task(
                    task,
                    ops,
                    challenge_name,
                    default_warmup_iterations,
                    default_iterations,
                    default_warmup_time_period,
                    default_time_period,
                    default_ramp_up_time_period,
                    completed_by,
                )
            )
        for task in tasks:
            if task.ramp_up_time_period != default_ramp_up_time_period:
                if default_ramp_up_time_period is None:
                    self._error(
                        f"task '{task.name}' in 'parallel' element of challenge '{challenge_name}' specifies "
                        f"a ramp-up-time-period but it is only allowed on the 'parallel' element."
                    )
                else:
                    self._error(
                        f"task '{task.name}' specifies a different ramp-up-time-period than its enclosing "
                        f"'parallel' element in challenge '{challenge_name}'."
                    )

        if completed_by:
            has_completion_task = False
            for task in tasks:
                if task.completes_parent and not has_completion_task:
                    has_completion_task = True
                elif task.completes_parent:
                    self._error(
                        f"'parallel' element for challenge '{challenge_name}' contains multiple tasks with the "
                        f"name '{completed_by}' marked with 'completed-by' but only task is allowed to match."
                    )
                elif task.any_completes_parent:
                    has_completion_task = True
            if not has_completion_task:
                self._error(
                    f"'parallel' element for challenge '{challenge_name}' is marked with 'completed-by' with "
                    f"task name '{completed_by}' but no task with this name exists."
                )
        return track.Parallel(tasks, clients)

    def parse_task(
        self,
        task_spec,
        ops,
        challenge_name,
        default_warmup_iterations=None,
        default_iterations=None,
        default_warmup_time_period=None,
        default_time_period=None,
        default_ramp_up_time_period=None,
        completed_by_name=None,
    ):

        op_spec = task_spec["operation"]
        if isinstance(op_spec, str) and op_spec in ops:
            op = ops[op_spec]
        else:
            # may as well an inline operation
            op = self.parse_operation(op_spec, error_ctx="inline operation in challenge %s" % challenge_name)

        schedule = self._r(task_spec, "schedule", error_ctx=op.name, mandatory=False)
        task_name = self._r(task_spec, "name", error_ctx=op.name, mandatory=False, default_value=op.name)

        task = track.Task(
            name=task_name,
            operation=op,
            tags=self._r(task_spec, "tags", error_ctx=op.name, mandatory=False),
            meta_data=self._r(task_spec, "meta", error_ctx=op.name, mandatory=False),
            warmup_iterations=self._r(
                task_spec, "warmup-iterations", error_ctx=op.name, mandatory=False, default_value=default_warmup_iterations
            ),
            iterations=self._r(task_spec, "iterations", error_ctx=op.name, mandatory=False, default_value=default_iterations),
            warmup_time_period=self._r(
                task_spec, "warmup-time-period", error_ctx=op.name, mandatory=False, default_value=default_warmup_time_period
            ),
            time_period=self._r(task_spec, "time-period", error_ctx=op.name, mandatory=False, default_value=default_time_period),
            ramp_up_time_period=self._r(
                task_spec, "ramp-up-time-period", error_ctx=op.name, mandatory=False, default_value=default_ramp_up_time_period
            ),
            clients=self._r(task_spec, "clients", error_ctx=op.name, mandatory=False, default_value=1),
            completes_parent=(task_name == completed_by_name),
            any_completes_parent=(completed_by_name == "any"),
            schedule=schedule,
            # this is to provide scheduler-specific parameters for custom schedulers.
            params=task_spec,
        )
        if task.warmup_iterations is not None and task.time_period is not None:
            self._error(
                f"Operation '{op.name}' in challenge '{challenge_name}' defines {task.warmup_iterations} "
                f"warmup iterations and a time period of {task.time_period} seconds but mixing time periods "
                f"and iterations is not allowed."
            )
        elif task.warmup_time_period is not None and task.iterations is not None:
            self._error(
                f"Operation '{op.name}' in challenge '{challenge_name}' defines a warmup time period of "
                f"{task.warmup_time_period} seconds and {task.iterations} iterations but mixing time periods "
                f"and iterations is not allowed."
            )

        if (task.warmup_iterations is not None or task.iterations is not None) and task.ramp_up_time_period is not None:
            self._error(
                f"Operation '{op.name}' in challenge '{challenge_name}' defines a ramp-up time period of "
                f"{task.ramp_up_time_period} seconds as well as {task.warmup_iterations} warmup iterations and "
                f"{task.iterations} iterations but mixing time periods and iterations is not allowed."
            )

        if task.ramp_up_time_period is not None:
            if task.warmup_time_period is None:
                self._error(
                    f"Operation '{op.name}' in challenge '{challenge_name}' defines a ramp-up time period of "
                    f"{task.ramp_up_time_period} seconds but no warmup-time-period."
                )
            elif task.warmup_time_period < task.ramp_up_time_period:
                self._error(
                    f"The warmup-time-period of operation '{op.name}' in challenge '{challenge_name}' is "
                    f"{task.warmup_time_period} seconds but must be greater than or equal to the "
                    f"ramp-up-time-period of {task.ramp_up_time_period} seconds."
                )

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
            op = track.OperationType.from_hyphenated_string(op_type_name)
            if "include-in-reporting" not in params:
                params["include-in-reporting"] = not op.admin_op
            self.logger.debug("Using built-in operation type [%s] for operation [%s].", op_type_name, op_name)
        except KeyError:
            self.logger.info("Using user-provided operation type [%s] for operation [%s].", op_type_name, op_name)

        try:
            return track.Operation(name=op_name, meta_data=meta_data, operation_type=op_type_name, params=params, param_source=param_source)
        except exceptions.InvalidSyntax as e:
            raise TrackSyntaxError("Invalid operation [%s]: %s" % (op_name, str(e)))
