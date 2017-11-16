import re
import unittest.mock as mock
from unittest import TestCase

import jinja2

from esrally import exceptions, config
from esrally.utils import io
from esrally.track import loader, track


def strip_ws(s):
    return re.sub(r"\s", "", s)


class StaticClock:
    NOW = 1453362707.0

    @staticmethod
    def now():
        return StaticClock.NOW

    @staticmethod
    def stop_watch():
        return None


class SimpleTrackRepositoryTests(TestCase):
    @mock.patch("os.path.exists")
    @mock.patch("os.path.isdir")
    def test_track_from_directory(self, is_dir, path_exists):
        is_dir.return_value = True
        path_exists.return_value = True

        repo = loader.SimpleTrackRepository("/path/to/track/unit-test")
        self.assertEqual("unit-test", repo.track_name)
        self.assertEqual(["unit-test"], repo.track_names)
        self.assertEqual("/path/to/track/unit-test", repo.track_dir("unit-test"))
        self.assertEqual("/path/to/track/unit-test/track.json", repo.track_file("unit-test"))

    @mock.patch("os.path.exists")
    @mock.patch("os.path.isdir")
    @mock.patch("os.path.isfile")
    def test_track_from_file(self, is_file, is_dir, path_exists):
        is_file.return_value = True
        is_dir.return_value = False
        path_exists.return_value = True

        repo = loader.SimpleTrackRepository("/path/to/track/unit-test/my-track.json")
        self.assertEqual("my-track", repo.track_name)
        self.assertEqual(["my-track"], repo.track_names)
        self.assertEqual("/path/to/track/unit-test", repo.track_dir("my-track"))
        self.assertEqual("/path/to/track/unit-test/my-track.json", repo.track_file("my-track"))

    @mock.patch("os.path.exists")
    @mock.patch("os.path.isdir")
    @mock.patch("os.path.isfile")
    def test_track_from_named_pipe(self, is_file, is_dir, path_exists):
        is_file.return_value = False
        is_dir.return_value = False
        path_exists.return_value = True

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            loader.SimpleTrackRepository("a named pipe cannot point to a track")
        self.assertEqual("a named pipe cannot point to a track is neither a file nor a directory", ctx.exception.args[0])

    @mock.patch("os.path.exists")
    def test_track_from_non_existing_path(self, path_exists):
        path_exists.return_value = False
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            loader.SimpleTrackRepository("/path/does/not/exist")
        self.assertEqual("Track path /path/does/not/exist does not exist", ctx.exception.args[0])

    @mock.patch("os.path.isdir")
    @mock.patch("os.path.exists")
    def test_track_from_directory_without_track(self, path_exists, is_dir):
        # directory exists, but not the file
        path_exists.side_effect = [True, False]
        is_dir.return_value = True
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            loader.SimpleTrackRepository("/path/to/not/a/track")
        self.assertEqual("Could not find track.json in /path/to/not/a/track", ctx.exception.args[0])

    @mock.patch("os.path.exists")
    @mock.patch("os.path.isdir")
    @mock.patch("os.path.isfile")
    def test_track_from_file_but_not_json(self, is_file, is_dir, path_exists):
        is_file.return_value = True
        is_dir.return_value = False
        path_exists.return_value = True

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            loader.SimpleTrackRepository("/path/to/track/unit-test/my-track.xml")
        self.assertEqual("/path/to/track/unit-test/my-track.xml has to be a JSON file", ctx.exception.args[0])


class GitRepositoryTests(TestCase):
    class MockGitRepo:
        def __init__(self, remote_url, root_dir, repo_name, resource_name, offline, fetch=True):
            self.repo_dir = "%s/%s" % (root_dir, repo_name)

    @mock.patch("os.path.exists")
    @mock.patch("os.walk")
    def test_track_from_existing_repo(self, walk, exists):
        walk.return_value = iter([(".", ["unittest", "unittest2", "unittest3"], [])])
        exists.return_value = True
        cfg = config.Config()
        cfg.add(config.Scope.application, "track", "track.name", "unittest")
        cfg.add(config.Scope.application, "track", "repository.name", "default")
        cfg.add(config.Scope.application, "system", "offline.mode", False)
        cfg.add(config.Scope.application, "node", "root.dir", "/tmp")
        cfg.add(config.Scope.application, "benchmarks", "track.repository.dir", "tracks")

        repo = loader.GitTrackRepository(cfg, fetch=False, update=False, repo_class=GitRepositoryTests.MockGitRepo)

        self.assertEqual("unittest", repo.track_name)
        self.assertEqual(["unittest", "unittest2", "unittest3"], list(repo.track_names))
        self.assertEqual("/tmp/tracks/default/unittest", repo.track_dir("unittest"))
        self.assertEqual("/tmp/tracks/default/unittest/track.json", repo.track_file("unittest"))


class TrackPreparationTests(TestCase):
    @mock.patch("esrally.utils.io.prepare_file_offset_table")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_does_nothing_if_document_file_available(self, is_file, get_size, prepare_file_offset_table):
        is_file.return_value = True
        get_size.return_value = 2000
        prepare_file_offset_table.return_value = 5

        loader.prepare_corpus(track_name="unit-test",
                              source_root_url="http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/unit-test",
                              data_root="/tmp",
                              type=track.Type(name="test-type", mapping=None, document_file="docs.json", document_archive="docs.json.bz2",
                                              number_of_documents=5, compressed_size_in_bytes=200, uncompressed_size_in_bytes=2000),
                              offline=False,
                              test_mode=False)

        prepare_file_offset_table.assert_called_with("/tmp/docs.json")

    @mock.patch("esrally.utils.io.prepare_file_offset_table")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_decompresses_if_archive_available(self, is_file, get_size, prepare_file_offset_table):
        is_file.return_value = True
        get_size.return_value = 2000
        prepare_file_offset_table.return_value = 5

        loader.prepare_corpus(track_name="unit-test",
                              source_root_url="http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/unit-test",
                              data_root="/tmp",
                              type=track.Type(name="test-type", mapping=None, document_file="docs.json", document_archive="docs.json.bz2",
                                              number_of_documents=5, compressed_size_in_bytes=200, uncompressed_size_in_bytes=2000),
                              offline=False,
                              test_mode=False)

        prepare_file_offset_table.assert_called_with("/tmp/docs.json")

    @mock.patch("esrally.utils.io.decompress")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_raise_error_on_wrong_uncompressed_file_size(self, is_file, get_size, decompress):
        # uncompressed file does not exist
        # compressed file exists
        # after decompression, uncompressed file exists
        is_file.side_effect = [False, True, True]
        # compressed file size is 200
        # uncompressed is corrupt, only 1 byte available
        get_size.side_effect = [200, 1]

        with self.assertRaises(exceptions.DataError) as ctx:
            loader.prepare_corpus(track_name="unit-test",
                                  source_root_url="http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/unit-test",
                                  data_root="/tmp",
                                  type=track.Type(name="test-type", mapping=None, document_file="docs.json",
                                                  document_archive="docs.json.bz2", number_of_documents=5, compressed_size_in_bytes=200,
                                                  uncompressed_size_in_bytes=2000),
                                  offline=False,
                                  test_mode=False)
        self.assertEqual("[/tmp/docs.json] is corrupt. Extracted [1] bytes but [2000] bytes are expected.", ctx.exception.args[0])

        decompress.assert_called_with("/tmp/docs.json.bz2", "/tmp")

    @mock.patch("esrally.utils.io.decompress")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_raise_error_if_compressed_does_not_contain_expected_document_file(self, is_file, get_size, decompress):
        # uncompressed file does not exist
        # compressed file exists
        # after decompression, uncompressed file does not exist (e.g. because the output file name is called differently)
        is_file.side_effect = [False, True, False]
        # compressed file size is 200
        get_size.return_value = 200

        with self.assertRaises(exceptions.DataError) as ctx:
            loader.prepare_corpus(track_name="unit-test",
                                  source_root_url="http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/unit-test",
                                  data_root="/tmp",
                                  type=track.Type(name="test-type", mapping=None, document_file="docs.json",
                                                  document_archive="docs.json.bz2", number_of_documents=5, compressed_size_in_bytes=200,
                                                  uncompressed_size_in_bytes=2000),
                                  offline=False,
                                  test_mode=False)
        self.assertEqual("Decompressing [/tmp/docs.json.bz2] did not create [/tmp/docs.json]. Please check with the track author if the "
                         "compressed archive has been created correctly.", ctx.exception.args[0])

        decompress.assert_called_with("/tmp/docs.json.bz2", "/tmp")

    @mock.patch("esrally.utils.io.prepare_file_offset_table")
    @mock.patch("esrally.utils.io.decompress")
    @mock.patch("esrally.utils.net.download")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_download_document_archive_if_no_file_available(self, is_file, get_size, ensure_dir, download, decompress, prepare_file_offset_table):
        # uncompressed file does not exist
        # compressed file does not exist
        # after download compressed file exists
        # after download uncompressed file still does not exist (in main loop)
        # after download compressed file exists (in main loop)
        # after decompression, uncompressed file exists
        is_file.side_effect = [False, False, True, False, True, True, True]
        # compressed file size is 200 after download
        # compressed file size is 200 after download (in main loop)
        # uncompressed file size is 2000 after decompression
        # uncompressed file size is 2000 after decompression (in main loop)
        get_size.side_effect = [200, 200, 2000, 2000]

        prepare_file_offset_table.return_value = 5

        loader.prepare_corpus(track_name="unit-test",
                              source_root_url="http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/unit-test",
                              data_root="/tmp",
                              type=track.Type(name="test-type", mapping=None, document_file="docs.json",
                                              document_archive="docs.json.bz2", number_of_documents=5, compressed_size_in_bytes=200,
                                              uncompressed_size_in_bytes=2000),
                              offline=False,
                              test_mode=False)

        ensure_dir.assert_called_with("/tmp")
        decompress.assert_called_with("/tmp/docs.json.bz2", "/tmp")
        download.assert_called_with("http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/unit-test/docs.json.bz2",
                                    "/tmp/docs.json.bz2", 200, progress_indicator=mock.ANY)
        prepare_file_offset_table.assert_called_with("/tmp/docs.json")

    @mock.patch("esrally.utils.io.prepare_file_offset_table")
    @mock.patch("esrally.utils.net.download")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_download_document_file_if_no_file_available(self, is_file, get_size, ensure_dir, download, prepare_file_offset_table):
        # uncompressed file does not exist
        # after download uncompressed file exists
        # after download uncompressed file exists (main loop)
        is_file.side_effect = [False, True, True]
        # uncompressed file size is 2000
        get_size.return_value = 2000

        prepare_file_offset_table.return_value = 5

        loader.prepare_corpus(track_name="unit-test",
                              source_root_url="http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/unit-test",
                              data_root="/tmp",
                              type=track.Type(name="test-type", mapping=None, document_file="docs.json",
                                              # --> We don't provide a document archive here <--
                                              document_archive=None, number_of_documents=5, uncompressed_size_in_bytes=2000),
                              offline=False,
                              test_mode=False)

        ensure_dir.assert_called_with("/tmp")
        download.assert_called_with("http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/unit-test/docs.json",
                                    "/tmp/docs.json", 2000, progress_indicator=mock.ANY)
        prepare_file_offset_table.assert_called_with("/tmp/docs.json")

    @mock.patch("esrally.utils.net.download")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("os.path.isfile")
    def test_raise_download_error_if_offline(self, is_file, ensure_dir, download):
        # uncompressed file does not exist
        is_file.return_value = False

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            loader.prepare_corpus(track_name="unit-test",
                                  source_root_url="http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/unit-test",
                                  data_root="/tmp",
                                  type=track.Type(name="test-type", mapping=None, document_file="docs.json",
                                                  number_of_documents=5, uncompressed_size_in_bytes=2000),
                                  offline=True,
                                  test_mode=False)

        self.assertEqual("Cannot find /tmp/docs.json. Please disable offline mode and retry again.", ctx.exception.args[0])

        ensure_dir.assert_not_called()
        download.assert_not_called()

    @mock.patch("esrally.utils.net.download")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("os.path.isfile")
    def test_raise_download_error_if_no_url_provided(self, is_file, ensure_dir, download):
        # uncompressed file does not exist
        is_file.return_value = False

        with self.assertRaises(exceptions.DataError) as ctx:
            loader.prepare_corpus(track_name="unit-test",
                                  source_root_url=None,
                                  data_root="/tmp",
                                  type=track.Type(name="test-type", mapping=None, document_file="docs.json",
                                                  number_of_documents=5, uncompressed_size_in_bytes=2000),
                                  offline=False,
                                  test_mode=False)

        self.assertEqual("/tmp/docs.json is missing and it cannot be downloaded because no source URL is provided in the track.",
                         ctx.exception.args[0])

        ensure_dir.assert_not_called()
        download.assert_not_called()

    @mock.patch("esrally.utils.net.download")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("os.path.isfile")
    def test_raise_download_error_no_test_mode_file(self, is_file, ensure_dir, download):
        import urllib.error

        # uncompressed file does not exist
        is_file.return_value = False

        download.side_effect = urllib.error.HTTPError("http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/unit-test/docs-1k.json",
                                                      404, "", None, None)

        with self.assertRaises(exceptions.DataError) as ctx:
            loader.prepare_corpus(track_name="unit-test",
                                  source_root_url="http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/unit-test",
                                  data_root="/tmp",
                                  type=track.Type(name="test-type", mapping=None, document_file="docs-1k.json",
                                                  number_of_documents=5, uncompressed_size_in_bytes=None),
                                  offline=False,
                                  test_mode=True)

        self.assertEqual("Track [unit-test] does not support test mode. Please ask the track author to add it or disable test mode "
                         "and retry.", ctx.exception.args[0])

        ensure_dir.assert_called_with("/tmp")
        download.assert_called_with("http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/unit-test/docs-1k.json",
                                    "/tmp/docs-1k.json", None, progress_indicator=mock.ANY)

    @mock.patch("esrally.utils.net.download")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("os.path.isfile")
    def test_raise_download_error_on_connection_problems(self, is_file, ensure_dir, download):
        import urllib.error

        # uncompressed file does not exist
        is_file.return_value = False

        download.side_effect = urllib.error.HTTPError("http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/unit-test/docs-1k.json",
                                                      500, "Internal Server Error", None, None)

        with self.assertRaises(exceptions.DataError) as ctx:
            loader.prepare_corpus(track_name="unit-test",
                                  source_root_url="http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/unit-test",
                                  data_root="/tmp",
                                  type=track.Type(name="test-type", mapping=None, document_file="docs.json",
                                                  number_of_documents=5, uncompressed_size_in_bytes=2000),
                                  offline=False,
                                  test_mode=False)

        self.assertEqual("Could not download [http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/unit-test/docs.json] "
                         "to [/tmp/docs.json] (HTTP status: 500, reason: Internal Server Error)", ctx.exception.args[0])

        ensure_dir.assert_called_with("/tmp")
        download.assert_called_with("http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/unit-test/docs.json",
                                    "/tmp/docs.json", 2000, progress_indicator=mock.ANY)


class TemplateRenderTests(TestCase):
    def test_render_simple_template(self):
        template = """
        {
            "key": {{'01-01-2000' | days_ago(now)}},
            "key2": "static value"
        }
        """

        rendered = loader.render_template(
            loader=jinja2.DictLoader({"unittest": template}), template_name="unittest", clock=StaticClock)

        expected = """
        {
            "key": 5864,
            "key2": "static value"
        }
        """
        self.assertEqual(expected, rendered)

    def test_render_template_with_external_variables(self):
        template = """
        {
            "greeting": "{{greeting | default("Aloha")}}",
            "name": "{{name | default("stranger")}}"
        }
        """

        rendered = loader.render_template(
            loader=jinja2.DictLoader({"unittest": template}), template_name="unittest", template_vars={"greeting": "Hi"}, clock=StaticClock)

        expected = """
        {
            "greeting": "Hi",
            "name": "stranger"
        }
        """
        self.assertEqual(expected, rendered)

    def test_render_template_with_globbing(self):
        def key_globber(e):
            if e == "dynamic-key-*":
                return [
                    "dynamic-key-1",
                    "dynamic-key-2",
                    "dynamic-key-3",
                ]
            else:
                return []

        template = """
        {% import "rally.helpers" as rally %}
        {
            "key1": "static value",
            {{ rally.collect(parts="dynamic-key-*") }}

        }
        """

        rendered = loader.render_template(
            loader=jinja2.DictLoader(
                {
                    "unittest": template,
                    "dynamic-key-1": '"dkey1": "value1"',
                    "dynamic-key-2": '"dkey2": "value2"',
                    "dynamic-key-3": '"dkey3": "value3"',
                 }),
            template_name="unittest", glob_helper=key_globber, clock=StaticClock)

        expected = """
        {
            "key1": "static value",
            "dkey1": "value1",
            "dkey2": "value2",
            "dkey3": "value3"

        }
        """
        self.assertEqualIgnoreWhitespace(expected, rendered)

    def test_render_template_with_variables(self):
        def key_globber(e):
            if e == "dynamic-key-*":
                return ["dynamic-key-1", "dynamic-key-2"]
            else:
                return []

        template = """
        {% set _clients = clients if clients is defined else 16 %}
        {% set _bulk_size = bulk_size if bulk_size is defined else 100 %}
        {% import "rally.helpers" as rally with context %}
        {
            "key1": "static value",
            {{ rally.collect(parts="dynamic-key-*") }}

        }
        """
        rendered = loader.render_template(
            loader=jinja2.DictLoader(
                {
                    "unittest": template,
                    "dynamic-key-1": '"dkey1": {{ _clients }}',
                    "dynamic-key-2": '"dkey2": {{ _bulk_size }}',
                 }),
            template_name="unittest", template_vars={"clients": 8}, glob_helper=key_globber, clock=StaticClock)

        expected = """
        {
            "key1": "static value",
            "dkey1": 8,
            "dkey2": 100
        }
        """
        self.assertEqualIgnoreWhitespace(expected, rendered)

    def assertEqualIgnoreWhitespace(self, expected, actual):
        self.assertEqual(strip_ws(expected), strip_ws(actual))


class TrackPostProcessingTests(TestCase):
    def test_post_processes_track_spec(self):
        track_specification = {
            "short-description": "short description for unit test",
            "description": "longer description of this track for unit test",
            "indices": [
                {
                    "name": "test-index",
                    "types": [
                        {
                            "name": "test-type",
                            "documents": "documents.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                            "mapping": "type-mappings.json"
                        }
                    ]
                }
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index",
                    "bulk-size": 5000
                },
                {
                    "name": "search",
                    "operation-type": "search"
                }
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "description": "Default challenge",
                    "index-settings": {},
                    "schedule": [
                        {
                            "clients": 8,
                            "operation": "index-append",
                            "warmup-time-period": 100,
                            "time-period": 240,
                        },
                        {
                            "parallel": {
                                "tasks": [
                                    {
                                        "name": "search #1",
                                        "clients": 4,
                                        "operation": "search",
                                        "warmup-iterations": 1000,
                                        "iterations": 2000
                                    },
                                    {
                                        "name": "search #2",
                                        "clients": 1,
                                        "operation": "search",
                                        "warmup-iterations": 1000,
                                        "iterations": 2000
                                    },
                                    {
                                        "name": "search #3",
                                        "clients": 1,
                                        "operation": "search",
                                        "iterations": 1
                                    }
                                ]
                            }
                        }
                    ]
                }
            ]
        }

        expected_post_processed = {
            "short-description": "short description for unit test",
            "description": "longer description of this track for unit test",
            "indices": [
                {
                    "name": "test-index",
                    "types": [
                        {
                            "name": "test-type",
                            "documents": "documents.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                            "mapping": "type-mappings.json"
                        }
                    ]
                }
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index",
                    "bulk-size": 5000
                },
                {
                    "name": "search",
                    "operation-type": "search"
                }
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "description": "Default challenge",
                    "index-settings": {},
                    "schedule": [
                        {
                            "clients": 8,
                            "operation": "index-append",
                            "warmup-time-period": 0,
                            "time-period": 10,
                        },
                        {
                            "parallel": {
                                "tasks": [
                                    {
                                        "name": "search #1",
                                        "clients": 4,
                                        "operation": "search",
                                        "warmup-iterations": 4,
                                        "iterations": 4
                                    },
                                    {
                                        "name": "search #2",
                                        "clients": 1,
                                        "operation": "search",
                                        "warmup-iterations": 1,
                                        "iterations": 1
                                    },
                                    {
                                        "name": "search #3",
                                        "clients": 1,
                                        "operation": "search",
                                        "iterations": 1
                                    }
                                ]
                            }
                        }
                    ]
                }
            ]
        }

        self.assertEqual(self.as_track(expected_post_processed),
                         loader.post_process_for_test_mode(self.as_track(track_specification)))

    def as_track(self, track_specification):
        reader = loader.TrackSpecificationReader(source=io.DictStringFileSourceFactory({
            "/mappings/type-mappings.json": ['{"test-type": "empty-for-test"}']
        }))
        return reader("unittest", track_specification, "/mappings")


class TrackPathTests(TestCase):
    def test_sets_absolute_path(self):
        from esrally import config
        from esrally.track import track

        cfg = config.Config()
        cfg.add(config.Scope.application, "benchmarks", "local.dataset.cache", "/data")

        default_challenge = track.Challenge("default", default=True, schedule=[
            track.Task(name="index", operation=track.Operation("index", operation_type=track.OperationType.Index), clients=4)
        ])
        another_challenge = track.Challenge("other", default=False)
        t = track.Track(name="unittest", description="unittest track", challenges=[another_challenge, default_challenge],
                        indices=[
                            track.Index(name="test",
                                        auto_managed=True,
                                        types=[track.Type("docs",
                                                          mapping={},
                                                          document_file="docs/documents.json",
                                                          document_archive="docs/documents.json.bz2")])
                        ])

        loader.set_absolute_data_path(cfg, t)

        self.assertEqual("/data/unittest/docs/documents.json", t.indices[0].types[0].document_file)
        self.assertEqual("/data/unittest/docs/documents.json.bz2", t.indices[0].types[0].document_archive)


class TrackFilterTests(TestCase):
    def test_create_filters_from_empty_included_tasks(self):
        self.assertEqual(0, len(loader.filters_from_included_tasks(None)))
        self.assertEqual(0, len(loader.filters_from_included_tasks([])))

    def test_create_filters_from_mixed_included_tasks(self):
        from esrally.track import track
        filters = loader.filters_from_included_tasks(["force-merge", "type:search"])
        self.assertListEqual([track.TaskNameFilter("force-merge"), track.TaskOpTypeFilter("search")], filters)

    def test_rejects_invalid_syntax(self):
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            loader.filters_from_included_tasks(["valid", "a:b:c"])
        self.assertEqual("Invalid format for included tasks: [a:b:c]", ctx.exception.args[0])

    def test_rejects_unknown_filter_type(self):
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            loader.filters_from_included_tasks(["valid", "op-type:index"])
        self.assertEqual("Invalid format for included tasks: [op-type:index]. Expected [type] but got [op-type].", ctx.exception.args[0])

    def test_filters_tasks(self):
        from esrally.track import track
        track_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "bulk-index",
                    "operation-type": "index"
                },
                {
                    "name": "node-stats",
                    "operation-type": "node-stats"
                },
                {
                    "name": "cluster-stats",
                    "operation-type": "custom-operation-type"
                },
                {
                    "name": "match-all",
                    "operation-type": "search",
                    "body": {
                        "query": {
                            "match_all": {}
                        }
                    }
                },
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "schedule": [
                        {
                            "parallel": {
                                "tasks": [
                                    {
                                        "name": "index-1",
                                        "operation": "bulk-index",
                                    },
                                    {
                                        "name": "index-2",
                                        "operation": "bulk-index",
                                    },
                                    {
                                        "name": "index-3",
                                        "operation": "bulk-index",
                                    },
                                    {
                                        "name": "match-all-parallel",
                                        "operation": "match-all",
                                    },
                                ]
                            }
                        },
                        {
                            "operation": "node-stats"
                        },
                        {
                            "name": "match-all-serial",
                            "operation": "match-all"
                        },
                        {
                            "operation": "cluster-stats"
                        }
                    ]
                }
            ]
        }
        reader = loader.TrackSpecificationReader()
        full_track = reader("unittest", track_specification, "/mappings")
        self.assertEqual(4, len(full_track.challenges[0].schedule))

        filtered = loader.filter_included_tasks(full_track, [track.TaskNameFilter("index-3"),
                                                             track.TaskOpTypeFilter("search"),
                                                             # Filtering should also work for non-core operation types.
                                                             track.TaskOpTypeFilter("custom-operation-type")
                                                             ])

        schedule = filtered.challenges[0].schedule
        self.assertEqual(3, len(schedule))
        self.assertEqual(["index-3", "match-all-parallel"], [t.name for t in schedule[0].tasks])
        self.assertEqual("match-all-serial", schedule[1].name)
        self.assertEqual("cluster-stats", schedule[2].name)


class TrackSpecificationReaderTests(TestCase):
    def test_description_is_optional(self):
        track_specification = {
            # no description here
            "challenges": []
        }
        reader = loader.TrackSpecificationReader()

        resulting_track = reader("unittest", track_specification, "/mappings")
        self.assertEqual("unittest", resulting_track.name)
        self.assertEqual("", resulting_track.description)

    def test_can_read_track_info(self):
        track_specification = {
            "description": "description for unit test",
            "data-url": "https://localhost/data",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [],
            "challenges": []
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings")
        self.assertEqual("unittest", resulting_track.name)
        self.assertEqual("description for unit test", resulting_track.description)
        self.assertEqual("https://localhost/data", resulting_track.source_root_url)

    def test_document_count_mandatory_if_file_present(self):
        track_specification = {
            "description": "description for unit test",
            "data-url": "https://localhost/data",
            "indices": [{"name": "test-index", "types": [{"name": "docs", "documents": "documents.json.bz2"}]}],
            "operations": [],
            "challenges": []
        }
        reader = loader.TrackSpecificationReader()
        with self.assertRaises(loader.TrackSyntaxError) as ctx:
            reader("unittest", track_specification, "/mappings")
        self.assertEqual("Track 'unittest' is invalid. Mandatory element 'document-count' is missing.", ctx.exception.args[0])

    def test_parse_with_mixed_warmup_iterations_and_measurement(self):
        track_specification = {
            "description": "description for unit test",
            "data-url": "https://localhost/data",
            "indices": [
                {
                    "name": "test-index",
                    "types": [
                        {
                            "name": "main",
                            "documents": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                            "mapping": "main-type-mappings.json"
                        }
                    ]
                }
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index",
                    "bulk-size": 5000,
                }
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "index-settings": {},
                    "schedule": [
                        {
                            "clients": 8,
                            "operation": "index-append",
                            "warmup-iterations": 3,
                            "time-period": 60
                        }
                    ]
                }

            ]
        }

        reader = loader.TrackSpecificationReader(source=io.DictStringFileSourceFactory({
            "/mappings/main-type-mappings.json": ['{"main": "empty-for-test"}'],
        }))
        with self.assertRaises(loader.TrackSyntaxError) as ctx:
            reader("unittest", track_specification, "/mappings")
        self.assertEqual("Track 'unittest' is invalid. Operation 'index-append' in challenge 'default-challenge' defines '3' warmup "
                         "iterations and a time period of '60' seconds. Please do not mix time periods and iterations.",
                         ctx.exception.args[0])

    def test_parse_missing_challenge_or_challenges(self):
        track_specification = {
            "description": "description for unit test",
            "data-url": "https://localhost/data",
            "indices": [
                {
                    "name": "test-index",
                    "types": [
                        {
                            "name": "main",
                            "documents": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                            "mapping": "main-type-mappings.json"
                        }
                    ]
                }
            ]
            # no challenge or challenges element
        }
        reader = loader.TrackSpecificationReader(source=io.DictStringFileSourceFactory({
            "/mappings/main-type-mappings.json": ['{"main": "empty-for-test"}'],
        }))
        with self.assertRaises(loader.TrackSyntaxError) as ctx:
            reader("unittest", track_specification, "/mappings")
        self.assertEqual("Track 'unittest' is invalid. You must define either 'challenge' or 'challenges' but none is specified.",
                         ctx.exception.args[0])

    def test_parse_challenge_and_challenges_are_defined(self):
        track_specification = {
            "description": "description for unit test",
            "data-url": "https://localhost/data",
            "indices": [
                {
                    "name": "test-index",
                    "types": [
                        {
                            "name": "main",
                            "documents": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                            "mapping": "main-type-mappings.json"
                        }
                    ]
                }
            ],
            # We define both. Note that challenges without any properties would not pass JSON schema validation but we don't test this here.
            "challenge": {},
            "challenges": []
        }
        reader = loader.TrackSpecificationReader(source=io.DictStringFileSourceFactory({
            "/mappings/main-type-mappings.json": ['{"main": "empty-for-test"}'],
        }))
        with self.assertRaises(loader.TrackSyntaxError) as ctx:
            reader("unittest", track_specification, "/mappings")
        self.assertEqual("Track 'unittest' is invalid. 'challenge' and 'challenges' are defined but only one of them is allowed.",
                         ctx.exception.args[0])

    def test_parse_with_mixed_warmup_time_period_and_iterations(self):
        track_specification = {
            "description": "description for unit test",
            "data-url": "https://localhost/data",
            "indices": [
                {
                    "name": "test-index",
                    "types": [
                        {
                            "name": "main",
                            "documents": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                            "mapping": "main-type-mappings.json"
                        }
                    ]
                }
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index",
                    "bulk-size": 5000,
                }
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "index-settings": {},
                    "schedule": [
                        {
                            "clients": 8,
                            "operation": "index-append",
                            "warmup-time-period": 20,
                            "iterations": 1000
                        }
                    ]
                }

            ]
        }

        reader = loader.TrackSpecificationReader(source=io.DictStringFileSourceFactory({
            "/mappings/main-type-mappings.json": ['{"main": "empty-for-test"}'],
        }))
        with self.assertRaises(loader.TrackSyntaxError) as ctx:
            reader("unittest", track_specification, "/mappings")
        self.assertEqual("Track 'unittest' is invalid. Operation 'index-append' in challenge 'default-challenge' defines a warmup time "
                         "period of '20' seconds and '1000' iterations. Please do not mix time periods and iterations.",
                         ctx.exception.args[0])

    def test_parse_duplicate_implicit_task_names(self):
        track_specification = {
            "description": "description for unit test",
            "operations": [
                {
                    "name": "search",
                    "operation-type": "search",
                    "index": "_all"
                }
            ],
            "challenge": {
                "name": "default-challenge",
                "schedule": [
                    {
                        "operation": "search",
                        "clients": 1
                    },
                    {
                        "operation": "search",
                        "clients": 2
                    }
                ]
            }
        }
        reader = loader.TrackSpecificationReader()
        with self.assertRaises(loader.TrackSyntaxError) as ctx:
            reader("unittest", track_specification, "/mappings")
        self.assertEqual("Track 'unittest' is invalid. Challenge 'default-challenge' contains multiple tasks with the name 'search'. Please"
                         " use the task's name property to assign a unique name for each task.",
                         ctx.exception.args[0])

    def test_parse_duplicate_explicit_task_names(self):
        track_specification = {
            "description": "description for unit test",
            "operations": [
                {
                    "name": "search",
                    "operation-type": "search",
                    "index": "_all"
                }
            ],
            "challenge": {
                "name": "default-challenge",
                "schedule": [
                    {
                        "name": "duplicate-task-name",
                        "operation": "search",
                        "clients": 1
                    },
                    {
                        "name": "duplicate-task-name",
                        "operation": "search",
                        "clients": 2
                    }
                ]
            }
        }
        reader = loader.TrackSpecificationReader()
        with self.assertRaises(loader.TrackSyntaxError) as ctx:
            reader("unittest", track_specification, "/mappings")
        self.assertEqual("Track 'unittest' is invalid. Challenge 'default-challenge' contains multiple tasks with the name "
                         "'duplicate-task-name'. Please use the task's name property to assign a unique name for each task.",
                         ctx.exception.args[0])

    def test_parse_unique_task_names(self):
        track_specification = {
            "description": "description for unit test",
            "operations": [
                {
                    "name": "search",
                    "operation-type": "search",
                    "index": "_all"
                }
            ],
            "challenge": {
                "name": "default-challenge",
                "schedule": [
                    {
                        "name": "search-one-client",
                        "operation": "search",
                        "clients": 1
                    },
                    {
                        "name": "search-two-clients",
                        "operation": "search",
                        "clients": 2
                    }
                ]
            }
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings")
        self.assertEqual("unittest", resulting_track.name)
        schedule = resulting_track.challenges[0].schedule
        self.assertEqual(2, len(schedule))
        self.assertEqual("search-one-client", schedule[0].name)
        self.assertEqual("search", schedule[0].operation.name)
        self.assertEqual("search-two-clients", schedule[1].name)
        self.assertEqual("search", schedule[1].operation.name)

    def test_parse_valid_track_specification(self):
        track_specification = {
            "description": "description for unit test",
            "data-url": "https://localhost/data",
            "indices": [
                {
                    "name": "index-historical",
                    "types": [
                        {
                            "name": "main",
                            "documents": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                            "mapping": "main-type-mappings.json"
                        },
                        {
                            "name": "secondary",
                            "documents": "documents-secondary.json.bz2",
                            "includes-action-and-meta-data": True,
                            "document-count": 20,
                            "compressed-bytes": 200,
                            "uncompressed-bytes": 20000,
                            "mapping": "secondary-type-mappings.json"
                        }

                    ]
                }
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index",
                    "bulk-size": 5000,
                    "meta": {
                        "append": True
                    }
                },
                {
                    "name": "search",
                    "operation-type": "search",
                    "index": "index-historical"
                }
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "description": "Default challenge",
                    "meta": {
                        "mixed": True,
                        "max-clients": 8
                    },
                    "index-settings": {
                        "index.number_of_replicas": 2
                    },
                    "schedule": [
                        {
                            "clients": 8,
                            "operation": "index-append",
                            "meta": {
                                "operation-index": 0
                            }
                        },
                        {
                            "clients": 1,
                            "operation": "search"
                        }
                    ]
                }
            ]
        }
        reader = loader.TrackSpecificationReader(source=io.DictStringFileSourceFactory({
            "/mappings/main-type-mappings.json": ['{"main": "empty-for-test"}'],
            "/mappings/secondary-type-mappings.json": ['{"secondary": "empty-for-test"}'],
        }))
        resulting_track = reader("unittest", track_specification, "/mappings")
        self.assertEqual("unittest", resulting_track.name)
        self.assertEqual("description for unit test", resulting_track.description)
        self.assertEqual(1, len(resulting_track.indices))
        self.assertEqual("index-historical", resulting_track.indices[0].name)
        self.assertEqual(2, len(resulting_track.indices[0].types))
        self.assertEqual("main", resulting_track.indices[0].types[0].name)
        self.assertFalse(resulting_track.indices[0].types[0].includes_action_and_meta_data)
        self.assertEqual("documents-main.json.bz2", resulting_track.indices[0].types[0].document_archive)
        self.assertEqual("documents-main.json", resulting_track.indices[0].types[0].document_file)
        self.assertDictEqual({"main": "empty-for-test"}, resulting_track.indices[0].types[0].mapping)
        self.assertEqual("secondary", resulting_track.indices[0].types[1].name)
        self.assertDictEqual({"secondary": "empty-for-test"}, resulting_track.indices[0].types[1].mapping)
        self.assertTrue(resulting_track.indices[0].types[1].includes_action_and_meta_data)
        self.assertEqual(1, len(resulting_track.challenges))
        self.assertEqual("default-challenge", resulting_track.challenges[0].name)
        self.assertEqual("Default challenge", resulting_track.challenges[0].description)
        self.assertEqual(1, len(resulting_track.challenges[0].index_settings))
        self.assertEqual(2, resulting_track.challenges[0].index_settings["index.number_of_replicas"])
        self.assertEqual({"mixed": True, "max-clients": 8}, resulting_track.challenges[0].meta_data)
        self.assertEqual({"append": True}, resulting_track.challenges[0].schedule[0].operation.meta_data)
        self.assertEqual({"operation-index": 0}, resulting_track.challenges[0].schedule[0].meta_data)

    def test_parse_valid_track_specification_with_index_template(self):
        track_specification = {
            "description": "description for unit test",
            "templates": [
                {
                    "name": "my-index-template",
                    "index-pattern": "*",
                    "template": "default-template.json"
                }
            ],
            "operations": [],
            "challenges": []
        }
        reader = loader.TrackSpecificationReader(source=io.DictStringFileSourceFactory({
            "/mappings/default-template.json": ['{"some-index-template": "empty-for-test"}'],
        }))
        resulting_track = reader("unittest", track_specification, "/mappings")
        self.assertEqual("unittest", resulting_track.name)
        self.assertEqual("description for unit test", resulting_track.description)
        self.assertEqual(0, len(resulting_track.indices))
        self.assertEqual(1, len(resulting_track.templates))
        self.assertEqual("my-index-template", resulting_track.templates[0].name)
        self.assertEqual("*", resulting_track.templates[0].pattern)
        self.assertEqual({"some-index-template": "empty-for-test"}, resulting_track.templates[0].content)
        self.assertEqual(0, len(resulting_track.challenges))

    def test_types_are_optional_for_user_managed_indices(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [],
            "challenges": []
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings")
        self.assertEqual("unittest", resulting_track.name)
        self.assertEqual("description for unit test", resulting_track.description)
        self.assertEqual(1, len(resulting_track.indices))
        self.assertEqual(0, len(resulting_track.templates))
        self.assertEqual("test-index", resulting_track.indices[0].name)
        self.assertEqual(0, len(resulting_track.indices[0].types))

    def test_unique_challenge_names(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index"
                }
            ],
            "challenges": [
                {
                    "name": "test-challenge",
                    "description": "Some challenge",
                    "default": True,
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                },
                {
                    "name": "test-challenge",
                    "description": "Another challenge with the same name",
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                }

            ]
        }
        reader = loader.TrackSpecificationReader()
        with self.assertRaises(loader.TrackSyntaxError) as ctx:
            reader("unittest", track_specification, "/mappings")
        self.assertEqual("Track 'unittest' is invalid. Duplicate challenge with name 'test-challenge'.", ctx.exception.args[0])

    def test_not_more_than_one_default_challenge_possible(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index"
                }
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "description": "Default challenge",
                    "default": True,
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                },
                {
                    "name": "another-challenge",
                    "description": "See if we can sneek it in as another default",
                    "default": True,
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                }

            ]
        }
        reader = loader.TrackSpecificationReader()
        with self.assertRaises(loader.TrackSyntaxError) as ctx:
            reader("unittest", track_specification, "/mappings")
        self.assertEqual("Track 'unittest' is invalid. Both 'default-challenge' and 'another-challenge' are defined as default challenges. "
                         "Please define only one of them as default.", ctx.exception.args[0])

    def test_at_least_one_default_challenge(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index"
                }
            ],
            "challenges": [
                {
                    "name": "challenge",
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                },
                {
                    "name": "another-challenge",
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                }

            ]
        }
        reader = loader.TrackSpecificationReader()
        with self.assertRaises(loader.TrackSyntaxError) as ctx:
            reader("unittest", track_specification, "/mappings")
        self.assertEqual("Track 'unittest' is invalid. No default challenge specified. Please edit the track and add \"default\": true "
                         "to one of the challenges challenge, another-challenge.", ctx.exception.args[0])

    def test_exactly_one_default_challenge(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index"
                }
            ],
            "challenges": [
                {
                    "name": "challenge",
                    "default": True,
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                },
                {
                    "name": "another-challenge",
                    "schedule": [
                        {
                            "operation": "index-append"
                        }
                    ]
                }

            ]
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings")
        self.assertEqual(2, len(resulting_track.challenges))
        self.assertEqual("challenge", resulting_track.challenges[0].name)
        self.assertTrue(resulting_track.challenges[0].default)
        self.assertFalse(resulting_track.challenges[1].default)

    def test_selects_sole_challenge_implicitly_as_default(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index"
                }
            ],
            "challenge": {
                "name": "challenge",
                "schedule": [
                    {
                        "operation": "index-append"
                    }
                ]
            }
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings")
        self.assertEqual(1, len(resulting_track.challenges))
        self.assertEqual("challenge", resulting_track.challenges[0].name)
        self.assertTrue(resulting_track.challenges[0].default)

    def test_inline_operations(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "challenge": {
                "name": "challenge",
                "schedule": [
                    # an operation with parameters still needs to define a type
                    {
                        "operation": {
                            "operation-type": "index",
                            "bulk-size": 5000
                        }
                    },
                    # a parameterless operation can just use the operation type as implicit reference to the operation
                    {
                        "operation": "force-merge"
                    }
                ]
            }
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings")

        self.assertEqual(2, len(resulting_track.challenges[0].schedule))
        self.assertEqual(track.OperationType.Index.name, resulting_track.challenges[0].schedule[0].operation.type)
        self.assertEqual(track.OperationType.ForceMerge.name, resulting_track.challenges[0].schedule[1].operation.type)

    def test_supports_target_throughput(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index"
                }
            ],
            "challenge": {
                "name": "default-challenge",
                "schedule": [
                    {
                        "operation": "index-append",
                        "target-throughput": 10,
                    }
                ]
            }
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings")
        self.assertEqual(10, resulting_track.challenges[0].schedule[0].params["target-throughput"])

    def test_supports_target_interval(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index"
                }
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "schedule": [
                        {
                            "operation": "index-append",
                            "target-interval": 5,
                        }
                    ]
                }
            ]
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings")
        self.assertEqual(5, resulting_track.challenges[0].schedule[0].params["target-interval"])

    def test_parallel_tasks_with_default_values(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "index-1",
                    "operation-type": "index"
                },
                {
                    "name": "index-2",
                    "operation-type": "index"
                },
                {
                    "name": "index-3",
                    "operation-type": "index"
                },
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "schedule": [
                        {
                            "parallel": {
                                "warmup-time-period": 2400,
                                "time-period": 36000,
                                "tasks": [
                                    {
                                        "operation": "index-1",
                                        "warmup-time-period": 300,
                                        "clients": 2
                                    },
                                    {
                                        "operation": "index-2",
                                        "time-period": 3600,
                                        "clients": 4
                                    },
                                    {
                                        "operation": "index-3",
                                        "target-throughput": 10,
                                        "clients": 16
                                    },
                                ]
                            }
                        }
                    ]
                }
            ]
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings")
        parallel_element = resulting_track.challenges[0].schedule[0]
        parallel_tasks = parallel_element.tasks

        self.assertEqual(22, parallel_element.clients)
        self.assertEqual(3, len(parallel_tasks))

        self.assertEqual("index-1", parallel_tasks[0].operation.name)
        self.assertEqual(300, parallel_tasks[0].warmup_time_period)
        self.assertEqual(36000, parallel_tasks[0].time_period)
        self.assertEqual(2, parallel_tasks[0].clients)
        self.assertFalse("target-throughput" in parallel_tasks[0].params)

        self.assertEqual("index-2", parallel_tasks[1].operation.name)
        self.assertEqual(2400, parallel_tasks[1].warmup_time_period)
        self.assertEqual(3600, parallel_tasks[1].time_period)
        self.assertEqual(4, parallel_tasks[1].clients)
        self.assertFalse("target-throughput" in parallel_tasks[1].params)

        self.assertEqual("index-3", parallel_tasks[2].operation.name)
        self.assertEqual(2400, parallel_tasks[2].warmup_time_period)
        self.assertEqual(36000, parallel_tasks[2].time_period)
        self.assertEqual(16, parallel_tasks[2].clients)
        self.assertEqual(10, parallel_tasks[2].params["target-throughput"])

    def test_parallel_tasks_with_default_clients_does_not_propagate(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "index-1",
                    "operation-type": "index"
                }
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "schedule": [
                        {
                            "parallel": {
                                "warmup-time-period": 2400,
                                "time-period": 36000,
                                "clients": 2,
                                "tasks": [
                                    {
                                        "name": "index-1-1",
                                        "operation": "index-1"
                                    },
                                    {
                                        "name": "index-1-2",
                                        "operation": "index-1"
                                    },
                                    {
                                        "name": "index-1-3",
                                        "operation": "index-1"
                                    },
                                    {
                                        "name": "index-1-4",
                                        "operation": "index-1"
                                    }
                                ]
                            }
                        }
                    ]
                }
            ]
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings")
        parallel_element = resulting_track.challenges[0].schedule[0]
        parallel_tasks = parallel_element.tasks

        # we will only have two clients *in total*
        self.assertEqual(2, parallel_element.clients)
        self.assertEqual(4, len(parallel_tasks))
        for task in parallel_tasks:
            self.assertEqual(1, task.clients)

    def test_parallel_tasks_with_completed_by_set(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "index-1",
                    "operation-type": "index"
                },
                {
                    "name": "index-2",
                    "operation-type": "index"
                }
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "schedule": [
                        {
                            "parallel": {
                                "warmup-time-period": 2400,
                                "time-period": 36000,
                                "completed-by": "index-2",
                                "tasks": [
                                    {
                                        "operation": "index-1"
                                    },
                                    {
                                        "operation": "index-2"
                                    }
                                ]
                            }
                        }
                    ]
                }
            ]
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings")
        parallel_element = resulting_track.challenges[0].schedule[0]
        parallel_tasks = parallel_element.tasks

        # we will only have two clients *in total*
        self.assertEqual(2, parallel_element.clients)
        self.assertEqual(2, len(parallel_tasks))

        self.assertEqual("index-1", parallel_tasks[0].operation.name)
        self.assertFalse(parallel_tasks[0].completes_parent)

        self.assertEqual("index-2", parallel_tasks[1].operation.name)
        self.assertTrue(parallel_tasks[1].completes_parent)

    def test_parallel_tasks_with_completed_by_set_no_task_matches(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "index-1",
                    "operation-type": "index"
                },
                {
                    "name": "index-2",
                    "operation-type": "index"
                }
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "schedule": [
                        {
                            "parallel": {
                                "completed-by": "non-existing-task",
                                "tasks": [
                                    {
                                        "operation": "index-1"
                                    },
                                    {
                                        "operation": "index-2"
                                    }
                                ]
                            }
                        }
                    ]
                }
            ]
        }
        reader = loader.TrackSpecificationReader()

        with self.assertRaises(loader.TrackSyntaxError) as ctx:
            reader("unittest", track_specification, "/mappings")
        self.assertEqual("Track 'unittest' is invalid. 'parallel' element for challenge 'default-challenge' is marked with 'completed-by' "
                         "with task name 'non-existing-task' but no task with this name exists.", ctx.exception.args[0])

    def test_parallel_tasks_with_completed_by_set_multiple_tasks_match(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "index-1",
                    "operation-type": "index"
                }
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "schedule": [
                        {
                            "parallel": {
                                "completed-by": "index-1",
                                "tasks": [
                                    {
                                        "operation": "index-1"
                                    },
                                    {
                                        "operation": "index-1"
                                    }
                                ]
                            }
                        }
                    ]
                }
            ]
        }
        reader = loader.TrackSpecificationReader()

        with self.assertRaises(loader.TrackSyntaxError) as ctx:
            reader("unittest", track_specification, "/mappings")
        self.assertEqual("Track 'unittest' is invalid. 'parallel' element for challenge 'default-challenge' contains multiple tasks with "
                         "the name 'index-1' which are marked with 'completed-by' but only task is allowed to match.", ctx.exception.args[0])
