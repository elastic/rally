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

import copy
import os
import random
import re
import textwrap
import urllib.error
from unittest import mock

import pytest

from esrally import config, exceptions
from esrally.track import loader, track
from esrally.utils import io


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


class TestSimpleTrackRepository:
    @mock.patch("os.path.exists")
    @mock.patch("os.path.isdir")
    def test_track_from_directory(self, is_dir, path_exists):
        is_dir.return_value = True
        path_exists.return_value = True

        repo = loader.SimpleTrackRepository("/path/to/track/unit-test")
        assert repo.track_name == "unit-test"
        assert repo.track_names == ["unit-test"]
        assert repo.track_dir("unit-test") == "/path/to/track/unit-test"
        assert repo.track_file("unit-test") == "/path/to/track/unit-test/track.json"

    @mock.patch("os.path.exists")
    @mock.patch("os.path.isdir")
    @mock.patch("os.path.isfile")
    def test_track_from_file(self, is_file, is_dir, path_exists):
        is_file.return_value = True
        is_dir.return_value = False
        path_exists.return_value = True

        repo = loader.SimpleTrackRepository("/path/to/track/unit-test/my-track.json")
        assert repo.track_name == "my-track"
        assert repo.track_names == ["my-track"]
        assert repo.track_dir("my-track") == "/path/to/track/unit-test"
        assert repo.track_file("my-track") == "/path/to/track/unit-test/my-track.json"

    @mock.patch("os.path.exists")
    @mock.patch("os.path.isdir")
    @mock.patch("os.path.isfile")
    def test_track_from_named_pipe(self, is_file, is_dir, path_exists):
        is_file.return_value = False
        is_dir.return_value = False
        path_exists.return_value = True

        with pytest.raises(exceptions.SystemSetupError) as exc:
            loader.SimpleTrackRepository("a named pipe cannot point to a track")
        assert exc.value.args[0] == "a named pipe cannot point to a track is neither a file nor a directory"

    @mock.patch("os.path.exists")
    def test_track_from_non_existing_path(self, path_exists):
        path_exists.return_value = False
        with pytest.raises(exceptions.SystemSetupError) as exc:
            loader.SimpleTrackRepository("/path/does/not/exist")
        assert exc.value.args[0] == "Track path /path/does/not/exist does not exist"

    @mock.patch("os.path.isdir")
    @mock.patch("os.path.exists")
    def test_track_from_directory_without_track(self, path_exists, is_dir):
        # directory exists, but not the file
        path_exists.side_effect = [True, False]
        is_dir.return_value = True
        with pytest.raises(exceptions.SystemSetupError) as exc:
            loader.SimpleTrackRepository("/path/to/not/a/track")
        assert exc.value.args[0] == "Could not find track.json in /path/to/not/a/track"

    @mock.patch("os.path.exists")
    @mock.patch("os.path.isdir")
    @mock.patch("os.path.isfile")
    def test_track_from_file_but_not_json(self, is_file, is_dir, path_exists):
        is_file.return_value = True
        is_dir.return_value = False
        path_exists.return_value = True

        with pytest.raises(exceptions.SystemSetupError) as exc:
            loader.SimpleTrackRepository("/path/to/track/unit-test/my-track.xml")
        assert exc.value.args[0] == "/path/to/track/unit-test/my-track.xml has to be a JSON file"


class TestGitRepository:
    class MockGitRepo:
        def __init__(self, remote_url, root_dir, repo_name, resource_name, offline, fetch=True):
            self.repo_dir = f"{root_dir}/{repo_name}"

    @mock.patch("os.path.exists")
    @mock.patch("os.walk")
    def test_track_from_existing_repo(self, walk, exists):
        """
        simulates scanning track repositories with the following structure:
        <root>/
            unittest/
                track.json
            unittest2/
                track.json
            unittest3/
                nested/
                    track.json
                track.json
            src/
                utils.py
        """
        walk.return_value = iter(
            [
                ("/tmp/tracks/default", ["unittest", "unittest2", "unittest3", "src"], []),
                ("/tmp/tracks/default/unittest", [], ["track.json"]),
                ("/tmp/tracks/default/unittest2", [], ["track.json"]),
                ("/tmp/tracks/default/unittest3", ["nested"], ["track.json"]),
                ("/tmp/tracks/default/unittest3/nested", [], ["track.json"]),
                ("/tmp/tracks/src", [], ["utils.py"]),
            ]
        )
        exists.return_value = True
        cfg = config.Config()
        cfg.add(config.Scope.application, "track", "track.name", "unittest")
        cfg.add(config.Scope.application, "track", "repository.name", "default")
        cfg.add(config.Scope.application, "system", "offline.mode", False)
        cfg.add(config.Scope.application, "node", "root.dir", "/tmp")
        cfg.add(config.Scope.application, "benchmarks", "track.repository.dir", "tracks")

        repo = loader.GitTrackRepository(cfg, fetch=False, update=False, repo_class=self.MockGitRepo)

        assert repo.track_name == "unittest"
        assert repo.track_names == ["unittest", "unittest2", "unittest3", "unittest3/nested"]
        assert repo.track_dir("unittest") == "/tmp/tracks/default/unittest"
        assert repo.track_file("unittest") == "/tmp/tracks/default/unittest/track.json"


class TestTrackPreparation:
    @mock.patch("esrally.utils.io.prepare_file_offset_table")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_does_nothing_if_document_file_available(self, is_file, get_size, prepare_file_offset_table):
        is_file.return_value = True
        get_size.return_value = 2000
        prepare_file_offset_table.return_value = 5

        p = loader.DocumentSetPreparator(
            track_name="unit-test", downloader=loader.Downloader(offline=False, test_mode=False), decompressor=loader.Decompressor()
        )

        p.prepare_document_set(
            document_set=track.Documents(
                source_format=track.Documents.SOURCE_FORMAT_BULK,
                document_file="docs.json",
                document_archive="docs.json.bz2",
                number_of_documents=5,
                compressed_size_in_bytes=200,
                uncompressed_size_in_bytes=2000,
            ),
            data_root="/tmp",
        )

        prepare_file_offset_table.assert_called_with("/tmp/docs.json")

    @mock.patch("esrally.utils.io.prepare_file_offset_table")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_decompresses_if_archive_available(self, is_file, get_size, prepare_file_offset_table):
        is_file.return_value = True
        get_size.return_value = 2000
        prepare_file_offset_table.return_value = 5

        p = loader.DocumentSetPreparator(
            track_name="unit-test", downloader=loader.Downloader(offline=False, test_mode=False), decompressor=loader.Decompressor()
        )

        p.prepare_document_set(
            document_set=track.Documents(
                source_format=track.Documents.SOURCE_FORMAT_BULK,
                document_file="docs.json",
                document_archive="docs.json.bz2",
                number_of_documents=5,
                compressed_size_in_bytes=200,
                uncompressed_size_in_bytes=2000,
            ),
            data_root="/tmp",
        )

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

        p = loader.DocumentSetPreparator(
            track_name="unit-test", downloader=loader.Downloader(offline=False, test_mode=False), decompressor=loader.Decompressor()
        )

        with pytest.raises(exceptions.DataError) as exc:
            p.prepare_document_set(
                document_set=track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    document_file="docs.json",
                    document_archive="docs.json.bz2",
                    number_of_documents=5,
                    compressed_size_in_bytes=200,
                    uncompressed_size_in_bytes=2000,
                ),
                data_root="/tmp",
            )
        assert exc.value.args[0] == "[/tmp/docs.json] is corrupt. Extracted [1] bytes but [2000] bytes are expected."

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

        p = loader.DocumentSetPreparator(
            track_name="unit-test", downloader=loader.Downloader(offline=False, test_mode=False), decompressor=loader.Decompressor()
        )

        with pytest.raises(exceptions.DataError) as exc:
            p.prepare_document_set(
                document_set=track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    base_url="http://benchmarks.elasticsearch.org/corpora/unit-test",
                    document_file="docs.json",
                    document_archive="docs.json.bz2",
                    number_of_documents=5,
                    compressed_size_in_bytes=200,
                    uncompressed_size_in_bytes=2000,
                ),
                data_root="/tmp",
            )
        assert exc.value.args[0] == (
            "Decompressing [/tmp/docs.json.bz2] did not create [/tmp/docs.json]. Please check with the track author if the "
            "compressed archive has been created correctly."
        )

        decompress.assert_called_with("/tmp/docs.json.bz2", "/tmp")

    @mock.patch("esrally.utils.io.prepare_file_offset_table")
    @mock.patch("esrally.utils.io.decompress")
    @mock.patch("esrally.utils.net.download")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_download_document_archive_if_no_file_available(
        self, is_file, get_size, ensure_dir, download, decompress, prepare_file_offset_table
    ):
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

        p = loader.DocumentSetPreparator(
            track_name="unit-test", downloader=loader.Downloader(offline=False, test_mode=False), decompressor=loader.Decompressor()
        )

        p.prepare_document_set(
            document_set=track.Documents(
                source_format=track.Documents.SOURCE_FORMAT_BULK,
                base_url="http://benchmarks.elasticsearch.org/corpora/unit-test",
                document_file="docs.json",
                document_archive="docs.json.bz2",
                number_of_documents=5,
                compressed_size_in_bytes=200,
                uncompressed_size_in_bytes=2000,
            ),
            data_root="/tmp",
        )

        ensure_dir.assert_called_with("/tmp")
        decompress.assert_called_with("/tmp/docs.json.bz2", "/tmp")
        download.assert_called_with(
            "http://benchmarks.elasticsearch.org/corpora/unit-test/docs.json.bz2", "/tmp/docs.json.bz2", 200, progress_indicator=mock.ANY
        )
        prepare_file_offset_table.assert_called_with("/tmp/docs.json")

    @mock.patch("esrally.utils.io.prepare_file_offset_table")
    @mock.patch("esrally.utils.io.decompress")
    @mock.patch("esrally.utils.net.download")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_download_document_with_trailing_baseurl_slash(
        self, is_file, get_size, ensure_dir, download, decompress, prepare_file_offset_table
    ):
        # uncompressed file does not exist
        # after download uncompressed file exists
        # after download uncompressed file exists (main loop)
        is_file.side_effect = [False, True, True]
        # uncompressed file size is 2000
        get_size.return_value = 2000
        scheme = random.choice(["http", "https", "s3", "gs"])

        prepare_file_offset_table.return_value = 5

        p = loader.DocumentSetPreparator(
            track_name="unit-test", downloader=loader.Downloader(offline=False, test_mode=False), decompressor=loader.Decompressor()
        )

        p.prepare_document_set(
            document_set=track.Documents(
                source_format=track.Documents.SOURCE_FORMAT_BULK,
                base_url=f"{scheme}://benchmarks.elasticsearch.org/corpora/unit-test/",
                document_file="docs.json",
                # --> We don't provide a document archive here <--
                document_archive=None,
                number_of_documents=5,
                compressed_size_in_bytes=200,
                uncompressed_size_in_bytes=2000,
            ),
            data_root="/tmp",
        )

        ensure_dir.assert_called_with("/tmp")
        download.assert_called_with(
            f"{scheme}://benchmarks.elasticsearch.org/corpora/unit-test/docs.json", "/tmp/docs.json", 2000, progress_indicator=mock.ANY
        )
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

        p = loader.DocumentSetPreparator(
            track_name="unit-test", downloader=loader.Downloader(offline=False, test_mode=False), decompressor=loader.Decompressor()
        )

        p.prepare_document_set(
            document_set=track.Documents(
                source_format=track.Documents.SOURCE_FORMAT_BULK,
                base_url="http://benchmarks.elasticsearch.org/corpora/unit-test",
                document_file="docs.json",
                # --> We don't provide a document archive here <--
                document_archive=None,
                number_of_documents=5,
                compressed_size_in_bytes=200,
                uncompressed_size_in_bytes=2000,
            ),
            data_root="/tmp",
        )

        ensure_dir.assert_called_with("/tmp")
        download.assert_called_with(
            "http://benchmarks.elasticsearch.org/corpora/unit-test/docs.json", "/tmp/docs.json", 2000, progress_indicator=mock.ANY
        )
        prepare_file_offset_table.assert_called_with("/tmp/docs.json")

    @mock.patch("esrally.utils.net.download")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("os.path.isfile")
    def test_raise_download_error_if_offline(self, is_file, ensure_dir, download):
        # uncompressed file does not exist
        is_file.return_value = False

        p = loader.DocumentSetPreparator(
            track_name="unit-test", downloader=loader.Downloader(offline=True, test_mode=False), decompressor=loader.Decompressor()
        )

        with pytest.raises(exceptions.SystemSetupError) as exc:
            p.prepare_document_set(
                document_set=track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    base_url="http://benchmarks.elasticsearch.org/corpora/unit-test",
                    document_file="docs.json",
                    number_of_documents=5,
                    uncompressed_size_in_bytes=2000,
                ),
                data_root="/tmp",
            )

        assert exc.value.args[0] == "Cannot find [/tmp/docs.json]. Please disable offline mode and retry."

        assert ensure_dir.call_count == 0
        assert download.call_count == 0

    @mock.patch("esrally.utils.net.download")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("os.path.isfile")
    def test_raise_download_error_if_no_url_provided_and_file_missing(self, is_file, ensure_dir, download):
        # uncompressed file does not exist
        is_file.return_value = False

        p = loader.DocumentSetPreparator(
            track_name="unit-test", downloader=loader.Downloader(offline=False, test_mode=False), decompressor=loader.Decompressor()
        )

        with pytest.raises(exceptions.DataError) as exc:
            p.prepare_document_set(
                document_set=track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    base_url=None,
                    document_file="docs.json",
                    document_archive=None,
                    number_of_documents=5,
                    uncompressed_size_in_bytes=2000,
                ),
                data_root="/tmp",
            )

        assert exc.value.args[0] == "Cannot download data because no base URL is provided."

        assert ensure_dir.call_count == 0
        assert download.call_count == 0

    @mock.patch("esrally.utils.net.download")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_raise_download_error_if_no_url_provided_and_wrong_file_size(self, is_file, get_size, ensure_dir, download):
        # uncompressed file exists...
        is_file.return_value = True
        # but it's size is wrong
        get_size.return_value = 100

        p = loader.DocumentSetPreparator(
            track_name="unit-test", downloader=loader.Downloader(offline=False, test_mode=False), decompressor=loader.Decompressor()
        )

        with pytest.raises(exceptions.DataError) as exc:
            p.prepare_document_set(
                document_set=track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    document_file="docs.json",
                    number_of_documents=5,
                    uncompressed_size_in_bytes=2000,
                ),
                data_root="/tmp",
            )

        assert exc.value.args[0] == (
            "[/tmp/docs.json] is present but does not have the expected size of [2000] bytes and it "
            "cannot be downloaded because no base URL is provided."
        )

        assert ensure_dir.call_count == 0
        assert download.call_count == 0

    @mock.patch("esrally.utils.net.download")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("os.path.isfile")
    def test_raise_download_error_no_test_mode_file(self, is_file, ensure_dir, download):
        # uncompressed file does not exist
        is_file.return_value = False

        download.side_effect = urllib.error.HTTPError(
            "http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/unit-test/docs-1k.json", 404, "", None, None
        )

        p = loader.DocumentSetPreparator(
            track_name="unit-test", downloader=loader.Downloader(offline=False, test_mode=True), decompressor=loader.Decompressor()
        )

        with pytest.raises(exceptions.DataError) as exc:
            p.prepare_document_set(
                document_set=track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    base_url="http://benchmarks.elasticsearch.org/corpora/unit-test",
                    document_file="docs-1k.json",
                    number_of_documents=5,
                    uncompressed_size_in_bytes=None,
                ),
                data_root="/tmp",
            )

        assert exc.value.args[0] == "This track does not support test mode. Ask the track author to add it or disable test mode and retry."

        ensure_dir.assert_called_with("/tmp")
        download.assert_called_with(
            "http://benchmarks.elasticsearch.org/corpora/unit-test/docs-1k.json", "/tmp/docs-1k.json", None, progress_indicator=mock.ANY
        )

    @mock.patch("esrally.utils.net.download")
    @mock.patch("esrally.utils.io.ensure_dir")
    @mock.patch("os.path.isfile")
    def test_raise_download_error_on_connection_problems(self, is_file, ensure_dir, download):
        # uncompressed file does not exist
        is_file.return_value = False

        download.side_effect = urllib.error.HTTPError(
            "http://benchmarks.elasticsearch.org/corpora/unit-test/docs.json", 500, "Internal Server Error", None, None
        )

        p = loader.DocumentSetPreparator(
            track_name="unit-test", downloader=loader.Downloader(offline=False, test_mode=False), decompressor=loader.Decompressor()
        )

        with pytest.raises(exceptions.DataError) as exc:
            p.prepare_document_set(
                document_set=track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    base_url="http://benchmarks.elasticsearch.org/corpora/unit-test",
                    document_file="docs.json",
                    number_of_documents=5,
                    uncompressed_size_in_bytes=2000,
                ),
                data_root="/tmp",
            )

        assert exc.value.args[0] == (
            "Could not download [http://benchmarks.elasticsearch.org/corpora/unit-test/docs.json] "
            "to [/tmp/docs.json] (HTTP status: 500, reason: Internal Server Error)"
        )

        ensure_dir.assert_called_with("/tmp")
        download.assert_called_with(
            "http://benchmarks.elasticsearch.org/corpora/unit-test/docs.json", "/tmp/docs.json", 2000, progress_indicator=mock.ANY
        )

    @mock.patch("esrally.utils.io.prepare_file_offset_table")
    @mock.patch("esrally.utils.io.decompress")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_prepare_bundled_document_set_if_document_file_available(self, is_file, get_size, decompress, prepare_file_offset_table):
        is_file.return_value = True
        # check only uncompressed
        get_size.side_effect = [2000]
        prepare_file_offset_table.return_value = 5

        p = loader.DocumentSetPreparator(
            track_name="unit-test", downloader=loader.Downloader(offline=False, test_mode=False), decompressor=loader.Decompressor()
        )

        assert p.prepare_bundled_document_set(
            document_set=track.Documents(
                source_format=track.Documents.SOURCE_FORMAT_BULK,
                document_file="docs.json",
                document_archive="docs.json.bz2",
                number_of_documents=5,
                compressed_size_in_bytes=200,
                uncompressed_size_in_bytes=2000,
            ),
            data_root=".",
        )

        prepare_file_offset_table.assert_called_with("./docs.json")

    @mock.patch("esrally.utils.io.prepare_file_offset_table")
    @mock.patch("esrally.utils.io.decompress")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_prepare_bundled_document_set_does_nothing_if_no_document_files(self, is_file, get_size, decompress, prepare_file_offset_table):
        # no files present
        is_file.return_value = False

        p = loader.DocumentSetPreparator(
            track_name="unit-test", downloader=loader.Downloader(offline=False, test_mode=False), decompressor=loader.Decompressor()
        )

        assert not p.prepare_bundled_document_set(
            document_set=track.Documents(
                source_format=track.Documents.SOURCE_FORMAT_BULK,
                document_file="docs.json",
                document_archive="docs.json.bz2",
                number_of_documents=5,
                compressed_size_in_bytes=200,
                uncompressed_size_in_bytes=2000,
            ),
            data_root=".",
        )

        assert decompress.call_count == 0
        assert prepare_file_offset_table.call_count == 0

    def test_used_corpora(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {"name": "logs-181998"},
                {"name": "logs-191998"},
                {"name": "logs-201998"},
            ],
            "corpora": [
                {
                    "name": "http_logs_unparsed",
                    "target-type": "type",
                    "documents": [
                        {
                            "target-index": "logs-181998",
                            "source-file": "documents-181998.unparsed.json.bz2",
                            "document-count": 2708746,
                            "compressed-bytes": 13064317,
                            "uncompressed-bytes": 303920342,
                        },
                        {
                            "target-index": "logs-191998",
                            "source-file": "documents-191998.unparsed.json.bz2",
                            "document-count": 9697882,
                            "compressed-bytes": 47211781,
                            "uncompressed-bytes": 1088378738,
                        },
                        {
                            "target-index": "logs-201998",
                            "source-file": "documents-201998.unparsed.json.bz2",
                            "document-count": 13053463,
                            "compressed-bytes": 63174979,
                            "uncompressed-bytes": 1456836090,
                        },
                    ],
                },
                {
                    "name": "http_logs",
                    "target-type": "type",
                    "documents": [
                        {
                            "target-index": "logs-181998",
                            "source-file": "documents-181998.json.bz2",
                            "document-count": 2708746,
                            "compressed-bytes": 13815456,
                            "uncompressed-bytes": 363512754,
                        },
                        {
                            "target-index": "logs-191998",
                            "source-file": "documents-191998.json.bz2",
                            "document-count": 9697882,
                            "compressed-bytes": 49439633,
                            "uncompressed-bytes": 1301732149,
                        },
                        {
                            "target-index": "logs-201998",
                            "source-file": "documents-201998.json.bz2",
                            "document-count": 13053463,
                            "compressed-bytes": 65623436,
                            "uncompressed-bytes": 1744012279,
                        },
                    ],
                },
            ],
            "operations": [
                {
                    "name": "bulk-index-1",
                    "operation-type": "bulk",
                    "corpora": ["http_logs"],
                    "indices": ["logs-181998"],
                    "bulk-size": 500,
                },
                {
                    "name": "bulk-index-2",
                    "operation-type": "bulk",
                    "corpora": ["http_logs"],
                    "indices": ["logs-191998"],
                    "bulk-size": 500,
                },
                {
                    "name": "bulk-index-3",
                    "operation-type": "bulk",
                    "corpora": ["http_logs_unparsed"],
                    "indices": ["logs-201998"],
                    "bulk-size": 500,
                },
                {"name": "node-stats", "operation-type": "node-stats"},
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
                                        "operation": "bulk-index-1",
                                    },
                                    {
                                        "name": "index-2",
                                        "operation": "bulk-index-2",
                                    },
                                    {
                                        "name": "index-3",
                                        "operation": "bulk-index-3",
                                    },
                                ]
                            }
                        },
                        {"operation": "node-stats"},
                    ],
                }
            ],
        }
        reader = loader.TrackSpecificationReader(selected_challenge="default-challenge")
        full_track = reader("unittest", track_specification, "/mappings")
        used_corpora = sorted(loader.used_corpora(full_track), key=lambda c: c.name)
        assert len(used_corpora) == 2
        assert used_corpora[0].name == "http_logs"
        # each bulk operation requires a different data file but they should have been merged properly.
        assert {d.document_archive for d in used_corpora[0].documents} == {"documents-181998.json.bz2", "documents-191998.json.bz2"}

        assert used_corpora[1].name == "http_logs_unparsed"
        assert {d.document_archive for d in used_corpora[1].documents} == {"documents-201998.unparsed.json.bz2"}

    @mock.patch("esrally.utils.io.prepare_file_offset_table")
    @mock.patch("esrally.utils.io.decompress")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_prepare_bundled_document_set_decompresses_compressed_docs(self, is_file, get_size, decompress, prepare_file_offset_table):
        # uncompressed is missing
        # decompressed is present
        # check if uncompressed is present after decompression
        # final loop iteration - uncompressed is present now
        is_file.side_effect = [False, True, True, True]
        # compressed
        # uncompressed after decompression
        # uncompressed in final loop iteration
        get_size.side_effect = [200, 2000, 2000]
        prepare_file_offset_table.return_value = 5

        p = loader.DocumentSetPreparator(
            track_name="unit-test", downloader=loader.Downloader(offline=False, test_mode=False), decompressor=loader.Decompressor()
        )

        assert p.prepare_bundled_document_set(
            document_set=track.Documents(
                source_format=track.Documents.SOURCE_FORMAT_BULK,
                document_file="docs.json",
                document_archive="docs.json.bz2",
                number_of_documents=5,
                compressed_size_in_bytes=200,
                uncompressed_size_in_bytes=2000,
            ),
            data_root=".",
        )

        prepare_file_offset_table.assert_called_with("./docs.json")

    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_prepare_bundled_document_set_error_compressed_docs_wrong_size(self, is_file, get_size):
        # uncompressed is missing
        # decompressed is present
        is_file.side_effect = [False, True]
        # compressed has wrong size
        get_size.side_effect = [150]

        p = loader.DocumentSetPreparator(
            track_name="unit-test", downloader=loader.Downloader(offline=False, test_mode=False), decompressor=loader.Decompressor()
        )

        with pytest.raises(exceptions.DataError) as exc:
            p.prepare_bundled_document_set(
                document_set=track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    document_file="docs.json",
                    document_archive="docs.json.bz2",
                    number_of_documents=5,
                    compressed_size_in_bytes=200,
                    uncompressed_size_in_bytes=2000,
                ),
                data_root=".",
            )

        assert exc.value.args[0] == "[./docs.json.bz2] is present but does not have the expected size of [200] bytes."

    @mock.patch("esrally.utils.io.prepare_file_offset_table")
    @mock.patch("esrally.utils.io.decompress")
    @mock.patch("os.path.getsize")
    @mock.patch("os.path.isfile")
    def test_prepare_bundled_document_set_uncompressed_docs_wrong_size(self, is_file, get_size, decompress, prepare_file_offset_table):
        # uncompressed is present
        is_file.side_effect = [True]
        # uncompressed
        get_size.side_effect = [1500]

        p = loader.DocumentSetPreparator(
            track_name="unit-test", downloader=loader.Downloader(offline=False, test_mode=False), decompressor=loader.Decompressor()
        )

        with pytest.raises(exceptions.DataError) as exc:
            p.prepare_bundled_document_set(
                document_set=track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    document_file="docs.json",
                    document_archive="docs.json.bz2",
                    number_of_documents=5,
                    compressed_size_in_bytes=200,
                    uncompressed_size_in_bytes=2000,
                ),
                data_root=".",
            )
        assert exc.value.args[0] == "[./docs.json] is present but does not have the expected size of [2000] bytes."

        assert prepare_file_offset_table.call_count == 0


class TestTemplateSource:
    @mock.patch("esrally.utils.io.dirname")
    @mock.patch.object(loader.TemplateSource, "read_glob_files")
    def test_entrypoint_of_replace_includes(self, patched_read_glob, patched_dirname):
        track = textwrap.dedent(
            """
        {% import "rally.helpers" as rally with context %}
        {
          "version": 2,
          "description": "unittest track",
          "data-url": "http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/geonames",
          "indices": [
            {
              "name": "geonames",
              "body": "index.json"
            }
          ],
          "corpora": [
            {
              "name": "geonames",
              "base-url": "http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/geonames",
              "documents": [
                {
                  "source-file": "documents-2.json.bz2",
                  "document-count": 11396505,
                  "compressed-bytes": 264698741,
                  "uncompressed-bytes": 3547614383
                }
              ]
            }
          ],
          "operations": [
            {{ rally.collect(parts="operations/*.json") }}
          ],
          "challenges": [
            {{ rally.collect(parts="challenges/*.json") }}
          ]
        }
        """
        )

        def dummy_read_glob(c):
            return f'{{"replaced {c}": "true"}}'

        patched_read_glob.side_effect = dummy_read_glob

        base_path = "~/.rally/benchmarks/tracks/default/geonames"
        template_file_name = "track.json"
        tmpl_src = loader.TemplateSource(base_path, template_file_name)
        expected_response = textwrap.dedent(
            """
            {% import "rally.helpers" as rally with context %}
            {
              "version": 2,
              "description": "unittest track",
              "data-url": "http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/geonames",
              "indices": [
                {
                  "name": "geonames",
                  "body": "index.json"
                }
              ],
              "corpora": [
                {
                  "name": "geonames",
                  "base-url": "http://benchmarks.elasticsearch.org.s3.amazonaws.com/corpora/geonames",
                  "documents": [
                    {
                      "source-file": "documents-2.json.bz2",
                      "document-count": 11396505,
                      "compressed-bytes": 264698741,
                      "uncompressed-bytes": 3547614383
                    }
                  ]
                }
              ],
              "operations": [
                {"replaced ~/.rally/benchmarks/tracks/default/geonames/operations/*.json": "true"}
              ],
              "challenges": [
                {"replaced ~/.rally/benchmarks/tracks/default/geonames/challenges/*.json": "true"}
              ]
            }
            """
        )

        assert tmpl_src.replace_includes(base_path, track) == expected_response

    def test_read_glob_files(self):
        tmpl_obj = loader.TemplateSource(
            base_path="/some/path/to/a/rally/track",
            template_file_name="track.json",
            fileglobber=lambda pat: [
                os.path.join(os.path.dirname(__file__), "resources", "track_fragment_1.json"),
                os.path.join(os.path.dirname(__file__), "resources", "track_fragment_2.json"),
            ],
        )
        response = tmpl_obj.read_glob_files("*track_fragment_*.json")
        expected_response = '{\n  "item1": "value1"\n}\n,\n{\n  "item2": "value2"\n}\n'

        assert response == expected_response


def assert_equal_ignore_whitespace(expected, actual):
    assert strip_ws(expected) == strip_ws(actual)


class TestTemplateRender:
    unittest_template_internal_vars = loader.default_internal_template_vars(
        clock=StaticClock, build_flavor="test", serverless_operator=False
    )

    def test_render_simple_template(self):
        template = """
        {
            "key": {{'01-01-2000' | days_ago(now)}},
            "key2": "static value",
            "key3": "{{build_flavor}}",
            "key4": {{serverless_operator}}
        }
        """

        rendered = loader.render_template(template, template_internal_vars=self.unittest_template_internal_vars)

        expected = """
        {
            "key": 5864,
            "key2": "static value",
            "key3": "test",
            "key4": False
        }
        """
        assert rendered == expected

    def test_render_template_with_external_variables(self):
        template = """
        {
            "greeting": "{{greeting | default("Aloha")}}",
            "name": "{{name | default("stranger")}}"
        }
        """

        rendered = loader.render_template(
            template, template_vars={"greeting": "Hi"}, template_internal_vars=self.unittest_template_internal_vars
        )

        expected = """
        {
            "greeting": "Hi",
            "name": "stranger"
        }
        """
        assert rendered == expected

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

        source = io.DictStringFileSourceFactory(
            {
                "dynamic-key-1": [textwrap.dedent('"dkey1": "value1"')],
                "dynamic-key-2": [textwrap.dedent('"dkey2": "value2"')],
                "dynamic-key-3": [textwrap.dedent('"dkey3": "value3"')],
            }
        )

        template_source = loader.TemplateSource("", "track.json", source=source, fileglobber=key_globber)
        template_source.load_template_from_string(template)

        rendered = loader.render_template(template_source.assembled_source, template_internal_vars=self.unittest_template_internal_vars)

        expected = """
        {
            "key1": "static value",
            "dkey1": "value1",
            "dkey2": "value2",
            "dkey3": "value3"

        }
        """
        assert_equal_ignore_whitespace(expected, rendered)

    def test_render_template_with_variables(self):
        template = """
        {% set _clients = clients if clients is defined else 16 %}
        {% set _bulk_size = bulk_size if bulk_size is defined else 100 %}
        {% import "rally.helpers" as rally with context %}
        {
            "key1": "static value",
            "dkey1": {{ _clients }},
            "dkey2": {{ _bulk_size }}
        }
        """
        rendered = loader.render_template(
            template, template_vars={"clients": 8}, template_internal_vars=self.unittest_template_internal_vars
        )

        expected = """
        {
            "key1": "static value",
            "dkey1": 8,
            "dkey2": 100
        }
        """
        assert_equal_ignore_whitespace(expected, rendered)

    def test_render_template_with_conditions(self):
        template = """
        {
            {%- if build_flavor != "test" %}
            "key1": "build_flavor"
            {%- elif serverless_operator %}
            "key1": "serverless_operator"
            {%- elif lifecycle == "ilm" %}
            "key1": "ilm"
            {%- else %}
            "key1": "else"
            {%- endif %}
        }
        """
        rendered = loader.render_template(
            template, template_vars={"lifecycle": "ilm"}, template_internal_vars=self.unittest_template_internal_vars
        )

        expected = """
        {
            "key1": "ilm"
        }
        """
        assert_equal_ignore_whitespace(expected, rendered)


class TestCompleteTrackParams:
    assembled_source = textwrap.dedent(
        """{% import "rally.helpers" as rally with context %}
        "key1": "value1",
        "key2": {{ value2 | default(3) }},
        "key3": {{ value3 | default("default_value3") }}
        "key4": {{ value2 | default(3) }}
        "key5": {{ build_flavor }}
    """
    )

    def test_check_complete_track_params_contains_all_track_params(self):
        complete_track_params = loader.CompleteTrackParams()
        loader.register_all_params_in_track(self.assembled_source, complete_track_params)

        assert complete_track_params.sorted_track_defined_params == ["value2", "value3"]

    def test_check_complete_track_params_does_not_fail_with_no_track_params(self):
        complete_track_params = loader.CompleteTrackParams()
        loader.register_all_params_in_track("{}", complete_track_params)

        assert complete_track_params.sorted_track_defined_params == []

    def test_internal_user_defined_track_params(self):
        # track params that deliberatly collide with internal variables
        track_params = {"now": "test", "build_flavor": "test"}
        complete_track_params = loader.CompleteTrackParams(user_specified_track_params=track_params)

        assert sorted(complete_track_params.internal_user_defined_track_params()) == ["build_flavor", "now"]

    def test_unused_user_defined_track_params(self):
        track_params = {"number_of_repliacs": 1, "enable_source": True, "number_of_shards": 5}  # deliberate typo  # unknown parameter

        complete_track_params = loader.CompleteTrackParams(user_specified_track_params=track_params)
        complete_track_params.populate_track_defined_params(
            list_of_track_params=[
                "bulk_indexing_clients",
                "bulk_indexing_iterations",
                "bulk_size",
                "cluster_health",
                "number_of_replicas",
                "number_of_shards",
            ]
        )

        assert sorted(complete_track_params.unused_user_defined_track_params()) == ["enable_source", "number_of_repliacs"]

    def test_unused_user_defined_track_params_doesnt_fail_with_detaults(self):
        complete_track_params = loader.CompleteTrackParams()
        complete_track_params.populate_track_defined_params(
            list_of_track_params=[
                "bulk_indexing_clients",
                "bulk_indexing_iterations",
                "bulk_size",
                "cluster_health",
                "number_of_replicas",
                "number_of_shards",
            ]
        )

        assert sorted(complete_track_params.unused_user_defined_track_params()) == []


class TestTrackPostProcessing:
    track_with_params_as_string = textwrap.dedent(
        """{
        "indices": [
            {
                "name": "test-index",
                "body": "test-index-body.json",
                "types": ["test-type"]
            }
        ],
        "corpora": [
            {
                "name": "unittest",
                "documents": [
                    {
                        "source-file": "documents.json.bz2",
                        "document-count": 10,
                        "compressed-bytes": 100,
                        "uncompressed-bytes": 10000
                    }
                ]
            }
        ],
        "operations": [
            {
                "name": "index-append",
                "operation-type": "bulk",
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
                "schedule": [
                    {
                        "clients": {{ bulk_indexing_clients | default(8) }},
                        "operation": "index-append",
                        "warmup-time-period": 100,
                        "time-period": 240
                    },
                    {
                        "parallel": {
                            "tasks": [
                                {
                                    "name": "search #1",
                                    "clients": 4,
                                    "operation": "search",
                                    "warmup-iterations": 1000,
                                    "iterations": 2000,
                                    "target-interval": 30
                                },
                                {
                                    "name": "search #2",
                                    "clients": 1,
                                    "operation": "search",
                                    "warmup-iterations": 1000,
                                    "iterations": 2000,
                                    "target-throughput": 200
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
    }"""
    )

    def test_post_processes_track_spec(self):
        track_specification = {
            "indices": [
                {
                    "name": "test-index",
                    "body": "test-index-body.json",
                    "types": [
                        "test-type",
                    ],
                },
            ],
            "corpora": [
                {
                    "name": "unittest",
                    "documents": [
                        {
                            "source-file": "documents.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                        }
                    ],
                }
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk",
                    "bulk-size": 5000,
                },
                {
                    "name": "search",
                    "operation-type": "search",
                },
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "description": "Default challenge",
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
                                        "iterations": 2000,
                                        "target-interval": 30,
                                    },
                                    {
                                        "name": "search #2",
                                        "clients": 1,
                                        "operation": "search",
                                        "warmup-iterations": 1000,
                                        "iterations": 2000,
                                        "target-throughput": 200,
                                    },
                                    {
                                        "name": "search #3",
                                        "clients": 1,
                                        "operation": "search",
                                        "iterations": 1,
                                    },
                                ]
                            }
                        },
                    ],
                }
            ],
        }

        expected_post_processed = {
            "indices": [
                {
                    "name": "test-index",
                    "body": "test-index-body.json",
                    "types": ["test-type"],
                },
            ],
            "corpora": [
                {
                    "name": "unittest",
                    "documents": [
                        {"source-file": "documents-1k.json.bz2", "document-count": 1000},
                    ],
                },
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk",
                    "bulk-size": 5000,
                },
                {
                    "name": "search",
                    "operation-type": "search",
                },
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "description": "Default challenge",
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
                                        "iterations": 4,
                                    },
                                    {
                                        "name": "search #2",
                                        "clients": 1,
                                        "operation": "search",
                                        "warmup-iterations": 1,
                                        "iterations": 1,
                                    },
                                    {
                                        "name": "search #3",
                                        "clients": 1,
                                        "operation": "search",
                                        "iterations": 1,
                                    },
                                ]
                            }
                        },
                    ],
                }
            ],
        }

        complete_track_params = loader.CompleteTrackParams()
        index_body = (
            '{"settings": {"index.number_of_shards": {{ number_of_shards | default(5) }}, '
            '"index.number_of_replicas": {{ number_of_replicas | default(0)}} }}'
        )

        cfg = config.Config()
        cfg.add(config.Scope.application, "track", "test.mode.enabled", True)

        expected = self.as_track(
            expected_post_processed,
            complete_track_params=complete_track_params,
            index_body=index_body,
        )
        actual = loader.TestModeTrackProcessor(cfg).on_after_load_track(
            self.as_track(
                track_specification,
                complete_track_params=complete_track_params,
                index_body=index_body,
            )
        )
        assert actual == expected

        assert complete_track_params.sorted_track_defined_params == ["number_of_replicas", "number_of_shards"]

    def as_track(self, track_specification, track_params=None, complete_track_params=None, index_body=None):
        reader = loader.TrackSpecificationReader(
            track_params=track_params,
            complete_track_params=complete_track_params,
            source=io.DictStringFileSourceFactory({"/mappings/test-index-body.json": [index_body]}),
        )
        return reader("unittest", track_specification, "/mappings")


class TestTrackPath:
    @mock.patch("os.path.exists")
    def test_sets_absolute_path(self, path_exists):
        path_exists.return_value = True

        cfg = config.Config()
        cfg.add(config.Scope.application, "benchmarks", "local.dataset.cache", "/data")

        default_challenge = track.Challenge(
            "default",
            default=True,
            schedule=[track.Task(name="index", operation=track.Operation("index", operation_type=track.OperationType.Bulk), clients=4)],
        )
        another_challenge = track.Challenge("other", default=False)
        t = track.Track(
            name="u",
            challenges=[another_challenge, default_challenge],
            corpora=[
                track.DocumentCorpus(
                    "unittest",
                    documents=[
                        track.Documents(
                            source_format=track.Documents.SOURCE_FORMAT_BULK,
                            document_file="docs/documents.json",
                            document_archive="docs/documents.json.bz2",
                        )
                    ],
                )
            ],
            indices=[track.Index(name="test", types=["docs"])],
        )

        loader.set_absolute_data_path(cfg, t)

        assert t.corpora[0].documents[0].document_file == "/data/unittest/docs/documents.json"
        assert t.corpora[0].documents[0].document_archive == "/data/unittest/docs/documents.json.bz2"


class TestTrackFilter:
    def filter(self, track_specification, *, include_tasks=None, exclude_tasks=None):
        cfg = config.Config()
        cfg.add(config.Scope.application, "track", "include.tasks", include_tasks)
        cfg.add(config.Scope.application, "track", "exclude.tasks", exclude_tasks)

        processor = loader.TaskFilterTrackProcessor(cfg)
        return processor.on_after_load_track(track_specification)

    def test_rejects_invalid_syntax(self):
        with pytest.raises(exceptions.SystemSetupError) as exc:
            self.filter(track_specification=None, include_tasks=["valid", "a:b:c"])
        assert exc.value.args[0] == "Invalid format for filtered tasks: [a:b:c]"

    def test_rejects_unknown_filter_type(self):
        with pytest.raises(exceptions.SystemSetupError) as exc:
            self.filter(track_specification=None, include_tasks=["valid", "op-type:index"])
        assert exc.value.args[0] == "Invalid format for filtered tasks: [op-type:index]. Expected [type] but got [op-type]."

    def test_filters_tasks(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "auto-managed": False}],
            "operations": [
                {
                    "name": "create-index",
                    "operation-type": "create-index",
                },
                {
                    "name": "bulk-index",
                    "operation-type": "bulk",
                },
                {
                    "name": "node-stats",
                    "operation-type": "node-stats",
                },
                {
                    "name": "cluster-stats",
                    "operation-type": "custom-operation-type",
                },
                {
                    "name": "match-all",
                    "operation-type": "search",
                    "body": {
                        "query": {
                            "match_all": {},
                        },
                    },
                },
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "schedule": [
                        {
                            "operation": "create-index",
                        },
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
                            "operation": "node-stats",
                        },
                        {
                            "name": "match-all-serial",
                            "operation": "match-all",
                        },
                        {
                            "name": "cluster-stats",
                            "operation": "cluster-stats",
                        },
                        {
                            "parallel": {
                                "tasks": [
                                    {
                                        "name": "query-filtered",
                                        "tags": "include-me",
                                        "operation": "match-all",
                                    },
                                    {
                                        "name": "index-4",
                                        "tags": ["include-me", "bulk-task"],
                                        "operation": "bulk-index",
                                    },
                                    {
                                        "name": "index-5",
                                        "operation": "bulk-index",
                                    },
                                ]
                            }
                        },
                        {
                            "name": "final-cluster-stats",
                            "operation": "cluster-stats",
                            "tags": "include-me",
                        },
                    ],
                }
            ],
        }
        reader = loader.TrackSpecificationReader()
        full_track = reader("unittest", track_specification, "/mappings")
        assert len(full_track.challenges[0].schedule) == 7

        filtered_track = self.filter(
            full_track,
            include_tasks=[
                "index-3",
                "type:search",
                # Filtering should also work for non-core operation types.
                "type:custom-operation-type",
                "tag:include-me",
            ],
        )

        schedule = filtered_track.challenges[0].schedule
        assert len(schedule) == 5
        assert [t.name for t in schedule[0].tasks] == ["index-3", "match-all-parallel"]
        assert schedule[1].name == "match-all-serial"
        assert schedule[2].name == "cluster-stats"
        assert [t.name for t in schedule[3].tasks] == ["query-filtered", "index-4"]
        assert schedule[4].name == "final-cluster-stats"

    def test_filters_exclude_tasks(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                    "auto-managed": False,
                },
            ],
            "operations": [
                {
                    "name": "create-index",
                    "operation-type": "create-index",
                },
                {
                    "name": "bulk-index",
                    "operation-type": "bulk",
                },
                {
                    "name": "node-stats",
                    "operation-type": "node-stats",
                },
                {
                    "name": "cluster-stats",
                    "operation-type": "custom-operation-type",
                },
                {
                    "name": "match-all",
                    "operation-type": "search",
                    "body": {
                        "query": {
                            "match_all": {},
                        },
                    },
                },
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "schedule": [
                        {
                            "operation": "create-index",
                        },
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
                            "operation": "node-stats",
                        },
                        {
                            "name": "match-all-serial",
                            "operation": "match-all",
                        },
                        {
                            "operation": "cluster-stats",
                        },
                    ],
                }
            ],
        }
        reader = loader.TrackSpecificationReader()
        full_track = reader("unittest", track_specification, "/mappings")
        assert len(full_track.challenges[0].schedule) == 5

        filtered_track = self.filter(full_track, exclude_tasks=["index-3", "type:search", "create-index"])

        schedule = filtered_track.challenges[0].schedule
        assert len(schedule) == 3
        assert [t.name for t in schedule[0].tasks] == ["index-1", "index-2"]
        assert schedule[1].name == "node-stats"
        assert schedule[2].name == "cluster-stats"

    def test_unmatched_exclude_runs_everything(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                    "auto-managed": False,
                },
            ],
            "operations": [
                {
                    "name": "create-index",
                    "operation-type": "create-index",
                },
                {
                    "name": "bulk-index",
                    "operation-type": "bulk",
                },
                {
                    "name": "node-stats",
                    "operation-type": "node-stats",
                },
                {
                    "name": "cluster-stats",
                    "operation-type": "custom-operation-type",
                },
                {
                    "name": "match-all",
                    "operation-type": "search",
                    "body": {
                        "query": {
                            "match_all": {},
                        },
                    },
                },
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "schedule": [
                        {
                            "operation": "create-index",
                        },
                        {
                            "operation": "bulk-index",
                        },
                        {
                            "operation": "node-stats",
                        },
                        {
                            "name": "match-all-serial",
                            "operation": "match-all",
                        },
                        {
                            "operation": "cluster-stats",
                        },
                    ],
                }
            ],
        }

        reader = loader.TrackSpecificationReader()
        full_track = reader("unittest", track_specification, "/mappings")
        assert len(full_track.challenges[0].schedule) == 5

        expected_schedule = full_track.challenges[0].schedule.copy()
        filtered_track = self.filter(full_track, exclude_tasks=["nothing"])

        schedule = filtered_track.challenges[0].schedule
        assert schedule == expected_schedule

    def test_unmatched_include_runs_nothing(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                    "auto-managed": False,
                },
            ],
            "operations": [
                {
                    "name": "create-index",
                    "operation-type": "create-index",
                },
                {
                    "name": "bulk-index",
                    "operation-type": "bulk",
                },
                {
                    "name": "node-stats",
                    "operation-type": "node-stats",
                },
                {
                    "name": "cluster-stats",
                    "operation-type": "custom-operation-type",
                },
                {
                    "name": "match-all",
                    "operation-type": "search",
                    "body": {
                        "query": {
                            "match_all": {},
                        },
                    },
                },
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "schedule": [
                        {
                            "operation": "create-index",
                        },
                        {
                            "operation": "bulk-index",
                        },
                        {
                            "operation": "node-stats",
                        },
                        {
                            "name": "match-all-serial",
                            "operation": "match-all",
                        },
                        {
                            "operation": "cluster-stats",
                        },
                    ],
                }
            ],
        }

        reader = loader.TrackSpecificationReader()
        full_track = reader("unittest", track_specification, "/mappings")
        assert len(full_track.challenges[0].schedule) == 5

        expected_schedule = []
        filtered_track = self.filter(full_track, include_tasks=["nothing"])

        schedule = filtered_track.challenges[0].schedule
        assert schedule == expected_schedule


class TestServerlessTrackFilter:
    TRACK_SPECIFICATION = {
        "description": "description for unit test",
        "indices": [{"name": "test-index", "auto-managed": False}],
        "operations": [
            {
                "name": "create-index",
                "operation-type": "create-index",
            },
            {
                "name": "bulk-index",
                "operation-type": "bulk",
            },
            {
                "name": "node-stats",
                "operation-type": "node-stats",
            },
            {
                "name": "cluster-stats",
                "operation-type": "custom-operation-type",
            },
            {
                "name": "load-posts",
                "operation-type": "composite",
                "requests": [],
            },
        ],
        "challenges": [
            {
                "name": "default-challenge",
                "schedule": [
                    {
                        "operation": "create-index",
                    },
                    {
                        "operation": {
                            "operation-type": "bulk-index",
                            "run-on-serverless": False,
                        }
                    },
                    {
                        "parallel": {
                            "tasks": [
                                {
                                    "name": "index-1",
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
                        "operation": "shrink-index",
                    },
                    {
                        "operation": {
                            "operation-type": "create-index-template",
                            "run-on-serverless": True,
                        }
                    },
                    {
                        "operation": "node-stats",
                    },
                    {
                        "operation": "cluster-stats",
                    },
                    {
                        "name": "load-posts",
                        "operation": "load-posts",
                    },
                ],
            }
        ],
    }

    def filter(self, track_specification, *, serverless_mode, serverless_operator):
        cfg = config.Config()
        cfg.add(config.Scope.application, "driver", "serverless.mode", serverless_mode)
        cfg.add(config.Scope.application, "driver", "serverless.operator", serverless_operator)

        processor = loader.ServerlessFilterTrackProcessor(cfg)
        return processor.on_after_load_track(track_specification)

    def test_noop_if_not_serverless(self):
        filtered_track = self.filter(track_specification={"foo": "bar"}, serverless_mode=False, serverless_operator=False)
        assert filtered_track == {"foo": "bar"}

    def test_no_message_if_no_filter(self):
        track_specification = {
            "challenges": [
                {
                    "name": "default-challenge",
                    "schedule": [],
                }
            ]
        }
        reader = loader.TrackSpecificationReader()
        full_track = reader("unittest", copy.deepcopy(track_specification), "/mappings")
        filtered_track = self.filter(full_track, serverless_mode=True, serverless_operator=True)
        assert filtered_track.challenges[0].serverless_info == []

    def test_filters_tasks_operator_false(self):
        reader = loader.TrackSpecificationReader()
        full_track = reader("unittest", copy.deepcopy(self.TRACK_SPECIFICATION), "/mappings")
        assert len(full_track.challenges[0].schedule) == 8

        filtered_track = self.filter(full_track, serverless_mode=True, serverless_operator=False)
        assert filtered_track.challenges[0].serverless_info == [
            "Treating parallel task in challenge [default-challenge] as public.",
            "Excluding [bulk-index], [shrink-index], [node-stats] as challenge [default-challenge] is run on serverless.",
        ]

        schedule = filtered_track.challenges[0].schedule
        assert len(schedule) == 5
        assert schedule[0].name == "create-index"
        assert [t.name for t in schedule[1].tasks] == ["index-1", "match-all-parallel"]
        assert schedule[2].name == "create-index-template"
        assert schedule[3].name == "cluster-stats"
        assert schedule[4].name == "load-posts"

    def test_filters_tasks_operator_true(self):
        reader = loader.TrackSpecificationReader()
        full_track = reader("unittest", copy.deepcopy(self.TRACK_SPECIFICATION), "/mappings")
        assert len(full_track.challenges[0].schedule) == 8

        filtered_track = self.filter(full_track, serverless_mode=True, serverless_operator=True)
        assert filtered_track.challenges[0].serverless_info == [
            "Treating parallel task in challenge [default-challenge] as public.",
            "Excluding [bulk-index], [shrink-index] as challenge [default-challenge] is run on serverless.",
        ]

        schedule = filtered_track.challenges[0].schedule
        assert len(schedule) == 6
        assert schedule[0].name == "create-index"
        assert [t.name for t in schedule[1].tasks] == ["index-1", "match-all-parallel"]
        assert schedule[2].name == "create-index-template"
        assert schedule[3].name == "node-stats"
        assert schedule[4].name == "cluster-stats"
        assert schedule[5].name == "load-posts"


# pylint: disable=too-many-public-methods
class TestTrackSpecificationReader:
    def test_description_is_optional(self):
        track_specification = {
            # no description here
            "challenges": []
        }
        reader = loader.TrackSpecificationReader()

        resulting_track = reader("unittest", track_specification, "/mappings")
        assert resulting_track.name == "unittest"
        assert resulting_track.description == ""

    def test_can_read_track_info(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                    "types": ["test-type"],
                },
            ],
            "data-streams": [],
            "corpora": [],
            "operations": [],
            "challenges": [],
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings")
        assert resulting_track.name == "unittest"
        assert resulting_track.description == "description for unit test"

    def test_document_count_mandatory_if_file_present(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                    "types": ["docs"],
                },
            ],
            "corpora": [
                {
                    "name": "test",
                    "base-url": "https://localhost/data",
                    "documents": [
                        {"source-file": "documents-main.json.bz2"},
                    ],
                },
            ],
            "challenges": [],
        }
        reader = loader.TrackSpecificationReader()
        with pytest.raises(loader.TrackSyntaxError) as exc:
            reader("unittest", track_specification, "/mappings")
        assert exc.value.args[0] == "Track 'unittest' is invalid. Mandatory element 'document-count' is missing."

    def test_parse_with_mixed_warmup_iterations_and_measurement(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                    "body": "index.json",
                    "types": ["docs"],
                }
            ],
            "corpora": [
                {
                    "name": "test",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                        }
                    ],
                }
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk",
                    "bulk-size": 5000,
                }
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "schedule": [
                        {
                            "clients": 8,
                            "operation": "index-append",
                            "warmup-iterations": 3,
                            "time-period": 60,
                        },
                    ],
                }
            ],
        }

        reader = loader.TrackSpecificationReader(
            source=io.DictStringFileSourceFactory(
                {
                    "/mappings/index.json": ['{"mappings": {"docs": "empty-for-test"}}'],
                }
            )
        )
        with pytest.raises(loader.TrackSyntaxError) as exc:
            reader("unittest", track_specification, "/mappings")
        assert exc.value.args[0] == (
            "Track 'unittest' is invalid. Operation 'index-append' in challenge 'default-challenge' defines 3 warmup "
            "iterations and a time period of 60 seconds but mixing time periods and iterations is not allowed."
        )

    def test_parse_with_mixed_iterations_and_ramp_up(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "body": "index.json", "types": ["docs"]}],
            "corpora": [
                {
                    "name": "test",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                        }
                    ],
                }
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk",
                    "bulk-size": 5000,
                }
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "schedule": [
                        {
                            "clients": 8,
                            "operation": "index-append",
                            "ramp-up-time-period": 120,
                            "warmup-iterations": 3,
                            "iterations": 5,
                        }
                    ],
                }
            ],
        }

        reader = loader.TrackSpecificationReader(
            source=io.DictStringFileSourceFactory(
                {
                    "/mappings/index.json": ['{"mappings": {"docs": "empty-for-test"}}'],
                }
            )
        )
        with pytest.raises(loader.TrackSyntaxError) as exc:
            reader("unittest", track_specification, "/mappings")
        assert exc.value.args[0] == (
            "Track 'unittest' is invalid. Operation 'index-append' in challenge 'default-challenge' "
            "defines a ramp-up time period of 120 seconds as well as 3 warmup iterations and 5 iterations "
            "but mixing time periods and iterations is not allowed."
        )

    def test_parse_missing_challenge_or_challenges(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                    "body": "index.json",
                    "types": ["docs"],
                },
            ],
            "corpora": [
                {
                    "name": "test",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                        }
                    ],
                }
            ],
            # no challenge or challenges element
        }
        reader = loader.TrackSpecificationReader(
            source=io.DictStringFileSourceFactory(
                {
                    "/mappings/index.json": ['{"mappings": {"docs": "empty-for-test"}}'],
                }
            )
        )
        with pytest.raises(loader.TrackSyntaxError) as exc:
            reader("unittest", track_specification, "/mappings")
        assert exc.value.args[0] == (
            "Track 'unittest' is invalid. You must define 'challenge', 'challenges' or 'schedule' but none is specified."
        )

    def test_parse_challenge_and_challenges_are_defined(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index", "body": "index.json", "types": ["docs"]}],
            "corpora": [
                {
                    "name": "test",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                        }
                    ],
                }
            ],
            # We define both. Note that challenges without any properties would not pass JSON schema validation but we don't test this here.
            "challenge": {},
            "challenges": [],
        }
        reader = loader.TrackSpecificationReader(
            source=io.DictStringFileSourceFactory(
                {
                    "/mappings/index.json": ['{"mappings": {"docs": "empty-for-test"}}'],
                }
            )
        )
        with pytest.raises(loader.TrackSyntaxError) as exc:
            reader("unittest", track_specification, "/mappings")
        assert exc.value.args[0] == (
            "Track 'unittest' is invalid. Multiple out of 'challenge', 'challenges' or 'schedule' are defined but only "
            "one of them is allowed."
        )

    def test_parse_with_mixed_warmup_time_period_and_iterations(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                    "body": "index.json",
                    "types": ["docs"],
                },
            ],
            "corpora": [
                {
                    "name": "test",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                        }
                    ],
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
                    "schedule": [
                        {
                            "clients": 8,
                            "operation": "index-append",
                            "warmup-time-period": 20,
                            "iterations": 1000,
                        },
                    ],
                }
            ],
        }

        reader = loader.TrackSpecificationReader(
            source=io.DictStringFileSourceFactory(
                {
                    "/mappings/index.json": ['{"mappings": {"docs": "empty-for-test"}}'],
                }
            )
        )
        with pytest.raises(loader.TrackSyntaxError) as exc:
            reader("unittest", track_specification, "/mappings")
        assert exc.value.args[0] == (
            "Track 'unittest' is invalid. Operation 'index-append' in challenge 'default-challenge' defines a warmup time "
            "period of 20 seconds and 1000 iterations but mixing time periods and iterations is not allowed."
        )

    def test_parse_duplicate_implicit_task_names(self):
        track_specification = {
            "description": "description for unit test",
            "operations": [
                {
                    "name": "search",
                    "operation-type": "search",
                    "index": "_all",
                },
            ],
            "challenge": {
                "name": "default-challenge",
                "schedule": [
                    {
                        "operation": "search",
                        "clients": 1,
                    },
                    {
                        "operation": "search",
                        "clients": 2,
                    },
                ],
            },
        }
        reader = loader.TrackSpecificationReader()
        with pytest.raises(loader.TrackSyntaxError) as exc:
            reader("unittest", track_specification, "/mappings")
        assert exc.value.args[0] == (
            "Track 'unittest' is invalid. Challenge 'default-challenge' contains multiple tasks with the name 'search'. Please"
            " use the task's name property to assign a unique name for each task."
        )

    def test_parse_duplicate_explicit_task_names(self):
        track_specification = {
            "description": "description for unit test",
            "operations": [
                {
                    "name": "search",
                    "operation-type": "search",
                    "index": "_all",
                },
            ],
            "challenge": {
                "name": "default-challenge",
                "schedule": [
                    {
                        "name": "duplicate-task-name",
                        "operation": "search",
                        "clients": 1,
                    },
                    {
                        "name": "duplicate-task-name",
                        "operation": "search",
                        "clients": 2,
                    },
                ],
            },
        }
        reader = loader.TrackSpecificationReader()
        with pytest.raises(loader.TrackSyntaxError) as exc:
            reader("unittest", track_specification, "/mappings")
        assert exc.value.args[0] == (
            "Track 'unittest' is invalid. Challenge 'default-challenge' contains multiple tasks with the name "
            "'duplicate-task-name'. Please use the task's name property to assign a unique name for each task."
        )

    def test_load_invalid_index_body(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "index-historical",
                    "body": "body.json",
                    "types": ["_doc"],
                },
            ],
            "corpora": [
                {
                    "name": "test",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                        }
                    ],
                }
            ],
            "schedule": [
                {
                    "clients": 8,
                    "operation": {
                        "name": "index-append",
                        "operation-type": "index",
                        "bulk-size": 5000,
                    },
                },
            ],
        }
        reader = loader.TrackSpecificationReader(
            track_params={"number_of_shards": 3},
            source=io.DictStringFileSourceFactory(
                {
                    "/mappings/body.json": [
                        """
            {
                "settings": {
                    "number_of_shards": {{ number_of_shards }}
                },
                "mappings": {
                    "_doc": "no closing quotation mark!!,
                }
            }
            """
                    ]
                }
            ),
        )
        with pytest.raises(loader.TrackSyntaxError) as exc:
            reader("unittest", track_specification, "/mappings")
        assert exc.value.args[0] == "Could not load file template for 'definition for index index-historical in body.json'"

    def test_parse_unique_task_names(self):
        track_specification = {
            "description": "description for unit test",
            "operations": [
                {
                    "name": "search",
                    "operation-type": "search",
                    "index": "_all",
                },
            ],
            "challenge": {
                "name": "default-challenge",
                "schedule": [
                    {
                        "name": "search-one-client",
                        "operation": "search",
                        "clients": 1,
                    },
                    {
                        "name": "search-two-clients",
                        "operation": "search",
                        "clients": 2,
                    },
                ],
            },
        }
        reader = loader.TrackSpecificationReader(selected_challenge="default-challenge")
        resulting_track = reader("unittest", track_specification, "/mappings")
        assert resulting_track.name == "unittest"
        challenge = resulting_track.challenges[0]
        assert challenge.selected
        schedule = challenge.schedule
        assert len(schedule) == 2
        assert schedule[0].name == "search-one-client"
        assert schedule[0].operation.name == "search"
        assert schedule[1].name == "search-two-clients"
        assert schedule[1].operation.name == "search"

    def test_parse_indices_valid_track_specification(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "index-historical",
                    "body": "body.json",
                    "types": ["main", "secondary"],
                },
            ],
            "corpora": [
                {
                    "name": "test",
                    "base-url": "https://localhost/data",
                    "meta": {"test-corpus": True},
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                            "target-index": "index-historical",
                            "target-type": "main",
                            "meta": {"test-docs": True, "role": "main"},
                        },
                        {
                            "source-file": "documents-secondary.json.bz2",
                            "includes-action-and-meta-data": True,
                            "document-count": 20,
                            "compressed-bytes": 200,
                            "uncompressed-bytes": 20000,
                            "meta": {"test-docs": True, "role": "secondary"},
                        },
                    ],
                }
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index",
                    "bulk-size": 5000,
                    "meta": {"append": True},
                },
                {
                    "name": "search",
                    "operation-type": "search",
                    "index": "index-historical",
                },
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "description": "Default challenge",
                    "meta": {"mixed": True, "max-clients": 8},
                    "schedule": [
                        {
                            "clients": 8,
                            "operation": "index-append",
                            "meta": {"operation-index": 0},
                        },
                        {
                            "clients": 1,
                            "operation": "search",
                        },
                    ],
                }
            ],
        }
        complete_track_params = loader.CompleteTrackParams()
        reader = loader.TrackSpecificationReader(
            track_params={"number_of_shards": 3},
            complete_track_params=complete_track_params,
            source=io.DictStringFileSourceFactory(
                {
                    "/mappings/body.json": [
                        """
            {
                "settings": {
                    "number_of_shards": {{ number_of_shards }}
                },
                "mappings": {
                    "main": "empty-for-test",
                    "secondary": "empty-for-test"
                }
            }
            """
                    ]
                }
            ),
        )
        resulting_track = reader("unittest", track_specification, "/mappings")
        # j2 variables defined in the track -- used for checking mismatching user track params
        assert complete_track_params.sorted_track_defined_params == ["number_of_shards"]
        assert resulting_track.name == "unittest"
        assert resulting_track.description == "description for unit test"
        # indices
        assert len(resulting_track.indices) == 1
        assert resulting_track.indices[0].name == "index-historical"
        assert {
            "settings": {
                "number_of_shards": 3,
            },
            "mappings": {
                "main": "empty-for-test",
                "secondary": "empty-for-test",
            },
        } == resulting_track.indices[0].body
        assert len(resulting_track.indices[0].types) == 2
        assert resulting_track.indices[0].types[0] == "main"
        assert resulting_track.indices[0].types[1] == "secondary"
        # corpora
        assert len(resulting_track.corpora) == 1
        assert resulting_track.corpora[0].name == "test"
        assert resulting_track.corpora[0].meta_data == {"test-corpus": True}
        assert len(resulting_track.corpora[0].documents) == 2

        docs_primary = resulting_track.corpora[0].documents[0]
        assert track.Documents.SOURCE_FORMAT_BULK == docs_primary.source_format
        assert docs_primary.document_file == "documents-main.json"
        assert docs_primary.document_archive == "documents-main.json.bz2"
        assert docs_primary.base_url == "https://localhost/data"
        assert not docs_primary.includes_action_and_meta_data
        assert docs_primary.number_of_documents == 10
        assert docs_primary.compressed_size_in_bytes == 100
        assert docs_primary.uncompressed_size_in_bytes == 10000
        assert docs_primary.target_index == "index-historical"
        assert docs_primary.target_type == "main"
        assert docs_primary.meta_data == {"test-docs": True, "role": "main"}

        docs_secondary = resulting_track.corpora[0].documents[1]
        assert track.Documents.SOURCE_FORMAT_BULK == docs_secondary.source_format
        assert docs_secondary.document_file == "documents-secondary.json"
        assert docs_secondary.document_archive == "documents-secondary.json.bz2"
        assert docs_secondary.base_url == "https://localhost/data"
        assert docs_secondary.includes_action_and_meta_data
        assert docs_secondary.number_of_documents == 20
        assert docs_secondary.compressed_size_in_bytes == 200
        assert docs_secondary.uncompressed_size_in_bytes == 20000
        # This is defined by the action-and-meta-data line!
        assert docs_secondary.target_index is None
        assert docs_secondary.target_type is None
        assert docs_secondary.meta_data == {"test-docs": True, "role": "secondary"}

        # challenges
        assert len(resulting_track.challenges) == 1
        assert resulting_track.challenges[0].name == "default-challenge"
        assert resulting_track.challenges[0].description == "Default challenge"
        assert resulting_track.challenges[0].meta_data == {"mixed": True, "max-clients": 8}
        assert resulting_track.challenges[0].schedule[0].operation.meta_data == {"append": True}
        assert resulting_track.challenges[0].schedule[0].meta_data == {"operation-index": 0}

    def test_parse_data_streams_valid_track_specification(self):
        track_specification = {
            "description": "description for unit test",
            "data-streams": [{"name": "data-stream-historical"}],
            "corpora": [
                {
                    "name": "test",
                    "base-url": "https://localhost/data",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                            "target-data-stream": "data-stream-historical",
                        },
                        {
                            "source-file": "documents-secondary.json.bz2",
                            "includes-action-and-meta-data": True,
                            "document-count": 20,
                            "compressed-bytes": 200,
                            "uncompressed-bytes": 20000,
                        },
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                            "target-data-stream": "data-stream-historical",
                        },
                    ],
                }
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "index",
                    "bulk-size": 5000,
                    "meta": {"append": True},
                },
                {
                    "name": "search",
                    "operation-type": "search",
                    "data-stream": "data-stream-historical",
                },
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "description": "Default challenge",
                    "meta": {
                        "mixed": True,
                        "max-clients": 8,
                    },
                    "schedule": [
                        {
                            "clients": 8,
                            "operation": "index-append",
                            "meta": {"operation-index": 0},
                        },
                        {
                            "clients": 1,
                            "operation": "search",
                        },
                    ],
                }
            ],
        }
        complete_track_params = loader.CompleteTrackParams()
        reader = loader.TrackSpecificationReader(complete_track_params=complete_track_params)
        resulting_track = reader("unittest", track_specification, "/mappings")
        # j2 variables defined in the track -- used for checking mismatching user track params
        assert resulting_track.name == "unittest"
        assert resulting_track.description == "description for unit test"
        # data streams
        assert len(resulting_track.data_streams) == 1
        assert resulting_track.data_streams[0].name == "data-stream-historical"
        # corpora
        assert len(resulting_track.corpora) == 1
        assert resulting_track.corpora[0].name == "test"
        assert len(resulting_track.corpora[0].documents) == 3

        docs_primary = resulting_track.corpora[0].documents[0]
        assert track.Documents.SOURCE_FORMAT_BULK == docs_primary.source_format
        assert docs_primary.document_file == "documents-main.json"
        assert docs_primary.document_archive == "documents-main.json.bz2"
        assert docs_primary.base_url == "https://localhost/data"
        assert not docs_primary.includes_action_and_meta_data
        assert docs_primary.number_of_documents == 10
        assert docs_primary.compressed_size_in_bytes == 100
        assert docs_primary.uncompressed_size_in_bytes == 10000
        assert docs_primary.target_data_stream == "data-stream-historical"
        assert docs_primary.target_index is None
        assert docs_primary.target_type is None

        docs_secondary = resulting_track.corpora[0].documents[1]
        assert track.Documents.SOURCE_FORMAT_BULK == docs_secondary.source_format
        assert docs_secondary.document_file == "documents-secondary.json"
        assert docs_secondary.document_archive == "documents-secondary.json.bz2"
        assert docs_secondary.base_url == "https://localhost/data"
        assert docs_secondary.includes_action_and_meta_data
        assert docs_secondary.number_of_documents == 20
        assert docs_secondary.compressed_size_in_bytes == 200
        assert docs_secondary.uncompressed_size_in_bytes == 20000
        # This is defined by the action-and-meta-data line!
        assert docs_secondary.target_data_stream is None
        assert docs_secondary.target_index is None
        assert docs_secondary.target_type is None

        docs_tertiary = resulting_track.corpora[0].documents[2]
        assert track.Documents.SOURCE_FORMAT_BULK == docs_tertiary.source_format
        assert docs_tertiary.document_file == "documents-main.json"
        assert docs_tertiary.document_archive == "documents-main.json.bz2"
        assert docs_tertiary.base_url == "https://localhost/data"
        assert not docs_tertiary.includes_action_and_meta_data
        assert docs_tertiary.number_of_documents == 10
        assert docs_tertiary.compressed_size_in_bytes == 100
        assert docs_tertiary.target_index is None
        assert docs_tertiary.target_type is None
        assert docs_tertiary.target_data_stream == "data-stream-historical"

        # challenges
        assert len(resulting_track.challenges) == 1
        assert resulting_track.challenges[0].name == "default-challenge"
        assert resulting_track.challenges[0].description == "Default challenge"
        assert resulting_track.challenges[0].meta_data == {"mixed": True, "max-clients": 8}
        assert resulting_track.challenges[0].schedule[0].operation.meta_data == {"append": True}
        assert resulting_track.challenges[0].schedule[0].meta_data == {"operation-index": 0}

    def test_parse_valid_without_types(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "index-historical",
                    "body": "body.json"
                    # no type information here
                }
            ],
            "corpora": [
                {
                    "name": "test",
                    "base-url": "https://localhost/data",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                        },
                    ],
                }
            ],
            "schedule": [
                {
                    "clients": 8,
                    "operation": {
                        "name": "index-append",
                        "operation-type": "bulk",
                        "bulk-size": 5000,
                    },
                },
            ],
        }
        reader = loader.TrackSpecificationReader(
            track_params={"number_of_shards": 3},
            source=io.DictStringFileSourceFactory(
                {
                    "/mappings/body.json": [
                        """
            {
                "settings": {
                    "number_of_shards": {{ number_of_shards }}
                }
            }
            """
                    ]
                }
            ),
        )
        resulting_track = reader("unittest", track_specification, "/mappings")
        assert resulting_track.name == "unittest"
        assert resulting_track.description == "description for unit test"
        # indices
        assert len(resulting_track.indices) == 1
        assert resulting_track.indices[0].name == "index-historical"
        assert resulting_track.indices[0].body == {
            "settings": {
                "number_of_shards": 3,
            },
        }
        assert len(resulting_track.indices[0].types) == 0
        # corpora
        assert len(resulting_track.corpora) == 1
        assert resulting_track.corpora[0].name == "test"
        assert len(resulting_track.corpora[0].documents) == 1

        docs_primary = resulting_track.corpora[0].documents[0]
        assert track.Documents.SOURCE_FORMAT_BULK == docs_primary.source_format
        assert docs_primary.document_file == "documents-main.json"
        assert docs_primary.document_archive == "documents-main.json.bz2"
        assert docs_primary.base_url == "https://localhost/data"
        assert not docs_primary.includes_action_and_meta_data
        assert docs_primary.number_of_documents == 10
        assert docs_primary.compressed_size_in_bytes == 100
        assert docs_primary.uncompressed_size_in_bytes == 10000
        assert docs_primary.target_index == "index-historical"
        assert docs_primary.target_type is None
        assert docs_primary.target_data_stream is None

        # challenges
        assert len(resulting_track.challenges) == 1

    def test_parse_invalid_data_streams_with_indices(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "index-historical",
                    # no type information here
                }
            ],
            "data-streams": [
                {
                    "name": "historical-data-stream",
                },
            ],
            "corpora": [
                {
                    "name": "test",
                    "base-url": "https://localhost/data",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                        },
                    ],
                }
            ],
            "schedule": [
                {
                    "clients": 8,
                    "operation": {
                        "name": "index-append",
                        "operation-type": "bulk",
                        "bulk-size": 5000,
                    },
                }
            ],
        }
        complete_track_params = loader.CompleteTrackParams()
        reader = loader.TrackSpecificationReader(complete_track_params=complete_track_params)
        with pytest.raises(loader.TrackSyntaxError):
            reader("unittest", track_specification, "/mapping")

    def test_parse_invalid_data_streams_with_target_index(self):
        track_specification = {
            "description": "description for unit test",
            "data-streams": [
                {
                    "name": "historical-data-stream",
                },
            ],
            "corpora": [
                {
                    "name": "test",
                    "base-url": "https://localhost/data",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                            "target-index": "historical-index",
                        },
                    ],
                }
            ],
            "schedule": [
                {
                    "clients": 8,
                    "operation": {
                        "name": "index-append",
                        "operation-type": "bulk",
                        "bulk-size": 5000,
                    },
                },
            ],
        }
        complete_track_params = loader.CompleteTrackParams()
        reader = loader.TrackSpecificationReader(complete_track_params=complete_track_params)
        with pytest.raises(loader.TrackSyntaxError):
            reader("unittest", track_specification, "/mapping")

    def test_parse_invalid_data_streams_with_target_type(self):
        track_specification = {
            "description": "description for unit test",
            "data-streams": [{"name": "historical-data-stream"}],
            "corpora": [
                {
                    "name": "test",
                    "base-url": "https://localhost/data",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                            "target-type": "_doc",
                        },
                    ],
                }
            ],
            "schedule": [
                {
                    "clients": 8,
                    "operation": {
                        "name": "index-append",
                        "operation-type": "bulk",
                        "bulk-size": 5000,
                    },
                },
            ],
        }
        complete_track_params = loader.CompleteTrackParams()
        reader = loader.TrackSpecificationReader(complete_track_params=complete_track_params)
        with pytest.raises(loader.TrackSyntaxError):
            reader("unittest", track_specification, "/mapping")

    def test_parse_invalid_no_data_stream_target(self):
        track_specification = {
            "description": "description for unit test",
            "data-streams": [
                {
                    "name": "historical-data-stream",
                },
                {
                    "name": "historical-data-stream-2",
                },
            ],
            "corpora": [
                {
                    "name": "test",
                    "base-url": "https://localhost/data",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                        }
                    ],
                }
            ],
            "schedule": [
                {
                    "clients": 8,
                    "operation": {
                        "name": "index-append",
                        "operation-type": "bulk",
                        "bulk-size": 5000,
                    },
                },
            ],
        }
        complete_track_params = loader.CompleteTrackParams()
        reader = loader.TrackSpecificationReader(complete_track_params=complete_track_params)
        with pytest.raises(loader.TrackSyntaxError):
            reader("unittest", track_specification, "/mapping")

    def test_parse_valid_without_indices(self):
        track_specification = {
            "description": "description for unit test",
            "data-streams": [{"name": "historical-data-stream"}],
            "corpora": [
                {
                    "name": "test",
                    "base-url": "https://localhost/data",
                    "documents": [
                        {
                            "source-file": "documents-main.json.bz2",
                            "document-count": 10,
                            "compressed-bytes": 100,
                            "uncompressed-bytes": 10000,
                        },
                    ],
                }
            ],
            "schedule": [
                {
                    "clients": 8,
                    "operation": {
                        "name": "index-append",
                        "operation-type": "bulk",
                        "bulk-size": 5000,
                    },
                },
            ],
        }
        reader = loader.TrackSpecificationReader(
            track_params={"number_of_shards": 3},
            source=io.DictStringFileSourceFactory(
                {
                    "/mappings/body.json": [
                        """
                {
                    "settings": {
                        "number_of_shards": {{ number_of_shards }}
                    }
                }
                """
                    ]
                }
            ),
        )
        resulting_track = reader("unittest", track_specification, "/mappings")
        assert resulting_track.name == "unittest"
        assert resulting_track.description == "description for unit test"
        # indices
        assert len(resulting_track.indices) == 0
        # data streams
        assert len(resulting_track.data_streams) == 1
        assert resulting_track.data_streams[0].name == "historical-data-stream"
        # corpora
        assert len(resulting_track.corpora) == 1
        assert resulting_track.corpora[0].name == "test"
        assert len(resulting_track.corpora[0].documents) == 1

        docs_primary = resulting_track.corpora[0].documents[0]
        assert track.Documents.SOURCE_FORMAT_BULK == docs_primary.source_format
        assert docs_primary.document_file == "documents-main.json"
        assert docs_primary.document_archive == "documents-main.json.bz2"
        assert docs_primary.base_url == "https://localhost/data"
        assert not docs_primary.includes_action_and_meta_data
        assert docs_primary.number_of_documents == 10
        assert docs_primary.compressed_size_in_bytes == 100
        assert docs_primary.uncompressed_size_in_bytes == 10000
        assert docs_primary.target_data_stream == "historical-data-stream"
        assert docs_primary.target_type is None
        assert docs_primary.target_index is None

        # challenges
        assert len(resulting_track.challenges) == 1

    def test_parse_valid_track_specification_with_index_template(self):
        track_specification = {
            "description": "description for unit test",
            "templates": [
                {
                    "name": "my-index-template",
                    "index-pattern": "*",
                    "template": "default-template.json",
                },
            ],
            "operations": [],
            "challenges": [],
        }
        complete_track_params = loader.CompleteTrackParams()
        reader = loader.TrackSpecificationReader(
            track_params={"index_pattern": "*"},
            complete_track_params=complete_track_params,
            source=io.DictStringFileSourceFactory(
                {
                    "/mappings/default-template.json": [
                        """
                {
                    "index_patterns": [ "{{index_pattern}}"],
                    "settings": {
                        "number_of_shards": {{ number_of_shards | default(1) }}
                    }
                }
                """
                    ],
                }
            ),
        )
        resulting_track = reader("unittest", track_specification, "/mappings")
        assert complete_track_params.sorted_track_defined_params == ["index_pattern", "number_of_shards"]
        assert resulting_track.name == "unittest"
        assert resulting_track.description == "description for unit test"
        assert len(resulting_track.indices) == 0
        assert len(resulting_track.templates) == 1
        assert resulting_track.templates[0].name == "my-index-template"
        assert resulting_track.templates[0].pattern == "*"
        assert resulting_track.templates[0].content == {
            "index_patterns": ["*"],
            "settings": {
                "number_of_shards": 1,
            },
        }
        assert len(resulting_track.challenges) == 0

    def test_parse_valid_track_specification_with_composable_template(self):
        track_specification = {
            "description": "description for unit test",
            "composable-templates": [
                {
                    "name": "my-index-template",
                    "index-pattern": "*",
                    "template": "default-template.json",
                },
            ],
            "component-templates": [
                {
                    "name": "my-component-template-1",
                    "template": "component-template-1.json",
                },
                {
                    "name": "my-component-template-2",
                    "template": "component-template-2.json",
                },
            ],
            "operations": [],
            "challenges": [],
        }
        complete_track_params = loader.CompleteTrackParams()
        reader = loader.TrackSpecificationReader(
            track_params={"index_pattern": "logs-*", "number_of_replicas": 1},
            complete_track_params=complete_track_params,
            source=io.DictStringFileSourceFactory(
                {
                    "/mappings/default-template.json": [
                        """
                        {
                            "index_patterns": [ "{{index_pattern}}"],
                            "template": {
                                "settings": {
                                    "number_of_shards": {{ number_of_shards | default(1) }}
                                }
                            },
                            "composed_of": ["my-component-template-1", "my-component-template-2"]
                        }
                        """
                    ],
                    "/mappings/component-template-1.json": [
                        """
                        {
                            "template": {
                                "settings": {
                                  "index.number_of_shards": 2
                                }
                            }
                        }
                        """
                    ],
                    "/mappings/component-template-2.json": [
                        """
                        {
                            "template": {
                                "settings": {
                                  {%- if build_flavor == "test" %}
                                  "index.number_of_replicas": {{ number_of_replicas }}
                                  {%- else %}
                                  "index.number_of_replicas": 99
                                  {%- endif %}
                                },
                                "mappings": {
                                  "properties": {
                                    "@timestamp": {
                                      "type": "date"
                                    }
                                  }
                                }
                              }
                        }
                        """
                    ],
                }
            ),
            build_flavor="test",
        )
        resulting_track = reader("unittest", track_specification, "/mappings")
        assert ["index_pattern", "number_of_replicas", "number_of_shards"] == complete_track_params.sorted_track_defined_params
        assert resulting_track.name == "unittest"
        assert resulting_track.description == "description for unit test"
        assert len(resulting_track.indices) == 0
        assert len(resulting_track.composable_templates) == 1
        assert len(resulting_track.component_templates) == 2
        assert resulting_track.composable_templates[0].name == "my-index-template"
        assert resulting_track.composable_templates[0].pattern == "*"
        assert resulting_track.component_templates[0].name == "my-component-template-1"
        assert resulting_track.component_templates[1].name == "my-component-template-2"
        assert resulting_track.composable_templates[0].content == {
            "index_patterns": ["logs-*"],
            "template": {
                "settings": {
                    "number_of_shards": 1,
                },
            },
            "composed_of": [
                "my-component-template-1",
                "my-component-template-2",
            ],
        }
        assert resulting_track.component_templates[0].content == {
            "template": {
                "settings": {
                    "index.number_of_shards": 2,
                },
            },
        }
        assert resulting_track.component_templates[1].content == {
            "template": {
                "settings": {"index.number_of_replicas": 1},
                "mappings": {
                    "properties": {
                        "@timestamp": {"type": "date"},
                    },
                },
            },
        }
        assert len(resulting_track.challenges) == 0

    def test_parse_valid_track_specification_with_composable_template_path(self):
        track_specification = {
            "description": "description for unit test",
            "composable-templates": [
                {
                    "name": "my-index-template",
                    "index-pattern": "*",
                    "template": "default-template.json",
                    "template-path": "index_template",
                },
            ],
            "component-templates": [
                {
                    "name": "my-component-template-1",
                    "template": "component-template-1.json",
                    "template-path": "component_template",
                },
                {
                    "name": "my-component-template-2",
                    "template": "component-template-2.json",
                    "template-path": "component_template",
                },
            ],
            "operations": [],
            "challenges": [],
        }
        complete_track_params = loader.CompleteTrackParams()
        reader = loader.TrackSpecificationReader(
            track_params={"index_pattern": "logs-*", "number_of_replicas": 1},
            complete_track_params=complete_track_params,
            source=io.DictStringFileSourceFactory(
                {
                    "/mappings/default-template.json": [
                        """
                        {
                            "index_template": {
                                "index_patterns": [ "{{index_pattern}}"],
                                "template": {
                                    "settings": {
                                        "number_of_shards": {{ number_of_shards | default(1) }}
                                    }
                                },
                                "composed_of": ["my-component-template-1", "my-component-template-2"]
                            }
                        }
                        """
                    ],
                    "/mappings/component-template-1.json": [
                        """
                        {
                            "component_template": {
                                "template": {
                                    "settings": {
                                      "index.number_of_shards": 2
                                    }
                                }
                            }
                        }
                        """
                    ],
                    "/mappings/component-template-2.json": [
                        """
                        {
                            "component_template": {
                            "template": {
                                "settings": {
                                  "index.number_of_replicas": {{ number_of_replicas }}
                                },
                                "mappings": {
                                  "properties": {
                                    "@timestamp": {
                                      "type": "date"
                                    }
                                  }
                                }
                              }
                            }
                        }
                        """
                    ],
                }
            ),
        )
        resulting_track = reader("unittest", track_specification, "/mappings")
        assert ["index_pattern", "number_of_replicas", "number_of_shards"] == complete_track_params.sorted_track_defined_params
        assert resulting_track.name == "unittest"
        assert resulting_track.description == "description for unit test"
        assert len(resulting_track.indices) == 0
        assert len(resulting_track.composable_templates) == 1
        assert len(resulting_track.component_templates) == 2
        assert resulting_track.composable_templates[0].name == "my-index-template"
        assert resulting_track.composable_templates[0].pattern == "*"
        assert resulting_track.component_templates[0].name == "my-component-template-1"
        assert resulting_track.component_templates[1].name == "my-component-template-2"
        assert resulting_track.composable_templates[0].content == {
            "index_patterns": ["logs-*"],
            "template": {
                "settings": {
                    "number_of_shards": 1,
                },
            },
            "composed_of": [
                "my-component-template-1",
                "my-component-template-2",
            ],
        }
        assert resulting_track.component_templates[0].content == {
            "template": {
                "settings": {
                    "index.number_of_shards": 2,
                },
            },
        }
        assert resulting_track.component_templates[1].content == {
            "template": {
                "settings": {"index.number_of_replicas": 1},
                "mappings": {
                    "properties": {
                        "@timestamp": {"type": "date"},
                    },
                },
            },
        }
        assert len(resulting_track.challenges) == 0

    def test_parse_invalid_track_specification_with_composable_template(self):
        track_specification = {
            "description": "description for unit test",
            "component-templates": [
                {
                    "name": "my-component-template-2",
                },
            ],
            "operations": [],
            "challenges": [],
        }
        complete_track_params = loader.CompleteTrackParams()
        reader = loader.TrackSpecificationReader(
            track_params={"index_pattern": "logs-*", "number_of_replicas": 1}, complete_track_params=complete_track_params
        )
        with pytest.raises(loader.TrackSyntaxError) as exc:
            reader("unittest", track_specification, "/mappings")
        assert exc.value.args[0] == "Track 'unittest' is invalid. Mandatory element 'template' is missing."

    def test_unique_challenge_names(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index"}],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk",
                },
            ],
            "challenges": [
                {
                    "name": "test-challenge",
                    "description": "Some challenge",
                    "default": True,
                    "schedule": [{"operation": "index-append"}],
                },
                {
                    "name": "test-challenge",
                    "description": "Another challenge with the same name",
                    "schedule": [{"operation": "index-append"}],
                },
            ],
        }
        reader = loader.TrackSpecificationReader()
        with pytest.raises(loader.TrackSyntaxError) as exc:
            reader("unittest", track_specification, "/mappings")
        assert exc.value.args[0] == "Track 'unittest' is invalid. Duplicate challenge with name 'test-challenge'."

    def test_not_more_than_one_default_challenge_possible(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                },
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk",
                },
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "description": "Default challenge",
                    "default": True,
                    "schedule": [{"operation": "index-append"}],
                },
                {
                    "name": "another-challenge",
                    "description": "See if we can sneek it in as another default",
                    "default": True,
                    "schedule": [{"operation": "index-append"}],
                },
            ],
        }
        reader = loader.TrackSpecificationReader()
        with pytest.raises(loader.TrackSyntaxError) as exc:
            reader("unittest", track_specification, "/mappings")
        assert exc.value.args[0] == (
            "Track 'unittest' is invalid. Both 'default-challenge' and 'another-challenge' are defined as default challenges. "
            "Please define only one of them as default."
        )

    def test_at_least_one_default_challenge(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                },
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk",
                },
            ],
            "challenges": [
                {
                    "name": "challenge",
                    "schedule": [
                        {"operation": "index-append"},
                    ],
                },
                {
                    "name": "another-challenge",
                    "schedule": [
                        {"operation": "index-append"},
                    ],
                },
            ],
        }
        reader = loader.TrackSpecificationReader()
        with pytest.raises(loader.TrackSyntaxError) as exc:
            reader("unittest", track_specification, "/mappings")
        assert exc.value.args[0] == (
            "Track 'unittest' is invalid. No default challenge specified. Please edit the track and add \"default\": true "
            "to one of the challenges challenge, another-challenge."
        )

    def test_exactly_one_default_challenge(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                },
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk",
                },
            ],
            "challenges": [
                {
                    "name": "challenge",
                    "default": True,
                    "schedule": [
                        {"operation": "index-append"},
                    ],
                },
                {
                    "name": "another-challenge",
                    "schedule": [
                        {"operation": "index-append"},
                    ],
                },
            ],
        }
        reader = loader.TrackSpecificationReader(selected_challenge="another-challenge")
        resulting_track = reader("unittest", track_specification, "/mappings")
        assert len(resulting_track.challenges) == 2
        assert resulting_track.challenges[0].name == "challenge"
        assert resulting_track.challenges[0].default
        assert not resulting_track.challenges[1].default
        assert resulting_track.challenges[1].selected

    def test_selects_sole_challenge_implicitly_as_default(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {"name": "test-index"},
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk",
                },
            ],
            "challenge": {
                "name": "challenge",
                "schedule": [
                    {"operation": "index-append"},
                ],
            },
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings")
        assert len(resulting_track.challenges) == 1
        assert resulting_track.challenges[0].name == "challenge"
        assert resulting_track.challenges[0].default
        assert resulting_track.challenges[0].selected

    def test_auto_generates_challenge_from_schedule(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                },
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk",
                },
            ],
            "schedule": [
                {"operation": "index-append"},
            ],
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings")
        assert len(resulting_track.challenges) == 1
        assert resulting_track.challenges[0].auto_generated
        assert resulting_track.challenges[0].default
        assert resulting_track.challenges[0].selected

    def test_inline_operations(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                },
            ],
            "challenge": {
                "name": "challenge",
                "schedule": [
                    # an operation with parameters still needs to define a type
                    {
                        "operation": {"operation-type": "bulk", "bulk-size": 5000},
                    },
                    # a parameterless operation can just use the operation type as implicit reference to the operation
                    {
                        "operation": "force-merge",
                    },
                ],
            },
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings")

        challenge = resulting_track.challenges[0]
        assert len(challenge.schedule) == 2
        assert track.OperationType.Bulk.to_hyphenated_string() == challenge.schedule[0].operation.type
        assert track.OperationType.ForceMerge.to_hyphenated_string() == challenge.schedule[1].operation.type

    def test_supports_target_throughput(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                },
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk",
                },
            ],
            "challenge": {
                "name": "default-challenge",
                "schedule": [
                    {
                        "operation": "index-append",
                        "target-throughput": 10,
                        "warmup-time-period": 120,
                        "ramp-up-time-period": 60,
                    },
                ],
            },
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings")

        indexing_task = resulting_track.challenges[0].schedule[0]
        assert indexing_task.params["target-throughput"] == 10
        assert indexing_task.warmup_time_period == 120
        assert indexing_task.ramp_up_time_period == 60

    def test_supports_target_interval(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                },
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk",
                },
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "schedule": [
                        {
                            "operation": "index-append",
                            "target-interval": 5,
                        }
                    ],
                }
            ],
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings")
        assert resulting_track.challenges[0].schedule[0].params["target-interval"] == 5

    def test_ramp_up_but_no_warmup(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                },
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk",
                },
            ],
            "challenge": {
                "name": "default-challenge",
                "schedule": [
                    {
                        "operation": "index-append",
                        "target-throughput": 10,
                        "ramp-up-time-period": 60,
                    },
                ],
            },
        }

        reader = loader.TrackSpecificationReader()
        with pytest.raises(loader.TrackSyntaxError) as exc:
            reader("unittest", track_specification, "/mappings")
        assert exc.value.args[0] == (
            "Track 'unittest' is invalid. Operation 'index-append' in challenge 'default-challenge' "
            "defines a ramp-up time period of 60 seconds but no warmup-time-period."
        )

    def test_warmup_shorter_than_ramp_up(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                },
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk",
                },
            ],
            "challenge": {
                "name": "default-challenge",
                "schedule": [
                    {
                        "operation": "index-append",
                        "target-throughput": 10,
                        "ramp-up-time-period": 60,
                        "warmup-time-period": 59,
                    },
                ],
            },
        }

        reader = loader.TrackSpecificationReader()
        with pytest.raises(loader.TrackSyntaxError) as exc:
            reader("unittest", track_specification, "/mappings")
        assert exc.value.args[0] == (
            "Track 'unittest' is invalid. The warmup-time-period of operation 'index-append' in "
            "challenge 'default-challenge' is 59 seconds but must be greater than or equal to the "
            "ramp-up-time-period of 60 seconds."
        )

    def test_parallel_tasks_with_default_values(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index"}],
            "operations": [
                {"name": "index-1", "operation-type": "bulk"},
                {"name": "index-2", "operation-type": "bulk"},
                {"name": "index-3", "operation-type": "bulk"},
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "schedule": [
                        {
                            "parallel": {
                                "warmup-time-period": 2400,
                                "time-period": 36000,
                                "ramp-up-time-period": 300,
                                "tasks": [
                                    {
                                        "operation": "index-1",
                                        "warmup-time-period": 300,
                                        "clients": 2,
                                    },
                                    {
                                        "operation": "index-2",
                                        "time-period": 3600,
                                        "clients": 4,
                                    },
                                    {
                                        "operation": "index-3",
                                        "target-throughput": 10,
                                        "clients": 16,
                                    },
                                ],
                            }
                        }
                    ],
                }
            ],
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings")
        parallel_element = resulting_track.challenges[0].schedule[0]
        parallel_tasks = parallel_element.tasks

        assert parallel_element.clients == 22
        assert len(parallel_tasks) == 3

        assert parallel_tasks[0].operation.name == "index-1"
        assert parallel_tasks[0].ramp_up_time_period == 300
        assert parallel_tasks[0].warmup_time_period == 300
        assert parallel_tasks[0].time_period == 36000
        assert parallel_tasks[0].clients == 2
        assert "target-throughput" not in parallel_tasks[0].params

        assert parallel_tasks[1].operation.name == "index-2"
        assert parallel_tasks[1].ramp_up_time_period == 300
        assert parallel_tasks[1].warmup_time_period == 2400
        assert parallel_tasks[1].time_period == 3600
        assert parallel_tasks[1].clients == 4
        assert "target-throughput" not in parallel_tasks[1].params

        assert parallel_tasks[2].operation.name == "index-3"
        assert parallel_tasks[2].ramp_up_time_period == 300
        assert parallel_tasks[2].warmup_time_period == 2400
        assert parallel_tasks[2].time_period == 36000
        assert parallel_tasks[2].clients == 16
        assert parallel_tasks[2].params["target-throughput"] == 10

    def test_parallel_tasks_with_default_clients_does_not_propagate(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                },
            ],
            "operations": [
                {
                    "name": "index-1",
                    "operation-type": "bulk",
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
                                "clients": 2,
                                "tasks": [
                                    {
                                        "name": "index-1-1",
                                        "operation": "index-1",
                                    },
                                    {
                                        "name": "index-1-2",
                                        "operation": "index-1",
                                    },
                                    {
                                        "name": "index-1-3",
                                        "operation": "index-1",
                                    },
                                    {
                                        "name": "index-1-4",
                                        "operation": "index-1",
                                    },
                                ],
                            }
                        }
                    ],
                }
            ],
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings")
        parallel_element = resulting_track.challenges[0].schedule[0]
        parallel_tasks = parallel_element.tasks

        # we will only have two clients *in total*
        assert parallel_element.clients == 2
        assert len(parallel_tasks) == 4
        for task in parallel_tasks:
            assert task.clients == 1

    def test_parallel_tasks_with_completed_by_set(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                },
            ],
            "operations": [
                {
                    "name": "index-1",
                    "operation-type": "bulk",
                },
                {
                    "name": "index-2",
                    "operation-type": "bulk",
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
                                "completed-by": "index-2",
                                "tasks": [
                                    {"operation": "index-1"},
                                    {"operation": "index-2"},
                                ],
                            }
                        }
                    ],
                }
            ],
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings")
        parallel_element = resulting_track.challenges[0].schedule[0]
        parallel_tasks = parallel_element.tasks

        # we will only have two clients *in total*
        assert parallel_element.clients == 2
        assert len(parallel_tasks) == 2

        assert parallel_tasks[0].operation.name == "index-1"
        assert not parallel_tasks[0].completes_parent

        assert parallel_tasks[1].operation.name == "index-2"
        assert parallel_tasks[1].completes_parent

    def test_parallel_tasks_with_named_task_completed_by_set(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                },
            ],
            "operations": [
                {
                    "name": "index-1",
                    "operation-type": "bulk",
                },
                {
                    "name": "index-2",
                    "operation-type": "bulk",
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
                                "completed-by": "name-index-2",
                                "tasks": [
                                    {
                                        "name": "name-index-1",
                                        "operation": "index-1",
                                    },
                                    {
                                        "name": "name-index-2",
                                        "operation": "index-2",
                                    },
                                ],
                            }
                        }
                    ],
                }
            ],
        }
        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings")
        parallel_element = resulting_track.challenges[0].schedule[0]
        parallel_tasks = parallel_element.tasks

        # we will only have two clients *in total*
        assert parallel_element.clients == 2
        assert len(parallel_tasks) == 2

        assert parallel_tasks[0].operation.name == "index-1"
        assert not parallel_tasks[0].completes_parent

        assert parallel_tasks[1].operation.name == "index-2"
        assert parallel_tasks[1].completes_parent

    def test_parallel_tasks_with_any_task_completed_by_set(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                },
            ],
            "operations": [
                {
                    "name": "index-1",
                    "operation-type": "bulk",
                },
                {
                    "name": "index-2",
                    "operation-type": "bulk",
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
                                "completed-by": "any",
                                "tasks": [
                                    {
                                        "name": "name-index-1",
                                        "operation": "index-1",
                                    },
                                    {
                                        "name": "name-index-2",
                                        "operation": "index-2",
                                    },
                                ],
                            }
                        }
                    ],
                }
            ],
        }

        reader = loader.TrackSpecificationReader()
        resulting_track = reader("unittest", track_specification, "/mappings")
        parallel_element = resulting_track.challenges[0].schedule[0]
        parallel_tasks = parallel_element.tasks

        # we will only have two clients *in total*
        assert parallel_element.clients == 2
        assert len(parallel_tasks) == 2

        assert parallel_tasks[0].operation.name == "index-1"
        assert parallel_tasks[0].any_completes_parent
        assert not parallel_tasks[0].completes_parent

        assert parallel_tasks[1].operation.name == "index-2"
        assert parallel_tasks[1].any_completes_parent
        assert not parallel_tasks[1].completes_parent

    def test_parallel_tasks_with_completed_by_set_no_task_matches(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                },
            ],
            "operations": [
                {
                    "name": "index-1",
                    "operation-type": "bulk",
                },
                {
                    "name": "index-2",
                    "operation-type": "bulk",
                },
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "schedule": [
                        {
                            "parallel": {
                                "completed-by": "non-existing-task",
                                "tasks": [
                                    {"operation": "index-1"},
                                    {"operation": "index-2"},
                                ],
                            },
                        }
                    ],
                }
            ],
        }
        reader = loader.TrackSpecificationReader()

        with pytest.raises(loader.TrackSyntaxError) as exc:
            reader("unittest", track_specification, "/mappings")
        assert exc.value.args[0] == (
            "Track 'unittest' is invalid. 'parallel' element for challenge 'default-challenge' is marked with 'completed-by' "
            "with task name 'non-existing-task' but no task with this name exists."
        )

    def test_parallel_tasks_with_completed_by_set_multiple_tasks_match(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [
                {
                    "name": "test-index",
                },
            ],
            "operations": [
                {
                    "name": "index-1",
                    "operation-type": "bulk",
                },
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "schedule": [
                        {
                            "parallel": {
                                "completed-by": "index-1",
                                "tasks": [
                                    {"operation": "index-1"},
                                    {"operation": "index-1"},
                                ],
                            },
                        },
                    ],
                }
            ],
        }
        reader = loader.TrackSpecificationReader()

        with pytest.raises(loader.TrackSyntaxError) as exc:
            reader("unittest", track_specification, "/mappings")
        assert exc.value.args[0] == (
            "Track 'unittest' is invalid. 'parallel' element for challenge 'default-challenge' contains multiple tasks with "
            "the name 'index-1' marked with 'completed-by' but only task is allowed to match."
        )

    def test_parallel_tasks_ramp_up_cannot_be_overridden(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index"}],
            "operations": [
                {"name": "index-1", "operation-type": "bulk"},
                {"name": "index-2", "operation-type": "bulk"},
            ],
            "challenges": [
                {
                    "name": "default-challenge",
                    "schedule": [
                        {
                            "parallel": {
                                "warmup-time-period": 2400,
                                "time-period": 36000,
                                "ramp-up-time-period": 2400,
                                "tasks": [
                                    {
                                        "name": "name-index-1",
                                        "operation": "index-1",
                                        "ramp-up-time-period": 500,
                                    },
                                    {
                                        "name": "name-index-2",
                                        "operation": "index-2",
                                    },
                                ],
                            }
                        }
                    ],
                }
            ],
        }

        reader = loader.TrackSpecificationReader()
        with pytest.raises(loader.TrackSyntaxError) as exc:
            reader("unittest", track_specification, "/mappings")
        assert exc.value.args[0] == (
            "Track 'unittest' is invalid. task 'name-index-1' specifies a different ramp-up-time-period "
            "than its enclosing 'parallel' element in challenge 'default-challenge'."
        )

    def test_parallel_tasks_ramp_up_only_on_parallel(self):
        track_specification = {
            "description": "description for unit test",
            "indices": [{"name": "test-index"}],
            "operations": [{"name": "index-1", "operation-type": "bulk"}, {"name": "index-2", "operation-type": "bulk"}],
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
                                        "name": "name-index-1",
                                        "operation": "index-1",
                                        "ramp-up-time-period": 500,
                                    },
                                    {"name": "name-index-2", "operation": "index-2"},
                                ],
                            }
                        }
                    ],
                }
            ],
        }

        reader = loader.TrackSpecificationReader()
        with pytest.raises(loader.TrackSyntaxError) as exc:
            reader("unittest", track_specification, "/mappings")
        assert exc.value.args[0] == (
            "Track 'unittest' is invalid. task 'name-index-1' in 'parallel' element of challenge "
            "'default-challenge' specifies a ramp-up-time-period but it is only allowed on the 'parallel' "
            "element."
        )

    def test_propagate_parameters_to_challenge_level(self):
        track_specification = {
            "description": "description for unit test",
            "parameters": {
                "level": "track",
                "value": 7,
            },
            "indices": [
                {
                    "name": "test-index",
                },
            ],
            "operations": [
                {
                    "name": "index-append",
                    "operation-type": "bulk",
                },
            ],
            "challenges": [
                {
                    "name": "challenge",
                    "default": True,
                    "parameters": {
                        "level": "challenge",
                        "another-value": 17,
                    },
                    "schedule": [
                        {
                            "operation": "index-append",
                        },
                    ],
                },
                {
                    "name": "another-challenge",
                    "schedule": [
                        {
                            "operation": "index-append",
                        },
                    ],
                },
            ],
        }
        reader = loader.TrackSpecificationReader(selected_challenge="another-challenge")
        resulting_track = reader("unittest", track_specification, "/mappings")
        assert len(resulting_track.challenges) == 2
        assert resulting_track.challenges[0].name == "challenge"
        assert resulting_track.challenges[0].default
        assert resulting_track.challenges[0].parameters == {
            "level": "challenge",
            "value": 7,
            "another-value": 17,
        }

        assert not resulting_track.challenges[1].default
        assert resulting_track.challenges[1].selected
        assert resulting_track.challenges[1].parameters == {
            "level": "track",
            "value": 7,
        }


class MyMockTrackProcessor(loader.TrackProcessor):
    pass


class TestTrackProcessorRegistry:
    def test_default_track_processors(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "offline.mode", False)
        cfg.add(config.Scope.application, "driver", "serverless.mode", False)
        cfg.add(config.Scope.application, "driver", "serverless.operator", False)
        tpr = loader.TrackProcessorRegistry(cfg)
        expected_defaults = [
            loader.TaskFilterTrackProcessor,
            loader.ServerlessFilterTrackProcessor,
            loader.TestModeTrackProcessor,
            loader.DefaultTrackPreparator,
        ]
        actual_defaults = [proc.__class__ for proc in tpr.processors]
        assert len(expected_defaults) == len(actual_defaults)

    def test_override_default_preparator(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "offline.mode", False)
        cfg.add(config.Scope.application, "driver", "serverless.mode", False)
        cfg.add(config.Scope.application, "driver", "serverless.operator", False)
        tpr = loader.TrackProcessorRegistry(cfg)
        # call this once beforehand to make sure we don't "harden" the default in case calls are made out of order
        tpr.processors  # pylint: disable=pointless-statement
        tpr.register_track_processor(MyMockTrackProcessor())
        expected_processors = [
            loader.TaskFilterTrackProcessor,
            loader.ServerlessFilterTrackProcessor,
            loader.TestModeTrackProcessor,
            MyMockTrackProcessor,
        ]
        actual_processors = [proc.__class__ for proc in tpr.processors]
        assert len(expected_processors) == len(actual_processors)

    def test_allow_to_specify_default_preparator(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "offline.mode", False)
        cfg.add(config.Scope.application, "driver", "serverless.mode", False)
        cfg.add(config.Scope.application, "driver", "serverless.operator", False)
        tpr = loader.TrackProcessorRegistry(cfg)
        tpr.register_track_processor(MyMockTrackProcessor())
        # should be idempotent now that we have a custom config
        tpr.processors  # pylint: disable=pointless-statement
        tpr.register_track_processor(loader.DefaultTrackPreparator())
        expected_processors = [
            loader.TaskFilterTrackProcessor,
            loader.ServerlessFilterTrackProcessor,
            loader.TestModeTrackProcessor,
            MyMockTrackProcessor,
            loader.DefaultTrackPreparator,
        ]
        actual_processors = [proc.__class__ for proc in tpr.processors]
        assert len(expected_processors) == len(actual_processors)
