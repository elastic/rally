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
# pylint: disable=protected-access

import logging
import os
import subprocess
import tempfile
from unittest import mock

from esrally.utils import io


def mock_debian(args, fallback=None):
    if args[0] == "update-alternatives":
        return [
            "/usr/lib/jvm/java-7-openjdk-amd64/jre/bin/java",
            "/usr/lib/jvm/java-7-oracle/jre/bin/java",
            "/usr/lib/jvm/java-8-oracle/jre/bin/java",
            "/usr/lib/jvm/java-9-openjdk-amd64/bin/java",
        ]
    else:
        return fallback


def mock_red_hat(path):
    if path == "/etc/alternatives/java_sdk_1.8.0":
        return "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.91-5.b14.fc23.x86_64"
    else:
        return None


class TestIo:
    def test_normalize_path(self):
        assert io.normalize_path("/already/a/normalized/path") == "/already/a/normalized/path"
        assert io.normalize_path("/not/normalized/path/../") == "/not/normalized"
        assert io.normalize_path("~/Documents/..") == os.path.expanduser("~")

    def test_archive(self):
        assert io.is_archive("/tmp/some-archive.tar.gz")
        assert io.is_archive("/tmp/some-archive.tgz")
        assert io.is_archive("/tmp/some-archive.zst")
        # Rally does not recognize .7z
        assert not io.is_archive("/tmp/some-archive.7z")
        assert not io.is_archive("/tmp/some.log")
        assert not io.is_archive("some.log")

    def test_has_extension(self):
        assert io.has_extension("/tmp/some-archive.tar.gz", ".tar.gz")
        assert not io.has_extension("/tmp/some-archive.tar.gz", ".gz")
        assert io.has_extension("/tmp/text.txt", ".txt")
        # no extension whatsoever
        assert not io.has_extension("/tmp/README", "README")


class TestDecompression:
    def test_decompresses_supported_file_formats(self):
        for ext in io.SUPPORTED_ARCHIVE_FORMATS:
            tmp_dir = tempfile.mkdtemp()
            archive_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resources", f"test.txt{ext}")
            decompressed_path = os.path.join(tmp_dir, "test.txt")

            io.decompress(archive_path, target_directory=tmp_dir)

            assert (
                os.path.exists(decompressed_path) is True
            ), f"Could not decompress [{archive_path}] to [{decompressed_path}] (target file does not exist)"
            assert (
                self.read(decompressed_path) == "Sample text for DecompressionTests\n"
            ), f"Could not decompress [{archive_path}] to [{decompressed_path}] (target file is corrupt)"

    @mock.patch.object(io, "is_executable", return_value=False)
    def test_decompresses_supported_file_formats_with_lib_as_failover(self, mocked_is_executable):
        for ext in io.SUPPORTED_ARCHIVE_FORMATS:
            tmp_dir = tempfile.mkdtemp()
            archive_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resources", f"test.txt{ext}")
            decompressed_path = os.path.join(tmp_dir, "test.txt")

            logger = logging.getLogger("esrally.utils.io")
            with mock.patch.object(logger, "warning") as mocked_console_warn:
                io.decompress(archive_path, target_directory=tmp_dir)

                assert (
                    os.path.exists(decompressed_path) is True
                ), f"Could not decompress [{archive_path}] to [{decompressed_path}] (target file does not exist)"
                assert (
                    self.read(decompressed_path) == "Sample text for DecompressionTests\n"
                ), f"Could not decompress [{archive_path}] to [{decompressed_path}] (target file is corrupt)"

            if ext in ["bz2", "gz", "zst"]:
                assert "not found in PATH. Using standard library, decompression will take longer." in mocked_console_warn.call_args[0][0]

    @mock.patch("subprocess.run")
    def test_decompress_manually_external_fails_if_tool_missing(self, mocked_run):
        base_path_without_extension = "corpus"
        archive_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resources", "test.txt.bz2")
        tmp_dir = tempfile.mkdtemp()
        decompressor_bin = "pbzip2"
        decompress_cmd = f"{decompressor_bin} -d -k -m10000 -c ${archive_path}"
        stderr_msg = "Error details here"
        expected_err = "Failed to decompress [%s] with [%s]. Error [%s]. Falling back to standard library."
        mocked_run.side_effect = subprocess.CalledProcessError(cmd=decompress_cmd, returncode=1, stderr=stderr_msg)

        logger = logging.getLogger("esrally.utils.io")
        with mock.patch.object(logger, "warning") as mocked_warn_logger:
            result = io._do_decompress_manually_external(tmp_dir, archive_path, base_path_without_extension, [decompressor_bin])

        mocked_warn_logger.assert_called_once_with(expected_err, archive_path, decompress_cmd, stderr_msg)
        assert result is False

    def read(self, f):
        with open(f) as content_file:
            return content_file.read()
