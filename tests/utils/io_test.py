import os
import tempfile
import unittest.mock as mock
from unittest import TestCase

from esrally.utils import io


def mock_debian(args, fallback=None):
    if args[0] == "update-alternatives":
        return [
            "/usr/lib/jvm/java-7-openjdk-amd64/jre/bin/java",
            "/usr/lib/jvm/java-7-oracle/jre/bin/java",
            "/usr/lib/jvm/java-8-oracle/jre/bin/java",
            "/usr/lib/jvm/java-9-openjdk-amd64/bin/java"
        ]
    else:
        return fallback


def mock_red_hat(path):
    if path == "/etc/alternatives/java_sdk_1.8.0":
        return "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.91-5.b14.fc23.x86_64"
    else:
        return None


class IoTests(TestCase):
    def test_guess_java_home_on_mac_os_x(self):
        def runner(return_value):
            return lambda args, fallback=None: [return_value]

        java_home = io.guess_java_home(major_version=8, runner=runner("/Library/Java/JavaVirtualMachines/jdk1.8.0_74.jdk/Contents/Home"))
        self.assertEqual("/Library/Java/JavaVirtualMachines/jdk1.8.0_74.jdk/Contents/Home", java_home)

        java_home = io.guess_java_home(major_version=9, runner=runner("/Library/Java/JavaVirtualMachines/jdk-9.jdk/Contents/Home"))
        self.assertEqual("/Library/Java/JavaVirtualMachines/jdk-9.jdk/Contents/Home", java_home)

    def test_guess_java_home_on_debian(self):
        self.assertEqual("/usr/lib/jvm/java-9-openjdk-amd64", io.guess_java_home(major_version=9, runner=mock_debian))
        self.assertEqual("/usr/lib/jvm/java-8-oracle", io.guess_java_home(major_version=8, runner=mock_debian))
        self.assertEqual("/usr/lib/jvm/java-7-openjdk-amd64", io.guess_java_home(major_version=7, runner=mock_debian))

    @mock.patch("os.path.isdir")
    @mock.patch("os.path.islink")
    def test_guess_java_home_on_redhat(self, islink, isdir):
        def runner(args, fallback=None):
            return ["Unknown option --list"] if args[0] == "update-alternatives" else None

        islink.return_value = False
        isdir.return_value = True

        self.assertEqual("/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.91-5.b14.fc23.x86_64",
                         io.guess_java_home(major_version=8, runner=runner, read_symlink=mock_red_hat))
        # simulate not installed version
        self.assertIsNone(io.guess_java_home(major_version=7, runner=runner, read_symlink=mock_red_hat))

    def test_normalize_path(self):
        self.assertEqual("/already/a/normalized/path", io.normalize_path("/already/a/normalized/path"))
        self.assertEqual("/not/normalized", io.normalize_path("/not/normalized/path/../"))
        self.assertEqual(os.path.expanduser("~"), io.normalize_path("~/Documents/.."))

    def test_archive(self):
        self.assertTrue(io.is_archive("/tmp/some-archive.tar.gz"))
        self.assertTrue(io.is_archive("/tmp/some-archive.tgz"))
        # Rally does not recognize .7z
        self.assertFalse(io.is_archive("/tmp/some-archive.7z"))
        self.assertFalse(io.is_archive("/tmp/some.log"))
        self.assertFalse(io.is_archive("some.log"))

    def test_has_extension(self):
        self.assertTrue(io.has_extension("/tmp/some-archive.tar.gz", ".tar.gz"))
        self.assertFalse(io.has_extension("/tmp/some-archive.tar.gz", ".gz"))
        self.assertTrue(io.has_extension("/tmp/text.txt", ".txt"))
        # no extension whatsoever
        self.assertFalse(io.has_extension("/tmp/README", "README"))


class DecompressionTests(TestCase):
    def test_decompresses_supported_file_formats(self):
        for ext in ["zip", "gz", "bz2", "tgz", "tar.bz2", "tar.gz"]:
            tmp_dir = tempfile.mkdtemp()
            archive_path = "%s/resources/test.txt.%s" % (os.path.dirname(os.path.abspath(__file__)), ext)
            decompressed_path = "%s/test.txt" % tmp_dir

            io.decompress(archive_path, target_directory=tmp_dir)

            self.assertTrue(os.path.exists(decompressed_path), msg="Could not decompress [%s] to [%s] (target file does not exist)" %
                                                                   (archive_path, decompressed_path))
            self.assertEqual("Sample text for DecompressionTests\n", self.read(decompressed_path),
                             msg="Could not decompress [%s] to [%s] (target file is corrupt)" % (archive_path, decompressed_path))

    def read(self, f):
        with open(f, 'r') as content_file:
            return content_file.read()
