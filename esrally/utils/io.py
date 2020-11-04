# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import bz2
import gzip
import logging
import os
import re
import shutil
import subprocess
import tarfile
import zipfile
from contextlib import suppress

import mmap

from esrally.utils import console


class FileSource:
    """
    FileSource is a wrapper around a plain file which simplifies testing of file I/O calls.
    """
    def __init__(self, file_name, mode, encoding="utf-8"):
        self.file_name = file_name
        self.mode = mode
        self.encoding = encoding
        self.f = None

    def open(self):
        self.f = open(self.file_name, mode=self.mode, encoding=self.encoding)
        # allow for chaining
        return self

    def seek(self, offset):
        self.f.seek(offset)

    def read(self):
        return self.f.read()

    def readline(self):
        return self.f.readline()

    def readlines(self, num_lines):
        lines = []
        f = self.f
        for _ in range(num_lines):
            line = f.readline()
            if len(line) == 0:
                break
            lines.append(line)
        return lines

    def close(self):
        self.f.close()
        self.f = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __str__(self, *args, **kwargs):
        return self.file_name


class MmapSource:
    """
    MmapSource is a wrapper around a memory-mapped file which simplifies testing of file I/O calls.
    """
    def __init__(self, file_name, mode, encoding="utf-8"):
        self.file_name = file_name
        self.mode = mode
        self.encoding = encoding
        self.f = None
        self.mm = None

    def open(self):
        self.f = open(self.file_name, mode="r+b")
        self.mm = mmap.mmap(self.f.fileno(), 0, access=mmap.ACCESS_READ)
        # madvise is available in Python 3.8+
        with suppress(AttributeError):
            self.mm.madvise(mmap.MADV_SEQUENTIAL)

        # allow for chaining
        return self

    def seek(self, offset):
        self.mm.seek(offset)

    def read(self):
        return self.mm.read()

    def readline(self):
        return self.mm.readline()

    def readlines(self, num_lines):
        lines = []
        mm = self.mm
        for _ in range(num_lines):
            line = mm.readline()
            if line == b"":
                break
            lines.append(line)
        return lines

    def close(self):
        self.mm.close()
        self.mm = None
        self.f.close()
        self.f = None

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __str__(self, *args, **kwargs):
        return self.file_name


class DictStringFileSourceFactory:
    """
    Factory that can create `StringAsFileSource` for tests. Based on the provided dict, it will create a proper `StringAsFileSource`.

    It is intended for scenarios where multiple files may be read by client code.
    """
    def __init__(self, name_to_contents):
        self.name_to_contents = name_to_contents

    def __call__(self, name, mode, encoding="utf-8"):
        return StringAsFileSource(self.name_to_contents[name], mode, encoding)


class StringAsFileSource:
    """
    Implementation of ``FileSource`` intended for tests. It's kept close to ``FileSource`` to simplify maintenance but it is not meant to
     be used in production code.
    """
    def __init__(self, contents, mode, encoding="utf-8"):
        """
        :param contents: The file contents as an array of strings. Each item in the array should correspond to one line.
        :param mode: The file mode. It is ignored in this implementation but kept to implement the same interface as ``FileSource``.
        :param encoding: The file encoding. It is ignored in this implementation but kept to implement the same interface as ``FileSource``.
        """
        self.contents = contents
        self.current_index = 0
        self.opened = False

    def open(self):
        self.opened = True
        return self

    def seek(self, offset):
        self._assert_opened()
        if offset != 0:
            raise AssertionError("StringAsFileSource does not support random seeks")

    def read(self):
        self._assert_opened()
        return "\n".join(self.contents)

    def readline(self):
        self._assert_opened()
        if self.current_index >= len(self.contents):
            return ""
        line = self.contents[self.current_index]
        self.current_index += 1
        return line

    def readlines(self, num_lines):
        lines = []
        for _ in range(num_lines):
            line = self.readline()
            if len(line) == 0:
                break
            lines.append(line)
        return lines

    def close(self):
        self._assert_opened()
        self.contents = None
        self.opened = False

    def _assert_opened(self):
        assert self.opened

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __str__(self, *args, **kwargs):
        return "StringAsFileSource"


def ensure_dir(directory, mode=0o777):
    """
    Ensure that the provided directory and all of its parent directories exist.
    This function is safe to execute on existing directories (no op).

    :param directory: The directory to create (if it does not exist).
    :param mode: The permission flags to use (if it does not exist).
    """
    if directory:
        os.makedirs(directory, mode, exist_ok=True)


def _zipdir(source_directory, archive):
    for root, _, files in os.walk(source_directory):
        for file in files:
            archive.write(
                filename=os.path.join(root, file),
                arcname=os.path.relpath(os.path.join(root, file), os.path.join(source_directory, "..")))


def is_archive(name):
    """
    :param name: File name to check. Can be either just the file name or optionally also an absolute path.
    :return: True iff the given file name is an archive that is also recognized for decompression by Rally.
    """
    _, ext = splitext(name)
    return ext in [".zip", ".bz2", ".gz", ".tar", ".tar.gz", ".tgz", ".tar.bz2"]


def is_executable(name):
    """
    :param name: File name to check.
    :return: True iff given file name is executable and in PATH, all other cases False.
    """

    return shutil.which(name) is not None


def compress(source_directory, archive_name):
    """
    Compress a directory tree.

    :param source_directory: The source directory to compress. Must be readable.
    :param archive_name: The absolute path including the file name of the archive. Must have the extension .zip.
    """
    archive = zipfile.ZipFile(archive_name, "w", zipfile.ZIP_DEFLATED)
    _zipdir(source_directory, archive)


def decompress(zip_name, target_directory):
    """
    Decompresses the provided archive to the target directory. The following file extensions are supported:

    * zip
    * bz2
    * gz
    * tar
    * tar.gz
    * tgz
    * tar.bz2

    The decompression method is chosen based on the file extension.

    :param zip_name: The full path name to the file that should be decompressed.
    :param target_directory: The directory to which files should be decompressed. May or may not exist prior to calling
    this function.
    """
    _, extension = splitext(zip_name)
    if extension == ".zip":
        _do_decompress(target_directory, zipfile.ZipFile(zip_name))
    elif extension == ".bz2":
        decompressor_args = ["pbzip2", "-d", "-k", "-m10000", "-c"]
        decompressor_lib = bz2.open
        _do_decompress_manually(target_directory, zip_name, decompressor_args, decompressor_lib)
    elif extension == ".gz":
        decompressor_args = ["pigz", "-d", "-k", "-c"]
        decompressor_lib = gzip.open
        _do_decompress_manually(target_directory, zip_name, decompressor_args, decompressor_lib)
    elif extension in [".tar", ".tar.gz", ".tgz", ".tar.bz2"]:
        _do_decompress(target_directory, tarfile.open(zip_name))
    else:
        raise RuntimeError("Unsupported file extension [%s]. Cannot decompress [%s]" % (extension, zip_name))


def _do_decompress_manually(target_directory, filename, decompressor_args, decompressor_lib):
    decompressor_bin = decompressor_args[0]
    base_path_without_extension = basename(splitext(filename)[0])

    if is_executable(decompressor_bin):
        if _do_decompress_manually_external(target_directory, filename, base_path_without_extension, decompressor_args):
            return
    else:
        logging.getLogger(__name__).warning("%s not found in PATH. Using standard library, decompression will take longer.",
                                            decompressor_bin)

    _do_decompress_manually_with_lib(target_directory, filename, decompressor_lib(filename))


def _do_decompress_manually_external(target_directory, filename, base_path_without_extension, decompressor_args):
    with open(os.path.join(target_directory, base_path_without_extension), "wb") as new_file:
        try:
            subprocess.run(decompressor_args + [filename], stdout=new_file, stderr=subprocess.PIPE, check=True)
        except subprocess.CalledProcessError as err:
            logging.getLogger(__name__).warning("Failed to decompress [%s] with [%s]. Error [%s]. Falling back to standard library.",
                                                filename, err.cmd, err.stderr)
            return False
    return True


def _do_decompress_manually_with_lib(target_directory, filename, compressed_file):
    path_without_extension = basename(splitext(filename)[0])

    ensure_dir(target_directory)
    try:
        with open(os.path.join(target_directory, path_without_extension), "wb") as new_file:
            for data in iter(lambda: compressed_file.read(100 * 1024), b""):
                new_file.write(data)
    finally:
        compressed_file.close()


def _do_decompress(target_directory, compressed_file):
    try:
        compressed_file.extractall(path=target_directory)
    except BaseException:
        raise RuntimeError("Could not decompress provided archive [%s]" % compressed_file.filename)
    finally:
        compressed_file.close()


# just in a dedicated method to ease mocking
def dirname(path):
    return os.path.dirname(path)


def basename(path):
    return os.path.basename(path)


def exists(path):
    return os.path.exists(path)


def normalize_path(path, cwd="."):
    """
    Normalizes a path by removing redundant "../" and also expanding the "~" character to the user home directory.
    :param path: A possibly non-normalized path.
    :param cwd: The current working directory. "." by default.
    :return: A normalized path.
    """
    normalized = os.path.normpath(os.path.expanduser(path))
    # user specified only a file name? -> treat as relative to the current directory
    if dirname(normalized) == "":
        return os.path.join(cwd, normalized)
    else:
        return normalized


def escape_path(path):
    """
    Escapes any characters that might be problematic in shell interactions.

    :param path: The original path.
    :return: A potentially modified version of the path with all problematic characters escaped.
    """
    return path.replace("\\", "\\\\")


def splitext(file_name):
    if file_name.endswith(".tar.gz"):
        return file_name[0:-7], file_name[-7:]
    elif file_name.endswith(".tar.bz2"):
        return file_name[0:-8], file_name[-8:]
    else:
        return os.path.splitext(file_name)


def has_extension(file_name, extension):
    """
    Checks whether the given file name has the given extension.

    :param file_name: A file name to check (either just the name or an absolute path name).
    :param extension: The extension including the leading dot (i.e. it is ".txt", not "txt").
    :return: True iff the given ``file_name`` has the given ``extension``.
    """
    _, ext = splitext(file_name)
    return ext == extension


def prepare_file_offset_table(data_file_path):
    """
    Creates a file that contains a mapping from line numbers to file offsets for the provided path. This file is used internally by
    #skip_lines(data_file_path, data_file) to speed up line skipping.

    :param data_file_path: The path to a text file that is readable by this process.
    :return The number of lines read or ``None`` if it did not have to build the file offset table.
    """
    offset_file_path = "%s.offset" % data_file_path
    # recreate only if necessary as this can be time-consuming
    if not os.path.exists(offset_file_path) or os.path.getmtime(offset_file_path) < os.path.getmtime(data_file_path):
        console.info("Preparing file offset table for [%s] ... " % data_file_path, end="", flush=True)
        line_number = 0
        with open(offset_file_path, mode="wt", encoding="utf-8") as offset_file:
            with open(data_file_path, mode="rt", encoding="utf-8") as data_file:
                while True:
                    line = data_file.readline()
                    if len(line) == 0:
                        break
                    line_number += 1
                    if line_number % 50000 == 0:
                        print("%d;%d" % (line_number, data_file.tell()), file=offset_file)
        console.println("[OK]")
        return line_number
    else:
        return None


def remove_file_offset_table(data_file_path):
    """

    Attempts to remove the file offset table for the provided data path.

    :param data_file_path: The path to a text file that is readable by this process.
    """
    offset_file_path = "%s.offset" % data_file_path
    os.remove(offset_file_path)


def skip_lines(data_file_path, data_file, number_of_lines_to_skip):
    """
    Skips the first `number_of_lines_to_skip` lines in `data_file` as a side effect.

    :param data_file_path: The full path to the data file.
    :param data_file: The data file. It is assumed that this file is already open for reading and its file pointer is at position zero.
    :param number_of_lines_to_skip: A non-negative number of lines that should be skipped.
    """
    if number_of_lines_to_skip == 0:
        return

    offset_file_path = "%s.offset" % data_file_path
    offset = 0
    remaining_lines = number_of_lines_to_skip
    # can we fast forward?
    if os.path.exists(offset_file_path):
        with open(offset_file_path, mode="rt", encoding="utf-8") as offsets:
            for line in offsets:
                line_number, offset_in_bytes = [int(i) for i in line.strip().split(";")]
                if line_number <= number_of_lines_to_skip:
                    offset = offset_in_bytes
                    remaining_lines = number_of_lines_to_skip - line_number
                else:
                    break
    # fast forward to the last known file offset
    data_file.seek(offset)
    # forward the last remaining lines if needed
    if remaining_lines > 0:
        for line in range(remaining_lines):
            data_file.readline()


def get_size(start_path="."):
    total_size = 0
    for dirpath, _, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def _run(args, fallback=None, only_first_line=False):
    # noinspection PyBroadException
    try:
        lines = subprocess.Popen(args, stdout=subprocess.PIPE).communicate()[0].splitlines()
        result_lines = [line.decode("utf-8") for line in lines]
        if only_first_line:
            return result_lines[0]
        else:
            return result_lines
    # pylint: disable=W0702
    except:
        return fallback


def _read_symlink(path):
    try:
        return os.path.realpath(path)
    except FileNotFoundError:
        return None


def guess_install_location(binary_name, fallback=None):
    """
    Checks whether a given binary is available on the user's path.

    :param binary_name: The name of the binary, e.g. tail, gradle, mvn.
    :param fallback: A fallback to return if the binary could not be found on the path.
    :return: The full path to the provided binary or the provided fallback.
    """
    return _run(["which", binary_name], fallback=fallback, only_first_line=True)


def guess_java_home(major_version=8, fallback=None, runner=_run, read_symlink=_read_symlink):
    """
    Tries to find the JDK root directory for the provided version.

    :param major_version: The JDK major version that is expected.
    :param fallback: The fallback if the JDK home could not be found.
    :return: The full path to the JDK root directory or the fallback.
    """
    # Mac OS X
    if major_version < 9:
        java_home = runner(["/usr/libexec/java_home", "-F", "-v", "1.%d" % major_version])
    else:
        java_home = runner(["/usr/libexec/java_home", "-F", "-v", str(major_version)])

    if java_home:
        return java_home[0]
    else:
        # Debian based distributions:
        #
        # update-alternatives --list java
        # /usr/lib/jvm/java-7-openjdk-amd64/jre/bin/java
        # /usr/lib/jvm/java-7-oracle/jre/bin/java
        # /usr/lib/jvm/java-8-oracle/jre/bin/java
        # /usr/lib/jvm/java-9-openjdk-amd64/bin/java
        java_home = runner(["update-alternatives", "--list", "java"])
        if java_home:
            debian_jdk_pattern = re.compile(r"(/.*/(java-%d)[^/]*)/(jre/)?bin/java" % major_version)
            for j in java_home:
                m = debian_jdk_pattern.match(j)
                if m:
                    return m.group(1)
        # pylint: disable=line-too-long
        # Red Hat based distributions
        #
        # ls -l /etc/alternatives/jre_1.[789].0
        # lrwxrwxrwx. 1 root root 63 Sep 10 13:57 /etc/alternatives/jre_1.8.0 -> /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.144-5.b01.fc25.x86_64/jre
        # lrwxrwxrwx. 1 root root 51 Sep 13 15:04 /etc/alternatives/jre_1.9.0 -> /usr/lib/jvm/java-9-openjdk-9.0.0.163-1.fc25.x86_64        #
        #
        # We could also use the output of "alternatives --display java" on Red Hat but the output is so
        # verbose that it's easier to use the links.
        path = read_symlink("/etc/alternatives/java_sdk_1.%d.0" % major_version)
        # return path if and only if it is a proper directory
        if path and os.path.isdir(path) and not os.path.islink(path):
            return path
        else:
            return fallback
