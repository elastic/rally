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

# pylint: disable=consider-using-with

import bz2
import gzip
import logging
import mmap
import os
import shutil
import subprocess
import tarfile
import zipfile
from types import TracebackType
from typing import (
    IO,
    Any,
    AnyStr,
    Callable,
    Collection,
    Generic,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    overload,
)

import zstandard

# This was introduced in Python 3.11 to `typing`; older versions need `typing_extensions`
# but they are treated the same by mypy, so I'm not going to use conditional imports here
from typing_extensions import Self

from esrally.utils import console

SUPPORTED_ARCHIVE_FORMATS = [".zip", ".bz2", ".gz", ".tar", ".tar.gz", ".tgz", ".tar.bz2", ".zst"]


class FileSource(Generic[AnyStr]):
    """
    FileSource is a wrapper around a plain file which simplifies testing of file I/O calls.
    """

    def __init__(self, file_name: str, mode: str, encoding: str = "utf-8"):
        self.file_name = file_name
        self.mode = mode
        self.encoding = encoding
        self.f: Optional[IO[AnyStr]] = None

    def open(self) -> Self:
        self.f = open(self.file_name, mode=self.mode, encoding=self.encoding)
        # allow for chaining
        return self

    def seek(self, offset: int) -> None:
        assert self.f is not None, "File is not open"
        self.f.seek(offset)

    def read(self) -> AnyStr:
        assert self.f is not None, "File is not open"
        return self.f.read()

    def readline(self) -> AnyStr:
        assert self.f is not None, "File is not open"
        return self.f.readline()

    def readlines(self, num_lines: int) -> Sequence[AnyStr]:
        assert self.f is not None, "File is not open"
        lines = []
        f = self.f
        for _ in range(num_lines):
            line = f.readline()
            if len(line) == 0:
                break
            lines.append(line)
        return lines

    def close(self) -> None:
        assert self.f is not None, "File is not open"
        self.f.close()
        self.f = None

    def __enter__(self) -> Self:
        self.open()
        return self

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException], traceback: Optional[TracebackType]
    ) -> Literal[False]:
        self.close()
        return False

    def __str__(self, *args: Collection[Any], **kwargs: Mapping[str, Any]) -> str:
        return self.file_name


class MmapSource:
    """
    MmapSource is a wrapper around a memory-mapped file which simplifies testing of file I/O calls.
    """

    def __init__(self, file_name: str, mode: str, encoding: str = "utf-8"):
        self.file_name = file_name
        self.mode = mode
        self.encoding = encoding
        self.f: Optional[IO[bytes]] = None
        self.mm: Optional[mmap.mmap] = None

    def open(self) -> Self:
        self.f = open(self.file_name, mode="r+b")
        self.mm = mmap.mmap(self.f.fileno(), 0, access=mmap.ACCESS_READ)
        self.mm.madvise(mmap.MADV_SEQUENTIAL)

        # allow for chaining
        return self

    def seek(self, offset: int) -> None:
        assert self.mm is not None, "Source is not open"
        self.mm.seek(offset)

    def read(self) -> bytes:
        assert self.mm is not None, "Source is not open"
        return self.mm.read()

    def readline(self) -> bytes:
        assert self.mm is not None, "Source is not open"
        return self.mm.readline()

    def readlines(self, num_lines: int) -> Sequence[bytes]:
        assert self.mm is not None, "Source is not open"
        lines = []
        mm = self.mm
        for _ in range(num_lines):
            line = mm.readline()
            if line == b"":
                break
            lines.append(line)
        return lines

    def close(self) -> None:
        assert self.mm is not None, "Source is not open"
        self.mm.close()
        self.mm = None
        assert self.f is not None, "File is not open"
        self.f.close()
        self.f = None

    def __enter__(self) -> Self:
        self.open()
        return self

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException], traceback: Optional[TracebackType]
    ) -> Literal[False]:
        self.close()
        return False

    def __str__(self, *args: Collection[Any], **kwargs: Mapping[str, Any]) -> str:
        return self.file_name


class DictStringFileSourceFactory:
    """
    Factory that can create `StringAsFileSource` for tests. Based on the provided dict, it will create a proper `StringAsFileSource`.

    It is intended for scenarios where multiple files may be read by client code.
    """

    def __init__(self, name_to_contents: Mapping[str, Sequence[str]]):
        self.name_to_contents = name_to_contents

    def __call__(self, name: str, mode: str, encoding: str = "utf-8") -> "StringAsFileSource":
        return StringAsFileSource(self.name_to_contents[name], mode, encoding)


class StringAsFileSource:
    """
    Implementation of ``FileSource`` intended for tests. It's kept close to ``FileSource`` to simplify maintenance but it is not meant to
     be used in production code.
    """

    def __init__(self, contents: Sequence[str], mode: str, encoding: str = "utf-8"):
        """
        :param contents: The file contents as an array of strings. Each item in the array should correspond to one line.
        :param mode: The file mode. It is ignored in this implementation but kept to implement the same interface as ``FileSource``.
        :param encoding: The file encoding. It is ignored in this implementation but kept to implement the same interface as ``FileSource``.
        """
        self.contents = contents
        self.current_index = 0
        self.opened = False

    def open(self) -> Self:
        self.opened = True
        return self

    def seek(self, offset: int) -> None:
        self._assert_opened()
        if offset != 0:
            raise AssertionError("StringAsFileSource does not support random seeks")

    def read(self) -> str:
        self._assert_opened()
        return "\n".join(self.contents)

    def readline(self) -> str:
        self._assert_opened()
        if self.current_index >= len(self.contents):
            return ""
        line = self.contents[self.current_index]
        self.current_index += 1
        return line

    def readlines(self, num_lines: int) -> Sequence[str]:
        lines = []
        for _ in range(num_lines):
            line = self.readline()
            if len(line) == 0:
                break
            lines.append(line)
        return lines

    def close(self) -> None:
        self._assert_opened()
        self.contents = []
        self.opened = False

    def _assert_opened(self) -> None:
        assert self.opened

    def __enter__(self) -> Self:
        self.open()
        return self

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException], traceback: Optional[TracebackType]
    ) -> Literal[False]:
        self.close()
        return False

    def __str__(self, *args: Collection[Any], **kwargs: Mapping[str, Any]) -> str:
        return "StringAsFileSource"


class ZstAdapter:
    """
    Adapter class to make the zstandard API work with Rally's decompression abstractions
    """

    def __init__(self, path: str):
        self.fh = open(path, "rb")
        self.dctx = zstandard.ZstdDecompressor()
        self.reader = self.dctx.stream_reader(self.fh)

    def read(self, size: int) -> bytes:
        return self.reader.read(size)

    def close(self) -> None:
        self.reader.close()
        self.fh.close()


def ensure_dir(directory: str, mode: int = 0o777) -> None:
    """
    Ensure that the provided directory and all of its parent directories exist.
    This function is safe to execute on existing directories (no op).

    :param directory: The directory to create (if it does not exist).
    :param mode: The permission flags to use (if it does not exist).
    """
    if directory:
        os.makedirs(directory, mode, exist_ok=True)


def _zipdir(source_directory: str, archive: zipfile.ZipFile) -> None:
    for root, _, files in os.walk(source_directory):
        for file in files:
            archive.write(
                filename=os.path.join(root, file),
                arcname=os.path.relpath(os.path.join(root, file), os.path.join(source_directory, "..")),
            )


def is_archive(name: str) -> bool:
    """
    :param name: File name to check. Can be either just the file name or optionally also an absolute path.
    :return: True iff the given file name is an archive that is also recognized for decompression by Rally.
    """
    _, ext = splitext(name)
    return ext in SUPPORTED_ARCHIVE_FORMATS


def is_executable(name: str) -> bool:
    """
    :param name: File name to check.
    :return: True iff given file name is executable and in PATH, all other cases False.
    """

    return shutil.which(name) is not None


def compress(source_directory: str, archive_name: str) -> None:
    """
    Compress a directory tree.

    :param source_directory: The source directory to compress. Must be readable.
    :param archive_name: The absolute path including the file name of the archive. Must have the extension .zip.
    """
    archive = zipfile.ZipFile(archive_name, "w", zipfile.ZIP_DEFLATED)
    _zipdir(source_directory, archive)


def decompress(zip_name: str, target_directory: str) -> None:
    """
    Decompresses the provided archive to the target directory. The following file extensions are supported:

    * zip
    * bz2
    * gz
    * tar
    * tar.gz
    * tgz
    * tar.bz2
    * zst

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
        decompressor_lib_bz2 = bz2.open
        _do_decompress_manually(target_directory, zip_name, decompressor_args, decompressor_lib_bz2)
    elif extension == ".zst":
        decompressor_args = ["pzstd", "-f", "-d", "-c"]
        decompressor_lib_zst = ZstAdapter
        _do_decompress_manually(target_directory, zip_name, decompressor_args, decompressor_lib_zst)
    elif extension == ".gz":
        decompressor_args = ["pigz", "-d", "-k", "-c"]
        decompressor_lib_gzip = gzip.open
        _do_decompress_manually(target_directory, zip_name, decompressor_args, decompressor_lib_gzip)
    elif extension in [".tar", ".tar.gz", ".tgz", ".tar.bz2"]:
        _do_decompress(target_directory, tarfile.open(zip_name))
    else:
        raise RuntimeError("Unsupported file extension [%s]. Cannot decompress [%s]" % (extension, zip_name))


def _do_decompress_manually(target_directory: str, filename: str, decompressor_args: List[str], decompressor_lib: Callable) -> None:
    decompressor_bin = decompressor_args[0]
    base_path_without_extension = basename(splitext(filename)[0])

    if is_executable(decompressor_bin):
        if _do_decompress_manually_external(target_directory, filename, base_path_without_extension, decompressor_args):
            return
    else:
        logging.getLogger(__name__).warning(
            "%s not found in PATH. Using standard library, decompression will take longer.", decompressor_bin
        )

    _do_decompress_manually_with_lib(target_directory, filename, decompressor_lib(filename))


def _do_decompress_manually_external(
    target_directory: str, filename: str, base_path_without_extension: str, decompressor_args: List[str]
) -> bool:
    with open(os.path.join(target_directory, base_path_without_extension), "wb") as new_file:
        try:
            subprocess.run(decompressor_args + [filename], stdout=new_file, stderr=subprocess.PIPE, check=True)
        except subprocess.CalledProcessError as err:
            logging.getLogger(__name__).warning(
                "Failed to decompress [%s] with [%s]. Error [%s]. Falling back to standard library.", filename, err.cmd, err.stderr
            )
            return False
    return True


def _do_decompress_manually_with_lib(target_directory: str, filename: str, compressed_file: IO[bytes]) -> None:
    path_without_extension = basename(splitext(filename)[0])

    ensure_dir(target_directory)
    try:
        with open(os.path.join(target_directory, path_without_extension), "wb") as new_file:
            for data in iter(lambda: compressed_file.read(100 * 1024), b""):
                new_file.write(data)
    finally:
        compressed_file.close()


def _do_decompress(target_directory: str, compressed_file: Union[zipfile.ZipFile, tarfile.TarFile]) -> None:
    try:
        compressed_file.extractall(path=target_directory)
    except BaseException:
        if isinstance(compressed_file, zipfile.ZipFile):
            raise RuntimeError(
                f"Could not decompress provided archive [{compressed_file.filename}]. Please check if it is a valid zip file."
            )
        if isinstance(compressed_file, tarfile.TarFile):
            raise RuntimeError(f"Could not decompress provided archive [{compressed_file.name!r}]. Please check if it is a valid tar file.")
    finally:
        compressed_file.close()


# just in a dedicated method to ease mocking
def dirname(path: AnyStr) -> AnyStr:
    return os.path.dirname(path)


def basename(path: AnyStr) -> AnyStr:
    return os.path.basename(path)


def exists(path: AnyStr) -> bool:
    return os.path.exists(path)


@overload
def normalize_path(path: str) -> str: ...
@overload
def normalize_path(path: str, cwd: str = ".") -> str: ...
@overload
def normalize_path(path: bytes) -> bytes: ...
@overload
def normalize_path(path: bytes, cwd: bytes = b".") -> bytes: ...
def normalize_path(path, cwd="."):
    # This is a bug in mypy, see https://github.com/python/mypy/issues/3737
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


def escape_path(path: str) -> str:
    """
    Escapes any characters that might be problematic in shell interactions.

    :param path: The original path.
    :return: A potentially modified version of the path with all problematic characters escaped.
    """
    return path.replace("\\", "\\\\")


def splitext(file_name: str) -> Tuple[str, str]:
    if file_name.endswith(".tar.gz"):
        return file_name[0:-7], file_name[-7:]
    elif file_name.endswith(".tar.bz2"):
        return file_name[0:-8], file_name[-8:]
    else:
        return os.path.splitext(file_name)


def has_extension(file_name: str, extension: str) -> bool:
    """
    Checks whether the given file name has the given extension.

    :param file_name: A file name to check (either just the name or an absolute path name).
    :param extension: The extension including the leading dot (i.e. it is ".txt", not "txt").
    :return: True iff the given ``file_name`` has the given ``extension``.
    """
    _, ext = splitext(file_name)
    return ext == extension


class FileOffsetTable:
    """
    The FileOffsetTable represents a persistent mapping from lines in a data file to their offset in bytes in the
    data file. This helps bulk-indexing clients to advance quickly to a certain position in a large data file.
    """

    def __init__(self, data_file_path: str, offset_table_path: str, mode: str):
        """
        Creates a new FileOffsetTable instance. The constructor should not be called directly but instead the
        respective factory methods should be used.

        :param data_file_path: The absolute path to the data file. This file is assumed to exist at this point.
        :param offset_table_path: The absolute path to the corresponding offset table file. Only required to exist
                                  for read operations on the data file.
        :param mode: The mode in which the file offset table should be opened.
        """
        self.data_file_path = data_file_path
        self.offset_table_path = offset_table_path
        self.mode = mode
        self.offset_file: Optional[IO[str]] = None

    def exists(self) -> bool:
        """
        :return: True iff the file offset table already exists.
        """
        return os.path.exists(self.offset_table_path)

    def is_valid(self) -> bool:
        """
        :return: True iff the file offset table exists and it is up-to-date.
        """
        return self.exists() and os.path.getmtime(self.offset_table_path) >= os.path.getmtime(self.data_file_path)

    def __enter__(self) -> Self:
        self.offset_file = open(self.offset_table_path, self.mode)
        return self

    def add_offset(self, line_number: int, offset: int) -> None:
        """
        Adds a new offset mapping to the file offset table. This method has to be called inside a context-manager block.

        :param line_number: A line number to add.
        :param offset: The corresponding offset in bytes.
        """
        assert self.offset_file is not None, "File offset table must be opened in a context manager block."
        print(f"{line_number};{offset}", file=self.offset_file)

    def find_closest_offset(self, target_line_number: int) -> Tuple[int, int]:
        """
        Determines the offset in bytes for the line L in the corresponding data file with the following properties:

        * L <= target_line_number
        * For any line M, where M != L and M <= target_line_number: M > L (i.e. L is the closest match)

        :param target_line_number: A positive number representing a line number in the data file.
        :return: A tuple of file offset in bytes to the line with the closest match and the number of lines that
                 still need to be skipped.
        """
        prior_offset = 0
        prior_remaining_lines = target_line_number

        assert self.offset_file is not None, "File offset table must be opened in a context manager block."
        for line in self.offset_file:
            line_number, offset_in_bytes = (int(i) for i in line.strip().split(";"))
            if line_number <= target_line_number:
                prior_offset = offset_in_bytes
                prior_remaining_lines = target_line_number - line_number
            else:
                break

        return prior_offset, prior_remaining_lines

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException], traceback: Optional[TracebackType]
    ) -> Literal[False]:
        assert self.offset_file is not None, "File offset table must be opened in a context manager block."
        self.offset_file.close()
        self.offset_file = None
        return False

    @classmethod
    def create_for_data_file(cls, data_file_path: str) -> Self:
        """
        Factory method to create a new file offset table.

        :param data_file_path: The absolute path to the data file for which a file offset table should be created.
        """
        return cls(data_file_path, f"{data_file_path}.offset", "wt")

    @classmethod
    def read_for_data_file(cls, data_file_path: str) -> Self:
        """

        Factory method to read from an existing file offset table.

        :param data_file_path: The absolute path to the data file for which the file offset table should be read.
        """
        return cls(data_file_path, f"{data_file_path}.offset", "rt")

    @staticmethod
    def remove(data_file_path: str) -> None:
        """
        Removes a file offset table for the provided data path.

        :param data_file_path: The absolute path to the data file for which the file offset table should be deleted.
        """
        os.remove(f"{data_file_path}.offset")


def prepare_file_offset_table(data_file_path: str) -> Optional[int]:
    """
    Creates a file that contains a mapping from line numbers to file offsets for the provided path. This file is used internally by
    #skip_lines(data_file_path, data_file) to speed up line skipping.

    :param data_file_path: The path to a text file that is readable by this process.
    :return The number of lines read or ``None`` if it did not have to build the file offset table.
    """
    file_offset_table = FileOffsetTable.create_for_data_file(data_file_path)
    if not file_offset_table.is_valid():
        console.info("Preparing file offset table for [%s] ... " % data_file_path, end="", flush=True)
        line_number = 0
        with file_offset_table:
            with open(data_file_path, encoding="utf-8") as data_file:
                while True:
                    line = data_file.readline()
                    if len(line) == 0:
                        break
                    line_number += 1
                    if line_number % 50000 == 0:
                        file_offset_table.add_offset(line_number, data_file.tell())
        console.println("[OK]")
        return line_number
    else:
        return None


def remove_file_offset_table(data_file_path: str) -> None:
    """

    Attempts to remove the file offset table for the provided data path.

    :param data_file_path: The path to a text file that is readable by this process.
    """
    FileOffsetTable.remove(data_file_path)


def skip_lines(data_file_path: str, data_file: IO[AnyStr], number_of_lines_to_skip: int) -> None:
    """
    Skips the first `number_of_lines_to_skip` lines in `data_file` as a side effect.

    :param data_file_path: The full path to the data file.
    :param data_file: The data file. It is assumed that this file is already open for reading and its file pointer is at position zero.
    :param number_of_lines_to_skip: A non-negative number of lines that should be skipped.
    """
    if number_of_lines_to_skip == 0:
        return

    file_offset_table = FileOffsetTable.read_for_data_file(data_file_path)
    # can we fast forward?
    if file_offset_table.exists():
        with file_offset_table:
            offset, remaining_lines = file_offset_table.find_closest_offset(number_of_lines_to_skip)
    else:
        offset = 0
        remaining_lines = number_of_lines_to_skip

    # fast forward to the last known file offset
    data_file.seek(offset)
    # forward the last remaining lines if needed
    if remaining_lines > 0:
        for _ in range(remaining_lines):
            data_file.readline()


def get_size(start_path: str = ".") -> int:
    total_size = 0
    for dirpath, _, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size
