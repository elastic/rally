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
import struct
import subprocess
import sys
import tarfile
import zipfile
from collections.abc import Collection, Iterator, Mapping, Sequence
from types import TracebackType
from typing import IO, Any, AnyStr, Callable, Generic, Literal, Optional, overload

import zstandard

# This was introduced in Python 3.11 to `typing`; older versions need `typing_extensions`
# but they are treated the same by mypy, so I'm not going to use conditional imports here
from typing_extensions import Self

from esrally import exceptions
from esrally.utils import console, net

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
        self, exc_type: Optional[type[BaseException]], exc: Optional[BaseException], traceback: Optional[TracebackType]
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
        self.f = open(self.file_name, mode="rb")
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
        self, exc_type: Optional[type[BaseException]], exc: Optional[BaseException], traceback: Optional[TracebackType]
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
        self, exc_type: Optional[type[BaseException]], exc: Optional[BaseException], traceback: Optional[TracebackType]
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
        _do_zip_decompress(target_directory, zipfile.ZipFile(zip_name))
    elif extension == ".bz2":
        decompressor_args = ["pbzip2", "-d", "-k", "-m2000", "-c"]
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
        _do_tar_decompress(target_directory, tarfile.open(zip_name))
    else:
        raise RuntimeError("Unsupported file extension [%s]. Cannot decompress [%s]" % (extension, zip_name))


def _do_decompress_manually(target_directory: str, filename: str, decompressor_args: list[str], decompressor_lib: Callable) -> None:
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
    target_directory: str, filename: str, base_path_without_extension: str, decompressor_args: list[str]
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


def _do_tar_decompress(target_directory: str, compressed_file: tarfile.TarFile) -> None:
    """
    Extract a tar archive into ``target_directory`` and close the handle.

    On Python 3.12+, use ``tarfile.TarFile.extractall`` with ``filter="tar"`` (PEP 706) so extraction
    follows the documented tar safety profile; older interpreters use the legacy default, which does
    not apply PEP 706 member-path filtering.
    """
    try:
        if sys.version_info >= (3, 12):
            compressed_file.extractall(path=target_directory, filter="tar")
        else:
            # ``extractall`` has no ``filter`` argument before Python 3.12. Without PEP 706's "tar"
            # profile, member names can escape ``target_directory`` (e.g. via ``../``), which is
            # path traversal if the archive is not from a trusted source.
            compressed_file.extractall(path=target_directory)
    except Exception:
        raise RuntimeError(f"Could not decompress provided archive [{compressed_file.name!r}]. Please check if it is a valid tar file.")
    finally:
        compressed_file.close()


def _do_zip_decompress(target_directory: str, compressed_file: zipfile.ZipFile) -> None:
    try:
        compressed_file.extractall(path=target_directory)
    except Exception:
        raise RuntimeError(f"Could not decompress provided archive [{compressed_file.filename}]. Please check if it is a valid zip file.")
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


def splitext(file_name: str) -> tuple[str, str]:
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
        :return: True iff the file offset table exists, is up-to-date, and contains well-formed offset data.
        """
        if not self.exists():
            return False
        if os.path.getmtime(self.offset_table_path) < os.path.getmtime(self.data_file_path):
            return False
        return self._has_valid_format()

    def _has_valid_format(self) -> bool:
        valid_lines = 0
        try:
            with open(self.offset_table_path, encoding="utf-8") as f:
                for line in f:
                    parts = line.strip().split(";")
                    if len(parts) != 2:
                        return False
                    int(parts[0])
                    int(parts[1])
                    valid_lines += 1
            if valid_lines == 0:
                return False
            return True
        except (OSError, ValueError):
            return False

    def try_download_from_corpus_location(self, corpus_base_url: str | None) -> bool:
        """
        Attempts to download a pre-computed offset file from the corpus location.

        :param corpus_base_url: The base URL where the corpus data is hosted. If None, no download is attempted.
        :return: True if download was successful, False otherwise.
        """
        if not corpus_base_url:
            # No corpus base URL was provided, so a download cannot be attempted.
            return False

        logger = logging.getLogger(__name__)
        offset_file_name = os.path.basename(f"{self.data_file_path}.offset")
        remote_offset_url = f"{corpus_base_url.rstrip('/')}/{offset_file_name}"

        logger.info("Attempting to download offset file from [%s]", remote_offset_url)

        # Ensure the directory exists
        os.makedirs(os.path.dirname(self.offset_table_path), exist_ok=True)
        try:
            net.download(remote_offset_url, self.offset_table_path)
            logger.info("Successfully downloaded offset file from [%s]", remote_offset_url)
            return True
        except Exception as e:
            logger.debug("Download failed: %s", str(e))
            return False

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

    def find_closest_offset(self, target_line_number: int) -> tuple[int, int]:
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
        self, exc_type: Optional[type[BaseException]], exc: Optional[BaseException], traceback: Optional[TracebackType]
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


def prepare_file_offset_table(data_file_path: str, corpus_base_url: str | None) -> int | None:
    """
    Creates a file that contains a mapping from line numbers to file offsets for the provided path. This file is used internally by
    #skip_lines(data_file_path, data_file) to speed up line skipping.

    :param data_file_path: The path to a text file that is readable by this process.
    :param corpus_base_url: Optional base URL where the corpus data (and potentially offset files) are hosted.
                           If provided, Rally will attempt to download a pre-computed .offset file before creating one locally.
    :return The number of lines read or ``None`` if it did not have to build the file offset table.
    """
    file_offset_table = FileOffsetTable.create_for_data_file(data_file_path)
    if not file_offset_table.is_valid():
        # First, try to download the offset file from the corpus location
        if corpus_base_url:
            console.info("Attempting to download offset file for [%s] from corpus location ... " % data_file_path, end="", flush=True)
            if file_offset_table.try_download_from_corpus_location(corpus_base_url):
                console.println("[DOWNLOADED]")
                # Verify the downloaded file is valid
                if file_offset_table.is_valid():
                    return None
                else:
                    console.println("[INVALID - will create locally]")
            else:
                console.println("[NOT FOUND - will create locally]")

        # Fall back to creating the offset file locally
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


def _convert_lines_batch(args: tuple[list[str], bool]) -> tuple[bytes, int]:
    """
    Worker function for parallel OTLP JSON → binary protobuf conversion.

    Each worker process imports the protobuf bindings lazily on first call (this happens once
    per worker process, since the function is a module-level callable that ``ProcessPoolExecutor``
    pickles by reference). Returns ``(concatenated_records_bytes, record_count)`` for the batch
    in source order.

    Format of returned bytes: a concatenation of length-prefixed records, each ``4 bytes big-endian
    length || payload``. When ``gzip_records`` is True, each payload is gzip-compressed independently
    (so the length prefix is the compressed size). This is exactly the on-disk format the main
    process appends to the corpus file.
    """
    lines, gzip_records = args
    # pylint: disable=import-outside-toplevel,no-name-in-module
    from google.protobuf.json_format import Parse
    from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
        ExportMetricsServiceRequest,
    )

    parts: list[bytes] = []
    for line in lines:
        msg = Parse(line, ExportMetricsServiceRequest())
        payload = msg.SerializeToString()
        if gzip_records:
            # mtime=0 keeps the output byte-deterministic across runs (no timestamp in the gzip header).
            payload = gzip.compress(payload, compresslevel=6, mtime=0)
        parts.append(struct.pack(">I", len(payload)))
        parts.append(payload)
    return b"".join(parts), len(lines)


class OtlpProtobufFile:
    """
    Manages the binary protobuf corpus file derived from an OTLP JSON source.

    On-disk format: sequence of length-prefixed records —
        4-byte big-endian uint32 (payload length) + binary ExportMetricsServiceRequest bytes.

    A companion ``{pb_path}.offset`` file maps record numbers to byte offsets for efficient
    multi-client partitioning, using the same ``record_number;byte_offset`` text format as
    FileOffsetTable. One entry is written every OFFSET_SAMPLING_INTERVAL records.
    """

    OFFSET_SAMPLING_INTERVAL = 1000

    def __init__(self, source_json_path: str, pb_path: str, gzip_records: bool = False):
        self.source_json_path = source_json_path
        self.pb_path = pb_path
        # When True, individual records in the file are stored gzip-compressed. The length prefix
        # is the compressed size. ``read_records`` yields the raw (still-compressed) payload bytes
        # — Rally ships them verbatim to ES with ``Content-Encoding: gzip``, avoiding any
        # decompress/recompress on the hot path.
        self.gzip_records = gzip_records

    def exists(self) -> bool:
        return os.path.exists(self.pb_path) and os.path.getsize(self.pb_path) > 0

    def is_valid(self) -> bool:
        if not self.exists():
            return False
        # if the source JSON is present, the .pb must be newer than it
        if os.path.exists(self.source_json_path):
            if os.path.getmtime(self.pb_path) < os.path.getmtime(self.source_json_path):
                return False
        return True

    def try_download_from_corpus_location(self, corpus_base_url: str | None) -> bool:
        """
        Attempts to download the pre-built .pb file from the corpus URL. Also attempts to
        download the companion .pb.offset file; if that's not available, partitioning will
        still work by scanning the .pb from the start (just slightly slower on startup).

        :return: True if the .pb file was downloaded successfully, False otherwise.
        """
        if not corpus_base_url:
            return False
        logger = logging.getLogger(__name__)
        pb_name = os.path.basename(self.pb_path)
        remote_url = f"{corpus_base_url.rstrip('/')}/{pb_name}"
        logger.info("Attempting to download binary protobuf file from [%s]", remote_url)
        os.makedirs(os.path.dirname(self.pb_path), exist_ok=True)
        try:
            net.download(remote_url, self.pb_path)
            logger.info("Successfully downloaded binary protobuf file from [%s]", remote_url)
        except Exception:
            logger.debug("Could not download binary protobuf file from [%s]", remote_url)
            return False

        # Best-effort: also fetch the offset index. Failure is non-fatal — read_records()
        # falls back to scanning from the start of the .pb if .offset is missing.
        offset_path = self.pb_path + ".offset"
        offset_url = f"{corpus_base_url.rstrip('/')}/{os.path.basename(offset_path)}"
        try:
            net.download(offset_url, offset_path)
            logger.info("Successfully downloaded offset index from [%s]", offset_url)
        except Exception:
            logger.debug("Could not download offset index from [%s] (will scan .pb directly)", offset_url)

        return True

    # batch size for parallel conversion. Smaller batches → less memory per in-flight batch.
    # The actual memory cost per batch is ~5× the raw input size (Python object overhead + parsed
    # protobuf message tree during conversion), so 500 lines × ~50 KB ≈ ~125 MB per in-flight batch.
    _CONVERSION_BATCH_SIZE = 500
    # in-flight queue depth. Just enough to keep workers fed while the main thread writes the next
    # completed batch to disk — adding more buffers grows memory without much throughput benefit.
    _QUEUE_BUFFER = 4

    def create(self, workers: int | None = None) -> int:
        """
        Parse the source OTLP JSON file and write binary protobuf records to the .pb file,
        also writing a companion .offset file for fast multi-client partitioning.

        JSON→protobuf conversion is parallelized across processes since each line is independent.
        Results are gathered in source order so the .pb byte offsets stay correct. Memory usage
        is bounded by limiting the number of in-flight batches — we do NOT buffer the whole input
        file (which is what ``ProcessPoolExecutor.map`` would do, since it eagerly consumes its
        iterable up front).

        :param workers: Number of worker processes for conversion. Defaults to ``os.cpu_count()``.
                        Peak memory ≈ ``workers × 325 MB`` regardless of source file size:
                        ~200 MB per worker process (interpreter + loaded protobuf bindings) plus
                        ~125 MB per in-flight batch (input strings + parsed proto tree + output).
                        Override via ``RALLY_OTLP_CONVERSION_WORKERS`` if you need to cap memory.
        :return: Total number of records written.
        :raises exceptions.SystemSetupError: if opentelemetry-proto is not installed.
        """
        # opentelemetry-proto is an optional dependency, only needed when preparing an OTLP corpus.
        # we probe the import here so we fail fast with a clear message rather than inside the worker.
        # pylint: disable=import-outside-toplevel,unused-import
        try:
            import opentelemetry.proto.collector.metrics.v1.metrics_service_pb2  # noqa: F401
        except ImportError:
            raise exceptions.SystemSetupError(
                "The 'opentelemetry-proto' package is required to pre-process OTLP corpus files. "
                "Install it with: pip install opentelemetry-proto"
            )

        import collections  # pylint: disable=import-outside-toplevel
        import concurrent.futures  # pylint: disable=import-outside-toplevel

        if workers and workers > 0:
            worker_count = workers
        else:
            worker_count = os.cpu_count() or 1
        # Bounded queue depth = workers + small buffer. Pickling each batch costs ~5× the raw input
        # size (input list lives in main + queue + worker simultaneously, plus the parsed protobuf
        # message tree dominates worker memory during JSON→proto conversion). Keeping the queue tight
        # caps peak memory at roughly ``(workers + _QUEUE_BUFFER) × ~125 MB`` plus ~200 MB per worker
        # process for the interpreter and loaded protobuf bindings.
        max_in_flight = worker_count + self._QUEUE_BUFFER

        logger = logging.getLogger(__name__)
        logger.info(
            "Converting OTLP JSON to binary protobuf using %d worker(s) (max in-flight batches: %d).",
            worker_count,
            max_in_flight,
        )

        offset_path = self.pb_path + ".offset"
        record_count = 0
        byte_offset = 0
        batch_index = 0
        # 8 MB output buffer for .pb — much larger than Python's default 8 KB, reduces syscall
        # overhead noticeably for multi-gigabyte writes.
        with (
            open(self.pb_path, "wb", buffering=8 * 1024 * 1024) as dst,
            open(offset_path, "w", encoding="utf-8", buffering=64 * 1024) as off,
            concurrent.futures.ProcessPoolExecutor(max_workers=worker_count) as pool,
        ):
            batch_iter = self._iter_line_batches(self._CONVERSION_BATCH_SIZE)
            in_flight: collections.deque = collections.deque()

            # prime the pipeline with up to max_in_flight batches
            for batch in batch_iter:
                in_flight.append(pool.submit(_convert_lines_batch, (batch, self.gzip_records)))
                if len(in_flight) >= max_in_flight:
                    break

            # consume one result at a time (FIFO preserves source order) and submit a replacement
            while in_flight:
                chunk_bytes, chunk_record_count = in_flight.popleft().result()
                # one offset entry at the start of each batch (BATCH_SIZE == OFFSET_SAMPLING_INTERVAL)
                off.write(f"{record_count};{byte_offset}\n")
                dst.write(chunk_bytes)
                record_count += chunk_record_count
                byte_offset += len(chunk_bytes)
                batch_index += 1
                if batch_index % 50 == 0:
                    logger.info(
                        "OTLP conversion progress: %d records, %d MB written.",
                        record_count,
                        byte_offset // (1024 * 1024),
                    )
                # keep the pipeline full by submitting the next batch (if any)
                try:
                    in_flight.append(pool.submit(_convert_lines_batch, (next(batch_iter), self.gzip_records)))
                except StopIteration:
                    pass

        return record_count

    def _iter_line_batches(self, batch_size: int) -> Iterator[list[str]]:
        """Stream the source JSON file as batches of non-blank lines (each line stripped)."""
        batch: list[str] = []
        with open(self.source_json_path, encoding="utf-8", buffering=8 * 1024 * 1024) as src:
            for line in src:
                line = line.strip()
                if not line:
                    continue
                batch.append(line)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch

    def read_records(self, start_record: int, end_record: int | None = None) -> Iterator[bytes]:
        """
        Generator yielding raw binary payloads from start_record up to (but not including) end_record.
        Uses the companion .offset file to seek efficiently.
        """
        seek_byte, records_to_skip = self._find_offset(start_record)
        with open(self.pb_path, "rb") as f:
            f.seek(seek_byte)
            for _ in range(records_to_skip):
                length_data = f.read(4)
                if len(length_data) < 4:
                    return
                (length,) = struct.unpack(">I", length_data)
                f.seek(length, 1)
            count = 0
            target = None if end_record is None else (end_record - start_record)
            while target is None or count < target:
                length_data = f.read(4)
                if len(length_data) < 4:
                    return
                (length,) = struct.unpack(">I", length_data)
                payload = f.read(length)
                if len(payload) < length:
                    return
                count += 1
                yield payload

    def count_records(self) -> int | None:
        """
        Return the exact number of records in the .pb file. Uses the companion .pb.offset index
        (entries every OFFSET_SAMPLING_INTERVAL records) to jump near the end, then scans only the
        final partial chunk by reading length prefixes — so this is fast even for multi-GB files.

        Side effect: if no .pb.offset is present, generates one while scanning the file. The
        offset file lets subsequent workers (and future runs) seek directly to their partition's
        start record instead of walking from byte 0 — a big win for high-index partitions on
        multi-GB corpora.

        Returns ``None`` if the .pb file is missing.
        """
        if not os.path.exists(self.pb_path):
            return None
        offset_path = self.pb_path + ".offset"
        # if an offset file exists, use it to skip to the last sampled position before scanning the tail
        last_record = 0
        last_byte = 0
        if os.path.exists(offset_path):
            try:
                with open(offset_path, encoding="utf-8") as f:
                    for line in f:
                        parts = line.strip().split(";")
                        if len(parts) != 2:
                            continue
                        try:
                            rec_num, byte_off = int(parts[0]), int(parts[1])
                        except ValueError:
                            continue
                        if rec_num >= last_record:
                            last_record = rec_num
                            last_byte = byte_off
            except OSError:
                pass

        # If no offset file exists, generate one as we scan. Write to a temp file and atomically
        # rename at the end so concurrent workers don't see a half-written file. If another worker
        # beat us to the rename, that's fine — both versions of the file are byte-identical.
        offset_tmp_path: str | None = None
        offset_out: Optional[IO[str]] = None
        if not os.path.exists(offset_path):
            offset_tmp_path = f"{offset_path}.tmp.{os.getpid()}"
            offset_out = open(offset_tmp_path, "w", encoding="utf-8", buffering=64 * 1024)
            logging.getLogger(__name__).info("Generating %s while scanning .pb (one-time cost per machine).", offset_path)

        count = last_record
        try:
            with open(self.pb_path, "rb") as f:
                f.seek(last_byte)
                byte_offset = last_byte
                while True:
                    if offset_out is not None and count % self.OFFSET_SAMPLING_INTERVAL == 0:
                        offset_out.write(f"{count};{byte_offset}\n")
                    header = f.read(4)
                    if len(header) < 4:
                        break
                    (length,) = struct.unpack(">I", header)
                    # skip the payload without reading it into memory
                    f.seek(length, 1)
                    byte_offset += 4 + length
                    count += 1
        except OSError:
            if offset_out is not None and offset_tmp_path is not None:
                offset_out.close()
                try:
                    os.remove(offset_tmp_path)
                except OSError:
                    pass
            return None

        if offset_out is not None and offset_tmp_path is not None:
            offset_out.close()
            try:
                os.replace(offset_tmp_path, offset_path)
            except OSError:
                # best-effort — if we can't rename (e.g. another worker beat us), clean up our temp
                try:
                    os.remove(offset_tmp_path)
                except OSError:
                    pass
        return count

    def _find_offset(self, target_record: int) -> tuple[int, int]:
        """Return (byte_offset, records_still_to_skip) for the sampled position closest to target_record."""
        offset_path = self.pb_path + ".offset"
        if not os.path.exists(offset_path):
            return 0, target_record
        prior_byte = 0
        prior_remaining = target_record
        try:
            with open(offset_path, encoding="utf-8") as f:
                for line in f:
                    parts = line.strip().split(";")
                    if len(parts) != 2:
                        continue
                    rec_num, byte_off = int(parts[0]), int(parts[1])
                    if rec_num <= target_record:
                        prior_byte = byte_off
                        prior_remaining = target_record - rec_num
                    else:
                        break
        except OSError:
            pass
        return prior_byte, prior_remaining

    @classmethod
    def for_source_file(cls, source_json_path: str, gzip_records: bool = False) -> "OtlpProtobufFile":
        if not source_json_path:
            raise ValueError(
                "OtlpProtobufFile.for_source_file got an empty/None source path. "
                "This usually means set_absolute_data_path could not resolve the corpus path: "
                "neither the source .json nor the pre-built .pb was found in any data root. "
                "Check that prepare-track ran successfully and the .pb is on disk where Rally expects it."
            )
        ext = ".pbgz" if gzip_records else ".pb"
        return cls(source_json_path, f"{source_json_path}{ext}", gzip_records=gzip_records)


def prepare_otlp_protobuf_file(source_json_path: str, corpus_base_url: str | None) -> int | None:
    """
    Ensures a binary protobuf (.pb) file exists for the given OTLP JSON corpus.

    Strategy:
    1. If .pb is already valid locally, return None immediately.
    2. Try downloading the .pb from corpus_base_url (avoids downloading the larger JSON source).
    3. If JSON is present locally, convert it to .pb.

    Returns the record count if the .pb was created locally, or None if it already existed or
    was downloaded. Returns None without creating the file if the JSON source is absent and the
    download failed — the caller is responsible for ensuring the JSON is present if needed.
    """
    pb_file = OtlpProtobufFile.for_source_file(source_json_path)
    if pb_file.is_valid():
        return None

    if corpus_base_url:
        console.info(
            "Attempting to download binary protobuf file for [%s] ... " % os.path.basename(source_json_path),
            end="",
            flush=True,
        )
        if pb_file.try_download_from_corpus_location(corpus_base_url) and pb_file.is_valid():
            console.println("[DOWNLOADED]")
            return None
        console.println("[NOT FOUND - will create locally]")

    if not os.path.exists(source_json_path):
        return None

    console.info(
        "Converting OTLP JSON to binary protobuf for [%s] ... " % os.path.basename(source_json_path),
        end="",
        flush=True,
    )
    # honor RALLY_OTLP_CONVERSION_WORKERS for environments where the default (os.cpu_count()) needs
    # tuning. Each worker process consumes ~200 MB for the interpreter + protobuf bindings, plus an
    # in-flight batch of ~125 MB while it's parsing. Reduce on low-RAM machines, leave alone otherwise.
    workers_env = os.environ.get("RALLY_OTLP_CONVERSION_WORKERS")
    workers = int(workers_env) if workers_env and workers_env.isdigit() else None
    record_count = pb_file.create(workers=workers)
    console.println("[OK]")
    return record_count


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
