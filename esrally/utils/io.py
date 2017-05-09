import os
import errno
import re
import subprocess
import bz2
import gzip
import zipfile
import tarfile
import logging

from esrally.utils import console

logger = logging.getLogger("rally.utils.io")


class FileSource:
    """
    FileSource is a wrapper around a plain file which simplifies testing of file I/O calls.
    """
    def __init__(self, file_name, mode):
        self.file_name = file_name
        self.mode = mode
        self.f = None

    def open(self):
        self.f = open(self.file_name, self.mode)
        # allow for chaining
        return self

    def seek(self, offset):
        self.f.seek(offset)

    def read(self):
        return self.f.read()

    def readline(self):
        return self.f.readline()

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


class StringAsFileSource:
    """
    Implementation of ``FileSource`` intended for tests. It's kept close to ``FileSource`` to simplify maintenance but it is not meant to
     be used in production code.
    """
    def __init__(self, contents, mode):
        """
        :param contents: The file contents as an array of strings. Each item in the array should correspond to one line.
        :param mode: The file mode. It is ignored in this implementation but kept to implement the same interface as ``FileSource``.
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


def ensure_dir(directory):
    """
    Ensure that the provided directory and all of its parent directories exist.
    This function is safe to execute on existing directories (no op).

    :param directory: The directory to create (if it does not exist).
    """
    if directory:
        try:
            # avoid a race condition by trying to create the checkout directory
            os.makedirs(directory)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise


def _zipdir(source_directory, archive):
    for root, dirs, files in os.walk(source_directory):
        for file in files:
            archive.write(
                filename=os.path.join(root, file),
                arcname=os.path.relpath(os.path.join(root, file), os.path.join(source_directory, "..")))


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
    path_without_extension, extension = splitext(zip_name)
    filename = basename(path_without_extension)
    if extension == ".zip":
        _do_decompress(target_directory, zipfile.ZipFile(zip_name))
    elif extension == ".bz2":
        _do_decompress_manually(target_directory, filename, bz2.open(zip_name))
    elif extension == ".gz":
        _do_decompress_manually(target_directory, filename, gzip.open(zip_name))
    elif extension in [".tar", ".tar.gz", ".tgz", ".tar.bz2"]:
        _do_decompress(target_directory, tarfile.open(zip_name))
    else:
        raise RuntimeError("Unsupported file extension [%s]. Cannot decompress [%s]" % (extension, zip_name))


def _do_decompress_manually(target_directory, filename, compressed_file):
    ensure_dir(target_directory)
    try:
        with open("%s/%s" % (target_directory, filename), 'wb') as new_file:
            for data in iter(lambda: compressed_file.read(100 * 1024), b''):
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
        return "%s/%s" % (cwd, normalized)
    else:
        return normalized


def splitext(file_name):
    if file_name.endswith(".tar.gz"):
        return file_name[0:-7], file_name[-7:]
    elif file_name.endswith(".tar.bz2"):
        return file_name[0:-8], file_name[-8:]
    else:
        return os.path.splitext(file_name)


def prepare_file_offset_table(data_file_path):
    """
    Creates a file that contains a mapping from line numbers to file offsets for the provided path. This file is used internally by
    #skip_lines(data_file_path, data_file) to speed up line skipping.

    :param data_file_path: The path to a text file that is readable by this process.
    """
    offset_file_path = "%s.offset" % data_file_path
    # recreate only if necessary as this can be time-consuming
    if not os.path.exists(offset_file_path) or os.path.getmtime(offset_file_path) < os.path.getmtime(data_file_path):
        console.info("Preparing file offset table for [%s] ... " % data_file_path, end="", flush=True, logger=logger)
        line_number = 0
        with open(offset_file_path, mode="w") as offset_file:
            with open(data_file_path, mode="rt") as data_file:
                while True:
                    line = data_file.readline()
                    if len(line) == 0:
                        break
                    line_number += 1
                    if line_number % 50000 == 0:
                        print("%d;%d" % (line_number, data_file.tell()), file=offset_file)
        console.println("[OK]")
    else:
        logger.info("Skipping creation of file offset table at [%s] as it is still valid." % offset_file_path)


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
        with open(offset_file_path) as offsets:
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
    for dirpath, dirnames, filenames in os.walk(start_path):
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
        java_home = runner(["update-alternatives", "--list", "java"])
        if java_home:
            debian_jdk_pattern = re.compile(r"/.*/(java-%d).*/jre/bin/java" % major_version)
            for j in java_home:
                if debian_jdk_pattern.match(j):
                    return j[:-len("/jre/bin/java")]
        # Red Hat based distributions
        #
        # ls -l /etc/alternatives/jre_1.[789].0
        # lrwxrwxrwx. 1 root root 62 May 20 07:51 /etc/alternatives/jre_1.8.0 -> /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.91-5.b14.fc23.x86_64/jre
        #
        # We could also use the output of "alternatives --display java" on Red Hat but the output is so
        # verbose that it's easier to use the links.
        path = read_symlink("/etc/alternatives/java_sdk_1.%d.0" % major_version)
        # return path if and only if it is a proper directory
        if path and os.path.isdir(path) and not os.path.islink(path):
            return path
        else:
            return fallback
