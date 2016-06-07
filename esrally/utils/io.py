import os
import errno
import re
import subprocess
import bz2
import gzip
import zipfile
import tarfile


def ensure_dir(directory):
    """
    Ensure that the provided directory and all of its parent directories exist.
    This function is safe to execute on existing directories (no op).

    :param directory: The directory to create (if it does not exist).
    """
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
    filename, extension = splitext(zip_name)
    if extension == ".zip":
        _do_decompress(target_directory, zipfile.ZipFile(zip_name))
    elif extension == ".bz2":
        with open(filename, 'wb') as new_file, bz2.BZ2File(zip_name, 'rb') as file:
            for data in iter(lambda: file.read(100 * 1024), b''):
                new_file.write(data)
    elif extension == ".gz":
        _do_decompress(target_directory, gzip.open(zip_name))
    elif extension in [".tar", ".tar.gz", ".tgz", ".tar.bz2"]:
        _do_decompress(target_directory, tarfile.open(zip_name))
    else:
        raise RuntimeError("Unsupported file extension [%s]. Cannot decompress [%s]" % (extension, zip_name))


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


def normalize_path(path):
    """
    Normalizes a path by removing redundant "../" and also expanding the "~" character to the user home directory.
    :param path: A possibly non-normalized path.
    :return: A normalized path.
    """
    return os.path.normpath(os.path.expanduser(path))


def splitext(file_name):
    if file_name.endswith(".tar.gz"):
        return file_name[0:-7], file_name[-7:]
    elif file_name.endswith(".tar.bz2"):
        return file_name[0:-8], file_name[-8:]
    else:
        return os.path.splitext(file_name)


def get_size(start_path="."):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def _run(args, fallback=None, only_first_line=False):
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
        return os.readlink(path)
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
        else:
            # Red Hat based distributions
            #
            # [vagrant@localhost alternatives]$ ls -l /etc/alternatives/jre_1.[789].0
            # lrwxrwxrwx. 1 root root 62 May 20 07:51 /etc/alternatives/jre_1.8.0 -> /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.91-5.b14.fc23.x86_64/jre
            #
            # We could also use the output of "alternatives --display java" on Red Hat but the output is so
            # verbose that it's easier to use the links.
            path = read_symlink("/etc/alternatives/java_sdk_1.%d.0" % major_version)
            if path:
                return path
            else:
                return fallback
