import os
import errno
import glob
import subprocess
import bz2
import gzip
import zipfile
import tarfile

from esrally.utils import process


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


def zip(source_directory, zip_name):
    """
    Compress a directory tree.

    :param source_directory: The source directory to compress. Must be readable.
    :param zip_name: The absolute path including the file name of the ZIP archive. Must have the extension .zip.
    """
    archive = zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED)
    _zipdir(source_directory, archive)


def unzip(zip_name, target_directory):
    """
    Decompresses the provided archive to the target directory. The following file extensions are supported:

    * zip: Relies that the 'unzip' tool is available on the path
    * bz2: Can be uncompressed using standard library facilities, so no external tool is required.
    * gz: Can be uncompressed using standard library facilities, so no external tool is required.
    * tar: Can be uncompressed using standard library facilities, so no external tool is required.
    * tar.gz Can be uncompressed using standard library facilities, so no external tool is required.
    * tgz Can be uncompressed using standard library facilities, so no external tool is required.
    * tar.bz2 Can be uncompressed using standard library facilities, so no external tool is required.

    Did not implement LZMA because LZMAFile is not thread-safe.

    The decompression method is chosen based on the file extension.

    :param zip_name: The full path name to the file that should be decompressed.
    :param target_directory: The directory to which files should be decompressed. May or may not exist prior to calling
    this function.
    """
    filename, extension = splitext(zip_name)
    if extension == ".zip":
        if not process.run_subprocess_with_logging("unzip %s -d %s" % (zip_name, target_directory)):
            raise RuntimeError("Could not unzip %s to %s" % (zip_name, target_directory))
    elif extension == ".bz2":
        _do_unzip(target_directory, filename, bz2.BZ2File(zip_name, "rb"))
    elif extension == ".gz":
        _do_unzip(target_directory, filename, gzip.GzipFile(zip_name, "rb"))
    elif extension == ".tar":
        _do_unzip(target_directory, filename, tarfile.TarFile(zip_name, "rb"))
    elif extension == ".tar.gz":
        _do_unzip(target_directory, filename, tarfile.TarFile(zip_name, "r:gz"))
    elif extension == ".tgz":
        _do_unzip(target_directory, filename, tarfile.TarFile(zip_name, "r:gz"))
    elif extension == ".tar.bz2":
        _do_unzip(target_directory, filename, tarfile.TarFile(zip_name, "r:bz2"))

    else:
        raise RuntimeError("Unsupported file extension '%s'. Cannot unzip '%s'" % (extension, zip_name))


def _do_unzip(target_directory, target_filename, compressed_file):
    target_file = os.path.join(target_directory, target_filename)
    try:
        with open(target_file, "wb") as extracted:
            for data in iter(lambda: compressed_file.read(100 * 1024), b''):
                extracted.write(data)
    except BaseException:
        raise RuntimeError("Could not uncompress provided archive. Cannot unzip '%s'" % target_filename)

# just in a dedicated method to ease mocking
def dirname(path):
    return os.path.dirname(path)


# just in a dedicated method to ease mocking
def splitext(file_name):
    return os.path.splitext(file_name)


def get_size(start_path="."):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def guess_install_location(binary_name, fallback=None):
    """
    Checks whether a given binary is available on the user's path.

    :param binary_name: The name of the binary, e.g. tail, gradle, mvn.
    :param fallback: A fallback to return if the binary could not be found on the path.
    :return: The full path to the provided binary or the provided fallback.
    """
    try:
        lines = subprocess.Popen(["which", binary_name], stdout=subprocess.PIPE).communicate()[0].splitlines()
        return lines[0].decode("utf-8")
    except BaseException:
        # could not determine location
        return fallback


def guess_java_home(major_version=8, fallback=None):
    """
    Tries to find the JDK root directory for the provided version.

    :param major_version: The JDK major version that is expected.
    :param fallback: The fallback if the JDK home could not be found.
    :return: The full path to the JDK root directory or the fallback.
    """
    try:
        return os.environ["JAVA_HOME"]
    except KeyError:
        pass
    # obviously JAVA_HOME is not set, we try a bit harder for our developers on a Mac
    results = glob.glob("/Library/Java/JavaVirtualMachines/jdk1.%s*.jdk" % major_version)
    # don't do magic guesses if there are multiple versions and have the user specify one
    if results and len(results) == 1:
        return results[0] + "/Contents/Home"
    else:
        return fallback
