import os
import errno
import glob
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


def _run(args, fallback=None):
    try:
        lines = subprocess.Popen(args, stdout=subprocess.PIPE).communicate()[0].splitlines()
        return lines[0].decode("utf-8")
    except:
        return fallback


def guess_install_location(binary_name, fallback=None):
    """
    Checks whether a given binary is available on the user's path.

    :param binary_name: The name of the binary, e.g. tail, gradle, mvn.
    :param fallback: A fallback to return if the binary could not be found on the path.
    :return: The full path to the provided binary or the provided fallback.
    """
    return _run(["which", binary_name])


def guess_java_home(major_version=8, fallback=None):
    """
    Tries to find the JDK root directory for the provided version.

    :param major_version: The JDK major version that is expected.
    :param fallback: The fallback if the JDK home could not be found.
    :return: The full path to the JDK root directory or the fallback.
    """
    # Mac OS X
    if major_version < 9:
        java_home = _run(["/usr/libexec/java_home", "-F", "-v", "1.%d" % major_version])
    else:
        java_home = _run(["/usr/libexec/java_home", "-F", "-v", str(major_version)])

    if java_home:
        return java_home
    else:
        return fallback
        #TODO dm: Add better support for other platforms
        # try:
        #     return os.environ["JAVA_HOME"]
        # except KeyError:
        #     return fallback

# try:
    #     return os.environ["JAVA_HOME"]
    # except KeyError:
    #     pass
    # obviously JAVA_HOME is not set, we try a bit harder for our developers on a Mac
    #print("globbing for [/Library/Java/JavaVirtualMachines/jdk1.%s*.jdk]" % major_version)
    #results = glob.glob("/Library/Java/JavaVirtualMachines/jdk1.%s*.jdk" % major_version)
    # don't do magic guesses if there are multiple versions and have the user specify one
    #if results and len(results) == 1:
    #    return results[0] + "/Contents/Home"
    #else:
    #    return fallback
