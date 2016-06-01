import os
import shutil
import urllib.request

from esrally import exceptions
from esrally.utils import process


def download(url, local_path, expected_size_in_bytes=None):
    """
    Downloads a single file from a URL to the provided local path.

    :param url: The remote URL specifying one file that should be downloaded. May be either a HTTP or HTTPS URL. If s3cmd is set up correctly on the system, S3 URL are also supported.
    :param local_path: The local file name of the file that should be downloaded.
    :param expected_size_in_bytes: The expected file size in bytes if known. It will be used to verify that all data have been downloaded.
    """

    if url.startswith("http"):
        download_via_http(url, local_path, expected_size_in_bytes)
    elif url.startswith("s3"):
        download_via_s3(url, local_path, expected_size_in_bytes)
    else:
        raise exceptions.SystemSetupError(
            "Cannot download data from [%s]. Only http(s) and s3 are supported." % url)


def download_via_http(url, local_path, expected_size_in_bytes=None):
    tmp_data_set_path = local_path + ".tmp"
    try:
        urllib.request.urlretrieve(url, tmp_data_set_path)
        # with urllib.request.urlopen(url) as response, open(tmp_data_set_path, "wb") as out_file:
        #     shutil.copyfileobj(response, out_file)
    except:
        if os.path.isfile(tmp_data_set_path):
            os.remove(tmp_data_set_path)
        raise
    else:
        download_size = os.path.getsize(tmp_data_set_path)
        if expected_size_in_bytes is not None and download_size != expected_size_in_bytes:
            if os.path.isfile(tmp_data_set_path):
                os.remove(tmp_data_set_path)
            raise exceptions.DataError("Download of [%s] is corrupt. Downloaded [%d] bytes but [%d] bytes are expected. Please retry." %
                               (local_path, download_size, expected_size_in_bytes))
        os.rename(tmp_data_set_path, local_path)


def download_via_s3(url, data_set_path, size_in_bytes):
    tmp_data_set_path = data_set_path + ".tmp"
    s3cmd = "s3cmd -v get %s %s" % (url, tmp_data_set_path)
    try:
        success = process.run_subprocess_with_logging(s3cmd)
        # Exit code for s3cmd does not seem to be reliable so we also check the file size although this is rather fragile...
        if not success or (size_in_bytes is not None and os.path.getsize(tmp_data_set_path) != size_in_bytes):
            # cleanup probably corrupt data file...
            if os.path.isfile(tmp_data_set_path):
                os.remove(tmp_data_set_path)
            raise exceptions.SystemSetupError(
                    "Could not get benchmark data from S3: '%s'. Is s3cmd installed and set up properly?" % s3cmd)
    except:
        os.remove(tmp_data_set_path)
        raise
    else:
        os.rename(tmp_data_set_path, data_set_path)
