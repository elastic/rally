import os
import shutil
import urllib.request

from esrally import exceptions


def download(url, local_path, expected_size_in_bytes=None):
    """
    Downloads a single file from a URL to the provided local path.

    :param url: The remote URL specifying one file that should be downloaded. May be either a HTTP or HTTPS URL.
    :param local_path: The local file name of the file that should be downloaded.
    :param expected_size_in_bytes: The expected file size in bytes if known. It will be used to verify that all data have been downloaded.
    """
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
