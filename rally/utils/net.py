import os
import shutil
import urllib.request


def download(url, local_path):
    """
    Downloads a single file from a URL to the provided local path.

    :param url: The remote URL specifying one file that should be downloaded. May be either a HTTP or HTTPS URL.
    :param local_path: The local file name of the file that should be downloaded.
    """
    tmp_data_set_path = local_path + ".tmp"
    try:
        with urllib.request.urlopen(url) as response, open(tmp_data_set_path, "wb") as out_file:
            shutil.copyfileobj(response, out_file)
    except:
        if os.path.isfile(tmp_data_set_path):
            os.remove(tmp_data_set_path)
        raise
    else:
        os.rename(tmp_data_set_path, local_path)
