import logging
import os
import shutil
import socket

import certifi
import urllib3
from esrally import exceptions

__HTTP = None

logger = logging.getLogger("rally.net")


def init():
    global __HTTP
    proxy_url = os.getenv("http_proxy")
    if proxy_url and len(proxy_url) > 0:
        logger.info("Rally connects via proxy URL [%s] to the Internet (picked up from the environment variable [http_proxy])." % proxy_url)
        __HTTP = urllib3.ProxyManager(proxy_url, cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
    else:
        logger.info("Rally connects directly to the Internet (no proxy support).")
        __HTTP = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())


def download(url, local_path, expected_size_in_bytes=None):
    """
    Downloads a single file from a URL to the provided local path.

    :param url: The remote URL specifying one file that should be downloaded. May be either a HTTP or HTTPS URL.
    :param local_path: The local file name of the file that should be downloaded.
    :param expected_size_in_bytes: The expected file size in bytes if known. It will be used to verify that all data have been downloaded.
    """
    tmp_data_set_path = local_path + ".tmp"
    try:
        with __http().request("GET", url, preload_content=False, retries=10,
                              timeout=urllib3.Timeout(connect=45, read=240)) as r, open(tmp_data_set_path, "wb") as out_file:
            shutil.copyfileobj(r, out_file)
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


def retrieve_content_as_string(url):
    with __http().request("GET", url, timeout=urllib3.Timeout(connect=45, read=240)) as response:
        return response.read().decode("utf-8")


def has_internet_connection():
    try:
        # We connect to Github anyway later on so we use that to avoid touching too much different remote endpoints
        socket.create_connection(("www.github.com", 80))
        return True
    except OSError:
        return False


def __http():
    if not __HTTP:
        init()
    return __HTTP
