import logging
import os

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


class Progress:
    def __init__(self, msg, accuracy=0):
        from esrally.utils import console
        self.p = console.progress()
        # if we don't show a decimal sign, the maximum width is 3 (max value is 100 (%)). Else its 3 + 1 (for the decimal point)
        # the accuracy that the user requested.
        total_width = 3 if accuracy == 0 else 4 + accuracy
        # sample formatting string: [%5.1f%%] for an accuracy of 1
        self.percent_format = "[%%%d.%df%%%%]" % (total_width, accuracy)
        self.msg = msg

    def __call__(self, bytes_read, bytes_total):
        from esrally.utils import convert
        completed = bytes_read / bytes_total
        total_as_mb = convert.bytes_to_human_string(bytes_total)
        self.p.print("%s (%s total size)" % (self.msg, total_as_mb), self.percent_format % (completed * 100))

    def finish(self):
        self.p.finish()


def download(url, local_path, expected_size_in_bytes=None, progress_indicator=None):
    """
    Downloads a single file from a URL to the provided local path.

    :param url: The remote URL specifying one file that should be downloaded. May be either a HTTP or HTTPS URL.
    :param local_path: The local file name of the file that should be downloaded.
    :param expected_size_in_bytes: The expected file size in bytes if known. It will be used to verify that all data have been downloaded.
    :param progress_indicator A callable that can be use to report progress to the user. It is expected to take two parameters 
    ``bytes_read`` and ``total_bytes``. If not provided, no progress is shown. Note that ``total_bytes`` is derived from 
    the ``Content-Length`` header and not from the parameter ``expected_size_in_bytes``.
    """
    tmp_data_set_path = local_path + ".tmp"
    try:
        with __http().request("GET", url, preload_content=False, retries=10,
                              timeout=urllib3.Timeout(connect=45, read=240)) as r, open(tmp_data_set_path, "wb") as out_file:
            # noinspection PyBroadException
            try:
                size_from_content_header = int(r.getheader("Content-Length"))
            except BaseException:
                size_from_content_header = None

            chunk_size = 16 * 1014
            bytes_read = 0
            while True:
                chunk = r.read(chunk_size)
                if not chunk:
                    break
                out_file.write(chunk)
                bytes_read += len(chunk)
                if progress_indicator and size_from_content_header:
                    progress_indicator(bytes_read, size_from_content_header)
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
        # We connect to Github anyway later on so we use that to avoid touching too much different remote endpoints.
        probing_url = "https://github.com/"
        logger.debug("Checking for internet connection against [%s]" % probing_url)
        # We do a HTTP request here to respect the HTTP proxy setting. If we'd open a plain socket connection we circumvent the
        # proxy and erroneously conclude we don't have an Internet connection.
        response = __http().request("GET", probing_url)
        status = response.status
        logger.debug("Probing result is HTTP status [%s]" % str(status))
        return status == 200
    except BaseException:
        return False


def __http():
    if not __HTTP:
        init()
    return __HTTP
