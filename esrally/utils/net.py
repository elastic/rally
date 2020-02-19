# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging
import os
import urllib.error

import certifi
import urllib3

from esrally import exceptions

__HTTP = None


def init():
    logger = logging.getLogger(__name__)
    global __HTTP
    proxy_url = os.getenv("http_proxy")
    if proxy_url and len(proxy_url) > 0:
        parsed_url = urllib3.util.parse_url(proxy_url)
        logger.info("Connecting via proxy URL [%s] to the Internet (picked up from the env variable [http_proxy]).",
                    proxy_url)
        __HTTP = urllib3.ProxyManager(proxy_url,
                                      cert_reqs='CERT_REQUIRED',
                                      ca_certs=certifi.where(),
                                      # appropriate headers will only be set if there is auth info
                                      proxy_headers=urllib3.make_headers(proxy_basic_auth=parsed_url.auth))
    else:
        logger.info("Connecting directly to the Internet (no proxy support).")
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
        if bytes_total:
            completed = bytes_read / bytes_total
            total_as_mb = convert.bytes_to_human_string(bytes_total)
            self.p.print("%s (%s total size)" % (self.msg, total_as_mb), self.percent_format % (completed * 100))
        else:
            self.p.print(self.msg, ".")

    def finish(self):
        self.p.finish()


# This function is not meant to be called externally and only exists for unit tests
def _download_from_s3_bucket(bucket_name, bucket_path, local_path, expected_size_in_bytes=None, progress_indicator=None):
    # lazily initialize S3 support - we might not need it
    import boto3.s3.transfer

    class S3ProgressAdapter:
        def __init__(self, size, progress):
            self._expected_size_in_bytes = size
            self._progress = progress
            self._bytes_read = 0

        def __call__(self, bytes_amount):
            self._bytes_read += bytes_amount
            self._progress(self._bytes_read, self._expected_size_in_bytes)

    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    if expected_size_in_bytes is None:
        expected_size_in_bytes = bucket.Object(bucket_path).content_length
    progress_callback = S3ProgressAdapter(expected_size_in_bytes, progress_indicator) if progress_indicator else None
    bucket.download_file(bucket_path, local_path,
                         Callback=progress_callback,
                         Config=boto3.s3.transfer.TransferConfig(use_threads=False))


def download_s3(url, local_path, expected_size_in_bytes=None, progress_indicator=None):
    logger = logging.getLogger(__name__)

    bucket_and_path = url[5:]
    bucket_end_index = bucket_and_path.find("/")
    bucket = bucket_and_path[:bucket_end_index]
    # we need to remove the leading "/"
    bucket_path = bucket_and_path[bucket_end_index + 1:]

    logger.info("Downloading from S3 bucket [%s] and path [%s] to [%s].", bucket, bucket_path, local_path)
    _download_from_s3_bucket(bucket, bucket_path, local_path, expected_size_in_bytes, progress_indicator)

    return expected_size_in_bytes


def download_http(url, local_path, expected_size_in_bytes=None, progress_indicator=None):
    with __http().request("GET", url, preload_content=False, retries=10,
                          timeout=urllib3.Timeout(connect=45, read=240)) as r, open(local_path, "wb") as out_file:
        if r.status > 299:
            raise urllib.error.HTTPError(url, r.status, "", None, None)
        # noinspection PyBroadException
        try:
            size_from_content_header = int(r.getheader("Content-Length"))
            if expected_size_in_bytes is None:
                expected_size_in_bytes = size_from_content_header
        except BaseException:
            size_from_content_header = None

        chunk_size = 2 ** 16
        bytes_read = 0

        for chunk in r.stream(chunk_size):
            out_file.write(chunk)
            bytes_read += len(chunk)
            if progress_indicator and size_from_content_header:
                progress_indicator(bytes_read, size_from_content_header)
        return expected_size_in_bytes


def download(url, local_path, expected_size_in_bytes=None, progress_indicator=None):
    """
    Downloads a single file from a URL to the provided local path.

    :param url: The remote URL specifying one file that should be downloaded. May be either a HTTP, HTTPS or S3 URL.
    :param local_path: The local file name of the file that should be downloaded.
    :param expected_size_in_bytes: The expected file size in bytes if known. It will be used to verify that all data have been downloaded.
    :param progress_indicator A callable that can be use to report progress to the user. It is expected to take two parameters
    ``bytes_read`` and ``total_bytes``. If not provided, no progress is shown. Note that ``total_bytes`` is derived from
    the ``Content-Length`` header and not from the parameter ``expected_size_in_bytes`` for downloads via HTTP(S).
    """
    tmp_data_set_path = local_path + ".tmp"
    try:
        if url.startswith("s3"):
            expected_size_in_bytes = download_s3(url, tmp_data_set_path, expected_size_in_bytes, progress_indicator)
        else:
            expected_size_in_bytes = download_http(url, tmp_data_set_path, expected_size_in_bytes, progress_indicator)
    except BaseException:
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


def has_internet_connection(probing_url):
    logger = logging.getLogger(__name__)
    try:
        # We try to connect to Github by default. We use that to avoid touching too much different remote endpoints.
        logger.debug("Checking for internet connection against [%s]", probing_url)
        # We do a HTTP request here to respect the HTTP proxy setting. If we'd open a plain socket connection we circumvent the
        # proxy and erroneously conclude we don't have an Internet connection.
        response = __http().request("GET", probing_url, timeout=2.0)
        status = response.status
        logger.debug("Probing result is HTTP status [%s]", str(status))
        return status == 200
    except BaseException:
        logger.debug("Could not detect a working Internet connection", exc_info=True)
        return False


def __http():
    if not __HTTP:
        init()
    return __HTTP


def resolve(hostname_or_ip):
    if hostname_or_ip and hostname_or_ip.startswith("127"):
        return hostname_or_ip

    import socket
    addrinfo = socket.getaddrinfo(hostname_or_ip, 22, 0, 0, socket.IPPROTO_TCP)
    for family, socktype, proto, canonname, sockaddr in addrinfo:
        # we're interested in the IPv4 address
        if family == socket.AddressFamily.AF_INET:
            ip, _ = sockaddr
            if ip[:3] != "127":
                return ip
    return None
