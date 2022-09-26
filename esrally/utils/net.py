# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import functools
import logging
import os
import socket
import time
import urllib.error
from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse

import certifi
import urllib3

from esrally import exceptions
from esrally.utils import console, convert

_HTTP = None
_HTTPS = None


def __proxy_manager_from_env(env_var, logger):
    proxy_url = os.getenv(env_var.lower()) or os.getenv(env_var.upper())
    if not proxy_url:
        env_var = "all_proxy"
        proxy_url = os.getenv(env_var) or os.getenv(env_var.upper())
    if proxy_url and len(proxy_url) > 0:
        parsed_url = urllib3.util.parse_url(proxy_url)
        logger.info("Connecting via proxy URL [%s] to the Internet (picked up from the environment variable [%s]).", proxy_url, env_var)
        return urllib3.ProxyManager(
            proxy_url,
            cert_reqs="CERT_REQUIRED",
            ca_certs=certifi.where(),
            # appropriate headers will only be set if there is auth info
            proxy_headers=urllib3.make_headers(proxy_basic_auth=parsed_url.auth),
        )
    else:
        logger.info("Connecting directly to the Internet (no proxy support) for [%s].", env_var)
        return urllib3.PoolManager(cert_reqs="CERT_REQUIRED", ca_certs=certifi.where())


def init():
    logger = logging.getLogger(__name__)
    global _HTTP, _HTTPS
    _HTTP = __proxy_manager_from_env("http_proxy", logger)
    _HTTPS = __proxy_manager_from_env("https_proxy", logger)


class Progress:
    def __init__(self, msg, accuracy=0):
        self.p = console.progress()
        # if we don't show a decimal sign, the maximum width is 3 (max value is 100 (%)). Else its 3 + 1 (for the decimal point)
        # the accuracy that the user requested.
        total_width = 3 if accuracy == 0 else 4 + accuracy
        # sample formatting string: [%5.1f%%] for an accuracy of 1
        self.percent_format = "[%%%d.%df%%%%]" % (total_width, accuracy)
        self.msg = msg

    def __call__(self, bytes_read, bytes_total):
        if bytes_total:
            completed = bytes_read / bytes_total
            total_as_mb = convert.bytes_to_human_string(bytes_total)
            self.p.print("%s (%s total size)" % (self.msg, total_as_mb), self.percent_format % (completed * 100))
        else:
            self.p.print(self.msg, ".")

    def finish(self):
        self.p.finish()


def _fake_import_boto3():
    # This function only exists to be mocked in tests to raise an ImportError, in
    # order to simulate the absence of boto3
    pass


def _download_from_s3_bucket(bucket_name, bucket_path, local_path, expected_size_in_bytes=None, progress_indicator=None):
    # pylint: disable=import-outside-toplevel
    # lazily initialize S3 support - it might not be available
    try:
        _fake_import_boto3()
        import boto3.s3.transfer
    except ImportError:
        console.error("S3 support is optional. Install it with `python -m pip install esrally[s3]`")
        raise

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
    bucket.download_file(bucket_path, local_path, Callback=progress_callback, Config=boto3.s3.transfer.TransferConfig(use_threads=False))


def _build_gcs_object_url(bucket_name, bucket_path):
    # / and other special characters must be urlencoded in bucket and object names
    # ref: https://cloud.google.com/storage/docs/request-endpoints#encoding

    return functools.reduce(
        urllib.parse.urljoin,
        [
            "https://storage.googleapis.com/storage/v1/b/",
            f"{quote(bucket_name.strip('/'), safe='')}/",
            "o/",
            f"{quote(bucket_path.strip('/'), safe='')}",
            "?alt=media",
        ],
    )


def _download_from_gcs_bucket(bucket_name, bucket_path, local_path, expected_size_in_bytes=None, progress_indicator=None):
    # pylint: disable=import-outside-toplevel
    # lazily initialize Google Cloud Storage support - we might not need it
    import google.auth
    import google.auth.transport.requests as tr_requests
    import google.oauth2.credentials

    # Using Google Resumable Media as the standard storage library doesn't support progress
    # (https://github.com/googleapis/python-storage/issues/27)
    from google.resumable_media.requests import ChunkedDownload

    ro_scope = "https://www.googleapis.com/auth/devstorage.read_only"

    access_token = os.environ.get("GOOGLE_AUTH_TOKEN")
    if access_token:
        credentials = google.oauth2.credentials.Credentials(token=access_token, scopes=(ro_scope,))
    else:
        # https://google-auth.readthedocs.io/en/latest/user-guide.html
        credentials, _ = google.auth.default(scopes=(ro_scope,))

    transport = tr_requests.AuthorizedSession(credentials)
    chunk_size = 50 * 1024 * 1024  # 50MB

    with open(local_path, "wb") as local_fp:
        media_url = _build_gcs_object_url(bucket_name, bucket_path)
        download = ChunkedDownload(media_url, chunk_size, local_fp)
        # allow us to calculate the total bytes
        download.consume_next_chunk(transport)
        if not expected_size_in_bytes:
            expected_size_in_bytes = download.total_bytes
        while not download.finished:
            if progress_indicator and download.bytes_downloaded and download.total_bytes:
                progress_indicator(download.bytes_downloaded, expected_size_in_bytes)
            download.consume_next_chunk(transport)


def download_from_bucket(blobstore, url, local_path, expected_size_in_bytes=None, progress_indicator=None):
    blob_downloader = {"s3": _download_from_s3_bucket, "gs": _download_from_gcs_bucket}
    logger = logging.getLogger(__name__)

    bucket_and_path = url[5:]  # s3:// or gs:// prefix for now
    bucket_end_index = bucket_and_path.find("/")
    bucket = bucket_and_path[:bucket_end_index]
    # we need to remove the leading "/"
    bucket_path = bucket_and_path[bucket_end_index + 1 :]

    logger.info("Downloading from [%s] bucket [%s] and path [%s] to [%s].", blobstore, bucket, bucket_path, local_path)
    blob_downloader[blobstore](bucket, bucket_path, local_path, expected_size_in_bytes, progress_indicator)

    return expected_size_in_bytes


HTTP_DOWNLOAD_RETRIES = 10


def _download_http(url, local_path, expected_size_in_bytes=None, progress_indicator=None):
    with _request(
        "GET", url, preload_content=False, enforce_content_length=True, retries=10, timeout=urllib3.Timeout(connect=45, read=240)
    ) as r, open(local_path, "wb") as out_file:
        if r.status > 299:
            raise urllib.error.HTTPError(url, r.status, "", None, None)
        try:
            size_from_content_header = int(r.getheader("Content-Length", ""))
            if expected_size_in_bytes is None:
                expected_size_in_bytes = size_from_content_header
        except ValueError:
            size_from_content_header = None

        chunk_size = 2**16
        bytes_read = 0

        for chunk in r.stream(chunk_size):
            out_file.write(chunk)
            bytes_read += len(chunk)
            if progress_indicator and size_from_content_header:
                progress_indicator(bytes_read, size_from_content_header)
        return expected_size_in_bytes


def download_http(url, local_path, expected_size_in_bytes=None, progress_indicator=None, *, sleep=time.sleep):
    logger = logging.getLogger(__name__)
    for i in range(HTTP_DOWNLOAD_RETRIES + 1):
        try:
            return _download_http(url, local_path, expected_size_in_bytes, progress_indicator)
        except urllib3.exceptions.ProtocolError as exc:
            if i == HTTP_DOWNLOAD_RETRIES:
                raise
            logger.warning("Retrying after %s", exc)
            sleep(5)
            continue


def add_url_param_elastic_no_kpi(url):
    scheme = urllib3.util.parse_url(url).scheme
    if scheme.startswith("http"):
        return _add_url_param(url, {"x-elastic-no-kpi": "true"})
    else:
        return url


def _add_url_param(url, params):
    url_parsed = urlparse(url)
    query = parse_qs(url_parsed.query)
    query.update(params)
    return urlunparse(
        (url_parsed.scheme, url_parsed.netloc, url_parsed.path, url_parsed.params, urlencode(query, doseq=True), url_parsed.fragment)
    )


def download(url, local_path, expected_size_in_bytes=None, progress_indicator=None):
    """
    Downloads a single file from a URL to the provided local path.

    :param url: The remote URL specifying one file that should be downloaded. May be either a HTTP, HTTPS, S3 or GS URL.
    :param local_path: The local file name of the file that should be downloaded.
    :param expected_size_in_bytes: The expected file size in bytes if known. It will be used to verify that all data have been downloaded.
    :param progress_indicator A callable that can be use to report progress to the user. It is expected to take two parameters
    ``bytes_read`` and ``total_bytes``. If not provided, no progress is shown. Note that ``total_bytes`` is derived from
    the ``Content-Length`` header and not from the parameter ``expected_size_in_bytes`` for downloads via HTTP(S).
    """
    tmp_data_set_path = local_path + ".tmp"
    try:
        scheme = urllib3.util.parse_url(url).scheme
        if scheme in ["s3", "gs"]:
            expected_size_in_bytes = download_from_bucket(scheme, url, tmp_data_set_path, expected_size_in_bytes, progress_indicator)
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
            raise exceptions.DataError(
                "Download of [%s] is corrupt. Downloaded [%d] bytes but [%d] bytes are expected. Please retry."
                % (local_path, download_size, expected_size_in_bytes)
            )
        os.rename(tmp_data_set_path, local_path)


def retrieve_content_as_string(url):
    with _request("GET", url, timeout=urllib3.Timeout(connect=45, read=240)) as response:
        return response.read().decode("utf-8")


def _request(method, url, **kwargs):
    if not _HTTP or not _HTTPS:
        init()
    parsed_url = urllib3.util.parse_url(url)
    manager = _HTTPS if parsed_url.scheme == "https" else _HTTP
    return manager.request(method, url, **kwargs)


def resolve(hostname_or_ip):
    if hostname_or_ip and hostname_or_ip.startswith("127"):
        return hostname_or_ip

    addrinfo = socket.getaddrinfo(hostname_or_ip, 22, 0, 0, socket.IPPROTO_TCP)
    for family, _, _, _, sockaddr in addrinfo:
        # we're interested in the IPv4 address
        if family == socket.AddressFamily.AF_INET:
            ip, _ = sockaddr
            if ip[:3] != "127":
                return ip
    return None
