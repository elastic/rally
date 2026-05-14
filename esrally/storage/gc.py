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
import dataclasses
import logging
import os
import urllib.parse

import google.auth
import google.oauth2.credentials
import requests.adapters
from google.auth.credentials import AnonymousCredentials, Credentials
from google.auth.transport.requests import AuthorizedSession
from typing_extensions import Self

from esrally.storage import GetResponse, Head, StorageConfig, http

LOG = logging.getLogger(__name__)


class GSAdapter(http.HTTPAdapter):
    """It implements support for downloading files from Google storage.

    Supported URLs are:
        - gs://<bucket_name>/<blob_name>/
        - https://storage.cloud.google.com/<bucket_name>/<blob_name>/
        - https://storage.googleapis.com/storage/v1/b/<bucket_name>/o/<blob_name>
    """

    @classmethod
    def match_url(cls, url: str) -> bool:
        try:
            GSAddress.from_url(url)
            return True
        except ValueError:
            return False

    @classmethod
    def session_from_config(cls, cfg: StorageConfig, session: requests.Session | None = None) -> requests.Session:
        if session is None:
            try:
                session = AuthorizedSession(cls.credentials_from_config(cfg))
                LOG.debug("Using authorized session to Google APIs.")
            except Exception as ex:
                LOG.exception("Failed to create authorized session to Google APIs. Error: %s", {ex})
                # HTTPAdapter will create an unauthorized session that should work for public blobs.
        return super().session_from_config(cfg, session)

    CREDENTIAL_SCOPES = ("https://www.googleapis.com/auth/devstorage.read_only",)

    @classmethod
    def credentials_from_config(cls, cfg: StorageConfig) -> Credentials:
        token = (cfg.google_auth_token or "").strip()
        if token:
            try:
                credentials = google.oauth2.credentials.Credentials(token=token, scopes=cls.CREDENTIAL_SCOPES)
            except Exception as ex:
                LOG.exception("Failed to create Google Cloud credentials with token. Error: %s", ex)
            else:
                LOG.info("Using Google APIs credentials with auth token.")
                return credentials

        try:
            credentials, _ = google.auth.default(scopes=cls.CREDENTIAL_SCOPES)
        except Exception as ex:
            LOG.exception("Failed to get default Google cloud credentials. Error: %s", ex)
        else:
            LOG.info("Using default Google APIs credentials.")
            return credentials

        LOG.warning("Using Google APYs anonymous credentials.")
        return AnonymousCredentials()

    def head(self, url: str) -> Head:
        # It sends the request using the http media APIs URL.
        head = super().head(api_url(url))
        head.accept_ranges = True  # It is known it does accept ranges.
        return head

    def get(self, url: str, *, check_head: Head | None = None) -> GetResponse:
        # It sends the request using the http media APIs URL.
        response = super().get(api_url(url), check_head=check_head)
        response.head.accept_ranges = True  # It is known it does accept ranges.
        return response


@dataclasses.dataclass
class GSAddress:

    @classmethod
    def from_url(cls, url: str) -> Self:
        url = url.strip()
        if not url:
            raise ValueError("unspecified remote file url")

        u = urllib.parse.urlparse(url, scheme="gcs")
        hostname: str = u.netloc
        path: str = os.path.normpath(u.path).strip("/")
        bucket_name: str
        blob_name: str
        match u.scheme:
            case "gs":
                bucket_name = hostname
                blob_name = path
            case "https":
                match hostname:
                    case "storage.cloud.google.com":
                        if "/" not in path:
                            raise ValueError(f"unspecified blob name file url: {url}")
                        bucket_name, blob_name = path.split("/", maxsplit=1)
                    case "storage.googleapis.com":
                        if not path.startswith("storage/v1/b/"):
                            raise ValueError(f"unspecified bucket name file url: {url}")
                        # It removes path prefix before the bucket name
                        _, path = path.split("/b/", maxsplit=1)
                        if "/o/" not in path:
                            raise ValueError(f"unspecified blob name file url: {url}")
                        # It separates bucket name from blob name
                        bucket_name, blob_name = path.split("/o/", maxsplit=1)
                    case _:
                        raise ValueError(f"unexpected hostname: {url}")
            case _:
                raise ValueError(f"Unsupported scheme: {u.scheme}")

        bucket_name = urllib.parse.unquote(bucket_name.strip("/"))
        if not bucket_name:
            raise ValueError(f"unspecified bucket name in URL: {url}")
        blob_name = urllib.parse.unquote(blob_name.strip("/"))
        if not blob_name:
            raise ValueError(f"unspecified blob name in URL: {url}")
        return cls(bucket_name=bucket_name, blob_name=blob_name)

    bucket_name: str
    blob_name: str | None = None

    @property
    def api_url(self) -> str:
        if not self.blob_name:
            raise ValueError("blob_name must be set")
        bucket = urllib.parse.quote(self.bucket_name, safe="")
        blob = urllib.parse.quote(self.blob_name, safe="")
        return f"https://storage.googleapis.com/storage/v1/b/{bucket}/o/{blob}?alt=media"


def api_url(url: str) -> str:
    return GSAddress.from_url(url).api_url
