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
from __future__ import annotations

from collections.abc import Iterator
from xml.etree import ElementTree

import requests

from esrally.storage._adapter import Head
from esrally.storage._http import HTTPAdapter

TRACKS_REPOSITORY_URL = "https://rally-tracks.elastic.co/"


class TracksRepositoryAdapter(HTTPAdapter):

    @classmethod
    def match_url(cls, url: str) -> str:
        if url.startswith(TRACKS_REPOSITORY_URL):
            return url
        raise NotImplementedError

    def list(self, url: str) -> Iterator[Head]:
        encoding = "utf-8"
        with requests.get(url, timeout=60.0) as res:
            res.raise_for_status()
            content_type = res.headers.get("content-type", "").replace(" ", "")
            if ";" in content_type:
                content_type, *others = content_type.split(";")
                if content_type != "application/xml":
                    raise ValueError(f"Invalid content type: '{content_type}' != 'application/xml'")
                for other in others:
                    if other.startswith("charset="):
                        encoding = other[len("charset=") :].lower()
            content = res.content.decode(encoding)

        base_url = url.rstrip("/") + "/"
        root = ElementTree.fromstring(content)
        for node in root.iter("{http://doc.s3.amazonaws.com/2006-03-01}Contents"):
            key = node.find("{http://doc.s3.amazonaws.com/2006-03-01}Key")
            if key is None or not key.text or key.text.endswith("/"):
                # Skip invalid nodes or directories
                continue

            content_length = None
            size = node.find("{http://doc.s3.amazonaws.com/2006-03-01}Size")
            if size is not None and size.text:
                content_length = int(size.text)
                if not content_length:
                    continue
            yield Head.create(url=base_url + key.text, content_length=content_length)
