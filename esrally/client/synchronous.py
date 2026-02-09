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

from collections.abc import Mapping
from typing import Any

from elastic_transport import ApiResponse
from elasticsearch import Elasticsearch

from esrally.client import common
from esrally.utils import versions


class RallySyncElasticsearch(Elasticsearch):
    def __init__(self, hosts: Any = None, *, distribution_version: str | None = None, distribution_flavor: str | None = None, **kwargs):
        super().__init__(hosts, **kwargs)
        self.distribution_version = distribution_version
        self.distribution_flavor = distribution_flavor

    @property
    def is_serverless(self):
        return versions.is_serverless(self.distribution_flavor)

    def options(self, *args, **kwargs):
        new_self = super().options(*args, **kwargs)
        new_self.distribution_version = self.distribution_version
        new_self.distribution_flavor = self.distribution_flavor
        return new_self

    def perform_request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        body: Any = None,
        endpoint_id: str | None = None,
        path_parts: Mapping[str, Any] | None = None,
        compatibility_mode: int | None = None,
    ) -> ApiResponse[Any]:
        """
        Perform an HTTP request to Elasticsearch, ensuring Accept/Content-Type headers
        match the cluster's compatibility mode (or distribution version), then delegate
        to the base client.

        Parameters passed to the Elasticsearch client:
        :param method: HTTP method (e.g. ``GET``, ``POST``).
        :param path: URL path for the request.
        :param params: Optional query string parameters.
        :param headers: Optional request headers; may be augmented with compatibility headers.
        :param body: Optional request body.
        :param endpoint_id: Optional endpoint identifier for the API.
        :param path_parts: Optional mapping for parameterized path segments.

        Parameters added by Rally:
        :param compatibility_mode: Optional Elasticsearch major version used to choose Accept/Content-Type
            headers; defaults to the minimal supported compatibility mode.
        :return: The API response from Elasticsearch.
        """
        headers = common.ensure_mimetype_headers(
            headers=headers,
            path=path,
            body=body,
            version=compatibility_mode or self.distribution_version,
        )
        return super().perform_request(
            method=method, path=path, params=params, headers=headers, body=body, endpoint_id=endpoint_id, path_parts=path_parts
        )
