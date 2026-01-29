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

import warnings
from collections.abc import Iterable, Mapping
from typing import Any, Optional

from elastic_transport import (
    ApiResponse,
    BinaryApiResponse,
    HeadApiResponse,
    ListApiResponse,
    ObjectApiResponse,
    OpenTelemetrySpan,
    TextApiResponse,
)
from elastic_transport.client_utils import DEFAULT
from elasticsearch import Elasticsearch
from elasticsearch.compat import warn_stacklevel
from elasticsearch.exceptions import (
    HTTP_EXCEPTIONS,
    ApiError,
    ElasticsearchWarning,
    UnsupportedProductError,
)

from esrally.client.common import _WARNING_RE, _quote_query, mimetype_headers_to_compat
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

    def _perform_request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        body: Optional[Any] = None,
        otel_span: OpenTelemetrySpan,
    ) -> ApiResponse[Any]:
        if headers:
            request_headers = self._headers.copy()
            request_headers.update(headers)
        else:
            request_headers = self._headers

        if body is not None:
            # It ensures content-type and accept headers are set.
            mimetype = "application/json"
            if path.endswith("/_bulk"):
                mimetype = "application/x-ndjson"
            for header in ("content-type", "accept"):
                request_headers.setdefault(header, mimetype)

        if not self.is_serverless:
            # Converts all parts of an Accept/Content-Type headers
            # from application/X -> application/vnd.elasticsearch+X
            # see https://github.com/elastic/elasticsearch/issues/51816
            mimetype_headers_to_compat(request_headers, self.distribution_version)

        if params:
            target = f"{path}?{_quote_query(params)}"
        else:
            target = path

        meta, resp_body = self.transport.perform_request(
            method,
            target,
            headers=request_headers,
            body=body,
            request_timeout=self._request_timeout,
            max_retries=self._max_retries,
            retry_on_status=self._retry_on_status,
            retry_on_timeout=self._retry_on_timeout,
            client_meta=self._client_meta,
            otel_span=otel_span,
        )

        # HEAD with a 404 is returned as a normal response
        # since this is used as an 'exists' functionality.
        if not (method == "HEAD" and meta.status == 404) and (
            not 200 <= meta.status < 299
            and (self._ignore_status is DEFAULT or self._ignore_status is None or meta.status not in self._ignore_status)
        ):
            message = str(resp_body)

            # If the response is an error response try parsing
            # the raw Elasticsearch error before raising.
            if isinstance(resp_body, dict):
                try:
                    error = resp_body.get("error", message)
                    if isinstance(error, dict) and "type" in error:
                        error = error["type"]
                    message = error
                except (ValueError, KeyError, TypeError):
                    pass

            raise HTTP_EXCEPTIONS.get(meta.status, ApiError)(message=message, meta=meta, body=resp_body)

        # 'X-Elastic-Product: Elasticsearch' should be on every 2XX response.
        if not self._verified_elasticsearch:
            # If the header is set we mark the server as verified.
            if meta.headers.get("x-elastic-product", "") == "Elasticsearch":
                self._verified_elasticsearch = True
            # Otherwise we only raise an error on 2XX responses.
            elif meta.status >= 200 and meta.status < 300:
                raise UnsupportedProductError(
                    message=("The client noticed that the server is not Elasticsearch " "and we do not support this unknown product"),
                    meta=meta,
                    body=resp_body,
                )

        # 'Warning' headers should be reraised as 'ElasticsearchWarning'
        if "warning" in meta.headers:
            warning_header = (meta.headers.get("warning") or "").strip()
            warning_messages: Iterable[str] = _WARNING_RE.findall(warning_header) or (warning_header,)
            stacklevel = warn_stacklevel()
            for warning_message in warning_messages:
                warnings.warn(
                    warning_message,
                    category=ElasticsearchWarning,
                    stacklevel=stacklevel,
                )

        if method == "HEAD":
            response = HeadApiResponse(meta=meta)
        elif isinstance(resp_body, dict):
            response = ObjectApiResponse(body=resp_body, meta=meta)  # type: ignore[assignment]
        elif isinstance(resp_body, list):
            response = ListApiResponse(body=resp_body, meta=meta)  # type: ignore[assignment]
        elif isinstance(resp_body, str):
            response = TextApiResponse(  # type: ignore[assignment]
                body=resp_body,
                meta=meta,
            )
        elif isinstance(resp_body, bytes):
            response = BinaryApiResponse(body=resp_body, meta=meta)  # type: ignore[assignment]
        else:
            response = ApiResponse(body=resp_body, meta=meta)  # type: ignore[assignment]

        return response
