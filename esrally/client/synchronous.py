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

import re
import warnings
from typing import Any, Iterable, Mapping, Optional

import elasticsearch
from elastic_transport import (
    ApiResponse,
    BinaryApiResponse,
    HeadApiResponse,
    ListApiResponse,
    ObjectApiResponse,
    TextApiResponse,
)
from elastic_transport.client_utils import DEFAULT
from elasticsearch.compat import warn_stacklevel
from elasticsearch.exceptions import (
    HTTP_EXCEPTIONS,
    ApiError,
    ElasticsearchWarning,
    UnsupportedProductError,
)

from esrally.client.common import _WARNING_RE, _mimetype_header_to_compat, _quote_query
from esrally.utils import versions


# This reproduces the product verification behavior of v7.14.0 of the client:
# https://github.com/elastic/elasticsearch-py/blob/v7.14.0/elasticsearch/transport.py#L606
#
# As of v8.0.0, the client determines whether the server is Elasticsearch by checking
# whether HTTP responses contain the `X-elastic-product` header. If they do not, it raises
# an `UnsupportedProductError`. This header was only introduced in Elasticsearch 7.14.0,
# however, so the client will consider any version of ES prior to 7.14.0 unsupported due to
# responses not including it.
#
# Because Rally needs to support versions of ES >= 6.8.0, we resurrect the previous
# logic for determining the authenticity of the server, which does not rely exclusively
# on this header.
class _ProductChecker:
    """Class which verifies we're connected to a supported product"""

    # States that can be returned from 'check_product'
    SUCCESS = True
    UNSUPPORTED_PRODUCT = 2
    UNSUPPORTED_DISTRIBUTION = 3

    @classmethod
    def raise_error(cls, state, meta, body):
        # These states mean the product_check() didn't fail so do nothing.
        if state in (None, True):
            return

        if state == cls.UNSUPPORTED_DISTRIBUTION:
            message = "The client noticed that the server is not a supported distribution of Elasticsearch"
        else:  # UNSUPPORTED_PRODUCT
            message = "The client noticed that the server is not Elasticsearch and we do not support this unknown product"
        raise UnsupportedProductError(message, meta=meta, body=body)

    @classmethod
    def check_product(cls, headers, response):
        # type: (dict[str, str], dict[str, str]) -> int
        """Verifies that the server we're talking to is Elasticsearch.
        Does this by checking HTTP headers and the deserialized
        response to the 'info' API. Returns one of the states above.
        """
        try:
            version = response.get("version", {})
            version_number = tuple(
                int(x) if x is not None else 999 for x in re.search(r"^([0-9]+)\.([0-9]+)(?:\.([0-9]+))?", version["number"]).groups()
            )
        except (KeyError, TypeError, ValueError, AttributeError):
            # No valid 'version.number' field, effectively 0.0.0
            version = {}
            version_number = (0, 0, 0)

        # Check all of the fields and headers for missing/valid values.
        try:
            bad_tagline = response.get("tagline", None) != "You Know, for Search"
            bad_build_flavor = version.get("build_flavor", None) != "default"
            bad_product_header = headers.get("x-elastic-product", None) != "Elasticsearch"
        except (AttributeError, TypeError):
            bad_tagline = True
            bad_build_flavor = True
            bad_product_header = True

        # 7.0-7.13 and there's a bad 'tagline' or unsupported 'build_flavor'
        if (7, 0, 0) <= version_number < (7, 14, 0):
            if bad_tagline:
                return cls.UNSUPPORTED_PRODUCT
            elif bad_build_flavor:
                return cls.UNSUPPORTED_DISTRIBUTION

        elif (
            # No version or version less than 6.x
            version_number < (6, 0, 0)
            # 6.x and there's a bad 'tagline'
            or ((6, 0, 0) <= version_number < (7, 0, 0) and bad_tagline)
            # 7.14+ and there's a bad 'X-Elastic-Product' HTTP header
            or ((7, 14, 0) <= version_number and bad_product_header)
        ):
            return cls.UNSUPPORTED_PRODUCT

        return True


class RallySyncElasticsearch(elasticsearch.Elasticsearch):
    def __init__(self, distro=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._verified_elasticsearch = None

        if distro is not None:
            self.distribution_version = versions.Version.from_string(distro)
        else:
            self.distribution_version = None

    def perform_request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        body: Optional[Any] = None,
    ) -> ApiResponse[Any]:

        # We need to ensure that we provide content-type and accept headers
        if body is not None:
            if headers is None:
                headers = {"content-type": "application/json", "accept": "application/json"}
            else:
                if headers.get("content-type") is None:
                    headers["content-type"] = "application/json"
                if headers.get("accept") is None:
                    headers["accept"] = "application/json"

        if headers:
            request_headers = self._headers.copy()
            request_headers.update(headers)
        else:
            request_headers = self._headers

        if self._verified_elasticsearch is None:
            info = self.transport.perform_request(method="GET", target="/", headers=request_headers)
            info_meta = info.meta
            info_body = info.body

            self._verified_elasticsearch = _ProductChecker.check_product(info_meta.headers, info_body)

            if self._verified_elasticsearch is not True:
                _ProductChecker.raise_error(self._verified_elasticsearch, info_meta, info_body)

        # Converts all parts of a Accept/Content-Type headers
        # from application/X -> application/vnd.elasticsearch+X
        # see https://github.com/elastic/elasticsearch/issues/51816
        if self.distribution_version is not None and self.distribution_version >= versions.Version.from_string("8.0.0"):
            _mimetype_header_to_compat("Accept", request_headers)
            _mimetype_header_to_compat("Content-Type", request_headers)

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
