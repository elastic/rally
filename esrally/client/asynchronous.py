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

import asyncio
import json
import logging
import warnings
from collections.abc import Iterable, Mapping
from typing import Any, Optional

import aiohttp
from aiohttp import BaseConnector, RequestInfo
from aiohttp.client_proto import ResponseHandler
from aiohttp.helpers import BaseTimerContext
from elastic_transport import (
    AiohttpHttpNode,
    ApiResponse,
    AsyncTransport,
    BinaryApiResponse,
    HeadApiResponse,
    ListApiResponse,
    ObjectApiResponse,
    TextApiResponse,
)
from elastic_transport.client_utils import DEFAULT
from elasticsearch import AsyncElasticsearch
from elasticsearch._async.client import IlmClient
from elasticsearch.compat import warn_stacklevel
from elasticsearch.exceptions import HTTP_EXCEPTIONS, ApiError, ElasticsearchWarning
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

from esrally.client.common import _WARNING_RE, _mimetype_header_to_compat, _quote_query
from esrally.client.context import RequestContextHolder
from esrally.utils import io, versions


class StaticTransport:
    def __init__(self):
        self.closed = False

    def is_closing(self):
        return False

    def close(self):
        self.closed = True

    def abort(self):
        self.close()


class StaticConnector(BaseConnector):
    async def _create_connection(self, req: "ClientRequest", traces: list["Trace"], timeout: "ClientTimeout") -> ResponseHandler:
        handler = ResponseHandler(self._loop)
        handler.transport = StaticTransport()
        handler.protocol = ""
        return handler


class StaticRequest(aiohttp.ClientRequest):
    RESPONSES = None

    async def send(self, conn: "Connection") -> "ClientResponse":
        self.response = self.response_class(
            self.method,
            self.original_url,
            writer=self._writer,
            continue100=self._continue,
            timer=self._timer,
            request_info=self.request_info,
            traces=self._traces,
            loop=self.loop,
            session=self._session,
        )
        path = self.original_url.path
        self.response.static_body = StaticRequest.RESPONSES.response(path)
        return self.response


# we use EmptyStreamReader here because it overrides all methods with
# no-op implementations that we need.
class StaticStreamReader(aiohttp.streams.EmptyStreamReader):
    def __init__(self, body):
        super().__init__()
        self.body = body

    async def read(self, n: int = -1) -> bytes:
        return self.body.encode("utf-8")


class StaticResponse(aiohttp.ClientResponse):
    def __init__(
        self,
        method: str,
        url: URL,
        *,
        writer: "asyncio.Task[None]",
        continue100: Optional["asyncio.Future[bool]"],
        timer: BaseTimerContext,
        request_info: RequestInfo,
        traces: list["Trace"],
        loop: asyncio.AbstractEventLoop,
        session: "ClientSession",
    ) -> None:
        super().__init__(
            method,
            url,
            writer=writer,
            continue100=continue100,
            timer=timer,
            request_info=request_info,
            traces=traces,
            loop=loop,
            session=session,
        )
        self.static_body = None

    async def start(self, connection: "Connection") -> "ClientResponse":
        self._closed = False
        self._protocol = connection.protocol
        self._connection = connection
        self._headers = CIMultiDictProxy(CIMultiDict())
        self.content = StaticStreamReader(self.static_body)
        self.status = 200
        return self


class ResponseMatcher:
    def __init__(self, responses):
        self.logger = logging.getLogger(__name__)
        self.responses = []

        for response in responses:
            path = response["path"]
            if path == "*":
                matcher = ResponseMatcher.always()
            elif path.startswith("*"):
                matcher = ResponseMatcher.endswith(path[1:])
            elif path.endswith("*"):
                matcher = ResponseMatcher.startswith(path[:-1])
            else:
                matcher = ResponseMatcher.equals(path)

            body = json.dumps(response["body"])

            self.responses.append((path, matcher, body))

    @staticmethod
    def always():
        def f(p):
            return True

        return f

    @staticmethod
    def startswith(path_pattern):
        def f(p):
            return p.startswith(path_pattern)

        return f

    @staticmethod
    def endswith(path_pattern):
        def f(p):
            return p.endswith(path_pattern)

        return f

    @staticmethod
    def equals(path_pattern):
        def f(p):
            return p == path_pattern

        return f

    def response(self, path):
        for path_pattern, matcher, body in self.responses:
            if matcher(path):
                self.logger.debug("Path pattern [%s] matches path [%s].", path_pattern, path)
                return body


class RallyTCPConnector(aiohttp.TCPConnector):
    def __init__(self, *args, **kwargs):
        self.client_id = kwargs.pop("client_id", None)
        self.logger = logging.getLogger(__name__)
        super().__init__(*args, **kwargs)

    async def _resolve_host(self, *args, **kwargs):
        hosts = await super()._resolve_host(*args, **kwargs)
        self.logger.debug("client id [%s] resolved hosts [{%s}]", self.client_id, hosts)
        # super()._resolve_host() does actually return all the IPs a given name resolves to, but the underlying
        # super()._create_direct_connection() logic only ever selects the first succesful host from this list from which
        # to establish a connection
        #
        # here we use the factory assigned client_id to deterministically return a IP from this list, which we then swap
        # to the beginning of the list to evenly distribute connections across _all_ clients
        # see https://github.com/elastic/rally/issues/1598
        idx = self.client_id % len(hosts)
        host = hosts[idx]
        self.logger.debug("client id [%s] selected host [{%s}]", self.client_id, host)
        # swap order of hosts
        hosts[0], hosts[idx] = hosts[idx], hosts[0]
        return hosts


class RallyAiohttpHttpNode(AiohttpHttpNode):
    def __init__(self, config):
        super().__init__(config)
        self._loop = None
        self.client_id = None
        self.trace_configs = None
        self.enable_cleanup_closed = False
        self._static_responses = None
        self._request_class = aiohttp.ClientRequest
        self._response_class = aiohttp.ClientResponse

    @property
    def static_responses(self):
        return self._static_responses

    @static_responses.setter
    def static_responses(self, static_responses):
        self._static_responses = static_responses
        if self._static_responses:
            # read static responses once and reuse them
            if not StaticRequest.RESPONSES:
                with open(io.normalize_path(self._static_responses)) as f:
                    StaticRequest.RESPONSES = ResponseMatcher(json.load(f))

            self._request_class = StaticRequest
            self._response_class = StaticResponse

    def _create_aiohttp_session(self):
        if self._loop is None:
            self._loop = asyncio.get_running_loop()

        if self._static_responses:
            connector = StaticConnector(limit_per_host=self._connections_per_node, enable_cleanup_closed=self.enable_cleanup_closed)
        else:
            connector = RallyTCPConnector(
                limit_per_host=self._connections_per_node,
                use_dns_cache=True,
                ssl=self._ssl_context,
                enable_cleanup_closed=self.enable_cleanup_closed,
                client_id=self.client_id,
            )

        self.session = aiohttp.ClientSession(
            headers=self.headers,
            auto_decompress=True,
            loop=self._loop,
            cookie_jar=aiohttp.DummyCookieJar(),
            request_class=self._request_class,
            response_class=self._response_class,
            connector=connector,
            trace_configs=self.trace_configs,
        )


class RallyAsyncTransport(AsyncTransport):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, node_class=RallyAiohttpHttpNode, **kwargs)


class RallyIlmClient(IlmClient):
    async def put_lifecycle(self, *args, **kwargs):
        """
        The 'elasticsearch-py' 8.x method signature renames the 'policy' param to 'name', and the previously so-called
        'body' param becomes 'policy'
        """
        if args:
            kwargs["name"] = args[0]

        if body := kwargs.pop("body", None):
            kwargs["policy"] = body.get("policy", {})
        # pylint: disable=missing-kwoa
        return await IlmClient.put_lifecycle(self, **kwargs)


class RallyAsyncElasticsearch(AsyncElasticsearch, RequestContextHolder):
    def __init__(self, hosts: Any = None, *, distribution_version: str | None = None, distribution_flavor: str | None = None, **kwargs):
        super().__init__(hosts, **kwargs)
        self.distribution_version = distribution_version
        self.distribution_flavor = distribution_flavor

        # some ILM method signatures changed in 'elasticsearch-py' 8.x,
        # so we override method(s) here to provide BWC for any custom
        # runners that aren't using the new kwargs
        self.ilm = RallyIlmClient(self)

    @property
    def is_serverless(self):
        return versions.is_serverless(self.distribution_flavor)

    def options(self, *args, **kwargs):
        new_self = super().options(*args, **kwargs)
        new_self.distribution_version = self.distribution_version
        new_self.distribution_flavor = self.distribution_flavor
        return new_self

    async def perform_request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        headers: Optional[Mapping[str, str]] = None,
        body: Optional[Any] = None,
        endpoint_id: Optional[str] = None,
        path_parts: Optional[Mapping[str, Any]] = None,
    ) -> ApiResponse[Any]:
        if endpoint_id is not None or path_parts is not None:
            raise NotImplementedError("Parameters endpoint_id and path_parts are not supported.")

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

        # Converts all parts of a Accept/Content-Type headers
        # from application/X -> application/vnd.elasticsearch+X
        # see https://github.com/elastic/elasticsearch/issues/51816
        # Not applicable to serverless
        if not self.is_serverless:
            if versions.is_version_identifier(self.distribution_version) and (
                versions.Version.from_string(self.distribution_version) >= versions.Version.from_string("8.0.0")
            ):
                _mimetype_header_to_compat("Accept", request_headers)
                _mimetype_header_to_compat("Content-Type", request_headers)

        if params:
            target = f"{path}?{_quote_query(params)}"
        else:
            target = path

        meta, resp_body = await self.transport.perform_request(
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
