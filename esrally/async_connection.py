import asyncio
import json
import logging
from typing import Optional, List

import aiohttp
import elasticsearch
from aiohttp import RequestInfo, BaseConnector
from aiohttp.client_proto import ResponseHandler
from aiohttp.helpers import BaseTimerContext
from multidict import CIMultiDictProxy, CIMultiDict
from yarl import URL

from esrally.utils import io


class StaticTransport:
    def __init__(self):
        self.closed = False

    def is_closing(self):
        return False

    def close(self):
        self.closed = True


class StaticConnector(BaseConnector):
    async def _create_connection(self, req: "ClientRequest", traces: List["Trace"],
                                 timeout: "ClientTimeout") -> ResponseHandler:
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


class StaticResponse(aiohttp.ClientResponse):
    def __init__(self, method: str, url: URL, *, writer: "asyncio.Task[None]",
                 continue100: Optional["asyncio.Future[bool]"], timer: BaseTimerContext, request_info: RequestInfo,
                 traces: List["Trace"], loop: asyncio.AbstractEventLoop, session: "ClientSession") -> None:
        super().__init__(method, url, writer=writer, continue100=continue100, timer=timer, request_info=request_info,
                         traces=traces, loop=loop, session=session)
        self.static_body = None

    async def start(self, connection: "Connection") -> "ClientResponse":
        self._closed = False
        self._protocol = connection.protocol
        self._connection = connection
        self._headers = CIMultiDictProxy(CIMultiDict())
        self.status = 200
        return self

    async def text(self, encoding=None, errors="strict"):
        return self.static_body


class RawClientResponse(aiohttp.ClientResponse):
    """
    Returns the body as bytes object (instead of a str) to avoid decoding overhead.
    """
    async def text(self, encoding=None, errors="strict"):
        """Read response payload and decode."""
        if self._body is None:
            await self.read()

        return self._body


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

            body = response["body"]
            body_encoding = response.get("body-encoding", "json")
            if body_encoding == "raw":
                body = json.dumps(body).encode("utf-8")
            elif body_encoding == "json":
                body = json.dumps(body)
            else:
                raise ValueError(f"Unknown body encoding [{body_encoding}] for path [{path}]")

            self.responses.append((path, matcher, body))

    @staticmethod
    def always():
        # pylint: disable=unused-variable
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


class AIOHttpConnection(elasticsearch.AIOHttpConnection):
    def __init__(self,
                 host="localhost",
                 port=None,
                 http_auth=None,
                 use_ssl=False,
                 ssl_assert_fingerprint=None,
                 headers=None,
                 ssl_context=None,
                 http_compress=None,
                 cloud_id=None,
                 api_key=None,
                 opaque_id=None,
                 loop=None,
                 trace_config=None,
                 **kwargs,):
        super().__init__(host=host,
                         port=port,
                         http_auth=http_auth,
                         use_ssl=use_ssl,
                         ssl_assert_fingerprint=ssl_assert_fingerprint,
                         # provided to the base class via `maxsize` to keep base class state consistent despite Rally
                         # calling the attribute differently.
                         maxsize=max(256, kwargs.get("max_connections", 0)),
                         headers=headers,
                         ssl_context=ssl_context,
                         http_compress=http_compress,
                         cloud_id=cloud_id,
                         api_key=api_key,
                         opaque_id=opaque_id,
                         loop=loop,
                         **kwargs,)

        self._trace_configs = [trace_config] if trace_config else None
        self._enable_cleanup_closed = kwargs.get("enable_cleanup_closed", False)

        static_responses = kwargs.get("static_responses")
        self.use_static_responses = static_responses is not None

        if self.use_static_responses:
            # read static responses once and reuse them
            if not StaticRequest.RESPONSES:
                with open(io.normalize_path(static_responses)) as f:
                    StaticRequest.RESPONSES = ResponseMatcher(json.load(f))

            self._request_class = StaticRequest
            self._response_class = StaticResponse
        else:
            self._request_class = aiohttp.ClientRequest
            self._response_class = RawClientResponse

    async def _create_aiohttp_session(self):
        if self.loop is None:
            self.loop = asyncio.get_running_loop()

        if self.use_static_responses:
            connector = StaticConnector(limit=self._limit, enable_cleanup_closed=self._enable_cleanup_closed)
        else:
            connector = aiohttp.TCPConnector(
                limit=self._limit,
                use_dns_cache=True,
                ssl_context=self._ssl_context,
                enable_cleanup_closed=self._enable_cleanup_closed
            )

        self.session = aiohttp.ClientSession(
            headers=self.headers,
            auto_decompress=True,
            loop=self.loop,
            cookie_jar=aiohttp.DummyCookieJar(),
            request_class=self._request_class,
            response_class=self._response_class,
            connector=connector,
            trace_configs=self._trace_configs,
        )
