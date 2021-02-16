import asyncio
import json
from ssl import SSLContext
from typing import Optional, Mapping, Iterable, Any, Type, Union, List

import aiohttp
import elasticsearch
from aiohttp import BasicAuth, http, Fingerprint, RequestInfo, BaseConnector
from aiohttp.client_proto import ResponseHandler
from aiohttp.helpers import BaseTimerContext
from aiohttp.typedefs import LooseHeaders, LooseCookies
from multidict import CIMultiDictProxy, CIMultiDict
from yarl import URL


class StaticTransport:
    def __init__(self):
        self.closed = False

    def is_closing(self):
        return False

    def close(self):
        self.closed = True
        pass


class StaticConnector(BaseConnector):
    async def _create_connection(self, req: "ClientRequest", traces: List["Trace"],
                                 timeout: "ClientTimeout") -> ResponseHandler:
        handler = ResponseHandler(self._loop)
        # must not be None...
        handler.transport = StaticTransport()
        handler.protocol = ""
        return handler


class StaticRequest(aiohttp.ClientRequest):
    RESPONSES = None

    DEFAULT_RESPONSE = json.dumps({}).encode("utf-8")

    def __init__(self, method: str, url: URL, *, params: Optional[Mapping[str, str]] = None,
                 headers: Optional[LooseHeaders] = None, skip_auto_headers: Iterable[str] = frozenset(),
                 data: Any = None, cookies: Optional[LooseCookies] = None, auth: Optional[BasicAuth] = None,
                 version: http.HttpVersion = http.HttpVersion11, compress: Optional[str] = None,
                 chunked: Optional[bool] = None, expect100: bool = False,
                 loop: Optional[asyncio.AbstractEventLoop] = None,
                 response_class: Optional[Type["ClientResponse"]] = None, proxy: Optional[URL] = None,
                 proxy_auth: Optional[BasicAuth] = None, timer: Optional[BaseTimerContext] = None,
                 session: Optional["ClientSession"] = None, ssl: Union[SSLContext, bool, Fingerprint, None] = None,
                 proxy_headers: Optional[LooseHeaders] = None, traces: Optional[List["Trace"]] = None):
        super().__init__(method, url, params=params, headers=headers, skip_auto_headers=skip_auto_headers, data=data,
                         cookies=cookies, auth=auth, version=version, compress=compress, chunked=chunked,
                         expect100=expect100, loop=loop, response_class=response_class, proxy=proxy,
                         proxy_auth=proxy_auth, timer=timer, session=session, ssl=ssl, proxy_headers=proxy_headers,
                         traces=traces)
        # TODO: Read JSON file into class variable
        if not StaticRequest.RESPONSES:
            StaticRequest.RESPONSES = {
                "_bulk": json.dumps({
                    "errors": False,
                    "took": 1
                }).encode("utf-8"),
                "/_cluster/health": json.dumps({
                    "status": "green",
                    "relocating_shards": 0
                }),
                "/geonames/_bulk": json.dumps({
                    "errors": False,
                    "took": 1
                }).encode("utf-8"),
                "/_cluster/health/geonames": json.dumps({
                    "status": "green",
                    "relocating_shards": 0
                }),
                "/_all/_stats/_all": json.dumps({
                    "_all": {
                        "total": {
                            "merges": {
                                "current": 0
                            }
                        }
                    }
                })
            }

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
        if path.endswith("_bulk"):
            self.response.static_body = StaticRequest.RESPONSES["_bulk"]
        elif path.startswith("/_cluster/health"):
            self.response.static_body = StaticRequest.RESPONSES["/_cluster/health"]
        else:
            self.response.static_body = StaticRequest.RESPONSES.get(path, StaticRequest.DEFAULT_RESPONSE)
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
        # TODO configurable?
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

        self.use_static_responses = kwargs.get("use_static_responses", False)

        if self.use_static_responses:
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
