import asyncio

import aiohttp
import elasticsearch


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

    async def _create_aiohttp_session(self):
        if self.loop is None:
            self.loop = asyncio.get_running_loop()
        self.session = aiohttp.ClientSession(
            headers=self.headers,
            auto_decompress=True,
            loop=self.loop,
            cookie_jar=aiohttp.DummyCookieJar(),
            response_class=RawClientResponse,
            connector=aiohttp.TCPConnector(
                limit=self._limit,
                use_dns_cache=True,
                ssl_context=self._ssl_context,
                enable_cleanup_closed=self._enable_cleanup_closed
            ),
            trace_configs=self._trace_configs,
        )
