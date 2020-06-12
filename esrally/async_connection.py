import asyncio
import ssl
import warnings

import aiohttp
from aiohttp.client_exceptions import ServerFingerprintMismatch
import async_timeout
import yarl

from elasticsearch.exceptions import ConnectionError, ConnectionTimeout, ImproperlyConfigured, SSLError
from elasticsearch.connection import Connection
from elasticsearch.compat import urlencode
from elasticsearch.connection.http_urllib3 import create_ssl_context


class RawClientResponse(aiohttp.ClientResponse):
    """
    Returns the body as bytes object (instead of a str) to avoid decoding overhead.
    """
    async def text(self, encoding=None, errors="strict"):
        """Read response payload and decode."""
        if self._body is None:
            await self.read()

        return self._body


# This is only needed because https://github.com/elastic/elasticsearch-py-async/pull/68 is not merged yet
# In addition we have raised the connection limit in TCPConnector from 100 to 10000.

# We want to keep the diff as small as possible thus suppressing pylint warnings that we would not allow in Rally
# pylint: disable=W0706
class AIOHttpConnection(Connection):
    def __init__(self, host='localhost', port=9200, http_auth=None,
                 use_ssl=False, verify_certs=False, ca_certs=None, client_cert=None,
                 client_key=None, loop=None, use_dns_cache=True, headers=None,
                 ssl_context=None, trace_config=None, **kwargs):
        super().__init__(host=host, port=port, **kwargs)

        self.loop = asyncio.get_event_loop() if loop is None else loop

        if http_auth is not None:
            if isinstance(http_auth, str):
                http_auth = tuple(http_auth.split(':', 1))

            if isinstance(http_auth, (tuple, list)):
                http_auth = aiohttp.BasicAuth(*http_auth)

        headers = headers or {}
        headers.setdefault('content-type', 'application/json')

        # if providing an SSL context, raise error if any other SSL related flag is used
        if ssl_context and (verify_certs or ca_certs):
            raise ImproperlyConfigured("When using `ssl_context`, `use_ssl`, `verify_certs`, `ca_certs` are not permitted")

        if use_ssl or ssl_context:
            cafile = ca_certs
            if not cafile and not ssl_context and verify_certs:
                # If no ca_certs and no sslcontext passed and asking to verify certs
                # raise error
                raise ImproperlyConfigured("Root certificates are missing for certificate "
                                           "validation. Either pass them in using the ca_certs parameter or "
                                           "install certifi to use it automatically.")
            if verify_certs or ca_certs:
                warnings.warn('Use of `verify_certs`, `ca_certs` have been deprecated in favor of using SSLContext`', DeprecationWarning)

            if not ssl_context:
                # if SSLContext hasn't been passed in, create one.
                # need to skip if sslContext isn't avail
                try:
                    ssl_context = create_ssl_context(cafile=cafile)
                except AttributeError:
                    ssl_context = None

                if not verify_certs and ssl_context is not None:
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE
                    warnings.warn(
                        'Connecting to %s using SSL with verify_certs=False is insecure.' % host)
            if ssl_context:
                verify_certs = True
                use_ssl = True

        trace_configs = [trace_config] if trace_config else None
        max_connections = max(256, kwargs.get("max_connections", 0))
        enable_cleanup_closed = kwargs.get("enable_cleanup_closed", False)
        self.session = aiohttp.ClientSession(
            auth=http_auth,
            timeout=self.timeout,
            connector=aiohttp.TCPConnector(
                loop=self.loop,
                verify_ssl=verify_certs,
                use_dns_cache=use_dns_cache,
                ssl_context=ssl_context,
                limit=max_connections,
                enable_cleanup_closed=enable_cleanup_closed
            ),
            headers=headers,
            trace_configs=trace_configs,
            response_class=RawClientResponse
        )
        self.scheme = "https" if use_ssl else "http"

    @asyncio.coroutine
    def close(self):
        yield from self.session.close()

    @asyncio.coroutine
    def perform_request(self, method, url, params=None, body=None, timeout=None, ignore=(), headers=None):
        url_path = url
        if params:
            query_string = urlencode(params)
        else:
            query_string = ""
        # Provide correct URL object to avoid string parsing in low-level code
        url = yarl.URL.build(scheme=self.scheme,
                             host=self.hostname,
                             port=self.port,
                             path=url,
                             query_string=query_string,
                             encoded=True)

        start = self.loop.time()
        response = None
        try:
            request_timeout = timeout or self.timeout.total
            with async_timeout.timeout(request_timeout, loop=self.loop):
                # override the default session timeout explicitly
                response = yield from self.session.request(method, url, data=body, headers=headers, timeout=request_timeout)
                raw_data = yield from response.text()
            duration = self.loop.time() - start

        except asyncio.CancelledError:
            raise

        except Exception as e:
            self.log_request_fail(method, url, url_path, body, self.loop.time() - start, exception=e)
            if isinstance(e, ServerFingerprintMismatch):
                raise SSLError('N/A', str(e), e)
            if isinstance(e, asyncio.TimeoutError):
                raise ConnectionTimeout('TIMEOUT', str(e), e)
            raise ConnectionError('N/A', str(e), e)

        finally:
            if response is not None:
                yield from response.release()

        # raise errors based on http status codes, let the client handle those if needed
        if not (200 <= response.status < 300) and response.status not in ignore:
            self.log_request_fail(method, url, url_path, body, duration, status_code=response.status, response=raw_data)
            self._raise_error(response.status, raw_data)

        self.log_request_success(method, url, url_path, body, response.status, raw_data, duration)

        return response.status, response.headers, raw_data
