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

import contextvars
import logging
import re
import time
import warnings
from datetime import date, datetime
from typing import Any, Iterable, Mapping, Optional

import certifi
import elastic_transport
import urllib3
from elastic_transport import (
    ApiResponse,
    BinaryApiResponse,
    HeadApiResponse,
    ListApiResponse,
    ObjectApiResponse,
    TextApiResponse,
)
from elastic_transport.client_utils import DEFAULT, percent_encode
from elasticsearch.compat import warn_stacklevel
from elasticsearch.exceptions import (
    HTTP_EXCEPTIONS,
    ApiError,
    ElasticsearchWarning,
    UnsupportedProductError,
)
from urllib3.connection import is_ipaddress

from esrally import doc_link, exceptions
from esrally.utils import console, convert, versions

_WARNING_RE = re.compile(r"\"([^\"]*)\"")
# TODO: get versionstr dynamically
_COMPAT_MIMETYPE_TEMPLATE = "application/vnd.elasticsearch+%s; compatible-with=" + str("8.2.0".partition(".")[0])
_COMPAT_MIMETYPE_RE = re.compile(r"application/(json|x-ndjson|vnd\.mapbox-vector-tile)")
_COMPAT_MIMETYPE_SUB = _COMPAT_MIMETYPE_TEMPLATE % (r"\g<1>",)


class RequestContextManager:
    """
    Ensures that request context span the defined scope and allow nesting of request contexts with proper propagation.
    This means that we can span a top-level request context, open sub-request contexts that can be used to measure
    individual timings and still measure the proper total time on the top-level request context.
    """

    def __init__(self, request_context_holder):
        self.ctx_holder = request_context_holder
        self.ctx = None
        self.token = None

    async def __aenter__(self):
        self.ctx, self.token = self.ctx_holder.init_request_context()
        return self

    @property
    def request_start(self):
        return self.ctx["request_start"]

    @property
    def request_end(self):
        return self.ctx["request_end"]

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # propagate earliest request start and most recent request end to parent
        request_start = self.request_start
        request_end = self.request_end
        self.ctx_holder.restore_context(self.token)
        # don't attempt to restore these values on the top-level context as they don't exist
        if self.token.old_value != contextvars.Token.MISSING:
            self.ctx_holder.update_request_start(request_start)
            self.ctx_holder.update_request_end(request_end)
        self.token = None
        return False


class RequestContextHolder:
    """
    Holds request context variables. This class is only meant to be used together with RequestContextManager.
    """

    request_context = contextvars.ContextVar("rally_request_context")

    def new_request_context(self):
        return RequestContextManager(self)

    @classmethod
    def init_request_context(cls):
        ctx = {}
        token = cls.request_context.set(ctx)
        return ctx, token

    @classmethod
    def restore_context(cls, token):
        cls.request_context.reset(token)

    @classmethod
    def update_request_start(cls, new_request_start):
        meta = cls.request_context.get()
        # this can happen if multiple requests are sent on the wire for one logical request (e.g. scrolls)
        if "request_start" not in meta:
            meta["request_start"] = new_request_start

    @classmethod
    def update_request_end(cls, new_request_end):
        meta = cls.request_context.get()
        meta["request_end"] = new_request_end

    @classmethod
    def on_request_start(cls):
        cls.update_request_start(time.perf_counter())

    @classmethod
    def on_request_end(cls):
        cls.update_request_end(time.perf_counter())

    @classmethod
    def return_raw_response(cls):
        ctx = cls.request_context.get()
        ctx["raw_response"] = True


class EsClientFactory:
    """
    Abstracts how the Elasticsearch client is created and customizes the client for backwards
    compatibility guarantees that are broader than the library's defaults.
    """

    def __init__(self, hosts, client_options, distribution_version=None):
        # We need to pass a list of connection strings to the client as of elasticsearch-py 8.0
        def host_string(host):
            protocol = "https" if client_options.get("use_ssl") else "http"
            return f"{protocol}://{host['host']}:{host['port']}"

        self.hosts = [host_string(h) for h in hosts]
        self.client_options = dict(client_options)
        self.ssl_context = None
        # This attribute is necessary for the backwards-compatibility logic contained in
        # RallySyncElasticsearch.perform_request() and RallyAsyncElasticsearch.perform_request().
        self.distribution_version = distribution_version
        self.logger = logging.getLogger(__name__)

        masked_client_options = dict(client_options)
        if "basic_auth_password" in masked_client_options:
            masked_client_options["basic_auth_password"] = "*****"
        if "http_auth" in masked_client_options:
            masked_client_options["http_auth"] = (masked_client_options["http_auth"][0], "*****")
        self.logger.info("Creating ES client connected to %s with options [%s]", hosts, masked_client_options)

        # we're using an SSL context now and it is not allowed to have use_ssl present in client options anymore
        if self.client_options.pop("use_ssl", False):
            # pylint: disable=import-outside-toplevel
            import ssl

            self.logger.info("SSL support: on")

            self.ssl_context = ssl.create_default_context(
                ssl.Purpose.SERVER_AUTH, cafile=self.client_options.pop("ca_certs", certifi.where())
            )

            # We call get() here instead of pop() in order to pass verify_certs through as a kwarg
            # to the elasticsearch.Elasticsearch constructor. Setting the ssl_context's verify_mode to
            # ssl.CERT_NONE is insufficient with version 8.0+ of elasticsearch-py.
            if not self.client_options.get("verify_certs", True):
                self.logger.info("SSL certificate verification: off")
                # order matters to avoid ValueError: check_hostname needs a SSL context with either CERT_OPTIONAL or CERT_REQUIRED
                self.ssl_context.check_hostname = False
                self.ssl_context.verify_mode = ssl.CERT_NONE

                self.logger.warning(
                    "User has enabled SSL but disabled certificate verification. This is dangerous but may be ok for a "
                    "benchmark. Disabling urllib warnings now to avoid a logging storm. "
                    "See https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings for details."
                )
                # disable:  "InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification is strongly \
                # advised. See: https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings"
                urllib3.disable_warnings()
            else:
                # check_hostname should not be set when host is an IP address
                self.ssl_context.check_hostname = self._only_hostnames(hosts)
                self.ssl_context.verify_mode = ssl.CERT_REQUIRED
                self.logger.info("SSL certificate verification: on")

            # When using SSL_context, all SSL related kwargs in client options get ignored
            client_cert = self.client_options.pop("client_cert", False)
            client_key = self.client_options.pop("client_key", False)

            if not client_cert and not client_key:
                self.logger.info("SSL client authentication: off")
            elif bool(client_cert) != bool(client_key):
                self.logger.error("Supplied client-options contain only one of client_cert/client_key. ")
                defined_client_ssl_option = "client_key" if client_key else "client_cert"
                missing_client_ssl_option = "client_cert" if client_key else "client_key"
                console.println(
                    "'{}' is missing from client-options but '{}' has been specified.\n"
                    "If your Elasticsearch setup requires client certificate verification both need to be supplied.\n"
                    "Read the documentation at {}\n".format(
                        missing_client_ssl_option,
                        defined_client_ssl_option,
                        console.format.link(doc_link("command_line_reference.html#client-options")),
                    )
                )
                raise exceptions.SystemSetupError(
                    "Cannot specify '{}' without also specifying '{}' in client-options.".format(
                        defined_client_ssl_option, missing_client_ssl_option
                    )
                )
            elif client_cert and client_key:
                self.logger.info("SSL client authentication: on")
                self.ssl_context.load_cert_chain(certfile=client_cert, keyfile=client_key)
        else:
            self.logger.info("SSL support: off")

        if self._is_set(self.client_options, "basic_auth_user") and self._is_set(self.client_options, "basic_auth_password"):
            self.logger.info("HTTP basic authentication: on")
            self.client_options["basic_auth"] = (self.client_options.pop("basic_auth_user"), self.client_options.pop("basic_auth_password"))
        else:
            self.logger.info("HTTP basic authentication: off")

        if self._is_set(self.client_options, "compressed"):
            console.warn("You set the deprecated client option 'compressed‘. Please use 'http_compress' instead.", logger=self.logger)
            self.client_options["http_compress"] = self.client_options.pop("compressed")

        if self._is_set(self.client_options, "http_compress"):
            self.logger.info("HTTP compression: on")
        else:
            self.logger.info("HTTP compression: off")

        if self._is_set(self.client_options, "enable_cleanup_closed"):
            self.client_options["enable_cleanup_closed"] = convert.to_bool(self.client_options.pop("enable_cleanup_closed"))

    @staticmethod
    def _only_hostnames(hosts):
        has_ip = False
        has_hostname = False
        for host in hosts:
            is_ip = is_ipaddress(host["host"])
            if is_ip:
                has_ip = True
            else:
                has_hostname = True

        if has_ip and has_hostname:
            raise exceptions.SystemSetupError("Cannot verify certs with mixed IP addresses and hostnames")

        return has_hostname

    def _is_set(self, client_opts, k):
        try:
            return client_opts[k]
        except KeyError:
            return False

    def create(self):
        # pylint: disable=import-outside-toplevel
        import elasticsearch

        distro = self.distribution_version

        # This reproduces the product verification behavior of v7.14.0 of the client:
        # https://github.com/elastic/elasticsearch-py/blob/v7.14.0/elasticsearch/transport.py#L606
        #
        # As of v8.0.0, the client determines whether the server is Elasticsearch by checking
        # whether HTTP responses contain the `X-elastic-product` header. If they do not, it raises
        # an `UnsupportedProductException`. This header was only introduced in Elasticsearch 7.14.0,
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
                        int(x) if x is not None else 999
                        for x in re.search(r"^([0-9]+)\.([0-9]+)(?:\.([0-9]+))?", version["number"]).groups()
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
            def __init__(self, *args, **kwargs):
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

                if body is not None:
                    if headers is None:
                        headers = {"content-type": "application/json", "accept": "application/json"}
                    else:
                        if headers.get("content-type") is None:
                            headers["content-type"] = "application/json"
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

                def mimetype_header_to_compat(header: str) -> None:
                    # Converts all parts of a Accept/Content-Type headers
                    # from application/X -> application/vnd.elasticsearch+X
                    nonlocal request_headers
                    mimetype = request_headers.get(header, None)
                    if mimetype:
                        request_headers[header] = _COMPAT_MIMETYPE_RE.sub(_COMPAT_MIMETYPE_SUB, mimetype)

                # Custom behavior for backwards compatibility with versions of ES that do not
                # recognize the compatible-with header.
                if self.distribution_version is not None and self.distribution_version >= versions.Version.from_string("8.0.0"):
                    mimetype_header_to_compat("Accept")
                    mimetype_header_to_compat("Content-Type")

                def _escape(value: Any) -> str:
                    """
                    Escape a single value of a URL string or a query parameter. If it is a list
                    or tuple, turn it into a comma-separated string first.
                    """

                    # make sequences into comma-separated stings
                    if isinstance(value, (list, tuple)):
                        value = ",".join([_escape(item) for item in value])

                    # dates and datetimes into isoformat
                    elif isinstance(value, (date, datetime)):
                        value = value.isoformat()

                    # make bools into true/false strings
                    elif isinstance(value, bool):
                        value = str(value).lower()

                    elif isinstance(value, bytes):
                        return value.decode("utf-8", "surrogatepass")

                    if not isinstance(value, str):
                        return str(value)
                    return value

                def _quote(value: Any) -> str:
                    return percent_encode(_escape(value), ",*")

                def _quote_query(query: Mapping[str, Any]) -> str:
                    return "&".join([f"{k}={_quote(v)}" for k, v in query.items()])

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

        return RallySyncElasticsearch(hosts=self.hosts, ssl_context=self.ssl_context, **self.client_options)

    def create_async(self):
        # pylint: disable=import-outside-toplevel
        import io

        import aiohttp
        import elasticsearch
        from elasticsearch.serializer import JSONSerializer

        import esrally.async_connection

        class LazyJSONSerializer(JSONSerializer):
            def loads(self, s):
                meta = RallyAsyncElasticsearch.request_context.get()
                if "raw_response" in meta:
                    return io.BytesIO(s)
                else:
                    return super().loads(s)

        async def on_request_start(session, trace_config_ctx, params):
            RallyAsyncElasticsearch.on_request_start()

        async def on_request_end(session, trace_config_ctx, params):
            RallyAsyncElasticsearch.on_request_end()

        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(on_request_start)
        trace_config.on_request_end.append(on_request_end)
        # ensure that we also stop the timer when a request "ends" with an exception (e.g. a timeout)
        trace_config.on_request_exception.append(on_request_end)

        # override the builtin JSON serializer
        self.client_options["serializer"] = LazyJSONSerializer()
        self.client_options["trace_config"] = trace_config

        client_options = self.client_options
        distro = self.distribution_version

        class RallyAsyncTransport(elastic_transport.AsyncTransport):
            def __init__(self, *args, **kwargs):
                # We need to pass a trace config to the session that's created in
                # async_connection.RallyAiohttphttpnode, which is a subclass of
                # elastic_transport.AiohttpHttpNode.
                #
                # Its constructor only accepts an elastic_transport.NodeConfig object.
                # Because we do not fully control creation of these objects , we need to
                # pass the trace_config by adding it to the NodeConfig's `extras`, which
                # can contain arbitrary metadata.
                client_options.update({"trace_config": [trace_config]})
                node_configs = args[0]
                for conf in node_configs:
                    extras = conf._extras
                    extras.update({"_rally_client_options": client_options})
                    conf._extras = extras
                original_args = args
                new_args = (node_configs, *original_args[1:])

                super().__init__(*new_args, node_class=esrally.async_connection.RallyAiohttpHttpNode, **kwargs)

        class RallyAsyncElasticsearch(elasticsearch.AsyncElasticsearch, RequestContextHolder):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                # skip verification at this point; we've already verified this earlier with the synchronous client.
                # The async client is used in the hot code path and we use customized overrides (such as that we don't
                # parse response bodies in some cases for performance reasons, e.g. when using the bulk API).
                self._verified_elasticsearch = True
                if distro is not None:
                    self.distribution_version = versions.Version.from_string(distro)
                else:
                    self.distribution_version = None

            async def perform_request(
                self,
                method: str,
                path: str,
                *,
                params: Optional[Mapping[str, Any]] = None,
                headers: Optional[Mapping[str, str]] = None,
                body: Optional[Any] = None,
            ) -> ApiResponse[Any]:

                if body is not None:
                    if headers is None:
                        headers = {"content-type": "application/json", "accept": "application/json"}
                    else:
                        if headers.get("content-type") is None:
                            headers["content-type"] = "application/json"
                            headers["accept"] = "application/json"

                if headers:
                    request_headers = self._headers.copy()
                    request_headers.update(headers)
                else:
                    request_headers = self._headers

                def mimetype_header_to_compat(header: str) -> None:
                    # Converts all parts of a Accept/Content-Type headers
                    # from application/X -> application/vnd.elasticsearch+X
                    nonlocal request_headers
                    mimetype = request_headers.get(header, None)
                    if mimetype:
                        request_headers[header] = _COMPAT_MIMETYPE_RE.sub(_COMPAT_MIMETYPE_SUB, mimetype)

                if self.distribution_version is not None and self.distribution_version >= versions.Version.from_string("8.0.0"):
                    mimetype_header_to_compat("Accept")
                    mimetype_header_to_compat("Content-Type")

                def _escape(value: Any) -> str:
                    """
                    Escape a single value of a URL string or a query parameter. If it is a list
                    or tuple, turn it into a comma-separated string first.
                    """

                    # make sequences into comma-separated stings
                    if isinstance(value, (list, tuple)):
                        value = ",".join([_escape(item) for item in value])

                    # dates and datetimes into isoformat
                    elif isinstance(value, (date, datetime)):
                        value = value.isoformat()

                    # make bools into true/false strings
                    elif isinstance(value, bool):
                        value = str(value).lower()

                    elif isinstance(value, bytes):
                        return value.decode("utf-8", "surrogatepass")

                    if not isinstance(value, str):
                        return str(value)
                    return value

                def _quote(value: Any) -> str:
                    return percent_encode(_escape(value), ",*")

                def _quote_query(query: Mapping[str, Any]) -> str:
                    return "&".join([f"{k}={_quote(v)}" for k, v in query.items()])

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

        # max_connections and trace_config are not valid kwargs, so we pop them
        max = self.client_options.pop("max_connections")
        self.client_options.pop("trace_config")

        return RallyAsyncElasticsearch(
            hosts=self.hosts,
            transport_class=RallyAsyncTransport,
            ssl_context=self.ssl_context,
            maxsize=max,
            **self.client_options,
        )


def wait_for_rest_layer(es, max_attempts=40):
    """
    Waits for ``max_attempts`` until Elasticsearch's REST API is available.

    :param es: Elasticsearch client to use for connecting.
    :param max_attempts: The maximum number of attempts to check whether the REST API is available.
    :return: True iff Elasticsearch's REST API is available.
    """
    # assume that at least the hosts that we expect to contact should be available. Note that this is not 100%
    # bullet-proof as a cluster could have e.g. dedicated masters which are not contained in our list of target hosts
    # but this is still better than just checking for any random node's REST API being reachable.
    expected_node_count = len(es.transport.node_pool)
    logger = logging.getLogger(__name__)
    for attempt in range(max_attempts):
        logger.debug("REST API is available after %s attempts", attempt)
        # pylint: disable=import-outside-toplevel
        import elasticsearch

        try:
            # see also WaitForHttpResource in Elasticsearch tests. Contrary to the ES tests we consider the API also
            # available when the cluster status is RED (as long as all required nodes are present)
            es.cluster.health(wait_for_nodes=">={}".format(expected_node_count))
            logger.info("REST API is available for >= [%s] nodes after [%s] attempts.", expected_node_count, attempt)
            return True
        except elasticsearch.ConnectionError as e:
            if "SSL: UNKNOWN_PROTOCOL" in str(e):
                raise exceptions.SystemSetupError("Could not connect to cluster via https. Is this an https endpoint?", e)
            else:
                logger.debug("Got connection error on attempt [%s]. Sleeping...", attempt)
                time.sleep(3)
        # TODO: distinguish between TransportError and ApiError
        except elasticsearch.TransportError as e:
            # cluster block, x-pack not initialized yet, our wait condition is not reached
            if e.message in (503, 401, 408):
                logger.debug("Got status code [%s] on attempt [%s]. Sleeping...", e.message, attempt)
                time.sleep(3)
            else:
                logger.warning("Got unexpected status code [%s] on attempt [%s].", e.message, attempt)
                raise e
    return False
